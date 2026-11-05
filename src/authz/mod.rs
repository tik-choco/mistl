//! Local authorization, open by default. Transport node ids key P2P accounts;
//! allows require a verified DID, while node-id denies also cover unverified
//! peers. Usage accounts are bounded and excess principals share `*`.

mod capability;
mod delegation;
mod policy;
mod tokens;
mod usage;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Duration, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

pub use capability::writer_did as escrow_writer_did;
#[allow(unused_imports)] // Public types for service and command integrations.
pub use policy::{
    AccessMode, Action, ApiPolicy, LimitOverride, Limits, Period, Policy, Rule, Service,
    ServicePolicy,
};
pub use tokens::ApiToken;
use tokens::TokenBook;
use usage::UsageBook;

static STORE: LazyLock<Mutex<Store>> = LazyLock::new(|| Mutex::new(Store::default()));

#[derive(Clone, Debug, Serialize)]
pub struct Principal {
    pub node: String,
    pub did: Option<String>,
}

impl Principal {
    pub fn resolve(room: Option<&str>, from: &str) -> Self {
        Self {
            node: from.into(),
            did: crate::tunnel::auth::verified_did_for_peer(room, from),
        }
    }

    pub fn account(&self) -> String {
        format!("node:{}", self.node)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Denied {
    pub code: &'static str,
    pub message: String,
}

impl Denied {
    fn forbidden() -> Self {
        Self {
            code: "forbidden",
            message: "Access denied".into(),
        }
    }

    fn unauthorized() -> Self {
        Self {
            code: "unauthorized",
            message: "A valid API token is required".into(),
        }
    }

    fn usage_limit() -> Self {
        Self {
            code: "usage_limit_exceeded",
            message: "Usage limit exceeded".into(),
        }
    }

    pub fn http_status(&self) -> u16 {
        match self.code {
            "unauthorized" => 401,
            "usage_limit_exceeded" => 429,
            _ => 403,
        }
    }
}

/// Proof of authorization, retaining the accounts and period to charge.
#[derive(Clone, Debug)]
pub struct Authorized {
    // Who was authorized; kept for callers that log or audit decisions.
    #[allow(dead_code)]
    pub principal: Option<Principal>,
    #[allow(dead_code)]
    pub token: Option<String>,
    accounts: Vec<String>,
    service: Service,
    limits: Limits,
    cap_accounts: Vec<(String, Limits)>,
}

impl Authorized {
    pub fn anonymous() -> Self {
        Self {
            principal: None,
            token: None,
            accounts: Vec::new(),
            service: Service::Ai,
            limits: Limits::default(),
            cap_accounts: Vec::new(),
        }
    }

    pub fn record(&self, requests: u64, output_chars: u64) {
        if self.accounts.is_empty() && self.cap_accounts.is_empty() {
            return;
        }
        let now = Utc::now();
        let mut store = STORE.lock().expect("authz store lock");
        store.record(self, requests, output_chars, now);
        if now.signed_duration_since(store.last_flush) >= Duration::seconds(30) {
            store.flush_best_effort(now);
        }
    }
}

#[derive(Clone)]
struct Store {
    policy: Policy,
    tokens: TokenBook,
    usage: UsageBook,
    dir: Option<PathBuf>,
    usage_dirty: bool,
    tokens_dirty: bool,
    last_flush: DateTime<Utc>,
    caps: delegation::Book,
    local_did: Option<String>,
}

impl Default for Store {
    fn default() -> Self {
        Self {
            policy: Policy::default(),
            tokens: TokenBook::default(),
            usage: UsageBook::default(),
            dir: None,
            usage_dirty: false,
            tokens_dirty: false,
            last_flush: Utc::now(),
            caps: delegation::Book::default(),
            local_did: None,
        }
    }
}

impl Store {
    fn load(data_dir: &Path, now: DateTime<Utc>) -> Result<Self> {
        let dir = data_dir.join("authz");
        crate::statefile::create_private_dir(&dir)?;
        Ok(Self {
            policy: load_or_recover(&dir, "policy.json", now, Policy::validate),
            tokens: load_or_recover(&dir, "tokens.json", now, TokenBook::validate),
            usage: load_or_recover(&dir, "usage.json", now, UsageBook::validate),
            caps: delegation::Book::load(&dir, now),
            dir: Some(dir),
            last_flush: now,
            ..Self::default()
        })
    }

    fn save<T: Serialize>(&self, file: &str, value: &T) -> Result<()> {
        if let Some(dir) = &self.dir {
            crate::statefile::write_private(&dir.join(file), &serde_json::to_vec_pretty(value)?)?;
        }
        Ok(())
    }

    fn flush_best_effort(&mut self, now: DateTime<Utc>) {
        if self.caps.dirty {
            match self.save_caps() {
                Ok(()) => self.caps.dirty = false,
                Err(error) => tracing::warn!(%error, "failed to save capabilities"),
            }
        }
        if self.usage_dirty {
            match self.save("usage.json", &self.usage) {
                Ok(()) => self.usage_dirty = false,
                Err(error) => tracing::warn!(%error, "failed to save authorization usage"),
            }
        }
        if self.tokens_dirty {
            match self.save("tokens.json", &self.tokens) {
                Ok(()) => self.tokens_dirty = false,
                Err(error) => tracing::warn!(%error, "failed to save API token activity"),
            }
        }
        self.last_flush = now;
    }

    #[cfg(test)]
    fn authorize_peer(
        &mut self,
        service: Service,
        action: Action,
        principal: Principal,
        resource: Option<&str>,
        now: DateTime<Utc>,
    ) -> std::result::Result<Authorized, Denied> {
        self.authorize_peer_owned(service, action, principal, resource, None, now)
    }

    fn authorize_peer_owned(
        &mut self,
        service: Service,
        action: Action,
        principal: Principal,
        resource: Option<&str>,
        resource_owner: Option<&str>,
        now: DateTime<Utc>,
    ) -> std::result::Result<Authorized, Denied> {
        let policy = self.policy.service(service);
        if policy.denied(&principal, now) {
            return Err(Denied::forbidden());
        }
        let cap = if policy.explicitly_allowed(&principal, now) {
            None
        } else {
            self.presented_cap(service, action, &principal, resource, resource_owner, now)
        };
        let policy = self.policy.service(service);
        if !policy.explicitly_allowed(&principal, now)
            && cap.is_none()
            && policy.mode != AccessMode::Open
        {
            return Err(Denied::forbidden());
        }
        if let Some(cap) = &cap {
            self.caps.mark_trusted(&principal.node, &cap.id);
        }
        let policy = self.policy.service(service);
        if service == Service::Escrow && cap.as_ref().is_none_or(|c| c.limits.is_none()) {
            return Ok(Authorized {
                principal: Some(principal),
                service,
                ..Authorized::anonymous()
            });
        }
        let mut cap_accounts = Vec::new();
        let account = self.usage.account(service, &principal.account());
        let limits = if account == "*" {
            &policy.limits
        } else {
            policy.limits_for(&principal)
        }
        .clone();
        self.usage_dirty = true;
        if service == Service::Ai {
            self.usage.check(service, &account, &limits, now)?;
        }
        if let Some(cap) = cap
            && let Some(limits) = cap.limits
        {
            let account = self.usage.account(service, &format!("cap:{}", cap.id));
            self.usage.check(service, &account, &limits, now)?;
            cap_accounts.push((account, limits));
        }
        Ok(Authorized {
            principal: Some(principal),
            token: None,
            accounts: vec![account],
            service,
            limits,
            cap_accounts,
        })
    }

    fn authorize_api(
        &mut self,
        authorization: Option<&str>,
        model: Option<&str>,
        now: DateTime<Utc>,
    ) -> std::result::Result<Authorized, Denied> {
        let Some(token) = self
            .tokens
            .lookup(authorization, self.policy.api.require_token, now)?
            .cloned()
        else {
            return Ok(Authorized::anonymous());
        };
        if !token.models.is_empty()
            && !model.is_some_and(|m| token.models.iter().any(|allowed| allowed == m))
        {
            return Err(Denied {
                code: "model_not_allowed",
                message: "Model is not allowed by this API token".into(),
            });
        }
        let account = self
            .usage
            .account(Service::Ai, &format!("token:{}", token.id));
        self.usage_dirty = true;
        self.usage
            .check(Service::Ai, &account, &token.limits, now)?;
        self.touch_token(&token.id, now);
        Ok(Authorized {
            principal: None,
            token: Some(token.id),
            accounts: vec![account],
            service: Service::Ai,
            limits: token.limits,
            cap_accounts: Vec::new(),
        })
    }

    fn model_filter(
        &mut self,
        authorization: Option<&str>,
        now: DateTime<Utc>,
    ) -> std::result::Result<Option<Vec<String>>, Denied> {
        let token = self
            .tokens
            .lookup(authorization, self.policy.api.require_token, now)?
            .cloned();
        if let Some(token) = token {
            self.touch_token(&token.id, now);
            Ok((!token.models.is_empty()).then_some(token.models))
        } else {
            Ok(None)
        }
    }

    fn touch_token(&mut self, id: &str, now: DateTime<Utc>) {
        if let Some(token) = self.tokens.0.iter_mut().find(|token| token.id == id) {
            token.last_used_at = Some(now);
            self.tokens_dirty = true;
        }
    }

    fn record(
        &mut self,
        authorized: &Authorized,
        requests: u64,
        output_chars: u64,
        now: DateTime<Utc>,
    ) {
        for account in &authorized.accounts {
            self.usage.record(
                authorized.service,
                account,
                &authorized.limits,
                requests,
                output_chars,
                now,
            );
        }
        for (account, limits) in &authorized.cap_accounts {
            self.usage.record(
                authorized.service,
                account,
                limits,
                requests,
                output_chars,
                now,
            );
        }
        self.usage_dirty |= !authorized.accounts.is_empty() || !authorized.cap_accounts.is_empty();
    }

    fn usage_rows(&self, service: Option<Service>, now: DateTime<Utc>) -> Value {
        let peers = crate::net::peer_auth::verified_peers();
        let mut ai = Vec::new();
        if service != Some(Service::Escrow) {
            for account in self.usage.ai.keys() {
                let mut row = json!({"account": account});
                let limits = if let Some(node) = account.strip_prefix("node:") {
                    let did = peers
                        .iter()
                        .find(|(_, n, _)| n == node)
                        .map(|(_, _, d)| d.clone());
                    row["node"] = json!(node);
                    if let Some(did) = &did {
                        row["did"] = json!(did);
                    }
                    self.policy
                        .ai
                        .limits_for(&Principal {
                            node: node.into(),
                            did,
                        })
                        .clone()
                } else if let Some(id) = account.strip_prefix("token:") {
                    row["token"] = json!(id);
                    if let Some(token) = self.tokens.0.iter().find(|t| t.id == id) {
                        row["name"] = json!(token.name);
                        token.limits.clone()
                    } else {
                        self.policy.ai.limits.clone()
                    }
                } else if let Some(id) = account.strip_prefix("cap:") {
                    row["cap"] = json!(id);
                    self.caps
                        .limits_for(id)
                        .unwrap_or_else(|| self.policy.ai.limits.clone())
                } else {
                    self.policy.ai.limits.clone()
                };
                let meter = self.usage.snapshot(Service::Ai, account, &limits, now);
                row["period"] = json!(meter.overflow_period.unwrap_or(limits.period));
                row["period_start"] = json!(meter.period_start);
                row["requests"] = json!(meter.requests);
                row["output_chars"] = json!(meter.output_chars);
                row["limits"] = json!(limits);
                ai.push(row);
            }
        }
        ai.sort_by(|a, b| a["account"].as_str().cmp(&b["account"].as_str()));
        let mut escrow = Vec::new();
        if service != Some(Service::Ai) {
            for (node, stored) in crate::escrow::owner_usage() {
                let did = peers
                    .iter()
                    .find(|(_, n, _)| *n == node)
                    .map(|(_, _, d)| d.clone());
                let principal = Principal {
                    node: node.clone(),
                    did: did.clone(),
                };
                escrow.push(json!({
                    "node": node,
                    "did": did,
                    "stored_bytes": stored,
                    "limit": self.policy.escrow.limits_for(&principal).max_stored_bytes,
                }));
            }
        }
        json!({"ai": ai, "escrow": escrow})
    }

    fn command(&mut self, cmd: &str, args: &Value, now: DateTime<Utc>) -> Result<Value> {
        ensure!(args.is_object(), "arguments must be an object");
        match cmd {
            "authz.show" => {
                return Ok(
                    json!({"policy": self.policy, "tokens": self.tokens.0.iter().map(ApiToken::info).collect::<Vec<_>>()}),
                );
            }
            "authz.token.list" => {
                return Ok(json!(
                    self.tokens.0.iter().map(ApiToken::info).collect::<Vec<_>>()
                ));
            }
            "authz.usage" => return Ok(self.usage_rows(optional_field(args, "service")?, now)),
            "authz.peers" => {
                return Ok(json!(
                    crate::net::peer_auth::verified_peers()
                        .into_iter()
                        .map(|(room, node, did)| json!({"room": room, "node": node, "did": did}))
                        .collect::<Vec<_>>()
                ));
            }
            _ => {}
        }

        // Persist a complete validated mutation before exposing it to running services.
        let mut next = self.clone();
        let (result, file) = match cmd {
            "authz.issuer.add" | "authz.issuer.remove" => {
                let did: String = field(args, "did")?;
                crate::identity::pubkey_from_did(&did)?;
                let issuers = &mut next.policy.service_mut(field(args, "service")?).issuers;
                if cmd.ends_with(".add") && !issuers.contains(&did) {
                    ensure!(
                        issuers.len() < policy::MAX_ENTRIES,
                        "too many capability issuers"
                    );
                    issuers.push(did);
                } else if cmd.ends_with(".remove") {
                    issuers.retain(|d| d != &did);
                }
                (None, "policy.json")
            }
            "authz.mode.set" => {
                next.policy.service_mut(field(args, "service")?).mode = field(args, "mode")?;
                (None, "policy.json")
            }
            "authz.allow.add" | "authz.deny.add" | "authz.allow.remove" | "authz.deny.remove" => {
                let subject: String = field(args, "subject")?;
                policy::validate_subject(&subject)?;
                let policy = next.policy.service_mut(field(args, "service")?);
                let rules = if cmd.starts_with("authz.allow.") {
                    &mut policy.allow
                } else {
                    &mut policy.deny
                };
                rules.retain(|r| r.active(now));
                if cmd.ends_with(".add") {
                    let rule = Rule {
                        subject: subject.clone(),
                        expires_at: expiry(args, now)?,
                        note: optional_field(args, "note")?,
                    };
                    ensure!(
                        rule.note.as_ref().is_none_or(|n| n.chars().count() <= 200),
                        "note exceeds 200 characters"
                    );
                    if let Some(existing) = rules.iter_mut().find(|r| r.subject == subject) {
                        *existing = rule;
                    } else {
                        ensure!(
                            rules.len() < policy::MAX_ENTRIES,
                            "too many policy rules (maximum {})",
                            policy::MAX_ENTRIES
                        );
                        rules.push(rule);
                    }
                } else {
                    rules.retain(|r| r.subject != subject);
                }
                (None, "policy.json")
            }
            "authz.limits.set" => {
                let subject: Option<String> = optional_field(args, "subject")?;
                let policy = next.policy.service_mut(field(args, "service")?);
                let limits = args.get("limits").context("missing `limits`")?;
                if let Some(subject) = subject {
                    policy::validate_subject(&subject)?;
                    if limits.is_null() {
                        policy.overrides.retain(|r| r.subject != subject);
                    } else {
                        let limits: Limits =
                            serde_json::from_value(limits.clone()).context("invalid `limits`")?;
                        if let Some(existing) =
                            policy.overrides.iter_mut().find(|r| r.subject == subject)
                        {
                            existing.limits = limits;
                        } else {
                            ensure!(
                                policy.overrides.len() < policy::MAX_ENTRIES,
                                "too many limit overrides"
                            );
                            policy.overrides.push(LimitOverride { subject, limits });
                        }
                    }
                } else {
                    policy.limits =
                        serde_json::from_value(limits.clone()).context("invalid `limits`")?;
                }
                (None, "policy.json")
            }
            "authz.api.set" => {
                next.policy.api.require_token = field(args, "require_token")?;
                (None, "policy.json")
            }
            "authz.usage.reset" => {
                let service = field(args, "service")?;
                let account: Option<String> = optional_field(args, "account")?;
                let meters = next.usage.meters_mut(service);
                let count = if let Some(account) = account {
                    usize::from(meters.remove(&account).is_some())
                } else {
                    let count = meters.len();
                    meters.clear();
                    count
                };
                (Some(json!({"reset": count})), "usage.json")
            }
            "authz.token.create" => {
                let (secret, token) = next.tokens.create(
                    field(args, "name")?,
                    optional_field(args, "models")?.unwrap_or_default(),
                    optional_field(args, "limits")?.unwrap_or_default(),
                    expiry(args, now)?,
                    now,
                )?;
                (
                    Some(json!({"token": secret, "info": token.info()})),
                    "tokens.json",
                )
            }
            "authz.token.update" | "authz.token.revoke" | "authz.token.delete" => {
                let id: String = field(args, "id")?;
                let token = next
                    .tokens
                    .0
                    .iter_mut()
                    .find(|t| t.id == id)
                    .context("unknown API token id")?;
                if cmd == "authz.token.delete" {
                    next.tokens.0.retain(|t| t.id != id);
                    (Some(json!({"deleted": true})), "tokens.json")
                } else {
                    if cmd == "authz.token.revoke" {
                        token.revoked_at = Some(now);
                    } else {
                        if args.get("name").is_some() {
                            token.name = field(args, "name")?;
                        }
                        if args.get("models").is_some() {
                            token.models = field(args, "models")?;
                        }
                        if args.get("limits").is_some() {
                            token.limits = field(args, "limits")?;
                        }
                        if args.get("expires_at").is_some() {
                            token.expires_at = expiry(args, now)?;
                        }
                    }
                    token.validate()?;
                    (Some(token.info()), "tokens.json")
                }
            }
            _ => bail!("unknown authorization command: {cmd}"),
        };
        match file {
            "policy.json" => {
                next.policy.prune(now);
                next.policy.validate()?;
                next.save(file, &next.policy)?;
            }
            "tokens.json" => {
                next.tokens.validate()?;
                next.save(file, &next.tokens)?;
                next.tokens_dirty = false;
            }
            _ => {
                next.save(file, &next.usage)?;
                next.usage_dirty = false;
            }
        }
        let result = result.unwrap_or_else(|| json!(next.policy));
        *self = next;
        Ok(result)
    }
}

fn field<T: DeserializeOwned>(args: &Value, key: &str) -> Result<T> {
    serde_json::from_value(
        args.get(key)
            .with_context(|| format!("missing `{key}`"))?
            .clone(),
    )
    .with_context(|| format!("invalid `{key}`"))
}

fn optional_field<T: DeserializeOwned>(args: &Value, key: &str) -> Result<Option<T>> {
    match args.get(key) {
        Some(value) if !value.is_null() => serde_json::from_value(value.clone())
            .map(Some)
            .with_context(|| format!("invalid `{key}`")),
        _ => Ok(None),
    }
}

fn expiry(args: &Value, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
    let expires_at: Option<DateTime<Utc>> = optional_field(args, "expires_at")?;
    let ttl: Option<i64> = optional_field(args, "ttl_secs")?;
    ensure!(
        expires_at.is_none() || ttl.is_none(),
        "use either expires_at or ttl_secs"
    );
    const MAX_TTL: i64 = 10 * 366 * 24 * 60 * 60;
    if let Some(ttl) = ttl {
        ensure!(
            (60..=MAX_TTL).contains(&ttl),
            "ttl_secs must be between 60 seconds and 10 years"
        );
        return Ok(Some(
            now.checked_add_signed(Duration::seconds(ttl))
                .context("expiry is out of range")?,
        ));
    }
    if let Some(expiry) = expires_at {
        let duration = expiry.signed_duration_since(now);
        ensure!(
            duration > Duration::zero() && duration <= Duration::seconds(MAX_TTL),
            "expires_at must be in the future within 10 years"
        );
    }
    Ok(expires_at)
}

fn load_or_recover<T: DeserializeOwned + Default>(
    dir: &Path,
    file: &str,
    now: DateTime<Utc>,
    validate: impl FnOnce(&T) -> Result<()>,
) -> T {
    let path = dir.join(file);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return T::default(),
        Err(error) => {
            tracing::warn!(file, %error, "cannot read authorization state; using defaults");
            return T::default();
        }
    };
    let parsed = serde_json::from_slice(&bytes)
        .context("invalid authorization state")
        .and_then(|value| {
            validate(&value)?;
            Ok(value)
        });
    match parsed {
        Ok(value) => {
            crate::statefile::restrict_existing(&path);
            value
        }
        Err(_) => {
            tracing::warn!(
                file,
                "corrupt authorization state; preserving a copy and using defaults"
            );
            let backup = dir.join(format!(
                "{file}.corrupt-{}-{:08x}",
                now.timestamp_millis(),
                rand::random::<u32>()
            ));
            if let Err(error) = crate::statefile::write_private(&backup, &bytes) {
                tracing::warn!(file, %error, "failed to preserve corrupt authorization state");
            }
            T::default()
        }
    }
}

pub fn init(data_dir: &Path) -> Result<()> {
    let mut store = STORE.lock().expect("authz store lock");
    if store.dir.is_none() {
        *store = Store::load(data_dir, Utc::now())?;
    }
    Ok(())
}

pub fn flush() {
    STORE
        .lock()
        .expect("authz store lock")
        .flush_best_effort(Utc::now());
}

pub fn start(identity: Arc<crate::identity::Identity>, runtime: tokio::runtime::Handle) {
    delegation::start(identity, runtime);
}

pub fn access_allowed(service: Service, room: Option<&str>, from: &str) -> bool {
    let principal = Principal::resolve(room, from);
    let store = STORE.lock().expect("authz store lock");
    let now = Utc::now();
    let policy = store.policy.service(service);
    !policy.denied(&principal, now)
        && (policy.access_allowed(&principal, now)
            || store.presented_access_allowed(service, &principal, now))
}

pub fn authorize_p2p(
    service: Service,
    action: Action,
    room: Option<&str>,
    from: &str,
    resource: Option<&str>,
) -> std::result::Result<Authorized, Denied> {
    authorize_p2p_owned(service, action, room, from, resource, None)
}

pub fn authorize_p2p_owned(
    service: Service,
    action: Action,
    room: Option<&str>,
    from: &str,
    resource: Option<&str>,
    resource_owner: Option<&str>,
) -> std::result::Result<Authorized, Denied> {
    let principal = Principal::resolve(room, from);
    STORE
        .lock()
        .expect("authz store lock")
        .authorize_peer_owned(
            service,
            action,
            principal,
            resource,
            resource_owner,
            Utc::now(),
        )
}

pub fn escrow_byte_limit(principal: &Principal) -> Option<u64> {
    STORE
        .lock()
        .expect("authz store lock")
        .policy
        .escrow
        .limits_for(principal)
        .max_stored_bytes
}

pub fn authorize_api(
    authorization: Option<&str>,
    model: Option<&str>,
) -> std::result::Result<Authorized, Denied> {
    STORE
        .lock()
        .expect("authz store lock")
        .authorize_api(authorization, model, Utc::now())
}

/// Whether the local OpenAI-compatible API refuses requests without a live token.
pub fn api_token_required() -> bool {
    STORE
        .lock()
        .expect("authz store lock")
        .policy
        .api
        .require_token
}

pub fn api_model_filter(
    authorization: Option<&str>,
) -> std::result::Result<Option<Vec<String>>, Denied> {
    STORE
        .lock()
        .expect("authz store lock")
        .model_filter(authorization, Utc::now())
}

/// Serialises tests that touch the process-wide store.
#[cfg(test)]
pub(crate) static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Resets the process-wide store to the in-memory open default.
#[cfg(test)]
pub(crate) fn test_reset() {
    *STORE.lock().expect("authz store lock") = Store::default();
}

/// Runs a mutating/reading `authz.*` command against the process-wide store
/// without a daemon (no `local` field in `authz.show`).
#[cfg(test)]
pub(crate) fn test_command(cmd: &str, args: Value) -> Result<Value> {
    STORE
        .lock()
        .expect("authz store lock")
        .command(cmd, &args, Utc::now())
}

pub async fn handle(cmd: &str, args: Value, state: &Arc<crate::daemon::AppState>) -> Result<Value> {
    if cmd.starts_with("authz.cap.") {
        return delegation::handle(cmd, args, state).await;
    }
    let local = if cmd == "authz.show" {
        let identity = crate::identity::current(state).await?;
        Some(json!({"node": identity.node_id(), "did": identity.did()}))
    } else {
        None
    };
    let mut result = STORE
        .lock()
        .expect("authz store lock")
        .command(cmd, &args, Utc::now())?;
    if let Some(local) = local {
        result["local"] = local;
    }
    Ok(result)
}
