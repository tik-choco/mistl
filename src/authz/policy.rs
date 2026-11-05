use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::Principal;

pub const MAX_ENTRIES: usize = 1024;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Service {
    #[default]
    Ai,
    Escrow,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    #[default]
    Infer,
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    #[default]
    Open,
    Allowlist,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Period {
    #[default]
    Day,
    Month,
    Total,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Rule {
    pub subject: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub note: Option<String>,
}

impl Rule {
    pub fn active(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_none_or(|expiry| expiry > now)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Limits {
    pub period: Period,
    pub max_requests: Option<u64>,
    pub max_output_chars: Option<u64>,
    pub max_stored_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct LimitOverride {
    pub subject: String,
    pub limits: Limits,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ServicePolicy {
    pub mode: AccessMode,
    pub allow: Vec<Rule>,
    pub deny: Vec<Rule>,
    pub limits: Limits,
    pub overrides: Vec<LimitOverride>,
    pub issuers: Vec<String>,
}

impl ServicePolicy {
    pub fn denied(&self, principal: &Principal, now: DateTime<Utc>) -> bool {
        self.deny
            .iter()
            .any(|r| r.active(now) && subject_matches(&r.subject, principal))
    }

    pub fn explicitly_allowed(&self, principal: &Principal, now: DateTime<Utc>) -> bool {
        principal.did.is_some()
            && self
                .allow
                .iter()
                .any(|r| r.active(now) && subject_matches(&r.subject, principal))
    }

    pub fn access_allowed(&self, principal: &Principal, now: DateTime<Utc>) -> bool {
        // Capability grants can be considered after explicit allows, before open mode.
        !self.denied(principal, now)
            && (self.explicitly_allowed(principal, now) || self.mode == AccessMode::Open)
    }

    pub fn limits_for(&self, principal: &Principal) -> &Limits {
        self.overrides
            .iter()
            .find(|r| subject_matches(&r.subject, principal))
            .map_or(&self.limits, |r| &r.limits)
    }

    fn validate(&self) -> Result<()> {
        for rules in [&self.allow, &self.deny] {
            ensure!(
                rules.len() <= MAX_ENTRIES,
                "too many policy rules (maximum {MAX_ENTRIES})"
            );
            for rule in rules {
                validate_subject(&rule.subject)?;
                ensure!(
                    rule.note.as_ref().is_none_or(|n| n.chars().count() <= 200),
                    "note exceeds 200 characters"
                );
            }
        }
        ensure!(
            self.overrides.len() <= MAX_ENTRIES,
            "too many limit overrides"
        );
        ensure!(
            self.issuers.len() <= MAX_ENTRIES,
            "too many capability issuers"
        );
        for entry in &self.overrides {
            validate_subject(&entry.subject)?;
        }
        for issuer in &self.issuers {
            crate::identity::pubkey_from_did(issuer)?;
        }
        Ok(())
    }

    fn prune(&mut self, now: DateTime<Utc>) {
        self.allow.retain(|r| r.active(now));
        self.deny.retain(|r| r.active(now));
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ApiPolicy {
    pub require_token: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Policy {
    pub ai: ServicePolicy,
    pub escrow: ServicePolicy,
    pub api: ApiPolicy,
}

impl Policy {
    pub fn service(&self, service: Service) -> &ServicePolicy {
        match service {
            Service::Ai => &self.ai,
            Service::Escrow => &self.escrow,
        }
    }

    pub fn service_mut(&mut self, service: Service) -> &mut ServicePolicy {
        match service {
            Service::Ai => &mut self.ai,
            Service::Escrow => &mut self.escrow,
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.ai.validate()?;
        self.escrow.validate()
    }

    pub fn prune(&mut self, now: DateTime<Utc>) {
        self.ai.prune(now);
        self.escrow.prune(now);
    }
}

fn subject_matches(subject: &str, principal: &Principal) -> bool {
    subject == principal.node || principal.did.as_deref() == Some(subject)
}

pub fn validate_subject(subject: &str) -> Result<()> {
    if subject.len() == 16
        && subject
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Ok(());
    }
    crate::identity::pubkey_from_did(subject)
        .map(|_| ())
        .map_err(|_| {
            anyhow::anyhow!(
                "subject must be a 16-character lowercase hex node id or an Ed25519 did:key"
            )
        })
}
