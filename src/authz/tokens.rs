use anyhow::{Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{Denied, Limits};

pub const MAX_TOKENS: usize = 256;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ApiToken {
    pub id: String,
    pub name: String,
    pub hash: String,
    pub hint: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub models: Vec<String>,
    pub limits: Limits,
}

impl ApiToken {
    pub fn info(&self) -> serde_json::Value {
        let mut info = serde_json::to_value(self).expect("token serialization");
        info.as_object_mut().unwrap().remove("hash");
        info
    }

    pub fn live(&self, now: DateTime<Utc>) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_none_or(|expiry| expiry > now)
    }

    pub fn validate(&self) -> Result<()> {
        validate_name(&self.name)?;
        validate_models(&self.models)?;
        ensure!(
            self.hash.len() == 64
                && self
                    .hash
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid token hash"
        );
        ensure!(self.id == self.hash[..12], "invalid token id");
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub struct TokenBook(pub Vec<ApiToken>);

impl TokenBook {
    pub fn create(
        &mut self,
        name: String,
        models: Vec<String>,
        limits: Limits,
        expires_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<(String, ApiToken)> {
        ensure!(
            self.0.len() < MAX_TOKENS,
            "too many API tokens (maximum {MAX_TOKENS})"
        );
        validate_name(&name)?;
        validate_models(&models)?;
        let mut random = [0; 32];
        rand::rngs::OsRng.fill_bytes(&mut random);
        let encoded = URL_SAFE_NO_PAD.encode(random);
        let secret = format!("mst_{encoded}");
        let hash = hash_secret(&secret);
        let token = ApiToken {
            id: hash[..12].into(),
            name,
            hash,
            hint: format!("mst_{}…", &encoded[..4]),
            created_at: now,
            expires_at,
            revoked_at: None,
            last_used_at: None,
            models,
            limits,
        };
        ensure!(
            !self.0.iter().any(|t| t.id == token.id),
            "token id collision; please retry"
        );
        self.0.push(token.clone());
        Ok((secret, token))
    }

    pub fn lookup(
        &self,
        authorization: Option<&str>,
        required: bool,
        now: DateTime<Utc>,
    ) -> Result<Option<&ApiToken>, Denied> {
        let secret = authorization.and_then(bearer);
        let Some(secret) = secret.filter(|s| s.starts_with("mst_")) else {
            return if required {
                Err(Denied::unauthorized())
            } else {
                Ok(None)
            };
        };
        // Bound hashing work for a header supplied by a remote caller.
        if secret.len() != 47 {
            return Err(Denied::unauthorized());
        }
        let hash = hash_secret(secret);
        self.0
            .iter()
            .find(|token| token.hash == hash && token.live(now))
            .map(Some)
            .ok_or_else(Denied::unauthorized)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(self.0.len() <= MAX_TOKENS, "too many API tokens");
        let mut ids = std::collections::HashSet::new();
        for token in &self.0 {
            token.validate()?;
            ensure!(ids.insert(&token.id), "duplicate API token id");
        }
        Ok(())
    }
}

fn bearer(header: &str) -> Option<&str> {
    let mut words = header.split_whitespace();
    let scheme = words.next()?;
    let secret = words.next()?;
    (scheme.eq_ignore_ascii_case("bearer") && words.next().is_none()).then_some(secret)
}

fn hash_secret(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

pub fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.trim().is_empty() && name.chars().count() <= 64,
        "token name must contain 1 to 64 characters"
    );
    Ok(())
}

pub fn validate_models(models: &[String]) -> Result<()> {
    ensure!(
        models.len() <= super::policy::MAX_ENTRIES,
        "too many model restrictions"
    );
    ensure!(
        models
            .iter()
            .all(|m| !m.trim().is_empty() && m.len() <= 256),
        "model names must contain 1 to 256 bytes"
    );
    Ok(())
}
