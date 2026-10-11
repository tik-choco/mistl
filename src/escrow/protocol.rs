//! tc-escrow v1 signed heads and replies. No plaintext or private keys enter this module.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const MAX_MESSAGE: usize = 16 * 1024;
const MAX_SAFE: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub v: u8,
    #[serde(rename = "type")]
    pub kind: String,
    pub slot: String,
    pub seq: u64,
    pub parent: Option<String>,
    pub cid: String,
    pub size: u64,
    pub sha256: String,
    pub comp: String,
    pub schema: String,
    pub writer: String,
    pub at: u64,
    pub sig: String,
}

pub fn hex_string(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

pub fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

impl Head {
    pub fn validate(&self, max_blob: u64) -> Result<(), Error> {
        if self.v != 1
            || self.kind != "tc-escrow-head"
            || !hex_string(&self.slot, 32)
            || self.seq == 0
            || self.seq > MAX_SAFE
            || self.at > MAX_SAFE
            || self.parent.as_ref().is_some_and(|p| !hex_string(p, 64))
            || !mistlib_core::storage::is_valid_cid(&self.cid)
            || !hex_string(&self.sha256, 64)
            || !matches!(self.comp.as_str(), "none" | "gzip")
            || self.schema.is_empty()
            || self.schema.len() > 256
            || self.writer.len() != 43
            || self.sig.len() != 86
        {
            return Err(Error::new("bad_request"));
        }
        if self.size > max_blob {
            return Err(Error::new("too_large"));
        }
        Ok(())
    }

    pub fn signing_payload(&self) -> Vec<u8> {
        let mut value = serde_json::to_value(self).expect("head serialization");
        value.as_object_mut().unwrap().remove("sig");
        format!(
            "tc-escrow/v1/head\n{}",
            crate::wiresign::stable_stringify(&value)
        )
        .into_bytes()
    }

    pub fn hash(&self) -> String {
        digest(crate::wiresign::stable_stringify(&serde_json::to_value(self).unwrap()).as_bytes())
    }

    pub fn verify(&self) -> Result<(), Error> {
        let key: [u8; 32] = URL_SAFE_NO_PAD
            .decode(&self.writer)
            .ok()
            .and_then(|k| k.try_into().ok())
            .ok_or_else(|| Error::new("bad_sig"))?;
        let sig = URL_SAFE_NO_PAD
            .decode(&self.sig)
            .ok()
            .and_then(|s| Signature::from_slice(&s).ok())
            .ok_or_else(|| Error::new("bad_sig"))?;
        VerifyingKey::from_bytes(&key)
            .ok()
            .and_then(|k| k.verify_strict(&self.signing_payload(), &sig).ok())
            .ok_or_else(|| Error::new("bad_sig"))
    }

    pub fn ack(&self, id: &str) -> Value {
        json!({"v":1,"type":"escrow_ack","id":id,"slot":self.slot,"seq":self.seq,"hash":self.hash()})
    }
}

#[derive(Debug)]
pub struct Error {
    pub code: &'static str,
    pub head: Option<Box<Head>>,
}

impl Error {
    pub fn new(code: &'static str) -> Self {
        Self { code, head: None }
    }
    pub fn conflict(head: &Head) -> Self {
        Self {
            code: "conflict",
            head: Some(Box::new(head.clone())),
        }
    }
    pub fn reply(&self, id: &str) -> Value {
        let mut reply =
            json!({"v":1,"type":"escrow_error","id":id,"code":self.code,"message":self.code});
        if self.code == "conflict" {
            reply["head"] = json!(self.head);
        }
        reply
    }
}

/// Returns true for an idempotent PUT, before attempting any blob fetch.
pub fn check_cas(head: &Head, current: Option<&Head>) -> Result<bool, Error> {
    if let Some(current) = current {
        if head.writer != current.writer {
            return Err(Error::new("writer_mismatch"));
        }
        if head == current {
            return Ok(true);
        }
        if head.parent.as_deref() != Some(current.hash().as_str()) || head.seq != current.seq + 1 {
            return Err(Error::conflict(current));
        }
    } else if head.parent.is_some() {
        return Err(Error::new("conflict"));
    }
    Ok(false)
}
