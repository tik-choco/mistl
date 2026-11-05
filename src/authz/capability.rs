//! Signed, bounded delegation proofs. Verification has no clock or policy globals.
use std::fmt;

use base64::Engine as _;
use serde_json::Value;

use super::{Action, Limits, Service};

pub const MAX_BYTES: usize = 8 * 1024;
pub const MAX_DEPTH: usize = 3;
pub const MAX_LIFETIME_MS: i64 = 366 * 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Oversize,
    Depth,
    Shape,
    Signature,
    Time,
    Audience,
    Coverage,
    Verifier,
    Attenuation,
    UntrustedRoot,
    Revoked,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Oversize => "capability exceeds 8 KiB",
            Self::Depth => "capability chain exceeds depth 3",
            Self::Shape => "invalid capability shape",
            Self::Signature => "invalid capability signature",
            Self::Time => "capability expired or not yet valid",
            Self::Audience => "capability audience differs from verified holder",
            Self::Coverage => "capability does not cover request",
            Self::Verifier => "capability excludes this verifier",
            Self::Attenuation => "delegation widens parent authority",
            Self::UntrustedRoot => "untrusted capability root",
            Self::Revoked => "capability chain is revoked",
        })
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug)]
pub struct Capability {
    pub wire: Value,
    pub id: String,
    pub issuer: String,
    pub aud: String,
    pub service: Service,
    pub actions: Vec<Action>,
    pub resource: String,
    pub to: Option<Vec<String>>,
    pub nbf: i64,
    pub exp: i64,
    pub limits: Option<Limits>,
    pub parent: Option<Box<Capability>>,
}

impl Capability {
    pub fn root(&self) -> &Self {
        self.parent.as_deref().map_or(self, Self::root)
    }

    pub fn chain(&self) -> Vec<&Self> {
        let mut links = vec![self];
        let mut cap = self;
        while let Some(parent) = cap.parent.as_deref() {
            links.push(parent);
            cap = parent;
        }
        links
    }

    pub fn contains_id(&self, id: &str) -> bool {
        self.chain().iter().any(|link| link.id == id)
    }
}

pub struct Context<'a, T, R> {
    pub now_ms: i64,
    pub holder: &'a str,
    pub local_did: &'a str,
    pub service: Service,
    pub action: Action,
    pub resource: &'a str,
    pub trusted_root: T,
    pub is_revoked: R,
}

pub fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// tc-escrow heads encode the Ed25519 writer key as base64url, not did:key.
pub fn writer_did(writer: &str) -> Option<String> {
    if writer.len() != 43 {
        return None;
    }
    let key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(writer)
        .ok()?;
    if key.len() != 32 {
        return None;
    }
    let mut multicodec = vec![0xed, 0x01];
    multicodec.extend(key);
    Some(format!(
        "did:key:z{}",
        bs58::encode(multicodec).into_string()
    ))
}

fn valid_did(did: &str) -> bool {
    crate::identity::pubkey_from_did(did).is_ok()
}

pub fn exact_resource(resource: &str) -> Option<(&str, &str)> {
    let (room, slot) = resource.rsplit_once('/')?;
    (crate::escrow::valid_room(room)
        && slot.len() == 32
        && slot
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
    .then_some((room, slot))
}

fn valid_resource(resource: &str) -> bool {
    resource == "*"
        || resource
            .strip_suffix("/*")
            .is_some_and(crate::escrow::valid_room)
        || exact_resource(resource).is_some()
}

pub fn covers(parent: &str, child: &str) -> bool {
    parent == "*"
        || parent == child
        || parent
            .strip_suffix("/*")
            .is_some_and(|room| child.rsplit_once('/').is_some_and(|(r, _)| r == room))
}

pub fn parse(wire: &Value) -> Result<Capability, Error> {
    if serde_json::to_vec(wire).map_err(|_| Error::Shape)?.len() > MAX_BYTES {
        return Err(Error::Oversize);
    }
    parse_link(wire, 1)
}

fn parse_link(wire: &Value, depth: usize) -> Result<Capability, Error> {
    if depth > MAX_DEPTH {
        return Err(Error::Depth);
    }
    let string = |key: &str| wire[key].as_str().ok_or(Error::Shape);
    if string("t")? != "mistl-cap-v1"
        || ["type", "v", "kind"]
            .iter()
            .any(|key| wire.get(key).is_some())
    {
        return Err(Error::Shape);
    }
    let id = string("id")?;
    let issuer = string("fromId")?;
    let aud = string("aud")?;
    let resource = string("res")?;
    let service: Service = serde_json::from_value(wire["svc"].clone()).map_err(|_| Error::Shape)?;
    let actions: Vec<Action> =
        serde_json::from_value(wire["act"].clone()).map_err(|_| Error::Shape)?;
    let nbf = wire["nbf"].as_i64().ok_or(Error::Shape)?;
    let exp = wire["exp"].as_i64().ok_or(Error::Shape)?;
    if !valid_id(id)
        || !valid_did(issuer)
        || !valid_did(aud)
        || !valid_resource(resource)
        || (service == Service::Ai && resource != "*")
        || nbf < 0
        || exp <= nbf
        || exp - nbf > MAX_LIFETIME_MS
        || actions.is_empty()
        || actions.len() > 2
        || actions.iter().any(|action| match service {
            Service::Ai => *action != Action::Infer,
            Service::Escrow => *action == Action::Infer,
        })
        || (actions.len() == 2 && actions[0] == actions[1])
    {
        return Err(Error::Shape);
    }
    let to: Option<Vec<String>> = wire
        .get("to")
        .map(|v| serde_json::from_value(v.clone()).map_err(|_| Error::Shape))
        .transpose()?;
    if to
        .as_ref()
        .is_some_and(|to| to.len() > 8 || to.iter().any(|did| !valid_did(did)))
    {
        return Err(Error::Shape);
    }
    let limits = wire
        .get("lim")
        .map(|v| {
            if !v.is_object() {
                return Err(Error::Shape);
            }
            serde_json::from_value(v.clone()).map_err(|_| Error::Shape)
        })
        .transpose()?;
    let parent = wire
        .get("prf")
        .map(|p| parse_link(p, depth + 1).map(Box::new))
        .transpose()?;
    Ok(Capability {
        wire: wire.clone(),
        id: id.into(),
        issuer: issuer.into(),
        aud: aud.into(),
        service,
        actions,
        resource: resource.into(),
        to,
        nbf,
        exp,
        limits,
        parent,
    })
}

fn limit_narrows(child: Option<u64>, parent: Option<u64>) -> bool {
    parent.is_none_or(|p| child.is_some_and(|c| c <= p))
}

pub fn attenuates(child: &Capability, parent: &Capability) -> bool {
    child.issuer == parent.aud
        && child.service == parent.service
        && child.actions.iter().all(|a| parent.actions.contains(a))
        && covers(&parent.resource, &child.resource)
        && child.exp <= parent.exp
        && child.nbf >= parent.nbf
        && parent.to.as_ref().is_none_or(|p| {
            child
                .to
                .as_ref()
                .is_some_and(|c| c.iter().all(|d| p.contains(d)))
        })
        && parent.limits.as_ref().is_none_or(|p| {
            let c = child.limits.clone().unwrap_or_default();
            let bounded = p.max_requests.is_some()
                || p.max_output_chars.is_some()
                || p.max_stored_bytes.is_some();
            (!bounded || c.period == p.period)
                && limit_narrows(c.max_requests, p.max_requests)
                && limit_narrows(c.max_output_chars, p.max_output_chars)
                && limit_narrows(c.max_stored_bytes, p.max_stored_bytes)
        })
}

pub fn verify<T, R>(wire: &Value, ctx: &Context<'_, T, R>) -> Result<Capability, Error>
where
    T: Fn(&str) -> bool,
    R: Fn(&str) -> bool,
{
    // Parse the entire bounded chain before doing any signature work.
    let cap = parse(wire)?;
    let mut holder = ctx.holder;
    for link in cap.chain() {
        if ctx.now_ms
            < link
                .nbf
                .saturating_sub(crate::net::peer_auth::MAX_CLOCK_SKEW_MS as i64)
            || ctx.now_ms >= link.exp
        {
            return Err(Error::Time);
        }
        if link.aud != holder {
            return Err(Error::Audience);
        }
        if link.service != ctx.service
            || !link.actions.contains(&ctx.action)
            || !covers(&link.resource, ctx.resource)
        {
            return Err(Error::Coverage);
        }
        if link
            .to
            .as_ref()
            .is_some_and(|to| !to.iter().any(|d| d == ctx.local_did))
        {
            return Err(Error::Verifier);
        }
        if (ctx.is_revoked)(&link.id) {
            return Err(Error::Revoked);
        }
        if link.parent.as_deref().is_some_and(|p| !attenuates(link, p)) {
            return Err(Error::Attenuation);
        }
        if !crate::wiresign::verify_wire(&link.wire).unwrap_or(false) {
            return Err(Error::Signature);
        }
        holder = &link.issuer;
    }
    if !(ctx.trusted_root)(&cap.root().issuer) {
        return Err(Error::UntrustedRoot);
    }
    Ok(cap)
}
