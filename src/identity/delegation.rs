//! DID delegation chains: a permanent "root" identity signs a short-lived
//! authorization for a per-origin "leaf" key, so browser apps (which each
//! get their own origin-scoped `did:key`, see `did-identity.md`) can all be
//! recognized as the same person once the user pairs their browser with a
//! root identity held in mistl.
//!
//! Canonical spec, byte-exact and shared with the independent TypeScript
//! implementation (mistai's `src/identity/`):
//! `protocol/docs/data-contracts/docs/did-delegation.md`. Do not change the
//! derivation, encoding, or field names here without updating that document
//! and the TS side in lockstep -- this crate and mistai each implement the
//! spec independently and must agree byte-for-byte.
//!
//! Chain depth is fixed at 1 (root -> leaf only, no sub-delegation), and
//! there is no revocation list: delegations are short-lived (`exp`) and
//! reissued rather than revoked.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Identity;

/// Sanity cap on delegation lifetime (spec verification rule 5): prevents an
/// effectively-permanent delegation. Recommended TTL is 60 days, range
/// 1-365 days (enforced by [`parse_ttl`]) -- this 400-day cap is a wider,
/// hard invariant checked on both issue and verify.
pub const MAX_TTL_DAYS: i64 = 400;

/// Clock skew tolerance for verification (spec rule 6).
const CLOCK_SKEW: Duration = Duration::minutes(5);

/// Cap on how many issued delegations [`record_delegation`] keeps on disk;
/// oldest entries are dropped first once exceeded.
const MAX_RECORDED: usize = 200;

/// A signed authorization from a permanent `root` identity to a per-origin
/// `leaf` key. See the module doc for the canonical spec this mirrors field
/// for field (`v`/`root`/`leaf`/`iat`/`exp`/`sig`, exactly those names -- the
/// TS side deserializes this shape directly).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationV1 {
    pub v: u8,
    pub root: String,
    pub leaf: String,
    pub iat: String,
    pub exp: String,
    pub sig: String,
}

impl DelegationV1 {
    /// Every field except `sig`, stably stringified -- the exact bytes that
    /// get Ed25519-signed/verified. Sorting keys (done by
    /// [`crate::wiresign::stable_stringify`]) always yields the field order
    /// `exp, iat, leaf, root, v` per the spec's "signature" section.
    fn signing_payload(&self) -> String {
        let mut value = serde_json::to_value(self).expect("DelegationV1 always serializes to JSON");
        if let Value::Object(map) = &mut value {
            map.remove("sig");
        }
        crate::wiresign::stable_stringify(&value)
    }
}

/// Issue a new root -> leaf delegation, signed by `identity` (which becomes
/// `root`). `now` is threaded through explicitly (rather than calling
/// `Utc::now()` internally) so tests can exercise expiry/skew behavior
/// deterministically.
pub fn issue(
    identity: &Identity,
    leaf: &str,
    ttl: Duration,
    now: DateTime<Utc>,
) -> Result<DelegationV1> {
    let root = identity.did();
    if leaf == root {
        bail!("cannot delegate to the root identity itself (root == leaf)");
    }
    // Spec rule 2: leaf must be a well-formed Ed25519 did:key. Reuses
    // identity::mod.rs's did:key decoder rather than re-parsing here.
    super::pubkey_from_did(leaf).context("leaf is not a well-formed Ed25519 did:key")?;

    if ttl <= Duration::zero() {
        bail!("ttl must be positive");
    }
    if ttl > Duration::days(MAX_TTL_DAYS) {
        bail!("ttl exceeds the {MAX_TTL_DAYS}-day sanity cap");
    }

    let exp_at = now + ttl;
    // RFC 3339, millisecond precision, `Z` suffix -- byte-identical to JS's
    // `Date#toISOString()`, per the spec's "time format" section. Must NOT
    // use the default `to_rfc3339()` (that emits a `+00:00` offset instead).
    let iat = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let exp = exp_at.to_rfc3339_opts(SecondsFormat::Millis, true);

    let mut delegation = DelegationV1 {
        v: 1,
        root: root.to_string(),
        leaf: leaf.to_string(),
        iat,
        exp,
        sig: String::new(),
    };
    let payload = delegation.signing_payload();
    // Same Ed25519 + unpadded base64url encoding as wiresign::sign_wire's
    // `signature` field -- the spec calls this out explicitly.
    delegation.sig = URL_SAFE_NO_PAD.encode(identity.sign(payload.as_bytes()));
    Ok(delegation)
}

/// Verify a delegation against every rule in the spec's "verification
/// rules" section (1-8). Untrusted input (received over the wire or read
/// back from disk), so this never panics and returns `false` -- never
/// `Err` -- for anything that isn't authentic, mirroring
/// `wiresign::verify_wire`'s convention. `expect_leaf`, when given, encodes
/// rule 8 (the delegation must belong to a specific leaf, e.g. the wire's
/// `fromId`).
///
/// Not yet called from mistl's own runtime: this crate currently only
/// *issues* delegations (as root custodian); *verifying* one attached to a
/// wire is the receiving peer's job (a browser app, per the spec's "受信側"
/// section -- mistai's TypeScript port of this same function). Kept public
/// and fully tested here so a future mistl-side consumer (e.g. the chat
/// relay resolving senders to their root identity) has a byte-compatible
/// implementation ready to call without redesigning the API.
#[allow(dead_code)]
pub fn verify(d: &DelegationV1, now: DateTime<Utc>, expect_leaf: Option<&str>) -> bool {
    // Rule 1: v must be exactly 1. (root/leaf/iat/exp/sig are already typed
    // as String by DelegationV1, so the "must be strings" half of rule 1 is
    // structural for any value that deserialized into this type at all.)
    if d.v != 1 {
        return false;
    }
    // Rule 2: root and leaf must both be well-formed Ed25519 did:keys.
    if super::pubkey_from_did(&d.root).is_err() || super::pubkey_from_did(&d.leaf).is_err() {
        return false;
    }
    // Rule 3: no self-delegation.
    if d.root == d.leaf {
        return false;
    }
    // Rule 4: iat/exp must parse and iat < exp.
    let Ok(iat) = DateTime::parse_from_rfc3339(&d.iat) else {
        return false;
    };
    let Ok(exp) = DateTime::parse_from_rfc3339(&d.exp) else {
        return false;
    };
    let iat = iat.with_timezone(&Utc);
    let exp = exp.with_timezone(&Utc);
    if iat >= exp {
        return false;
    }
    // Rule 5: sanity cap on the delegation's total lifetime.
    if exp - iat > Duration::days(MAX_TTL_DAYS) {
        return false;
    }
    // Rule 6: clock skew tolerance.
    if now < iat - CLOCK_SKEW || now > exp + CLOCK_SKEW {
        return false;
    }
    // Rule 7: sig must verify against root.
    let Ok(sig_bytes) = URL_SAFE_NO_PAD.decode(&d.sig) else {
        return false;
    };
    let payload = d.signing_payload();
    match super::verify(&d.root, payload.as_bytes(), &sig_bytes) {
        Ok(true) => {}
        _ => return false,
    }
    // Rule 8: caller-specified expected leaf, if any.
    if let Some(expect) = expect_leaf
        && d.leaf != expect
    {
        return false;
    }
    true
}

/// Parse a ttl/duration string like `"60d"`, `"90m"`, `"12h"`, `"30s"`
/// (suffix-less defaults to days, e.g. `"14"` == `"14d"`). No range
/// validation -- see [`parse_ttl`] for the delegation-specific 1-365 day
/// range, and pairing's own `--timeout` (a much shorter window) for the
/// other caller of this shared parser.
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        bail!("duration string is empty");
    }
    let last = s.chars().next_back().expect("checked non-empty above");
    let (number_part, unit) = if last.is_ascii_alphabetic() {
        (&s[..s.len() - last.len_utf8()], last.to_ascii_lowercase())
    } else {
        (s, 'd')
    };
    let n: i64 = number_part.parse().map_err(|_| {
        anyhow!("invalid duration {s:?}: expected a number, optionally suffixed s/m/h/d")
    })?;
    if n <= 0 {
        bail!("duration must be positive: {s:?}");
    }
    match unit {
        's' => Ok(Duration::seconds(n)),
        'm' => Ok(Duration::minutes(n)),
        'h' => Ok(Duration::hours(n)),
        'd' => Ok(Duration::days(n)),
        other => bail!("unknown duration unit {other:?} in {s:?} (expected one of s/m/h/d)"),
    }
}

/// Parse a delegation TTL string (`mistl key delegate --ttl`), validating
/// the spec's recommended range of 1-365 days.
pub fn parse_ttl(s: &str) -> Result<Duration> {
    let duration = parse_duration(s)?;
    if duration < Duration::days(1) || duration > Duration::days(365) {
        bail!("ttl must be between 1 and 365 days, got {s:?}");
    }
    Ok(duration)
}

/// Same shared parser as [`parse_ttl`], but for pairing's `--timeout`
/// (session lifetime, minutes-scale by default) which has no reason to
/// share the delegation TTL's 1-365 day range.
pub(super) fn parse_short_duration(s: &str) -> Result<Duration> {
    parse_duration(s)
}

fn delegations_path() -> Result<PathBuf> {
    Ok(crate::config::data_dir()?
        .join("identity")
        .join("delegations.json"))
}

/// Every delegation this daemon has ever issued (as root), most recent
/// last. Empty if none have been issued yet.
pub fn load_delegations() -> Result<Vec<DelegationV1>> {
    let path = delegations_path()?;
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Persist a newly-issued delegation, replacing any earlier entry for the
/// same `leaf` (a leaf only ever needs its latest delegation) and trimming
/// the oldest entries once [`MAX_RECORDED`] is exceeded.
pub fn record_delegation(delegation: DelegationV1) -> Result<()> {
    let mut delegations = load_delegations()?;
    delegations.retain(|d| d.leaf != delegation.leaf);
    delegations.push(delegation);
    if delegations.len() > MAX_RECORDED {
        let excess = delegations.len() - MAX_RECORDED;
        delegations.drain(0..excess);
    }

    let path = delegations_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(&path, serde_json::to_string_pretty(&delegations)?)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-07-28T00:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn issue_then_verify_round_trips() {
        let root = crate::identity::for_test();
        let leaf = crate::identity::for_test();
        let now = fixed_now();

        let d = issue(&root, leaf.did(), Duration::days(60), now).expect("issue succeeds");
        assert_eq!(d.v, 1);
        assert_eq!(d.root, root.did());
        assert_eq!(d.leaf, leaf.did());
        assert!(
            verify(&d, now, None),
            "freshly issued delegation must verify"
        );
        assert!(
            verify(&d, now, Some(leaf.did())),
            "must verify against its own leaf"
        );
        // A little later, still within TTL, still verifies.
        assert!(verify(&d, now + Duration::days(30), None));
    }

    #[test]
    fn verify_rejects_leaf_mismatch() {
        let root = crate::identity::for_test();
        let leaf = crate::identity::for_test();
        let other_leaf = crate::identity::for_test();
        let now = fixed_now();

        let d = issue(&root, leaf.did(), Duration::days(60), now).unwrap();
        assert!(!verify(&d, now, Some(other_leaf.did())));
    }

    #[test]
    fn verify_rejects_expired_delegation() {
        let root = crate::identity::for_test();
        let leaf = crate::identity::for_test();
        let now = fixed_now();

        let d = issue(&root, leaf.did(), Duration::days(60), now).unwrap();
        // Well past exp + the 5-minute skew allowance.
        let later = now + Duration::days(61);
        assert!(!verify(&d, later, None));
    }

    #[test]
    fn verify_rejects_iat_after_exp() {
        let root = crate::identity::for_test();
        let leaf = crate::identity::for_test();
        let now = fixed_now();

        let mut d = issue(&root, leaf.did(), Duration::days(60), now).unwrap();
        // Swap iat/exp so iat > exp, then re-sign so only rule 4 is under test.
        std::mem::swap(&mut d.iat, &mut d.exp);
        let payload = d.signing_payload();
        d.sig = URL_SAFE_NO_PAD.encode(root.sign(payload.as_bytes()));
        assert!(!verify(&d, now, None));
    }

    #[test]
    fn verify_rejects_ttl_over_400_days() {
        let root = crate::identity::for_test();
        let leaf = crate::identity::for_test();
        let now = fixed_now();

        // issue() itself refuses to create this; build the over-long
        // delegation by hand (as a malicious/buggy peer might) and confirm
        // verify() independently enforces the cap.
        let exp_at = now + Duration::days(401);
        let mut d = DelegationV1 {
            v: 1,
            root: root.did().to_string(),
            leaf: leaf.did().to_string(),
            iat: now.to_rfc3339_opts(SecondsFormat::Millis, true),
            exp: exp_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            sig: String::new(),
        };
        let payload = d.signing_payload();
        d.sig = URL_SAFE_NO_PAD.encode(root.sign(payload.as_bytes()));
        assert!(!verify(&d, now, None));
    }

    #[test]
    fn issue_rejects_ttl_over_400_days() {
        let root = crate::identity::for_test();
        let leaf = crate::identity::for_test();
        assert!(issue(&root, leaf.did(), Duration::days(401), fixed_now()).is_err());
    }

    #[test]
    fn issue_rejects_self_delegation() {
        let root = crate::identity::for_test();
        assert!(issue(&root, root.did(), Duration::days(60), fixed_now()).is_err());
    }

    #[test]
    fn verify_rejects_self_delegation_built_by_hand() {
        let root = crate::identity::for_test();
        let now = fixed_now();
        let mut d = DelegationV1 {
            v: 1,
            root: root.did().to_string(),
            leaf: root.did().to_string(),
            iat: now.to_rfc3339_opts(SecondsFormat::Millis, true),
            exp: (now + Duration::days(1)).to_rfc3339_opts(SecondsFormat::Millis, true),
            sig: String::new(),
        };
        let payload = d.signing_payload();
        d.sig = URL_SAFE_NO_PAD.encode(root.sign(payload.as_bytes()));
        assert!(!verify(&d, now, None));
    }

    #[test]
    fn verify_rejects_tampered_signature() {
        let root = crate::identity::for_test();
        let leaf = crate::identity::for_test();
        let now = fixed_now();
        let mut d = issue(&root, leaf.did(), Duration::days(60), now).unwrap();
        d.exp = (now + Duration::days(90)).to_rfc3339_opts(SecondsFormat::Millis, true);
        assert!(
            !verify(&d, now, None),
            "sig must not verify after a field is tampered with"
        );
    }

    #[test]
    fn issue_rejects_non_ed25519_leaf() {
        let root = crate::identity::for_test();
        assert!(
            issue(
                &root,
                "did:key:znotarealkey",
                Duration::days(60),
                fixed_now()
            )
            .is_err()
        );
        assert!(issue(&root, "not-a-did-at-all", Duration::days(60), fixed_now()).is_err());
    }

    #[test]
    fn verify_rejects_non_ed25519_root_or_leaf() {
        let leaf = crate::identity::for_test();
        let now = fixed_now();
        let d = DelegationV1 {
            v: 1,
            root: "did:key:znotarealkey".to_string(),
            leaf: leaf.did().to_string(),
            iat: now.to_rfc3339_opts(SecondsFormat::Millis, true),
            exp: (now + Duration::days(1)).to_rfc3339_opts(SecondsFormat::Millis, true),
            sig: "whatever".to_string(),
        };
        assert!(!verify(&d, now, None));
    }

    // ---- signing_payload / cross-implementation vectors ----
    //
    // These fixed iat/exp/root/leaf values and the resulting payload string
    // are a cross-implementation vector shared with mistai's TypeScript
    // delegation implementation (`src/identity/`) -- both sides derive
    // `signing_payload` from the same DelegationV1 shape via the same
    // `stableStringify`/`stable_stringify` rule (sorted keys, no signature
    // field), so the two must produce byte-identical strings for these
    // inputs. root/leaf below are syntactically well-formed did:keys but
    // arbitrary (signing_payload only stringifies fields, it never decodes
    // them), chosen to match the FIXED_DID pattern already used in
    // wiresign.rs's own interop vectors.
    const VECTOR_ROOT: &str = "did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4";
    const VECTOR_LEAF: &str = "did:key:z6MkoTHsgNNrby9J7Yaso8QUn2Aza4A5A8zk4B7EudJRDoLU";

    #[test]
    fn signing_payload_matches_sorted_key_order() {
        let d = DelegationV1 {
            v: 1,
            root: VECTOR_ROOT.to_string(),
            leaf: VECTOR_LEAF.to_string(),
            iat: "2026-01-01T00:00:00.000Z".to_string(),
            exp: "2026-03-01T00:00:00.000Z".to_string(),
            sig: "unused-for-this-check".to_string(),
        };
        let expected = format!(
            r#"{{"exp":"2026-03-01T00:00:00.000Z","iat":"2026-01-01T00:00:00.000Z","leaf":"{VECTOR_LEAF}","root":"{VECTOR_ROOT}","v":1}}"#
        );
        assert_eq!(d.signing_payload(), expected);
    }

    /// The strongest interop guarantee available without running a browser:
    /// this exact delegation was produced by mistai's *actual* TypeScript
    /// `signDelegation()` under Node's WebCrypto, signing with the same fixed
    /// Ed25519 seed (`ca22031e...`, whose DID is [`VECTOR_ROOT`]) that
    /// `wiresign.rs`'s vectors already use. Verifying it here proves the two
    /// implementations agree on the whole chain -- stable_stringify output,
    /// the RFC 3339 millisecond time format, unpadded base64url signature
    /// encoding, and did:key -> Ed25519 pubkey recovery. Regenerate only if
    /// the spec's signing rules change.
    const VECTOR_TS_SIGNED_SIG: &str =
        "hHTdbP1RnTGJuo9n6ksuWnnO_maEGUGvsL3d2pnKW9gxRZ6AWcTL09CxHQvdR8bgPO2schhNsvfKWUSdAcgFAA";

    fn ts_signed_vector() -> DelegationV1 {
        DelegationV1 {
            v: 1,
            root: VECTOR_ROOT.to_string(),
            leaf: VECTOR_LEAF.to_string(),
            iat: "2026-01-01T00:00:00.000Z".to_string(),
            exp: "2026-03-01T00:00:00.000Z".to_string(),
            sig: VECTOR_TS_SIGNED_SIG.to_string(),
        }
    }

    fn instant(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn verify_accepts_a_delegation_signed_by_the_typescript_implementation() {
        let d = ts_signed_vector();
        let within_window = instant("2026-02-01T00:00:00.000Z");
        assert!(verify(&d, within_window, None));
        assert!(verify(&d, within_window, Some(VECTOR_LEAF)));
    }

    #[test]
    fn verify_rejects_the_typescript_vector_outside_its_window_or_when_tampered() {
        let d = ts_signed_vector();
        // Past `exp` (plus skew) the same valid signature must stop counting.
        assert!(!verify(&d, instant("2026-06-01T00:00:00.000Z"), None));
        // Before `iat` (minus skew), likewise.
        assert!(!verify(&d, instant("2025-12-01T00:00:00.000Z"), None));

        let within_window = instant("2026-02-01T00:00:00.000Z");
        assert!(!verify(&d, within_window, Some(VECTOR_ROOT)));

        let mut tampered = ts_signed_vector();
        tampered.exp = "2027-03-01T00:00:00.000Z".to_string();
        assert!(!verify(&tampered, within_window, None));
    }

    // ---- parse_ttl ----

    #[test]
    fn parse_ttl_accepts_suffixed_and_bare_values() {
        assert_eq!(parse_ttl("60d").unwrap(), Duration::days(60));
        assert_eq!(parse_ttl("60").unwrap(), Duration::days(60));
        assert_eq!(parse_ttl("365d").unwrap(), Duration::days(365));
        assert_eq!(parse_ttl("1d").unwrap(), Duration::days(1));
        assert_eq!(parse_ttl("1440m").unwrap(), Duration::minutes(1440)); // == 1 day
    }

    #[test]
    fn parse_ttl_rejects_out_of_range_or_malformed() {
        assert!(parse_ttl("0d").is_err());
        assert!(parse_ttl("366d").is_err());
        assert!(parse_ttl("400d").is_err());
        assert!(parse_ttl("30m").is_err()); // under 1 day
        assert!(parse_ttl("abc").is_err());
        assert!(parse_ttl("").is_err());
        assert!(parse_ttl("-5d").is_err());
        assert!(parse_ttl("5x").is_err());
    }

    #[test]
    fn parse_short_duration_allows_sub_day_windows() {
        assert_eq!(parse_short_duration("5m").unwrap(), Duration::minutes(5));
        assert_eq!(parse_short_duration("30s").unwrap(), Duration::seconds(30));
        assert_eq!(parse_short_duration("2h").unwrap(), Duration::hours(2));
        assert!(parse_short_duration("0m").is_err());
    }
}
