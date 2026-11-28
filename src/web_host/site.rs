//! Pure formats for mistl web: site identifiers, the file manifest, the
//! signed site record, and the gateway's request-path / MIME rules.
//!
//! No networking, storage or daemon state lives here, so the same rules are
//! shared verbatim by upload (publisher side) and the gateway (serve side):
//! a path accepted by one is accepted by the other.

use std::path::Path;

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::identity::Identity;

/// Length of a siteId: 20 bytes as unpadded base32.
pub const SITE_ID_LEN: usize = 32;
/// Max files in one manifest.
pub const MAX_FILES: usize = 20_000;
/// Max size of a single site file.
pub const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;
/// Max total size of a site (also capped by `[web] max_site_bytes`).
pub const MAX_SITE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Max serialized manifest size.
pub const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
/// Max serialized site record size.
pub const MAX_RECORD_BYTES: usize = 4096;
/// Max name length.
pub const MAX_NAME_LEN: usize = 48;
/// Max manifest path length in bytes.
pub const MAX_PATH_BYTES: usize = 512;
/// Max CID length accepted in manifests and records.
pub const MAX_CID_LEN: usize = 128;
/// How far in the future a record's `at` may be (clock skew allowance).
pub const MAX_FUTURE_SKEW_MS: u64 = 10 * 60 * 1000;
/// First path segment reserved for gateway endpoints (`/.mistl/allow`, ...).
pub const RESERVED_SEGMENT: &str = ".mistl";

pub const MANIFEST_TYPE: &str = "mistl-web-manifest";
pub const RECORD_TYPE: &str = "mistl-web-record";

const SITE_ID_DOMAIN: &[u8] = b"mistl-web-site-v1\0";
const BASE32_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
/// Build-output directory names; the default site name comes from their parent.
const BUILD_DIR_NAMES: &[&str] = &["dist", "build", "out", "public", "www", "site", "html"];
const RECORD_KEYS: &[&str] = &[
    "v",
    "type",
    "fromId",
    "site",
    "name",
    "seq",
    "manifest",
    "size",
    "files",
    "at",
    "signature",
];

// ---- Identifiers ----

/// `base32(sha256("mistl-web-site-v1\0" || did || "\0" || name)[..20])`.
pub fn derive_site_id(did: &str, name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(SITE_ID_DOMAIN);
    hasher.update(did.as_bytes());
    hasher.update([0u8]);
    hasher.update(name.as_bytes());
    base32_lower(&hasher.finalize()[..20])
}

/// Lowercase RFC 4648 base32 without padding.
fn base32_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(5) * 8);
    let mut buf: u32 = 0;
    let mut bits = 0u32;
    for &b in bytes {
        buf = (buf << 8) | u32::from(b);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(BASE32_ALPHABET[((buf >> bits) & 31) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(BASE32_ALPHABET[((buf << (5 - bits)) & 31) as usize] as char);
    }
    out
}

/// `^[a-z2-7]{32}$`.
pub fn is_site_id(s: &str) -> bool {
    s.len() == SITE_ID_LEN
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
}

/// `^[a-z0-9](?:[a-z0-9-]{0,46}[a-z0-9])?$`.
pub fn valid_name(s: &str) -> bool {
    let b = s.as_bytes();
    let edge = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    !b.is_empty()
        && b.len() <= MAX_NAME_LEN
        && edge(b[0])
        && edge(b[b.len() - 1])
        && b.iter().all(|&c| edge(c) || c == b'-')
}

/// Default site name for an upload directory: the directory's basename, or
/// its parent's when the basename is a build-output name (`dist`, `build`,
/// ...), slugified. `None` when nothing usable remains. Pass an absolute
/// (canonicalized) path so `.` / `..` resolve to real names.
pub fn default_name(dir: &Path) -> Option<String> {
    let base = dir.file_name()?.to_str()?;
    let chosen = if BUILD_DIR_NAMES.contains(&base.to_ascii_lowercase().as_str()) {
        dir.parent()
            .and_then(Path::file_name)
            .and_then(|s| s.to_str())
            .unwrap_or(base)
    } else {
        base
    };
    let name = slugify(chosen);
    valid_name(&name).then_some(name)
}

/// Lowercase, non-alnum runs -> `-`, trim dashes, cut to [`MAX_NAME_LEN`].
fn slugify(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.is_empty() && !out.ends_with('-') {
            out.push('-');
        }
    }
    out.truncate(MAX_NAME_LEN);
    out.trim_end_matches('-').to_string()
}

/// A siteId, or a site URL `http://<siteId>.localhost[:port][/...]`, to the
/// siteId.
pub fn parse_site_ref(s: &str) -> Option<String> {
    let s = s.trim();
    if is_site_id(s) {
        return Some(s.to_string());
    }
    let rest = s.strip_prefix("http://")?;
    let authority = rest.split(['/', '?', '#']).next()?.to_ascii_lowercase();
    let host = match authority.split_once(':') {
        Some((host, port)) => {
            if port.is_empty() || port.len() > 5 || !port.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            host
        }
        None => authority.as_str(),
    };
    let id = host.strip_suffix(".localhost")?;
    is_site_id(id).then(|| id.to_string())
}

// ---- Paths ----

/// Manifest path rule, shared by upload and the gateway: UTF-8, ≤ 512 bytes,
/// `/`-separated and relative, no empty / `.` / `..` segments, no `\`,
/// control chars, `?`, `#`, `%`, and not under the reserved `.mistl/`.
pub fn validate_path(p: &str) -> Result<()> {
    if p.is_empty() {
        bail!("empty path");
    }
    if p.len() > MAX_PATH_BYTES {
        bail!("path longer than {MAX_PATH_BYTES} bytes");
    }
    if p.starts_with('/') {
        bail!("path must be relative");
    }
    if let Some(c) = p
        .chars()
        .find(|&c| c.is_control() || matches!(c, '\\' | '?' | '#' | '%'))
    {
        bail!("path contains a forbidden character {c:?}");
    }
    for (i, seg) in p.split('/').enumerate() {
        match seg {
            "" => bail!("path has an empty segment"),
            "." | ".." => bail!("path has a `{seg}` segment"),
            RESERVED_SEGMENT if i == 0 => bail!("path is under the reserved `.mistl/`"),
            _ => {}
        }
    }
    Ok(())
}

/// Request target path -> manifest file path. Strips query/fragment,
/// percent-decodes (strict UTF-8), strips the leading `/`. Returns `""` for
/// the site root (caller maps it to the entry); `x/` becomes `x/index.html`.
/// The result then passes [`validate_path`].
pub fn request_path_to_file(raw_path: &str) -> Result<String> {
    let path = raw_path.split(['?', '#']).next().unwrap_or("");
    let decoded = percent_decode(path)?;
    let mut p = decoded.strip_prefix('/').unwrap_or(&decoded).to_string();
    if p.is_empty() {
        return Ok(p);
    }
    if p.ends_with('/') {
        p.push_str("index.html");
    }
    validate_path(&p)?;
    Ok(p)
}

fn percent_decode(s: &str) -> Result<String> {
    let src = s.as_bytes();
    let mut out = Vec::with_capacity(src.len());
    let mut i = 0;
    while i < src.len() {
        if src[i] == b'%' {
            let hex = src
                .get(i + 1..i + 3)
                .filter(|h| h.iter().all(u8::is_ascii_hexdigit))
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| anyhow!("malformed percent-encoding"))?;
            out.push(hex);
            i += 3;
        } else {
            out.push(src[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| anyhow!("path is not valid UTF-8"))
}

// ---- MIME ----

/// How a file is served: `Content-Type` and optional `Content-Encoding`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Served {
    pub content_type: &'static str,
    pub encoding: Option<&'static str>,
}

/// Lowercased extension of the last path segment (a leading dot does not
/// start an extension: `.htaccess` has none).
fn extension(path: &str) -> Option<String> {
    let last = path.rsplit('/').next().unwrap_or(path);
    match last.rfind('.') {
        Some(i) if i > 0 && i + 1 < last.len() => Some(last[i + 1..].to_ascii_lowercase()),
        _ => None,
    }
}

/// Whether the last segment has an extension (SPA fallback applies only to
/// extension-less paths).
pub fn has_extension(path: &str) -> bool {
    extension(path).is_some()
}

fn content_type_for(ext: Option<&str>) -> &'static str {
    match ext.unwrap_or("") {
        "html" | "htm" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "webmanifest" => "application/manifest+json; charset=utf-8",
        "svg" => "image/svg+xml; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "xml" => "application/xml; charset=utf-8",
        "wasm" => "application/wasm",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "ico" => "image/x-icon",
        "bmp" => "image/bmp",
        "mp3" => "audio/mpeg",
        "ogg" => "audio/ogg",
        "wav" => "audio/wav",
        "m4a" => "audio/mp4",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "glb" => "model/gltf-binary",
        "gltf" => "model/gltf+json",
        // data / pck / unityweb / bin and anything unknown.
        _ => "application/octet-stream",
    }
}

/// Content type (and encoding for precompressed `*.br` / `*.gz`, typed by
/// the inner extension) for a manifest path.
pub fn served_type(path: &str, precompressed: bool) -> Served {
    let ext = extension(path);
    if precompressed {
        let encoding = match ext.as_deref() {
            Some("br") => Some("br"),
            Some("gz") => Some("gzip"),
            _ => None,
        };
        if let Some(encoding) = encoding {
            let inner = &path[..path.len() - ext.as_deref().map_or(0, |e| e.len() + 1)];
            return Served {
                content_type: content_type_for(extension(inner).as_deref()),
                encoding: Some(encoding),
            };
        }
    }
    Served {
        content_type: content_type_for(ext.as_deref()),
        encoding: None,
    }
}

// ---- Manifest ----

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileEntry {
    pub p: String,
    pub cid: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub v: u32,
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub entry: String,
    pub spa: bool,
    pub coi: bool,
    pub precompressed: bool,
    pub files: Vec<FileEntry>,
}

/// Non-empty, ≤ 128 chars, ASCII alphanumeric (CIDv1 base32 / CIDv0 base58).
pub fn sane_cid(cid: &str) -> bool {
    !cid.is_empty() && cid.len() <= MAX_CID_LEN && cid.bytes().all(|b| b.is_ascii_alphanumeric())
}

impl Manifest {
    /// Builds a v1 manifest with `files` sorted by path. Not validated: call
    /// [`Manifest::validate`] before storing it.
    pub fn new(
        name: &str,
        entry: &str,
        spa: bool,
        coi: bool,
        precompressed: bool,
        mut files: Vec<FileEntry>,
    ) -> Self {
        files.sort_by(|a, b| a.p.cmp(&b.p));
        Self {
            v: 1,
            kind: MANIFEST_TYPE.to_string(),
            name: name.to_string(),
            entry: entry.to_string(),
            spa,
            coi,
            precompressed,
            files,
        }
    }

    /// Compact JSON, as stored in the content store.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("manifest always serializes")
    }

    /// Parses and validates untrusted manifest bytes. `max_total` is the
    /// install budget (`[web] max_site_bytes`); [`MAX_SITE_BYTES`] always
    /// applies too.
    pub fn parse(bytes: &[u8], max_total: u64) -> Result<Self> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            bail!("manifest larger than {MAX_MANIFEST_BYTES} bytes");
        }
        let m: Self =
            serde_json::from_slice(bytes).map_err(|e| anyhow!("invalid manifest: {e}"))?;
        m.validate(max_total)?;
        Ok(m)
    }

    /// Every manifest rule except the serialized byte cap.
    pub fn validate(&self, max_total: u64) -> Result<()> {
        if self.v != 1 {
            bail!("unsupported manifest version {}", self.v);
        }
        if self.kind != MANIFEST_TYPE {
            bail!("not a {MANIFEST_TYPE}");
        }
        if !valid_name(&self.name) {
            bail!("invalid site name");
        }
        if self.files.is_empty() || self.files.len() > MAX_FILES {
            bail!("manifest must list 1..={MAX_FILES} files");
        }
        let cap = max_total.min(MAX_SITE_BYTES);
        let mut total: u64 = 0;
        for (i, f) in self.files.iter().enumerate() {
            if i > 0 && self.files[i - 1].p >= f.p {
                bail!("manifest files are not sorted and unique at {:?}", f.p);
            }
            validate_path(&f.p).map_err(|e| anyhow!("bad path {:?}: {e}", f.p))?;
            if f.size > MAX_FILE_BYTES {
                bail!("file {:?} larger than {MAX_FILE_BYTES} bytes", f.p);
            }
            if !sane_cid(&f.cid) {
                bail!("file {:?} has an invalid cid", f.p);
            }
            total = total.saturating_add(f.size);
        }
        if total > cap {
            bail!("site size {total} exceeds the limit {cap}");
        }
        if self.find(&self.entry).is_none() {
            bail!("entry {:?} is not in the manifest", self.entry);
        }
        Ok(())
    }

    pub fn total_size(&self) -> u64 {
        self.files
            .iter()
            .map(|f| f.size)
            .fold(0, u64::saturating_add)
    }

    /// Exact lookup (files are sorted by byte order).
    pub fn find(&self, p: &str) -> Option<&FileEntry> {
        self.files
            .binary_search_by(|f| f.p.as_str().cmp(p))
            .ok()
            .map(|i| &self.files[i])
    }

    /// Gateway resolution of a path from [`request_path_to_file`]: `""` ->
    /// entry; exact match; with `spa`, extension-less paths -> entry.
    pub fn resolve(&self, file_path: &str) -> Option<&FileEntry> {
        if file_path.is_empty() {
            return self.find(&self.entry);
        }
        if let Some(f) = self.find(file_path) {
            return Some(f);
        }
        if self.spa && !has_extension(file_path) {
            return self.find(&self.entry);
        }
        None
    }
}

// ---- Site record ----

/// A verified site record. `raw` is the exact signed JSON, for storing and
/// relaying unchanged.
#[derive(Debug, Clone, PartialEq)]
pub struct SiteRecord {
    pub did: String,
    pub site: String,
    pub name: String,
    pub seq: u64,
    pub manifest: String,
    pub size: u64,
    pub files: u32,
    pub at: u64,
    pub raw: Value,
}

/// Builds and signs a site record for `identity`'s site `name`.
pub fn build_record(
    identity: &Identity,
    name: &str,
    manifest_cid: &str,
    size: u64,
    files: u32,
    seq: u64,
    at: u64,
) -> Result<Value> {
    if !valid_name(name) {
        bail!("invalid site name {name:?}");
    }
    if !sane_cid(manifest_cid) {
        bail!("invalid manifest cid");
    }
    if seq == 0 {
        bail!("seq must be > 0");
    }
    if size > MAX_SITE_BYTES || files == 0 || files as usize > MAX_FILES {
        bail!("site size/files out of range");
    }
    let mut rec = json!({
        "v": 1,
        "type": RECORD_TYPE,
        "fromId": identity.did(),
        "site": derive_site_id(identity.did(), name),
        "name": name,
        "seq": seq,
        "manifest": manifest_cid,
        "size": size,
        "files": files,
        "at": at,
    });
    crate::wiresign::sign_wire(&mut rec, identity)?;
    Ok(rec)
}

/// Verifies an untrusted site record (from any peer: it authenticates
/// itself). Cheap shape checks run before the signature check.
pub fn verify_record(v: &Value, now_ms: u64) -> Result<SiteRecord> {
    verify_record_at(v, Some(now_ms))
}

/// Re-verifies a record loaded from local state. Same checks as
/// [`verify_record`] except the future-`at` skew check: that already passed
/// when the record arrived, and a clock that moved back since must not drop
/// installed sites.
pub fn verify_record_stored(v: &Value) -> Result<SiteRecord> {
    verify_record_at(v, None)
}

fn verify_record_at(v: &Value, now_ms: Option<u64>) -> Result<SiteRecord> {
    let map = v
        .as_object()
        .ok_or_else(|| anyhow!("record is not an object"))?;
    if map.len() != RECORD_KEYS.len() || !RECORD_KEYS.iter().all(|k| map.contains_key(*k)) {
        bail!("record has an unexpected key set");
    }
    if serde_json::to_vec(v)?.len() > MAX_RECORD_BYTES {
        bail!("record larger than {MAX_RECORD_BYTES} bytes");
    }
    let str_field = |k: &str| {
        map.get(k)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("record field `{k}` must be a string"))
    };
    let u64_field = |k: &str| {
        map.get(k)
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("record field `{k}` must be a non-negative integer"))
    };
    if u64_field("v")? != 1 {
        bail!("unsupported record version");
    }
    if str_field("type")? != RECORD_TYPE {
        bail!("not a {RECORD_TYPE}");
    }
    let did = str_field("fromId")?;
    let site = str_field("site")?;
    let name = str_field("name")?;
    let manifest = str_field("manifest")?;
    str_field("signature")?;
    let seq = u64_field("seq")?;
    let size = u64_field("size")?;
    let files = u64_field("files")?;
    let at = u64_field("at")?;
    if !valid_name(name) {
        bail!("invalid site name");
    }
    if site != derive_site_id(did, name) {
        bail!("site id does not match publisher and name");
    }
    if seq == 0 {
        bail!("seq must be > 0");
    }
    if let Some(now_ms) = now_ms
        && at > now_ms.saturating_add(MAX_FUTURE_SKEW_MS)
    {
        bail!("record is dated in the future");
    }
    if size > MAX_SITE_BYTES {
        bail!("site too large");
    }
    if files == 0 || files > MAX_FILES as u64 {
        bail!("file count out of range");
    }
    if !sane_cid(manifest) {
        bail!("invalid manifest cid");
    }
    if !crate::wiresign::verify_wire(v)? {
        bail!("invalid record signature");
    }
    Ok(SiteRecord {
        did: did.to_string(),
        site: site.to_string(),
        name: name.to_string(),
        seq,
        manifest: manifest.to_string(),
        size,
        files: files as u32,
        at,
        raw: v.clone(),
    })
}

/// Whether `candidate` supersedes `current`: same site, strictly higher seq.
/// Equal seq keeps the stored record even if the manifest differs.
pub fn is_newer(candidate: &SiteRecord, current: &SiteRecord) -> bool {
    candidate.site == current.site && candidate.seq > current.seq
}

/// Publisher's next seq: `max(now_ms, known + 1)`, never 0.
pub fn next_seq(now_ms: u64, known: Option<u64>) -> u64 {
    now_ms.max(known.map_or(0, |k| k.saturating_add(1))).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const NOW: u64 = 1_764_300_000_000;

    #[test]
    fn site_id_is_deterministic_32_chars_base32() {
        let did = crate::identity::did_for_seed(1);
        let a = derive_site_id(&did, "my-game");
        assert_eq!(a, derive_site_id(&did, "my-game"));
        assert_eq!(a.len(), SITE_ID_LEN);
        assert!(is_site_id(&a));
        assert_ne!(a, derive_site_id(&did, "my-game2"));
        assert_ne!(
            a,
            derive_site_id(&crate::identity::did_for_seed(2), "my-game")
        );
        // The NUL separator keeps (did, name) boundaries unambiguous.
        assert_ne!(derive_site_id("ab", "c"), derive_site_id("a", "bc"));
    }

    #[test]
    fn base32_matches_rfc4648_vectors() {
        assert_eq!(base32_lower(b""), "");
        assert_eq!(base32_lower(b"f"), "my");
        assert_eq!(base32_lower(b"fo"), "mzxq");
        assert_eq!(base32_lower(b"foo"), "mzxw6");
        assert_eq!(base32_lower(b"foob"), "mzxw6yq");
        assert_eq!(base32_lower(b"fooba"), "mzxw6ytb");
        assert_eq!(base32_lower(b"foobar"), "mzxw6ytboi");
    }

    #[test]
    fn is_site_id_rejects_bad_shapes() {
        assert!(is_site_id(&"a".repeat(32)));
        assert!(!is_site_id(&"a".repeat(31)));
        assert!(!is_site_id(&"a".repeat(33)));
        assert!(!is_site_id(&format!("{}1", "a".repeat(31))));
        assert!(!is_site_id(&format!("{}A", "a".repeat(31))));
    }

    #[test]
    fn name_validation() {
        let (max, over) = ("a".repeat(48), "a".repeat(49));
        for ok in ["a", "0", "my-game", "a-b-c", max.as_str()] {
            assert!(valid_name(ok), "{ok}");
        }
        for bad in ["", "-a", "a-", "My", "a_b", "a.b", "日本", over.as_str()] {
            assert!(!valid_name(bad), "{bad}");
        }
    }

    #[test]
    fn default_name_rules() {
        let p = |s: &str| PathBuf::from(s);
        assert_eq!(
            default_name(&p("./my-proj/dist")).as_deref(),
            Some("my-proj")
        );
        assert_eq!(default_name(&p("/x/My Game!")).as_deref(), Some("my-game"));
        assert_eq!(
            default_name(&p("/x/Cool__Proj/Build")).as_deref(),
            Some("cool-proj")
        );
        assert_eq!(default_name(&p("/x/game")).as_deref(), Some("game"));
        assert_eq!(default_name(&p("/x/日本語")), None);
        assert_eq!(default_name(&p("/x/---")), None);
        let long = format!("/x/{}-tail", "a".repeat(47));
        assert_eq!(
            default_name(&p(&long)).as_deref(),
            Some("a".repeat(47).as_str())
        );
    }

    #[test]
    fn parse_site_ref_accepts_id_and_url() {
        let id = derive_site_id(&crate::identity::did_for_seed(1), "g");
        assert_eq!(parse_site_ref(&id).as_deref(), Some(id.as_str()));
        for url in [
            format!("http://{id}.localhost:6490/"),
            format!("http://{id}.localhost:6490/a/b?x=1"),
            format!("http://{id}.localhost"),
            format!("  http://{}.LOCALHOST:6490  ", id.to_uppercase()),
        ] {
            assert_eq!(parse_site_ref(&url).as_deref(), Some(id.as_str()), "{url}");
        }
        for bad in [
            format!("https://{id}.localhost:6490/"),
            format!("http://{id}.example.com/"),
            format!("http://{id}.localhost:/"),
            format!("http://{id}.localhost:65a/"),
            format!("http://x.{id}.localhost/"),
            "not-a-site".to_string(),
        ] {
            assert_eq!(parse_site_ref(&bad), None, "{bad}");
        }
    }

    #[test]
    fn validate_path_table() {
        let (max, over) = ("a".repeat(512), "a".repeat(513));
        for ok in [
            "index.html",
            "Build/game.data.br",
            ".well-known/x",
            "a/.mistl/x",
            "日本/ファイル.png",
            max.as_str(),
        ] {
            assert!(validate_path(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "/a",
            "a/",
            "a//b",
            "../x",
            "a/./b",
            "a/..",
            ".",
            "a\\b",
            "a?b",
            "a#b",
            "a%20b",
            "a\nb",
            "a\u{7f}b",
            ".mistl",
            ".mistl/allow",
            over.as_str(),
        ] {
            assert!(validate_path(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn request_path_mapping() {
        let r = |s: &str| request_path_to_file(s);
        assert_eq!(r("/").unwrap(), "");
        assert_eq!(r("").unwrap(), "");
        assert_eq!(r("/?x=1").unwrap(), "");
        assert_eq!(r("/index.html").unwrap(), "index.html");
        assert_eq!(r("/x/").unwrap(), "x/index.html");
        assert_eq!(r("/a/b.js?v=3#frag").unwrap(), "a/b.js");
        assert_eq!(r("/my%20file.txt").unwrap(), "my file.txt");
        assert_eq!(r("/%E6%97%A5.png").unwrap(), "日.png");
        for bad in [
            "/%2e%2e/x",
            "/a/%2E/b",
            "/../x",
            "//x",
            "/a//b",
            "/.mistl/allow",
            "/%2emistl/status",
            "/a%2fb%2f%2f",
            "/a%5cb",
            "/a%25b",
            "/a%00b",
            "/a%zz",
            "/a%4",
            "/%ff.png",
        ] {
            assert!(r(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn mime_table() {
        let t = |p: &str, pre: bool| served_type(p, pre);
        assert_eq!(t("game.wasm", true).content_type, "application/wasm");
        assert_eq!(
            t("index.HTML", true).content_type,
            "text/html; charset=utf-8"
        );
        assert_eq!(
            t("a.mjs", false).content_type,
            "text/javascript; charset=utf-8"
        );
        assert_eq!(t("noext", false).content_type, "application/octet-stream");
        assert_eq!(
            t("x.unknown", false).content_type,
            "application/octet-stream"
        );
        assert_eq!(t("x.pck", false).content_type, "application/octet-stream");
        assert_eq!(
            t("Build/x.data.br", true),
            Served {
                content_type: "application/octet-stream",
                encoding: Some("br")
            }
        );
        assert_eq!(
            t("Build/x.js.gz", true),
            Served {
                content_type: "text/javascript; charset=utf-8",
                encoding: Some("gzip")
            }
        );
        assert_eq!(
            t("Build/x.wasm.BR", true),
            Served {
                content_type: "application/wasm",
                encoding: Some("br")
            }
        );
        assert_eq!(
            t("x.js.gz", false),
            Served {
                content_type: "application/octet-stream",
                encoding: None
            }
        );
        assert!(has_extension("a/b.js"));
        assert!(!has_extension("a.b/c"));
        assert!(!has_extension("a/.htaccess"));
        assert!(!has_extension("about"));
    }

    fn entry(p: &str, size: u64) -> FileEntry {
        FileEntry {
            p: p.to_string(),
            cid: format!("bafk{}", p.len()),
            size,
        }
    }

    fn sample(spa: bool) -> Manifest {
        Manifest::new(
            "my-game",
            "index.html",
            spa,
            false,
            true,
            vec![
                entry("index.html", 10),
                entry("Build/a.wasm", 20),
                entry("app.js", 5),
            ],
        )
    }

    #[test]
    fn manifest_round_trip_and_lookup() {
        let m = sample(false);
        assert_eq!(m.files[0].p, "Build/a.wasm"); // byte order: 'B' < 'a'
        let bytes = m.to_bytes();
        let back = Manifest::parse(&bytes, MAX_SITE_BYTES).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.total_size(), 35);
        assert!(
            std::str::from_utf8(&bytes)
                .unwrap()
                .contains(r#""type":"mistl-web-manifest""#)
        );
        assert_eq!(back.find("app.js").unwrap().size, 5);
        assert!(back.find("missing").is_none());
    }

    #[test]
    fn manifest_rejections() {
        let parse = |m: &Manifest| Manifest::parse(&m.to_bytes(), MAX_SITE_BYTES);

        let mut unsorted = sample(false);
        unsorted.files.swap(0, 1);
        assert!(parse(&unsorted).is_err());

        let mut dup = sample(false);
        dup.files.insert(1, dup.files[0].clone());
        assert!(parse(&dup).is_err());

        let mut big_file = sample(false);
        big_file.files[0].size = MAX_FILE_BYTES + 1;
        assert!(parse(&big_file).is_err());

        let m = sample(false);
        assert!(Manifest::parse(&m.to_bytes(), 34).is_err());
        assert!(Manifest::parse(&m.to_bytes(), 35).is_ok());

        let mut no_entry = sample(false);
        no_entry.entry = "main.html".into();
        assert!(parse(&no_entry).is_err());

        let mut bad_path = sample(false);
        bad_path.files[0].p = ".mistl/x".into();
        assert!(parse(&bad_path).is_err());

        let mut bad_cid = sample(false);
        bad_cid.files[0].cid = "baf/k".into();
        assert!(parse(&bad_cid).is_err());

        let mut empty = sample(false);
        empty.files.clear();
        assert!(parse(&empty).is_err());

        let mut v2 = sample(false);
        v2.v = 2;
        assert!(parse(&v2).is_err());

        let mut extra: Value = serde_json::from_slice(&sample(false).to_bytes()).unwrap();
        extra["extra"] = json!(1);
        assert!(Manifest::parse(&serde_json::to_vec(&extra).unwrap(), MAX_SITE_BYTES).is_err());
        let mut extra_file: Value = serde_json::from_slice(&sample(false).to_bytes()).unwrap();
        extra_file["files"][0]["mode"] = json!(1);
        assert!(
            Manifest::parse(&serde_json::to_vec(&extra_file).unwrap(), MAX_SITE_BYTES).is_err()
        );

        let oversized = vec![b' '; MAX_MANIFEST_BYTES + 1];
        assert!(Manifest::parse(&oversized, MAX_SITE_BYTES).is_err());
    }

    #[test]
    fn manifest_resolve_with_and_without_spa() {
        let m = sample(false);
        assert_eq!(m.resolve("").unwrap().p, "index.html");
        assert_eq!(m.resolve("app.js").unwrap().p, "app.js");
        assert!(m.resolve("about").is_none());
        assert!(m.resolve("missing.js").is_none());

        let spa = sample(true);
        assert_eq!(spa.resolve("about").unwrap().p, "index.html");
        assert_eq!(spa.resolve("users/42").unwrap().p, "index.html");
        assert!(spa.resolve("missing.js").is_none());
    }

    fn signed(identity: &Identity) -> Value {
        build_record(identity, "my-game", "bafkreiabc", 1234, 3, NOW, NOW).unwrap()
    }

    #[test]
    fn record_build_then_verify() {
        let id = crate::identity::for_test();
        let raw = signed(&id);
        let rec = verify_record(&raw, NOW).unwrap();
        assert_eq!(rec.did, id.did());
        assert_eq!(rec.site, derive_site_id(id.did(), "my-game"));
        assert_eq!((rec.seq, rec.size, rec.files, rec.at), (NOW, 1234, 3, NOW));
        assert_eq!(rec.manifest, "bafkreiabc");
        assert_eq!(rec.raw, raw);
    }

    #[test]
    fn record_rejections() {
        let id = crate::identity::for_test();
        let raw = signed(&id);

        let mut tampered = raw.clone();
        tampered["manifest"] = json!("bafkreievil");
        assert!(verify_record(&tampered, NOW).is_err());

        let mut wrong_site = raw.clone();
        wrong_site["site"] = json!(derive_site_id(id.did(), "other"));
        assert!(verify_record(&wrong_site, NOW).is_err());

        // A correctly signed record whose site does not match fromId+name.
        let mut forged = raw.clone();
        forged.as_object_mut().unwrap().remove("signature");
        forged["site"] = json!(derive_site_id(id.did(), "other"));
        crate::wiresign::sign_wire(&mut forged, &id).unwrap();
        assert!(verify_record(&forged, NOW).is_err());

        let mut extra = raw.clone();
        extra.as_object_mut().unwrap().remove("signature");
        extra["note"] = json!("x");
        crate::wiresign::sign_wire(&mut extra, &id).unwrap();
        assert!(verify_record(&extra, NOW).is_err());

        let mut missing = raw.clone();
        missing.as_object_mut().unwrap().remove("at");
        assert!(verify_record(&missing, NOW).is_err());

        let future = build_record(
            &id,
            "my-game",
            "bafk",
            1,
            1,
            1,
            NOW + MAX_FUTURE_SKEW_MS + 1,
        )
        .unwrap();
        assert!(verify_record(&future, NOW).is_err());
        // Stored records skip only the skew check.
        assert!(verify_record_stored(&future).is_ok());
        let mut tampered_future = future.clone();
        tampered_future["size"] = json!(2);
        assert!(verify_record_stored(&tampered_future).is_err());
        let near = build_record(&id, "my-game", "bafk", 1, 1, 1, NOW + MAX_FUTURE_SKEW_MS).unwrap();
        assert!(verify_record(&near, NOW).is_ok());

        // Serialized size over 4 KiB.
        let mut oversized = raw.clone();
        oversized["manifest"] = json!("a".repeat(MAX_RECORD_BYTES));
        assert!(verify_record(&oversized, NOW).is_err());

        let mut float_seq = raw.clone();
        float_seq["seq"] = json!(1.5);
        assert!(verify_record(&float_seq, NOW).is_err());

        assert!(build_record(&id, "my-game", "bafk", 1, 1, 0, NOW).is_err());
        assert!(build_record(&id, "my-game", "bafk", MAX_SITE_BYTES + 1, 1, 1, NOW).is_err());
        assert!(build_record(&id, "my-game", "bafk", 1, 0, 1, NOW).is_err());
        assert!(build_record(&id, "Bad Name", "bafk", 1, 1, 1, NOW).is_err());
    }

    #[test]
    fn newer_and_next_seq() {
        let id = crate::identity::for_test();
        let a = verify_record(&signed(&id), NOW).unwrap();
        let b = verify_record(
            &build_record(&id, "my-game", "bafknew", 1, 1, NOW + 1, NOW).unwrap(),
            NOW,
        )
        .unwrap();
        assert!(is_newer(&b, &a));
        assert!(!is_newer(&a, &b));
        assert!(!is_newer(&a, &a));
        let other = verify_record(
            &build_record(&id, "other", "bafk", 1, 1, NOW + 5, NOW).unwrap(),
            NOW,
        )
        .unwrap();
        assert!(!is_newer(&other, &a));

        assert_eq!(next_seq(NOW, None), NOW);
        assert_eq!(next_seq(NOW, Some(NOW - 5)), NOW);
        assert_eq!(next_seq(NOW, Some(NOW)), NOW + 1);
        assert_eq!(next_seq(NOW, Some(NOW + 100)), NOW + 101);
        assert_eq!(next_seq(0, None), 1);
        assert_eq!(next_seq(0, Some(u64::MAX)), u64::MAX);
    }
}
