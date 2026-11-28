//! `web.publish` / `mistl upload <dir>`: store a static folder as a site.
//!
//! A first pass only stats (so limits are enforced before anything is
//! stored); the second reads and stores one file at a time. Files are held
//! in memory whole while stored (`Store::put_pinned_claimed` takes a `Vec<u8>`), so
//! peak memory is about the largest file (≤ 512 MiB).

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use super::host::WebHost;
use super::site::{self, FileEntry, Manifest};
use super::state::Upsert;
use super::wire;
use crate::identity::Identity;

pub const MANIFEST_NAME: &str = "mistl-web-manifest.json";
const DEFAULT_ENTRY: &str = "index.html";

#[derive(Debug, Clone, Default)]
pub struct PublishOptions {
    pub name: Option<String>,
    pub entry: Option<String>,
    pub spa: bool,
    /// `None` = auto (on when a `.pck` and a `.wasm` are present).
    pub coi: Option<bool>,
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedFile {
    /// Manifest path (`/`-separated, validated).
    pub rel: String,
    pub abs: PathBuf,
    pub size: u64,
}

#[derive(Debug, Default)]
pub struct Scan {
    /// Sorted by `rel` (byte order).
    pub files: Vec<ScannedFile>,
    /// Skipped entries with the reason (symlinks, special files).
    pub skipped: Vec<String>,
    pub total: u64,
    /// JSON bytes occupied by file entries and their separating commas.
    manifest_files_bytes: usize,
}

/// Walk `dir` without following symlinks. Dotfiles / dot-dirs are skipped
/// except `.well-known`. Enforces the file count, per-file and `max_total`
/// limits and the shared path rule, failing with the offending path.
pub fn scan(dir: &Path, max_total: u64) -> Result<Scan> {
    let meta =
        std::fs::symlink_metadata(dir).with_context(|| format!("reading {}", dir.display()))?;
    if !meta.is_dir() {
        bail!("{} is not a directory", dir.display());
    }
    let cap = max_total.min(site::MAX_SITE_BYTES);
    let mut scan = Scan::default();
    // All stored file roots have the same encoded CID length as this one.
    let sample_cid = crate::storage::file_root_cid("", std::iter::empty());
    // (absolute dir, relative prefix)
    let mut stack = vec![(dir.to_path_buf(), String::new())];
    while let Some((abs_dir, prefix)) = stack.pop() {
        let mut entries = Vec::new();
        for entry in
            std::fs::read_dir(&abs_dir).with_context(|| format!("reading {}", abs_dir.display()))?
        {
            let entry = entry.with_context(|| format!("reading {}", abs_dir.display()))?;
            entries.push(entry);
        }
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let os_name = entry.file_name();
            let Some(name) = os_name.to_str() else {
                bail!(
                    "cannot upload {}: file name is not valid UTF-8",
                    entry.path().display()
                );
            };
            if name.starts_with('.') && name != ".well-known" {
                continue;
            }
            let rel = if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{prefix}/{name}")
            };
            // `file_type` does not follow symlinks (junctions count as
            // symlinks on Windows).
            let file_type = entry
                .file_type()
                .with_context(|| format!("reading {}", entry.path().display()))?;
            if file_type.is_symlink() {
                scan.skipped.push(format!("{rel} (symlink)"));
                continue;
            }
            if file_type.is_dir() {
                stack.push((entry.path(), rel));
                continue;
            }
            if !file_type.is_file() {
                scan.skipped.push(format!("{rel} (not a regular file)"));
                continue;
            }
            site::validate_path(&rel).map_err(|e| anyhow::anyhow!("cannot upload {rel:?}: {e}"))?;
            let size = entry
                .metadata()
                .with_context(|| format!("reading {}", entry.path().display()))?
                .len();
            if size > site::MAX_FILE_BYTES {
                bail!(
                    "cannot upload {rel:?}: {size} bytes is over the per-file limit of {} bytes",
                    site::MAX_FILE_BYTES
                );
            }
            scan.total = scan.total.saturating_add(size);
            if scan.total > cap {
                bail!("the site is over the size limit of {cap} bytes (at {rel:?})");
            }
            if scan.files.len() >= site::MAX_FILES {
                bail!("the site has more than {} files", site::MAX_FILES);
            }
            scan.manifest_files_bytes += serde_json::to_vec(&FileEntry {
                p: rel.clone(),
                cid: sample_cid.clone(),
                size,
            })?
            .len()
                + usize::from(!scan.files.is_empty());
            check_manifest_size(scan.manifest_files_bytes)?;
            scan.files.push(ScannedFile {
                rel,
                abs: entry.path(),
                size,
            });
        }
    }
    if scan.files.is_empty() {
        bail!("{} has no files to upload", dir.display());
    }
    scan.files.sort_by(|a, b| a.rel.cmp(&b.rel));
    Ok(scan)
}

fn check_manifest_size(bytes: usize) -> Result<()> {
    if bytes > site::MAX_MANIFEST_BYTES {
        bail!(
            "the site manifest would be over {} bytes; use fewer files or shorter paths",
            site::MAX_MANIFEST_BYTES
        );
    }
    Ok(())
}

/// Auto COI: Godot web exports (a `.pck` next to a `.wasm`) need
/// SharedArrayBuffer.
pub fn auto_coi(files: &[ScannedFile]) -> bool {
    let has = |ext: &str| {
        files.iter().any(|f| {
            let p = f.rel.to_ascii_lowercase();
            let p = p
                .strip_suffix(".br")
                .or_else(|| p.strip_suffix(".gz"))
                .unwrap_or(&p);
            p.ends_with(ext)
        })
    };
    has(".pck") && has(".wasm")
}

/// The site name: `--name`, else derived from the directory.
pub fn choose_name(dir: &Path, name: Option<&str>) -> Result<String> {
    let name = match name {
        Some(name) => name.trim().to_string(),
        None => site::default_name(dir).with_context(|| {
            format!(
                "cannot derive a site name from {}; pass --name",
                dir.display()
            )
        })?,
    };
    if !site::valid_name(&name) {
        bail!(
            "invalid site name {name:?}: use 1-48 lowercase letters, digits and `-` (not at the ends)"
        );
    }
    Ok(name)
}

pub async fn publish(
    host: &Arc<WebHost>,
    identity: &Identity,
    dir: &Path,
    opts: PublishOptions,
) -> Result<Value> {
    if !dir.is_absolute() {
        bail!("path must be absolute");
    }
    let max_total = host.app.config().web.max_site_bytes;
    let name = choose_name(dir, opts.name.as_deref())?;
    let scan = {
        let dir = dir.to_path_buf();
        tokio::task::spawn_blocking(move || scan(&dir, max_total))
            .await
            .context("scanning the directory")??
    };
    let entry = opts
        .entry
        .as_deref()
        .unwrap_or(DEFAULT_ENTRY)
        .trim()
        .to_string();
    site::validate_path(&entry).map_err(|e| anyhow::anyhow!("invalid entry {entry:?}: {e}"))?;
    if scan
        .files
        .binary_search_by(|f| f.rel.as_str().cmp(&entry))
        .is_err()
    {
        bail!(
            "entry {entry:?} not found in {}; pass --entry",
            dir.display()
        );
    }
    let coi = opts.coi.unwrap_or_else(|| auto_coi(&scan.files));
    let envelope = Manifest::new(&name, &entry, opts.spa, coi, true, Vec::new());
    check_manifest_size(envelope.to_bytes().len() + scan.manifest_files_bytes)?;
    let site_id = site::derive_site_id(identity.did(), &name);
    let url = host.site_url(&site_id);
    let base = json!({
        "site": site_id,
        "name": name,
        "url": url,
        "did": identity.did(),
        "entry": entry,
        "spa": opts.spa,
        "coi": coi,
        "files": scan.files.len(),
        "size": scan.total,
        "skipped": &scan.skipped,
    });
    if opts.dry_run {
        let mut out = base;
        out["dry_run"] = json!(true);
        return Ok(out);
    }

    // Store files one at a time, each pin protected by a claim until the
    // new version is in the ready cache.
    let token = host.claim(std::iter::empty());
    let outcome = match store_files(host, &scan, token).await {
        Ok(entries) => finish(host, identity, &name, &entry, opts.spa, coi, entries, token).await,
        Err(error) => Err(error),
    };
    host.drop_claim(token);
    host.reconcile().await;
    let (record, manifest_cid) = outcome?;
    let announced = host.broadcast(wire::record(&record.raw)).await;
    let mut out = base;
    out["seq"] = json!(record.seq);
    out["manifest"] = json!(manifest_cid);
    out["announced"] = json!(announced);
    Ok(out)
}

/// Predict and register the root before storing it, without holding a lock
/// during the data write.
async fn put_claimed(host: &WebHost, token: u64, name: &str, data: Vec<u8>) -> Result<String> {
    let cid = crate::storage::file_root_cid(name, data.chunks(mistlib_core::storage::CHUNK_SIZE));
    host.claim_extend(token, [cid.clone()]);
    host.track([cid.clone()]).await?;
    host.store().put_pinned_claimed(name, data, &cid).await?;
    Ok(cid)
}

/// Read + store each file, one at a time.
async fn store_files(host: &Arc<WebHost>, scan: &Scan, token: u64) -> Result<Vec<FileEntry>> {
    let mut entries = Vec::with_capacity(scan.files.len());
    for file in &scan.files {
        let data = read_checked(file).await?;
        let cid = put_claimed(host, token, &file.rel, data)
            .await
            .with_context(|| format!("storing {}", file.rel))?;
        entries.push(FileEntry {
            p: file.rel.clone(),
            cid,
            size: file.size,
        });
    }
    Ok(entries)
}

/// Read a scanned file, refusing it if it became a symlink or changed size
/// since the scan.
async fn read_checked(file: &ScannedFile) -> Result<Vec<u8>> {
    let abs = file.abs.clone();
    let rel = file.rel.clone();
    let expected = file.size;
    tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        let meta = std::fs::symlink_metadata(&abs).with_context(|| format!("reading {rel}"))?;
        if !meta.file_type().is_file() {
            bail!("{rel} changed during upload");
        }
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
        }
        let opened = options
            .open(&abs)
            .with_context(|| format!("opening {rel}"))?;
        let meta = opened
            .metadata()
            .with_context(|| format!("reading {rel}"))?;
        if !meta.is_file() || meta.len() != expected {
            bail!("{rel} changed during upload");
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if meta.file_attributes() & 0x400 != 0 {
                // FILE_ATTRIBUTE_REPARSE_POINT
                bail!("{rel} changed during upload");
            }
        }
        let mut data = Vec::with_capacity(expected as usize);
        opened
            .take(expected + 1)
            .read_to_end(&mut data)
            .with_context(|| format!("reading {rel}"))?;
        if data.len() as u64 != expected {
            bail!("{rel} changed during upload");
        }
        Ok(data)
    })
    .await
    .context("reading a file")?
}

/// Store the manifest, sign the record and install it as our own version.
/// The caller reconciles the pins before announcing the record.
#[allow(clippy::too_many_arguments)]
async fn finish(
    host: &Arc<WebHost>,
    identity: &Identity,
    name: &str,
    entry: &str,
    spa: bool,
    coi: bool,
    files: Vec<FileEntry>,
    token: u64,
) -> Result<(site::SiteRecord, String)> {
    let max_total = host.app.config().web.max_site_bytes;
    let manifest = Manifest::new(name, entry, spa, coi, true, files);
    manifest.validate(max_total)?;
    let bytes = manifest.to_bytes();
    check_manifest_size(bytes.len())?;
    let manifest_cid = put_claimed(host, token, MANIFEST_NAME, bytes).await?;

    let now = crate::net::peer_auth::now_ms();
    let site_id = site::derive_site_id(identity.did(), name);
    let known = {
        let st = host.sites.lock().await;
        st.sites.get(&site_id).map(|e| e.record.seq)
    };
    let seq = site::next_seq(now, known);
    let files = u32::try_from(manifest.files.len()).context("too many files")?;
    let raw = site::build_record(
        identity,
        name,
        &manifest_cid,
        manifest.total_size(),
        files,
        seq,
        now,
    )?;
    let record = site::verify_record(&raw, now)?;
    {
        let mut st = host.sites.lock().await;
        match st.upsert_record(record.clone(), now) {
            Upsert::Inserted | Upsert::Updated => {}
            Upsert::Unchanged | Upsert::Rollback => {
                bail!("a newer version of this site is already known; try again")
            }
            Upsert::Full => bail!("too many known sites; remove some with `mistl web rm`"),
        }
    }
    if !host
        .swap_installed(&site_id, seq, &manifest_cid, Arc::new(manifest), true)
        .await?
    {
        bail!(
            "a newer version of this site is already installed, or the site was removed; try again"
        );
    }
    Ok((record, manifest_cid))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Dir(PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn temp_dir() -> Dir {
        let p = std::env::temp_dir().join(format!("mistl-web-{:016x}", rand::random::<u64>()));
        std::fs::create_dir_all(&p).unwrap();
        Dir(p)
    }

    fn write(root: &Path, rel: &str, len: usize) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, vec![b'x'; len]).unwrap();
    }

    #[test]
    fn scan_walks_sorted_and_skips_dotfiles() {
        let d = temp_dir();
        write(&d.0, "index.html", 3);
        write(&d.0, "Build/game.wasm", 5);
        write(&d.0, "Build/game.pck", 1);
        write(&d.0, ".git/config", 1);
        write(&d.0, ".env", 1);
        write(&d.0, "a/.hidden", 1);
        write(&d.0, ".well-known/x.json", 2);
        let s = scan(&d.0, site::MAX_SITE_BYTES).unwrap();
        let rels: Vec<&str> = s.files.iter().map(|f| f.rel.as_str()).collect();
        assert_eq!(
            rels,
            [
                ".well-known/x.json",
                "Build/game.pck",
                "Build/game.wasm",
                "index.html"
            ]
        );
        assert_eq!(s.total, 11);
        assert!(auto_coi(&s.files));
        assert!(!auto_coi(&s.files[3..]));
    }

    #[test]
    fn scan_enforces_limits_and_path_rules() {
        let d = temp_dir();
        write(&d.0, "index.html", 10);
        write(&d.0, "b.bin", 10);
        assert!(
            scan(&d.0, 19)
                .unwrap_err()
                .to_string()
                .contains("size limit")
        );
        assert!(scan(&d.0, 20).is_ok());

        let bad = temp_dir();
        write(&bad.0, "a%20b.html", 1);
        let err = scan(&bad.0, site::MAX_SITE_BYTES).unwrap_err().to_string();
        assert!(err.contains("a%20b.html"), "{err}");

        let empty = temp_dir();
        assert!(scan(&empty.0, site::MAX_SITE_BYTES).is_err());
        assert!(scan(&empty.0.join("missing"), site::MAX_SITE_BYTES).is_err());
    }

    #[test]
    fn scan_estimates_serialized_manifest_size() {
        let d = temp_dir();
        write(&d.0, "index.html", 3);
        write(&d.0, "assets/a.bin", 12);
        let scan = scan(&d.0, site::MAX_SITE_BYTES).unwrap();
        let placeholder = crate::storage::file_root_cid("", std::iter::empty());
        let files = scan
            .files
            .iter()
            .map(|f| FileEntry {
                p: f.rel.clone(),
                cid: placeholder.clone(),
                size: f.size,
            })
            .collect();
        let full = Manifest::new("game", "index.html", false, false, true, files);
        let empty = Manifest::new("game", "index.html", false, false, true, Vec::new());
        assert_eq!(
            full.to_bytes().len(),
            empty.to_bytes().len() + scan.manifest_files_bytes
        );
        assert!(check_manifest_size(site::MAX_MANIFEST_BYTES).is_ok());
        assert!(check_manifest_size(site::MAX_MANIFEST_BYTES + 1).is_err());
    }

    #[tokio::test]
    async fn read_checked_rejects_changed_size() {
        let d = temp_dir();
        write(&d.0, "index.html", 3);
        let scan = scan(&d.0, site::MAX_SITE_BYTES).unwrap();
        let file = &scan.files[0];
        assert_eq!(read_checked(file).await.unwrap(), b"xxx");
        write(&d.0, "index.html", 4);
        assert!(read_checked(file).await.is_err());
        write(&d.0, "index.html", 2);
        assert!(read_checked(file).await.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn scan_skips_symlinks() {
        let d = temp_dir();
        let outside = temp_dir();
        write(&outside.0, "secret.txt", 4);
        write(&d.0, "index.html", 1);
        std::os::unix::fs::symlink(outside.0.join("secret.txt"), d.0.join("leak.txt")).unwrap();
        std::os::unix::fs::symlink(&outside.0, d.0.join("dir")).unwrap();
        let s = scan(&d.0, site::MAX_SITE_BYTES).unwrap();
        assert_eq!(s.files.len(), 1);
        assert_eq!(s.skipped, ["dir (symlink)", "leak.txt (symlink)"]);
    }

    #[test]
    fn name_choice() {
        let p = Path::new("/x/my-proj/dist");
        assert_eq!(choose_name(p, None).unwrap(), "my-proj");
        assert_eq!(choose_name(p, Some("game")).unwrap(), "game");
        assert!(choose_name(p, Some("Bad Name")).is_err());
        let err = choose_name(Path::new("/x/日本"), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--name"), "{err}");
    }
}
