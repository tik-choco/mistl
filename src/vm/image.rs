//! Base image download/verification/cache and qcow2 overlay creation.
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, Response, Url};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{io::AsyncWriteExt, process::Command};

use crate::config::VmConfig;

const IMAGE_LIMIT: u64 = 4 * 1024 * 1024 * 1024;
const SUMS_LIMIT: u64 = 1024 * 1024;
const INDEX_FILE: &str = "index.json";

static INDEX_LOCK: Mutex<()> = Mutex::new(());

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum IndexedHashes {
    One(String),
    Many(BTreeSet<String>),
}

impl IndexedHashes {
    fn iter(&self) -> impl Iterator<Item = &String> {
        let (one, many) = match self {
            Self::One(hash) => (Some(hash), None),
            Self::Many(hashes) => (None, Some(hashes)),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
}

type ImageIndex = BTreeMap<String, IndexedHashes>;

#[derive(Debug, Clone)]
pub struct BaseImage {
    pub path: PathBuf,
    pub sha256: String,
    pub url: String,
}

/// Returns the cached base image for `cfg.image_url`, downloading and
/// verifying it into `<vm_root>/images/<sha256>.img` first if needed.
pub async fn ensure_base_image(cfg: &VmConfig, vm_root: &Path) -> Result<BaseImage> {
    let url = Url::parse(&cfg.image_url).context("invalid VM image URL")?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "VM image URL must use HTTP or HTTPS"
    );
    let pin = cfg.image_sha256.trim();
    ensure!(
        url.scheme() == "https" || !pin.is_empty(),
        "HTTP VM images require a pinned image_sha256"
    );
    let images = vm_root.join("images");
    crate::statefile::create_private_dir(&images).context("creating VM image cache")?;
    let mut client = None;
    let hash = if pin.is_empty() {
        let filename = image_filename(&url)?;
        let http = download_client(false)?;
        let sums_url = url
            .join("SHA256SUMS")
            .context("locating VM image SHA256SUMS")?;
        let sums = match fetch_checksums(&http, sums_url).await {
            Ok(sums) => sums,
            Err(error)
                if error
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(|error| !error.is_redirect()) =>
            {
                if let Some(hash) = cached_for_url(&images, &cfg.image_url) {
                    tracing::warn!("VM image SHA256SUMS unavailable; using cached image");
                    return Ok(base_image(&images, hash, &cfg.image_url));
                }
                return Err(error).context("fetching VM image SHA256SUMS");
            }
            Err(error) => return Err(error),
        };
        client = Some(http);
        checksum_for(&sums, &filename)?
    } else {
        normalize_hash(pin).context("VM image_sha256 must be a 64-digit hexadecimal SHA-256")?
    };

    let image = base_image(&images, hash, &cfg.image_url);
    if !cached_file(&image.path) {
        let client = match client {
            Some(client) => client,
            None => download_client(true)?,
        };
        download_image(&client, url, &image).await?;
    }
    remember_image(&images, &image)?;
    Ok(image)
}

/// Resolve a checkpoint's exact base without consulting the current image settings.
pub(crate) async fn ensure_base_by_hash(
    vm_root: &Path,
    sha256: &str,
    base_url: &str,
    network_permitted: bool,
) -> Result<BaseImage> {
    let hash = normalize_hash(sha256).context("invalid checkpoint base image SHA-256")?;
    let images = vm_root.join("images");
    let image = base_image(&images, hash, base_url);
    if cached_file(&image.path) {
        let cached = image.clone();
        tokio::task::spawn_blocking(move || {
            let file = File::open(&cached.path).context("opening cached VM base image")?;
            let mut file = file.take(IMAGE_LIMIT + 1);
            let mut hasher = Sha256::new();
            let mut buf = vec![0; 1024 * 1024];
            let mut bytes = 0;
            loop {
                let count = file
                    .read(&mut buf)
                    .context("hashing cached VM base image")?;
                if count == 0 {
                    break;
                }
                bytes += count as u64;
                check_size(bytes, IMAGE_LIMIT, "cached VM base image")?;
                hasher.update(&buf[..count]);
            }
            ensure!(
                hex::encode(hasher.finalize()) == cached.sha256,
                "cached VM base image SHA-256 mismatch"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    } else {
        ensure!(
            !base_url.is_empty(),
            "checkpoint base image is missing and has no download URL"
        );
        ensure!(
            network_permitted,
            "checkpoint base image is missing and the network is blocked"
        );
        let url = Url::parse(base_url).context("invalid checkpoint base image URL")?;
        ensure!(
            matches!(url.scheme(), "http" | "https"),
            "checkpoint base image URL must use HTTP or HTTPS"
        );
        crate::statefile::create_private_dir(&images).context("creating VM image cache")?;
        download_image(&download_client(true)?, url, &image).await?;
        remember_image(&images, &image)?;
    }
    Ok(image)
}

fn download_client(pinned: bool) -> Result<Client> {
    let redirects = reqwest::redirect::Policy::custom(move |attempt| {
        if !pinned && attempt.url().scheme() != "https" {
            attempt.error("HTTP redirects for VM downloads require a pinned image_sha256")
        } else {
            reqwest::redirect::Policy::default().redirect(attempt)
        }
    });
    Client::builder()
        .no_proxy()
        .redirect(redirects)
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(60 * 60))
        .build()
        .context("building VM image HTTP client")
}

fn image_filename(url: &Url) -> Result<String> {
    let segment = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|segment| !segment.is_empty())
        .context("VM image URL must end with a filename")?;
    let mut decoded = Vec::new();
    let mut bytes = segment.as_bytes();
    while let Some((&byte, rest)) = bytes.split_first() {
        bytes = rest;
        if byte == b'%' {
            let mut decoded_byte = [0];
            let digits = bytes
                .get(..2)
                .context("invalid escape in VM image filename")?;
            hex::decode_to_slice(digits, &mut decoded_byte)
                .context("invalid escape in VM image filename")?;
            decoded.push(decoded_byte[0]);
            bytes = &bytes[2..];
        } else {
            decoded.push(byte);
        }
    }
    String::from_utf8(decoded).context("VM image filename is not UTF-8")
}

fn normalize_hash(hash: &str) -> Option<String> {
    (hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| hash.to_ascii_lowercase())
}

fn checksum_for(sums: &str, filename: &str) -> Result<String> {
    let mut expected = None;
    for line in sums.lines() {
        let Some((hash, name)) = line.split_once(' ') else {
            continue;
        };
        let Some(name) = name.strip_prefix(['*', ' ']) else {
            continue;
        };
        if name != filename {
            continue;
        }
        let hash = normalize_hash(hash).context("invalid SHA-256 for VM image in SHA256SUMS")?;
        ensure!(
            expected.as_ref().is_none_or(|previous| previous == &hash),
            "conflicting VM image checksums in SHA256SUMS"
        );
        expected = Some(hash);
    }
    expected.context("VM image filename missing from SHA256SUMS")
}

async fn fetch_checksums(client: &Client, url: Url) -> Result<String> {
    let mut response = client
        .get(url)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .and_then(Response::error_for_status)
        .map_err(reqwest::Error::without_url)?;
    check_size(
        response.content_length().unwrap_or(0),
        SUMS_LIMIT,
        "VM image SHA256SUMS",
    )?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        check_size(
            bytes.len() as u64 + chunk.len() as u64,
            SUMS_LIMIT,
            "VM image SHA256SUMS",
        )?;
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).context("VM image SHA256SUMS is not UTF-8")
}

fn base_image(images: &Path, hash: String, url: &str) -> BaseImage {
    BaseImage {
        path: images.join(format!("{hash}.img")),
        sha256: hash,
        url: url.to_owned(),
    }
}

fn cached_file(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file())
}

fn read_index(images: &Path) -> ImageIndex {
    crate::statefile::read(images, INDEX_FILE).unwrap_or_else(|error| {
        tracing::warn!("reading VM image cache index: {error:#}");
        ImageIndex::new()
    })
}

fn cached_for_url(images: &Path, url: &str) -> Option<String> {
    let index = read_index(images);
    let mut matches = index
        .get(url)?
        .iter()
        .filter_map(|hash| normalize_hash(hash))
        .filter(|hash| cached_file(&images.join(format!("{hash}.img"))));
    let hash = matches.next()?;
    matches.next().is_none().then_some(hash)
}

fn remember_image(images: &Path, image: &BaseImage) -> Result<()> {
    let _guard = INDEX_LOCK.lock().expect("VM image index lock");
    let mut index = read_index(images);
    let mut hashes = BTreeSet::from([image.sha256.clone()]);
    if let Some(old) = index.get(&image.url) {
        hashes.extend(
            old.iter()
                .filter_map(|hash| normalize_hash(hash))
                .filter(|hash| cached_file(&images.join(format!("{hash}.img")))),
        );
        if old.iter().eq(hashes.iter()) {
            return Ok(());
        }
    }
    let entry = if hashes.len() == 1 {
        IndexedHashes::One(image.sha256.clone())
    } else {
        IndexedHashes::Many(hashes)
    };
    index.insert(image.url.clone(), entry);
    let bytes = serde_json::to_vec_pretty(&index)?;
    crate::statefile::write_private(&images.join(INDEX_FILE), &bytes)
        .context("writing VM image cache index")
}

fn check_size(bytes: u64, limit: u64, subject: &str) -> Result<()> {
    ensure!(
        bytes <= limit,
        "{subject} exceeds the {limit}-byte download limit"
    );
    Ok(())
}

async fn download_image(client: &Client, url: Url, image: &BaseImage) -> Result<()> {
    let mut response = client
        .get(url)
        .send()
        .await
        .and_then(Response::error_for_status)
        .map_err(reqwest::Error::without_url)
        .context("requesting VM base image")?;
    let total = response.content_length().filter(|&length| length > 0);
    check_size(total.unwrap_or(0), IMAGE_LIMIT, "VM base image")?;
    let (temp, file) = TempFile::beside(&image.path)?;
    let mut file = tokio::fs::File::from_std(file);
    let mut hasher = Sha256::new();
    let mut bytes = 0;
    let mut last_step = 0;
    tracing::info!("downloading VM base image");
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)
        .context("reading VM base image")?
    {
        bytes += chunk.len() as u64;
        check_size(bytes, IMAGE_LIMIT, "VM base image")?;
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .context("writing temporary VM base image")?;
        let step = total.map_or(bytes / (64 * 1024 * 1024), |total| bytes * 10 / total);
        if step > last_step {
            tracing::info!(bytes, total, "downloading VM base image");
            last_step = step;
        }
    }
    let actual = hex::encode(hasher.finalize());
    ensure!(
        actual == image.sha256,
        "VM base image SHA-256 mismatch (expected {}, got {actual})",
        image.sha256
    );
    file.sync_all().await.context("syncing VM base image")?;
    drop(file);
    if let Err(error) = fs::rename(&temp.0, &image.path) {
        // Another successful download may have published and opened this image.
        if !cached_file(&image.path) {
            return Err(error).context("publishing verified VM base image");
        }
    }
    tracing::info!(bytes, sha256 = image.sha256, "verified VM base image");
    Ok(())
}

/// `qemu-img create -f qcow2 -F qcow2 -b <base> <overlay> <size_gb>G`.
pub async fn create_overlay(
    qemu_img: &Path,
    base: &Path,
    overlay: &Path,
    size_gb: u32,
) -> Result<()> {
    let overlay = std::path::absolute(overlay).context("resolving VM overlay path")?;
    match fs::symlink_metadata(&overlay) {
        Ok(_) => bail!("VM overlay already exists: {}", overlay.display()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("checking VM overlay path"),
    }
    let (temp, file) = TempFile::beside(&overlay)?;
    drop(file);

    let mut command = Command::new(qemu_img);
    command
        .args(overlay_args(base, &temp.0, size_gb)?)
        .stdin(Stdio::null());
    crate::child_process::prepare(&mut command);
    let result = command
        .output()
        .await
        .context("running qemu-img to create VM overlay")?;
    ensure!(
        result.status.success(),
        "qemu-img failed to create VM overlay ({}): {}",
        result.status,
        String::from_utf8_lossy(&result.stderr).trim()
    );
    rename_overlay(&temp.0, &overlay)
        .with_context(|| format!("publishing VM overlay {}", overlay.display()))
}

/// Rewrite a reconstructed overlay's backing path without opening its old base.
pub(crate) async fn rebase_unsafe(qemu_img: &Path, base: &Path, overlay: &Path) -> Result<()> {
    let base = std::path::absolute(base).context("resolving VM backing image path")?;
    let mut command = Command::new(qemu_img);
    command
        .args(["rebase", "-u", "-f", "qcow2", "-F", "qcow2", "-b"])
        .arg(base)
        .arg(overlay)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    crate::child_process::prepare(&mut command);
    let result = command.output().await.context("running qemu-img rebase")?;
    ensure!(
        result.status.success(),
        "qemu-img failed to rebase VM overlay ({}): {}",
        result.status,
        String::from_utf8_lossy(&result.stderr).trim()
    );
    Ok(())
}

fn overlay_args(base: &Path, output: &Path, size_gb: u32) -> Result<Vec<OsString>> {
    let base = std::path::absolute(base).context("resolving VM backing image path")?;
    let mut args = ["create", "-f", "qcow2", "-F", "qcow2", "-b"]
        .map(OsString::from)
        .to_vec();
    args.push(base.into_os_string());
    args.push(output.as_os_str().to_owned());
    args.push(format!("{size_gb}G").into());
    Ok(args)
}

struct TempFile(PathBuf);

impl TempFile {
    fn beside(target: &Path) -> Result<(Self, File)> {
        let path = target.with_extension(format!("{:016x}.tmp", rand::random::<u64>()));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .context("creating temporary VM image file")?;
        Ok((Self(path), file))
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

// A concurrent create must not overwrite the first VM's disk.
#[cfg(windows)]
fn rename_overlay(from: &Path, to: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MOVEFILE_WRITE_THROUGH, MoveFileExW};

    let from = fs::canonicalize(from)?;
    let to = fs::canonicalize(to.parent().context("missing VM overlay parent")?)?
        .join(to.file_name().context("missing VM overlay filename")?);
    let from: Vec<u16> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let to: Vec<u16> = to.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: both NUL-terminated paths are valid for the duration of the call.
    if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_WRITE_THROUGH) } == 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn rename_overlay(from: &Path, to: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let from_c = CString::new(from.as_os_str().as_bytes())?;
    let to_c = CString::new(to.as_os_str().as_bytes())?;
    // SAFETY: both NUL-terminated paths are valid for the duration of the call.
    let status = unsafe {
        #[cfg(target_os = "linux")]
        {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                from_c.as_ptr(),
                libc::AT_FDCWD,
                to_c.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        }
        #[cfg(target_os = "macos")]
        {
            libc::renamex_np(from_c.as_ptr(), to_c.as_ptr(), libc::RENAME_EXCL)
        }
    };
    if status != 0 {
        let error = io::Error::last_os_error();
        if matches!(
            error.raw_os_error(),
            Some(libc::ENOSYS | libc::EINVAL | libc::EOPNOTSUPP)
        ) {
            fs::hard_link(from, to)?;
        } else {
            return Err(error.into());
        }
    }
    Ok(())
}

#[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
fn rename_overlay(from: &Path, to: &Path) -> Result<()> {
    fs::hard_link(from, to)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_matches_the_exact_decoded_filename() {
        let url =
            Url::parse("https://example.test/release/cloud%20server.img?query=value").unwrap();
        assert_eq!(
            url.join("SHA256SUMS").unwrap().as_str(),
            "https://example.test/release/SHA256SUMS"
        );
        let filename = image_filename(&url).unwrap();
        assert_eq!(filename, "cloud server.img");
        let hash = "a".repeat(64);
        let other = "b".repeat(64);
        let manifest = format!(
            "{other} *other-cloud server.img\r\n{} *{filename}\r\n{hash}  {filename}\r\n",
            hash.to_ascii_uppercase()
        );
        assert_eq!(checksum_for(&manifest, &filename).unwrap(), hash);
        assert!(checksum_for(&manifest, "server.img").is_err());
        assert!(checksum_for(&format!("bad *{filename}"), &filename).is_err());
        assert!(checksum_for(&format!("{manifest}{other} *{filename}"), &filename).is_err());
        assert!(normalize_hash(&".".repeat(64)).is_none());
        assert!(check_size(IMAGE_LIMIT, IMAGE_LIMIT, "VM base image").is_ok());
        assert!(check_size(IMAGE_LIMIT + 1, IMAGE_LIMIT, "VM base image").is_err());
    }

    #[tokio::test]
    async fn http_image_requires_an_explicit_valid_pin() {
        let root =
            std::env::temp_dir().join(format!("mistl-vm-image-{:016x}", rand::random::<u64>()));
        let mut cfg = VmConfig {
            image_url: "http://example.test/image.img".into(),
            ..VmConfig::default()
        };
        assert_eq!(
            ensure_base_image(&cfg, &root)
                .await
                .unwrap_err()
                .to_string(),
            "HTTP VM images require a pinned image_sha256"
        );

        cfg.image_sha256 = "invalid".into();
        assert!(ensure_base_image(&cfg, &root).await.is_err());

        cfg.image_sha256 = "a".repeat(64);
        let image = base_image(
            &root.join("images"),
            cfg.image_sha256.clone(),
            &cfg.image_url,
        );
        fs::write(&image.path, b"cached").unwrap();
        assert_eq!(
            ensure_base_image(&cfg, &root).await.unwrap().path,
            image.path
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn pinned_cache_hit_and_url_fallback_require_one_cached_image() {
        let root =
            std::env::temp_dir().join(format!("mistl-vm-image-{:016x}", rand::random::<u64>()));
        let images = root.join("images");
        fs::create_dir_all(&images).unwrap();
        let mut cfg = VmConfig {
            image_url: "https://example.test/image.img".into(),
            image_sha256: "A".repeat(64),
            ..VmConfig::default()
        };
        let first = base_image(&images, "a".repeat(64), &cfg.image_url);
        fs::write(&first.path, b"cached").unwrap();
        assert_eq!(
            ensure_base_image(&cfg, &root).await.unwrap().path,
            first.path
        );
        assert_eq!(cached_for_url(&images, &cfg.image_url), Some(first.sha256));
        assert_eq!(
            cached_for_url(&images, "https://example.test/other.img"),
            None
        );

        cfg.image_sha256 = "b".repeat(64);
        let second = base_image(&images, cfg.image_sha256.clone(), &cfg.image_url);
        fs::write(&second.path, b"cached").unwrap();
        ensure_base_image(&cfg, &root).await.unwrap();
        assert_eq!(cached_for_url(&images, &cfg.image_url), None);
        fs::remove_file(second.path).unwrap();
        assert_eq!(
            cached_for_url(&images, &cfg.image_url),
            Some("a".repeat(64))
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn restore_accepts_only_the_exact_cached_base() {
        let root =
            std::env::temp_dir().join(format!("mistl-vm-image-{:016x}", rand::random::<u64>()));
        let images = root.join("images");
        fs::create_dir_all(&images).unwrap();
        let hash = hex::encode(Sha256::digest(b"base"));
        let path = images.join(format!("{hash}.img"));
        assert!(ensure_base_by_hash(&root, &hash, "", true).await.is_err());
        assert!(
            ensure_base_by_hash(&root, &hash, "https://example.test/base.img", false)
                .await
                .is_err()
        );
        fs::write(&path, b"wrong").unwrap();
        assert!(ensure_base_by_hash(&root, &hash, "", false).await.is_err());
        fs::write(&path, b"base").unwrap();
        assert_eq!(
            ensure_base_by_hash(&root, &hash, "", false)
                .await
                .unwrap()
                .path,
            path
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn overlay_uses_absolute_qcow2_backing_path_and_never_clobbers() {
        let base = Path::new("base image.img");
        let output = Path::new("temporary.img");
        let args = overlay_args(base, output, 20).unwrap();
        assert_eq!(
            &args[..6],
            &["create", "-f", "qcow2", "-F", "qcow2", "-b"].map(OsString::from)
        );
        assert_eq!(args[6], std::path::absolute(base).unwrap().into_os_string());
        assert_eq!(args[7], output.as_os_str());
        assert_eq!(args[8], "20G");

        let dir =
            std::env::temp_dir().join(format!("mistl-vm-overlay-{:016x}", rand::random::<u64>()));
        fs::create_dir(&dir).unwrap();
        let overlay = dir.join("disk.qcow2");
        let (temp, file) = TempFile::beside(&overlay).unwrap();
        drop(file);
        fs::write(&temp.0, b"new").unwrap();
        fs::write(&overlay, b"existing").unwrap();
        assert!(rename_overlay(&temp.0, &overlay).is_err());
        assert_eq!(fs::read(&overlay).unwrap(), b"existing");
        fs::remove_file(&overlay).unwrap();
        rename_overlay(&temp.0, &overlay).unwrap();
        assert_eq!(fs::read(&overlay).unwrap(), b"new");
        drop(temp);
        fs::remove_dir_all(dir).unwrap();
    }
}
