//! Blocking local operator operations. Network permission is deliberately irrelevant.
use super::portable::{Reader, Writer};
use super::repository::ImportOutcome;
use super::{Service, sha2_byte, valid_room};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};

fn rooms(args: &Value) -> Result<Option<HashSet<String>>> {
    if args["rooms"].is_null() {
        return Ok(None);
    }
    let rooms: Vec<String> = serde_json::from_value(args["rooms"].clone())
        .context("escrow: rooms must be an array of room IDs")?;
    ensure!(rooms.iter().all(|r| valid_room(r)), "escrow: invalid room");
    Ok(Some(rooms.into_iter().collect()))
}

fn selected(filter: &Option<HashSet<String>>, room: &str) -> bool {
    filter.as_ref().is_none_or(|rooms| rooms.contains(room))
}

struct Temporary {
    path: PathBuf,
    file: Option<File>,
}

impl Temporary {
    fn beside(path: &Path) -> Result<Self> {
        let name = path.file_name().context("escrow: missing filename")?;
        let mut name = name.to_os_string();
        name.push(format!(".{:016x}.tmp", rand::random::<u64>()));
        let path = path.with_file_name(name);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&path)?;
        Ok(Self {
            path,
            file: Some(file),
        })
    }
}

impl Drop for Temporary {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(super) fn handle(
    service: &Service,
    export: bool,
    args: Value,
    configured: &[String],
) -> Result<Value> {
    let path = PathBuf::from(args["path"].as_str().context("escrow: path is required")?);
    ensure!(path.is_absolute(), "escrow: path must be absolute");
    let filter = rooms(&args)?;
    if export {
        let overwrite = if args["overwrite"].is_null() {
            false
        } else {
            args["overwrite"]
                .as_bool()
                .context("escrow: overwrite must be a boolean")?
        };
        export_file(service, &path, &filter, overwrite)
    } else {
        import_file(service, &path, &filter, configured)
    }
}

fn export_file(
    service: &Service,
    path: &Path,
    filter: &Option<HashSet<String>>,
    overwrite: bool,
) -> Result<Value> {
    ensure!(
        overwrite || !path.try_exists()?,
        "escrow export: target already exists"
    );
    let mut temp = Temporary::beside(path)?;
    let result = export_to(service, BufWriter::new(temp.file.as_mut().unwrap()), filter)?;
    temp.file.as_ref().unwrap().sync_all()?;
    drop(temp.file.take());
    publish(&temp.path, path, overwrite)?;
    #[cfg(unix)]
    File::open(path.parent().context("escrow export: missing parent")?)?.sync_all()?;
    let mut result = result;
    result["path"] = json!(path);
    Ok(result)
}

#[cfg(windows)]
fn publish(from: &Path, to: &Path, overwrite: bool) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };
    let from = std::fs::canonicalize(from)?;
    let to = std::fs::canonicalize(to.parent().context("escrow export: missing parent")?)?
        .join(to.file_name().context("escrow export: missing filename")?);
    let from: Vec<u16> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let to: Vec<u16> = to.as_os_str().encode_wide().chain(Some(0)).collect();
    let flags = MOVEFILE_WRITE_THROUGH
        | if overwrite {
            MOVEFILE_REPLACE_EXISTING
        } else {
            0
        };
    // SAFETY: both NUL-terminated paths are owned for the duration of this call.
    ensure!(
        unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), flags) } != 0,
        "escrow export: publishing file: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(not(windows))]
fn publish(from: &Path, to: &Path, overwrite: bool) -> Result<()> {
    if overwrite {
        std::fs::rename(from, to)?;
    } else {
        // Atomic no-clobber publication, including a target created during export.
        std::fs::hard_link(from, to)?;
        let _ = std::fs::remove_file(from);
    }
    Ok(())
}

pub(super) fn export_to(
    service: &Service,
    output: impl Write,
    filter: &Option<HashSet<String>>,
) -> Result<Value> {
    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    let mut writer = Writer::new(output, created)?;
    let mut names = service.repo.rooms();
    names.extend(service.clusters.lock().unwrap().keys().cloned());
    names.sort();
    names.dedup();
    let mut rooms = Vec::new();
    let mut skipped = Vec::new();
    let (mut slots, mut bytes) = (0u64, 0u64);
    let mut not_held = 0u64;
    for room in names.into_iter().filter(|r| selected(filter, r)) {
        let (mut count, mut size) = (0u64, 0u64);
        if let Some(cluster) = service.cluster(&room) {
            let snapshot = cluster.export_heads()?;
            for head in &snapshot.heads {
                let (blob, manifest) = cluster
                    .export_bytes(head)
                    .map_err(|e| anyhow::anyhow!(e.code))?;
                writer.entry(&room, head, &manifest, &blob)?;
                count += 1;
                size += head.size;
            }
        } else {
            for head in service.repo.room_heads(&room) {
                let data = service
                    .repo
                    .current_bytes(&room, &head)
                    .map_err(|e| anyhow::anyhow!(e.code))?;
                if let Some((blob, manifest)) = data {
                    writer.entry(&room, &head, &manifest, &blob)?;
                    count += 1;
                    size += head.size;
                } else {
                    let reason = if service.repo.head(&room, &head.slot).as_ref() == Some(&head)
                        && !service.repo.held(&room, &head)
                    {
                        not_held += 1;
                        "not_held"
                    } else {
                        "changed"
                    };
                    skipped.push(json!({"room":room,"slot":head.slot,"reason":reason}));
                }
            }
        }
        slots += count;
        bytes += size;
        rooms.push(json!({"room":room,"slots":count,"bytes":size}));
    }
    writer.finish()?;
    Ok(json!({"slots":slots,"bytes":bytes,"rooms":rooms,"skipped":skipped,"not_held":not_held}))
}

struct CopyRead<'a> {
    source: File,
    copy: &'a mut File,
}
impl Read for CopyRead<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let size = self.source.read(buf)?;
        self.copy.write_all(&buf[..size])?;
        Ok(size)
    }
}

#[derive(Default)]
struct Counts {
    imported: u64,
    unchanged: u64,
    skipped: u64,
}

pub(super) fn import_file(
    service: &Service,
    path: &Path,
    filter: &Option<HashSet<String>>,
    configured: &[String],
) -> Result<Value> {
    // Validate and spool the complete source before mutation. Applying the owned
    // copy avoids a path replacement or in-place edit between the two passes.
    let scratch = service
        .replication_root
        .parent()
        .context("escrow import: missing repository")?
        .join("import");
    let mut temp = Temporary::beside(&scratch)?;
    {
        let copy = CopyRead {
            source: File::open(path).context("escrow import: opening file")?,
            copy: temp.file.as_mut().unwrap(),
        };
        let mut reader = Reader::new(BufReader::new(copy), service.cfg.max_blob_bytes)?;
        while reader.next()?.is_some() {}
    }
    temp.file.as_mut().unwrap().rewind()?;
    let mut reader = Reader::new(
        BufReader::new(temp.file.as_mut().unwrap()),
        service.cfg.max_blob_bytes,
    )?;
    let mut rooms: BTreeMap<String, Counts> = BTreeMap::new();
    let mut skipped = Vec::new();
    let (mut imported, mut unchanged) = (0u64, 0u64);
    while let Some(entry) = reader.next()? {
        if !selected(filter, &entry.room) {
            continue;
        }
        let counts = rooms.entry(entry.room.clone()).or_default();
        let outcome = if service.cluster(&entry.room).is_some() {
            ImportOutcome::Skipped("replicated")
        } else {
            let stripe = usize::from(sha2_byte(&entry.room, &entry.head.slot));
            // Only called on a blocking worker; wait for in-flight PUTs normally.
            let _slot = service.slots[stripe].blocking_lock();
            service
                .repo
                .import(&entry.room, &entry.head, &entry.blob, &entry.manifest)
                .map_err(|e| {
                    anyhow::anyhow!(
                        "escrow import (room {}, slot {}): {}",
                        entry.room,
                        entry.head.slot,
                        e.code
                    )
                })?
        };
        match outcome {
            ImportOutcome::Imported => {
                imported += 1;
                counts.imported += 1;
            }
            ImportOutcome::Unchanged => {
                unchanged += 1;
                counts.unchanged += 1;
            }
            ImportOutcome::Skipped(reason) => {
                counts.skipped += 1;
                skipped.push(json!({"room":entry.room,"slot":entry.head.slot,"reason":reason}));
            }
        }
    }
    let rooms: Vec<_> = rooms.into_iter().map(|(room, counts)| {
        json!({"configured":configured.contains(&room),"room":room,"imported":counts.imported,"unchanged":counts.unchanged,"skipped":counts.skipped})
    }).collect();
    Ok(json!({"imported":imported,"unchanged":unchanged,"skipped":skipped,"rooms":rooms}))
}
