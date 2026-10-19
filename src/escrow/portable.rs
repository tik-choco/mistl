//! Portable ciphertext-only escrow export, version 1.
use super::protocol::Head;
use super::repository::{MAX_MANIFEST, validate_blob, validate_manifest};
use anyhow::{Context, Result, ensure};
use mistlib_core::storage::CHUNK_SIZE;
use mistlib_core::storage::cid::{MULTICODEC_RAW, verify_cid};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{BufRead, Write};

const HEADER_LINE: usize = 4096;
const ENTRY_LINE: usize = 32768;
// A node can retain one standalone repository plus at most 64 Raft rooms.
const MAX_ENTRIES: usize = 65 * 65536;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    format: String,
    version: u64,
    created_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    room: String,
    head: Head,
    manifest: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Trailer {
    end: bool,
    entries: u64,
}

pub(super) struct Entry {
    pub room: String,
    pub head: Head,
    pub manifest: Vec<u8>,
    pub blob: Vec<u8>,
}

pub(super) fn validate(head: &Head, blob: &[u8], bytes: &[u8], max_blob: u64) -> Result<()> {
    head.validate(max_blob)
        .map_err(|e| anyhow::anyhow!(e.code))?;
    head.verify().map_err(|e| anyhow::anyhow!(e.code))?;
    validate_blob(head, blob).map_err(|e| anyhow::anyhow!(e.code))?;
    let manifest = validate_manifest(head, bytes).map_err(|e| anyhow::anyhow!(e.code))?;
    for (cid, chunk) in manifest.chunks.iter().zip(blob.chunks(CHUNK_SIZE)) {
        ensure!(
            verify_cid(cid, chunk, MULTICODEC_RAW),
            "hash mismatch: chunk CID"
        );
    }
    Ok(())
}

pub(super) struct Writer<W> {
    output: W,
    entries: u64,
    seen: HashSet<(String, String)>,
}

fn json_line(output: &mut impl Write, value: &impl Serialize, limit: usize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() < limit, "escrow export: oversized JSON line");
    output.write_all(&bytes)?;
    output.write_all(b"\n")?;
    Ok(())
}

impl<W: Write> Writer<W> {
    pub fn new(mut output: W, created_ms: u64) -> Result<Self> {
        json_line(
            &mut output,
            &Header {
                format: "mistl-escrow-export".into(),
                version: 1,
                created_ms,
            },
            HEADER_LINE,
        )?;
        Ok(Self {
            output,
            entries: 0,
            seen: HashSet::new(),
        })
    }

    pub fn entry(&mut self, room: &str, head: &Head, manifest: &[u8], blob: &[u8]) -> Result<()> {
        ensure!(super::valid_room(room), "escrow export: invalid room");
        validate(head, blob, manifest, 1024 * 1024 * 1024)?;
        ensure!(
            self.seen.insert((room.into(), head.slot.clone())),
            "escrow export: duplicate slot"
        );
        ensure!(
            self.seen.len() <= MAX_ENTRIES,
            "escrow export: too many slots"
        );
        json_line(
            &mut self.output,
            &Metadata {
                room: room.into(),
                head: head.clone(),
                manifest: manifest.len() as u64,
            },
            ENTRY_LINE,
        )?;
        self.output.write_all(manifest)?;
        self.output.write_all(blob)?;
        self.entries += 1;
        Ok(())
    }

    pub fn finish(mut self) -> Result<W> {
        json_line(
            &mut self.output,
            &Trailer {
                end: true,
                entries: self.entries,
            },
            ENTRY_LINE,
        )?;
        self.output.flush()?;
        Ok(self.output)
    }
}

// Include the newline in the bound; never let a missing newline grow the buffer.
fn line(input: &mut impl BufRead, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    loop {
        let available = input.fill_buf()?;
        ensure!(
            !available.is_empty(),
            "escrow import: truncated file or missing trailer"
        );
        let count = available
            .iter()
            .position(|b| *b == b'\n')
            .map_or(available.len(), |i| i + 1);
        ensure!(
            count <= limit - bytes.len(),
            "escrow import: oversized JSON line"
        );
        let done = available[count - 1] == b'\n';
        bytes.extend_from_slice(&available[..count]);
        input.consume(count);
        if done {
            return Ok(bytes);
        }
    }
}

pub(super) struct Reader<R> {
    input: R,
    max_blob: u64,
    entries: u64,
    seen: HashSet<(String, String)>,
    done: bool,
}

impl<R: BufRead> Reader<R> {
    pub fn new(mut input: R, max_blob: u64) -> Result<Self> {
        let header: Header = serde_json::from_slice(&line(&mut input, HEADER_LINE)?)
            .context("escrow import: invalid header")?;
        ensure!(
            header.format == "mistl-escrow-export" && header.version == 1,
            "escrow import: unsupported format or version"
        );
        Ok(Self {
            input,
            max_blob,
            entries: 0,
            seen: HashSet::new(),
            done: false,
        })
    }

    /// Consume every entry, including filtered-out rooms, to validate the whole file.
    pub fn next(&mut self) -> Result<Option<Entry>> {
        if self.done {
            return Ok(None);
        }
        let bytes = line(&mut self.input, ENTRY_LINE)?;
        if let Ok(trailer) = serde_json::from_slice::<Trailer>(&bytes) {
            ensure!(
                trailer.end && trailer.entries == self.entries,
                "escrow import: trailer count mismatch"
            );
            ensure!(
                self.input.fill_buf()?.is_empty(),
                "escrow import: trailing bytes after trailer"
            );
            self.done = true;
            return Ok(None);
        }
        let metadata: Metadata = serde_json::from_slice(&bytes)
            .with_context(|| format!("escrow import: invalid entry {}", self.entries + 1))?;
        let context = format!(
            "escrow import: entry {} (room {}, slot {})",
            self.entries + 1,
            metadata.room,
            metadata.head.slot
        );
        let entry = (|| -> Result<Entry> {
            ensure!(super::valid_room(&metadata.room), "invalid room");
            metadata
                .head
                .validate(self.max_blob)
                .map_err(|e| anyhow::anyhow!(e.code))?;
            metadata
                .head
                .verify()
                .map_err(|e| anyhow::anyhow!(e.code))?;
            ensure!(
                metadata.manifest <= MAX_MANIFEST as u64,
                "oversized manifest"
            );
            ensure!(
                self.seen
                    .insert((metadata.room.clone(), metadata.head.slot.clone())),
                "duplicate slot"
            );
            ensure!(self.seen.len() <= MAX_ENTRIES, "too many slots");
            let mut manifest = vec![0; metadata.manifest as usize];
            self.input
                .read_exact(&mut manifest)
                .context("truncated manifest")?;
            let mut blob = vec![0; usize::try_from(metadata.head.size).context("oversized blob")?];
            self.input.read_exact(&mut blob).context("truncated blob")?;
            validate(&metadata.head, &blob, &manifest, self.max_blob)?;
            Ok(Entry {
                room: metadata.room,
                head: metadata.head,
                manifest,
                blob,
            })
        })()
        .with_context(|| context)?;
        self.entries += 1;
        Ok(Some(entry))
    }
}
