//! Encrypted objects (files), descriptors, the encrypted catalog and pinsets.
use super::keys::OwnerKey;
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{Context, Result, ensure};
use mistlib_core::storage::cid::{MULTICODEC_RAW, cid_codec, compute_cid, verify_cid};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;

pub const SEGMENT: usize = 1024 * 1024;
pub const MAX_CHUNKS: usize = 8192;
pub const MAX_CATALOG_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_PINSET_ROOTS: usize = 16384;
/// Largest descriptor block (8192 CIDs plus JSON overhead).
pub const MAX_DESCRIPTOR_BYTES: usize = 1024 * 1024;
const MAX_PINSET_BYTES: usize = 2 * 1024 * 1024;
const MAX_PLAIN_BYTES: u64 = SEGMENT as u64 * MAX_CHUNKS as u64;
const TAG_BYTES: usize = 16;
const CATALOG_OVERHEAD: usize = 4 + 12 + TAG_BYTES;
const SEG_AAD: &[u8] = b"mistl-vault-seg-v1";
const CATALOG_AAD: &[u8] = b"mistl-vault-catalog-v1";

/// A block to be written: (cid, bytes).
pub type Block = (String, Vec<u8>);

/// Holder-visible object descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Descriptor {
    pub mvo: u32,
    #[serde(deserialize_with = "bounded_strings::<_, MAX_CHUNKS>")]
    pub chunks: Vec<String>,
    pub bytes: u64,
}

/// Result of encrypting one file.
pub struct Encrypted {
    pub root: String,
    pub dek: [u8; 32],
    pub size: u64,
    pub sha256: String,
    /// Chunk blocks followed by the descriptor block (last).
    pub blocks: Vec<Block>,
}

/// Encrypt plaintext into segment blocks + descriptor.
pub fn encrypt_object(plain: &[u8]) -> Result<Encrypted> {
    encrypt_reader(&mut &plain[..], plain.len() as u64, None)
}

/// Segment and descriptor sink: receives each block as soon as it is encrypted.
pub type Sink<'a> = &'a mut dyn FnMut(&str, &[u8]) -> Result<()>;

/// Read a file in SEGMENT pieces, rejecting files over max_bytes or 8 GiB.
/// Segments go to `sink` as they are produced, so memory stays bounded;
/// the returned `blocks` holds only the descriptor.
pub fn encrypt_file(path: &Path, max_bytes: u64, sink: Sink) -> Result<Encrypted> {
    let mut file = std::fs::File::open(path).context("opening vault input file")?;
    let metadata = file.metadata().context("reading vault input metadata")?;
    ensure!(metadata.is_file(), "vault input must be a regular file");
    ensure!(
        metadata.len() <= max_bytes,
        "object exceeds configured size cap"
    );
    encrypt_reader(&mut file, metadata.len(), Some(sink))
}

fn encrypt_reader(reader: &mut dyn Read, size: u64, mut sink: Option<Sink>) -> Result<Encrypted> {
    ensure!(size <= MAX_PLAIN_BYTES, "object has too many segments");
    let count = size.div_ceil(SEGMENT as u64);
    let mut dek = [0; 32];
    OsRng.fill_bytes(&mut dek);
    let cipher = Aes256Gcm::new((&dek).into());
    let mut hash = Sha256::new();
    let mut chunks = Vec::with_capacity(count as usize);
    let mut blocks = Vec::with_capacity(if sink.is_some() {
        1
    } else {
        count as usize + 1
    });
    let mut buffer = vec![0; SEGMENT];
    let mut bytes = 0;
    for i in 0..count {
        let len = (size - i * SEGMENT as u64).min(SEGMENT as u64) as usize;
        reader
            .read_exact(&mut buffer[..len])
            .context("reading vault segment (file may have changed)")?;
        hash.update(&buffer[..len]);
        let ct = cipher
            .encrypt(
                Nonce::from_slice(&segment_nonce(i)),
                Payload {
                    msg: &buffer[..len],
                    aad: &segment_aad(i, count),
                },
            )
            .map_err(|_| anyhow::anyhow!("encrypting vault segment"))?;
        bytes += ct.len() as u64;
        let cid = raw_cid(&ct);
        match sink.as_mut() {
            Some(sink) => sink(&cid, &ct)?,
            None => blocks.push((cid.clone(), ct)),
        }
        chunks.push(cid);
    }
    let mut extra = [0];
    ensure!(
        reader
            .read(&mut extra)
            .context("checking vault input length")?
            == 0,
        "vault input grew while encrypting"
    );
    let descriptor = Descriptor {
        mvo: 1,
        chunks,
        bytes,
    };
    let encoded = serde_json::to_vec(&descriptor)?;
    let root = raw_cid(&encoded);
    blocks.push((root.clone(), encoded));
    Ok(Encrypted {
        root,
        dek,
        size,
        sha256: hex::encode(hash.finalize()),
        blocks,
    })
}

fn segment_nonce(index: u64) -> [u8; 12] {
    let mut nonce = [0; 12];
    nonce[4..].copy_from_slice(&index.to_be_bytes());
    nonce
}

fn segment_aad(index: u64, count: u64) -> Vec<u8> {
    let mut aad = Vec::with_capacity(SEG_AAD.len() + 16);
    aad.extend_from_slice(SEG_AAD);
    aad.extend_from_slice(&index.to_be_bytes());
    aad.extend_from_slice(&count.to_be_bytes());
    aad
}

/// Parse and bound-check a descriptor block (chunk CIDs must be RAW CIDs).
pub fn parse_descriptor(bytes: &[u8]) -> Result<Descriptor> {
    ensure!(bytes.len() <= MAX_DESCRIPTOR_BYTES, "descriptor too large");
    let descriptor: Descriptor =
        serde_json::from_slice(bytes).context("invalid object descriptor")?;
    validate_descriptor(&descriptor)?;
    Ok(descriptor)
}

fn validate_descriptor(descriptor: &Descriptor) -> Result<()> {
    ensure!(descriptor.mvo == 1, "unsupported object version");
    let count = descriptor.chunks.len();
    ensure!(count <= MAX_CHUNKS, "too many object chunks");
    ensure!(
        descriptor.chunks.iter().all(|cid| valid_raw(cid)),
        "invalid chunk CID"
    );
    if count == 0 {
        ensure!(descriptor.bytes == 0, "empty descriptor has nonzero size");
    } else {
        let max = count as u64 * (SEGMENT + TAG_BYTES) as u64;
        let min = (count - 1) as u64 * (SEGMENT + TAG_BYTES) as u64 + TAG_BYTES as u64 + 1;
        ensure!(
            (min..=max).contains(&descriptor.bytes),
            "invalid descriptor ciphertext size"
        );
    }
    Ok(())
}

/// Decrypt and verify every chunk's CID, GCM tag, length, index and total count.
pub fn decrypt_object(
    descriptor: &Descriptor,
    dek: &[u8; 32],
    get: &dyn Fn(&str) -> Option<Vec<u8>>,
) -> Result<Vec<u8>> {
    validate_descriptor(descriptor)?;
    let cipher = Aes256Gcm::new(dek.into());
    let count = descriptor.chunks.len() as u64;
    let mut plain = Vec::new();
    let mut bytes = 0u64;
    for (i, cid) in descriptor.chunks.iter().enumerate() {
        let ct = get(cid).context("missing object chunk")?;
        let expected = if i + 1 == descriptor.chunks.len() {
            descriptor.bytes - i as u64 * (SEGMENT + TAG_BYTES) as u64
        } else {
            (SEGMENT + TAG_BYTES) as u64
        };
        ensure!(ct.len() as u64 == expected, "incorrect chunk length");
        ensure!(verify_raw(cid, &ct), "chunk CID mismatch");
        let segment = cipher
            .decrypt(
                Nonce::from_slice(&segment_nonce(i as u64)),
                Payload {
                    msg: &ct,
                    aad: &segment_aad(i as u64, count),
                },
            )
            .map_err(|_| anyhow::anyhow!("object segment authentication failed"))?;
        bytes += ct.len() as u64;
        plain
            .try_reserve(segment.len())
            .context("allocating decrypted object")?;
        plain.extend_from_slice(&segment);
    }
    ensure!(bytes == descriptor.bytes, "object ciphertext size mismatch");
    Ok(plain)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub object: String,
    /// Hex DEK.
    pub dek: String,
    pub size: u64,
    pub mtime: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    pub v: u32,
    pub files: BTreeMap<String, CatalogEntry>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            v: 1,
            files: BTreeMap::new(),
        }
    }
}

/// 1..=255 chars, no path separators or control chars, not dot or dot-dot.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().count() <= 255
        && name != "."
        && name != ".."
        && !name
            .chars()
            .any(|c| c == '/' || c == '\\' || c.is_control())
}

fn validate_catalog(catalog: &Catalog) -> Result<()> {
    ensure!(catalog.v == 1, "unsupported catalog version");
    // The encoded catalog is capped too; this prevents unbounded local serialization.
    ensure!(
        catalog.files.len() <= MAX_CATALOG_BYTES / 128,
        "too many catalog entries"
    );
    for (name, entry) in &catalog.files {
        ensure!(valid_name(name), "invalid catalog filename");
        ensure!(valid_raw(&entry.object), "invalid catalog object CID");
        let mut decoded = [0; 32];
        ensure!(
            hex::decode_to_slice(&entry.dek, &mut decoded).is_ok(),
            "invalid catalog DEK"
        );
        ensure!(
            hex::decode_to_slice(&entry.sha256, &mut decoded).is_ok(),
            "invalid plaintext hash"
        );
        ensure!(
            entry.size <= MAX_PLAIN_BYTES,
            "catalog object exceeds size limit"
        );
    }
    Ok(())
}

/// Encrypt the catalog into one RAW block MVC1 || nonce || ciphertext.
pub fn seal_catalog(catalog: &Catalog, key: &OwnerKey) -> Result<Block> {
    validate_catalog(catalog)?;
    let plain = serde_json::to_vec(catalog)?;
    ensure!(
        plain.len() <= MAX_CATALOG_BYTES - CATALOG_OVERHEAD,
        "catalog too large"
    );
    let cipher = Aes256Gcm::new((&key.enc_key()).into());
    let mut nonce = [0; 12];
    OsRng.fill_bytes(&mut nonce);
    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &plain,
                aad: CATALOG_AAD,
            },
        )
        .map_err(|_| anyhow::anyhow!("encrypting catalog"))?;
    let mut bytes = Vec::with_capacity(16 + ct.len());
    bytes.extend_from_slice(b"MVC1");
    bytes.extend_from_slice(&nonce);
    bytes.extend_from_slice(&ct);
    Ok((raw_cid(&bytes), bytes))
}

pub fn open_catalog(bytes: &[u8], key: &OwnerKey) -> Result<Catalog> {
    ensure!(
        (CATALOG_OVERHEAD..=MAX_CATALOG_BYTES).contains(&bytes.len()),
        "invalid catalog size"
    );
    ensure!(&bytes[..4] == b"MVC1", "invalid catalog magic");
    let cipher = Aes256Gcm::new((&key.enc_key()).into());
    let plain = cipher
        .decrypt(
            Nonce::from_slice(&bytes[4..16]),
            Payload {
                msg: &bytes[16..],
                aad: CATALOG_AAD,
            },
        )
        .map_err(|_| anyhow::anyhow!("catalog authentication failed"))?;
    let catalog: Catalog = serde_json::from_slice(&plain).context("invalid catalog JSON")?;
    validate_catalog(&catalog)?;
    Ok(catalog)
}

/// Holder-visible pinset: catalog first, then sorted unique object roots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pinset {
    pub mvp: u32,
    #[serde(deserialize_with = "bounded_strings::<_, MAX_PINSET_ROOTS>")]
    pub roots: Vec<String>,
}

pub fn build_pinset(catalog_cid: &str, objects: &[String]) -> Result<Block> {
    ensure!(valid_raw(catalog_cid), "invalid catalog CID");
    let mut objects_sorted = BTreeSet::new();
    for cid in objects {
        ensure!(valid_raw(cid), "invalid object CID");
        if cid != catalog_cid {
            objects_sorted.insert(cid.as_str());
            ensure!(
                objects_sorted.len() < MAX_PINSET_ROOTS,
                "too many pinset roots"
            );
        }
    }
    let mut roots = Vec::with_capacity(objects_sorted.len() + 1);
    roots.push(catalog_cid.to_owned());
    roots.extend(objects_sorted.into_iter().map(str::to_owned));
    let bytes = serde_json::to_vec(&Pinset { mvp: 1, roots })?;
    Ok((raw_cid(&bytes), bytes))
}

/// Parse and bound-check; first root is the catalog.
pub fn parse_pinset(bytes: &[u8]) -> Result<Pinset> {
    ensure!(bytes.len() <= MAX_PINSET_BYTES, "pinset too large");
    let pinset: Pinset = serde_json::from_slice(bytes).context("invalid pinset")?;
    ensure!(pinset.mvp == 1, "unsupported pinset version");
    ensure!(
        !pinset.roots.is_empty() && pinset.roots.len() <= MAX_PINSET_ROOTS,
        "invalid pinset root count"
    );
    ensure!(
        pinset.roots.iter().all(|cid| valid_raw(cid)),
        "invalid pinset CID"
    );
    ensure!(
        !pinset.roots[1..].contains(&pinset.roots[0]),
        "duplicate catalog root"
    );
    ensure!(
        pinset.roots[1..].windows(2).all(|pair| pair[0] < pair[1]),
        "pinset objects must be sorted and unique"
    );
    Ok(pinset)
}

/// RAW-codec CIDv1 sha2-256.
pub fn raw_cid(bytes: &[u8]) -> String {
    compute_cid(bytes, MULTICODEC_RAW)
}

pub fn verify_raw(cid: &str, bytes: &[u8]) -> bool {
    valid_raw(cid) && verify_cid(cid, bytes, MULTICODEC_RAW)
}

fn valid_raw(cid: &str) -> bool {
    cid.len() == 59 && cid_codec(cid) == Some(MULTICODEC_RAW)
}

fn bounded_strings<'de, D: serde::Deserializer<'de>, const N: usize>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error> {
    struct Strings<const N: usize>;
    impl<'de, const N: usize> serde::de::Visitor<'de> for Strings<N> {
        type Value = Vec<String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "at most {N} strings")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = seq.next_element::<String>()? {
                if values.len() == N {
                    return Err(serde::de::Error::custom("too many entries"));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Strings::<N>)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(encrypted: &Encrypted) -> Descriptor {
        let (cid, bytes) = encrypted.blocks.last().unwrap();
        assert_eq!(cid, &encrypted.root);
        assert!(verify_raw(cid, bytes));
        parse_descriptor(bytes).unwrap()
    }

    fn encrypt_file_collect(path: &Path, max_bytes: u64) -> Result<Encrypted> {
        let mut segments = Vec::new();
        let mut encrypted = encrypt_file(path, max_bytes, &mut |cid, bytes| {
            segments.push((cid.to_string(), bytes.to_vec()));
            Ok(())
        })?;
        assert_eq!(encrypted.blocks.len(), 1, "only the descriptor is buffered");
        segments.append(&mut encrypted.blocks);
        encrypted.blocks = segments;
        Ok(encrypted)
    }

    fn decrypt(encrypted: &Encrypted) -> Result<Vec<u8>> {
        decrypt_object(&descriptor(encrypted), &encrypted.dek, &|cid| {
            encrypted
                .blocks
                .iter()
                .find(|(id, _)| id == cid)
                .map(|(_, bytes)| bytes.clone())
        })
    }

    #[test]
    fn object_roundtrips_segment_boundaries_and_empty() {
        for size in [0, 1, SEGMENT - 1, SEGMENT, SEGMENT + 1, 2 * SEGMENT] {
            let plain: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            let encrypted = encrypt_object(&plain).unwrap();
            assert_eq!(encrypted.size, size as u64);
            assert_eq!(encrypted.sha256, hex::encode(Sha256::digest(&plain)));
            assert_eq!(encrypted.blocks.len(), size.div_ceil(SEGMENT) + 1);
            assert_eq!(decrypt(&encrypted).unwrap(), plain);
            let desc = descriptor(&encrypted);
            assert_eq!(desc.bytes, (size + TAG_BYTES * desc.chunks.len()) as u64);
            let expected = format!(
                "{{\"mvo\":1,\"chunks\":{},\"bytes\":{}}}",
                serde_json::to_string(&desc.chunks).unwrap(),
                desc.bytes
            );
            assert_eq!(encrypted.blocks.last().unwrap().1, expected.as_bytes());
        }
        assert_ne!(
            encrypt_object(b"x").unwrap().root,
            encrypt_object(b"x").unwrap().root
        );
    }

    #[test]
    fn segments_use_specified_nonce_and_aad() {
        let encrypted = encrypt_object(&vec![42; SEGMENT + 3]).unwrap();
        let cipher = Aes256Gcm::new((&encrypted.dek).into());
        for i in 0..2u64 {
            let mut nonce = [0; 12];
            nonce[4..].copy_from_slice(&i.to_be_bytes());
            let mut aad = b"mistl-vault-seg-v1".to_vec();
            aad.extend_from_slice(&i.to_be_bytes());
            aad.extend_from_slice(&2u64.to_be_bytes());
            let plain = cipher
                .decrypt(
                    Nonce::from_slice(&nonce),
                    Payload {
                        msg: &encrypted.blocks[i as usize].1,
                        aad: &aad,
                    },
                )
                .unwrap();
            assert_eq!(plain, vec![42; if i == 0 { SEGMENT } else { 3 }]);
        }
    }

    #[test]
    fn tampering_wrong_key_missing_swapped_reordered_truncated_fail() {
        let mut encrypted = encrypt_object(&vec![42; 2 * SEGMENT]).unwrap();
        let original = descriptor(&encrypted);
        let lookup = |cid: &str| {
            encrypted
                .blocks
                .iter()
                .find(|(id, _)| id == cid)
                .map(|(_, bytes)| bytes.clone())
        };
        assert!(decrypt_object(&original, &[0; 32], &lookup).is_err());
        assert!(decrypt_object(&original, &encrypted.dek, &|_| None).is_err());
        assert!(
            decrypt_object(&original, &encrypted.dek, &|_| Some(
                encrypted.blocks[1].1.clone()
            ))
            .is_err()
        );
        let mut reordered = original.clone();
        reordered.chunks.swap(0, 1);
        assert!(decrypt_object(&reordered, &encrypted.dek, &lookup).is_err());
        let mut truncated = original.clone();
        truncated.chunks.pop();
        truncated.bytes = (SEGMENT + TAG_BYTES) as u64;
        assert!(decrypt_object(&truncated, &encrypted.dek, &lookup).is_err());
        encrypted.blocks[0].1[0] ^= 1;
        assert!(decrypt(&encrypted).is_err());
        // Re-addressing tampered ciphertext cannot bypass its GCM tag.
        let cid = raw_cid(&encrypted.blocks[0].1);
        encrypted.blocks[0].0 = cid.clone();
        let mut desc = original;
        desc.chunks[0] = cid;
        assert!(
            decrypt_object(&desc, &encrypted.dek, &|cid| encrypted
                .blocks
                .iter()
                .find(|(id, _)| id == cid)
                .map(|(_, bytes)| bytes.clone()))
            .is_err()
        );
    }

    #[test]
    fn descriptors_reject_invalid_versions_cids_lengths_and_bounds() {
        let valid = Descriptor {
            mvo: 1,
            chunks: vec![raw_cid(b"x")],
            bytes: 17,
        };
        assert_eq!(
            parse_descriptor(&serde_json::to_vec(&valid).unwrap()).unwrap(),
            valid
        );
        for bad in [
            Descriptor {
                mvo: 2,
                ..valid.clone()
            },
            Descriptor {
                chunks: vec!["babc".into()],
                ..valid.clone()
            },
            Descriptor {
                chunks: vec![compute_cid(b"x", 0x71)],
                ..valid.clone()
            },
            Descriptor {
                bytes: 16,
                ..valid.clone()
            },
            Descriptor {
                bytes: u64::MAX,
                ..valid.clone()
            },
            Descriptor {
                chunks: vec![],
                bytes: 1,
                ..valid.clone()
            },
            Descriptor {
                chunks: vec![raw_cid(b"x"); MAX_CHUNKS + 1],
                bytes: 0,
                ..valid.clone()
            },
        ] {
            assert!(parse_descriptor(&serde_json::to_vec(&bad).unwrap()).is_err());
            assert!(decrypt_object(&bad, &[0; 32], &|_| None).is_err());
        }
        let limit = Descriptor {
            mvo: 1,
            chunks: vec![raw_cid(b"x"); MAX_CHUNKS],
            bytes: MAX_CHUNKS as u64 * (SEGMENT + TAG_BYTES) as u64,
        };
        assert!(parse_descriptor(&serde_json::to_vec(&limit).unwrap()).is_ok());
        assert!(parse_descriptor(&vec![b' '; MAX_DESCRIPTOR_BYTES + 1]).is_err());
    }

    #[test]
    fn file_streaming_caps_and_changed_input() {
        let path =
            std::env::temp_dir().join(format!("mistl-vault-file-{:016x}", rand::random::<u64>()));
        let plain = vec![17; SEGMENT + 3];
        std::fs::write(&path, &plain).unwrap();
        assert!(encrypt_file_collect(&path, SEGMENT as u64).is_err());
        assert_eq!(
            decrypt(&encrypt_file_collect(&path, plain.len() as u64).unwrap()).unwrap(),
            plain
        );
        std::fs::remove_file(&path).unwrap();
        assert!(encrypt_file_collect(&path, u64::MAX).is_err());
        assert!(encrypt_reader(&mut &b"short"[..], 6, None).is_err());
        assert!(encrypt_reader(&mut &b"longer"[..], 5, None).is_err());
        assert!(encrypt_reader(&mut &b""[..], MAX_PLAIN_BYTES + 1, None).is_err());
        std::fs::write(&path, []).unwrap();
        assert!(
            decrypt(&encrypt_file_collect(&path, 0).unwrap())
                .unwrap()
                .is_empty()
        );
        std::fs::remove_file(path).unwrap();
    }

    fn catalog() -> Catalog {
        Catalog {
            v: 1,
            files: BTreeMap::from([(
                "file.txt".into(),
                CatalogEntry {
                    object: raw_cid(b"descriptor"),
                    dek: hex::encode([3; 32]),
                    size: 123,
                    mtime: 456,
                    sha256: hex::encode(Sha256::digest(b"plain")),
                },
            )]),
        }
    }

    #[test]
    fn catalog_roundtrip_wrong_key_and_tampering() {
        let key = OwnerKey::from_secret([7; 32]);
        let catalog = catalog();
        let (cid, bytes) = seal_catalog(&catalog, &key).unwrap();
        assert!(verify_raw(&cid, &bytes));
        assert_eq!(&bytes[..4], b"MVC1");
        assert_eq!(open_catalog(&bytes, &key).unwrap(), catalog);
        assert_ne!(seal_catalog(&catalog, &key).unwrap().0, cid);
        assert!(open_catalog(&bytes, &OwnerKey::from_secret([8; 32])).is_err());
        for i in [0, 4, 16, bytes.len() - 1] {
            let mut changed = bytes.clone();
            changed[i] ^= 1;
            assert!(open_catalog(&changed, &key).is_err());
        }
        for len in 0..CATALOG_OVERHEAD {
            assert!(open_catalog(&bytes[..len], &key).is_err());
        }
        assert!(open_catalog(&vec![0; MAX_CATALOG_BYTES + 1], &key).is_err());
        assert_eq!(
            open_catalog(&seal_catalog(&Catalog::default(), &key).unwrap().1, &key).unwrap(),
            Catalog::default()
        );
    }

    #[test]
    fn catalog_validates_entries_and_size_before_sealing() {
        let key = OwnerKey::generate();
        let mut invalid = catalog();
        invalid.v = 2;
        assert!(seal_catalog(&invalid, &key).is_err());
        for name in ["", ".", "..", "a/b", "a\\b", "\0", "\u{7f}"] {
            let mut invalid = catalog();
            let entry = invalid.files.pop_first().unwrap().1;
            invalid.files.insert(name.into(), entry);
            assert!(seal_catalog(&invalid, &key).is_err());
        }
        for field in ["object", "dek", "sha256", "size"] {
            let mut invalid = catalog();
            let entry = invalid.files.get_mut("file.txt").unwrap();
            match field {
                "object" => entry.object = "babc".into(),
                "dek" => entry.dek = "ab".into(),
                "sha256" => entry.sha256 = "q".repeat(64),
                _ => entry.size = MAX_PLAIN_BYTES + 1,
            }
            assert!(seal_catalog(&invalid, &key).is_err());
        }
        let entry = catalog().files.pop_first().unwrap().1;
        let mut large = Catalog::default();
        for i in 0..9000 {
            large
                .files
                .insert(format!("{i:05}{}", "x".repeat(250)), entry.clone());
        }
        assert!(seal_catalog(&large, &key).is_err());
    }

    #[test]
    fn filenames_count_unicode_chars_and_reject_controls() {
        assert!(valid_name(&"あ".repeat(255)));
        assert!(!valid_name(&"a".repeat(256)));
        for name in ["", ".", "..", "a/b", "a\\b", "a\0b", "a\nb", "a\u{85}b"] {
            assert!(!valid_name(name));
        }
        for name in ["a", "file.txt", "...", "a b", "日本語"] {
            assert!(valid_name(name));
        }
    }

    #[test]
    fn pinsets_canonical_order_deduplication_and_invalid_inputs() {
        let catalog = raw_cid(b"catalog");
        let a = raw_cid(b"a");
        let b = raw_cid(b"b");
        let (cid, bytes) = build_pinset(
            &catalog,
            &[b.clone(), a.clone(), b.clone(), catalog.clone()],
        )
        .unwrap();
        let mut objects = vec![a, b];
        objects.sort();
        let expected = vec![catalog.clone(), objects[0].clone(), objects[1].clone()];
        assert_eq!(parse_pinset(&bytes).unwrap().roots, expected);
        assert!(verify_raw(&cid, &bytes));
        assert_eq!(
            bytes,
            format!(
                "{{\"mvp\":1,\"roots\":{}}}",
                serde_json::to_string(&expected).unwrap()
            )
            .as_bytes()
        );
        for roots in [
            vec![],
            vec!["babc".into()],
            vec![catalog.clone(), catalog.clone()],
            vec![catalog.clone(), objects[1].clone(), objects[0].clone()],
            vec![catalog.clone(); MAX_PINSET_ROOTS + 1],
        ] {
            assert!(parse_pinset(&serde_json::to_vec(&Pinset { mvp: 1, roots }).unwrap()).is_err());
        }
        assert!(build_pinset("bad", &[]).is_err());
        assert!(build_pinset(&catalog, &["bad".into()]).is_err());
        assert!(parse_pinset(&vec![b' '; MAX_PINSET_BYTES + 1]).is_err());
        assert!(
            parse_pinset(
                &serde_json::to_vec(&Pinset {
                    mvp: 2,
                    roots: vec![catalog]
                })
                .unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn pinset_exact_root_limit() {
        let catalog = raw_cid(b"catalog");
        let objects: Vec<_> = (0..MAX_PINSET_ROOTS)
            .map(|i| raw_cid(&i.to_be_bytes()))
            .collect();
        let (_, bytes) = build_pinset(&catalog, &objects[..MAX_PINSET_ROOTS - 1]).unwrap();
        assert_eq!(parse_pinset(&bytes).unwrap().roots.len(), MAX_PINSET_ROOTS);
        assert!(build_pinset(&catalog, &objects).is_err());
    }

    #[test]
    fn raw_cid_rejects_wrong_codec_and_mutation() {
        let cid = raw_cid(b"hello");
        assert!(verify_raw(&cid, b"hello"));
        assert!(!verify_raw(&cid, b"changed"));
        assert!(!verify_raw(&compute_cid(b"hello", 0x71), b"hello"));
        for cid in ["", "babc", "../../secret"] {
            assert!(!verify_raw(cid, b"hello"));
        }
    }
}
