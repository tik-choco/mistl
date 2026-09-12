//! WebCrypto-compatible symmetric encryption for values persisted alongside
//! the content-addressed [`super::Store`].
//!
//! Byte-compatible with the tc-storage web app's `EncryptedPayload` format
//! (browser `SubtleCrypto` PBKDF2 + AES-GCM) and with the Go reference
//! implementation at `tools/storage-cli/internal/protocol/crypto.go` in the
//! tc-storage repo. The exact contract:
//!
//! - KDF: PBKDF2-HMAC-SHA256, 210000 iterations, 32-byte derived key.
//! - Cipher: AES-256-GCM, 16-byte random salt, 12-byte random IV/nonce. GCM
//!   output is `ciphertext || tag` -- the `aes-gcm` crate's `encrypt` already
//!   appends the 16-byte tag, matching both the browser's
//!   `crypto.subtle.encrypt` and Go's `cipher.AEAD.Seal`.
//! - Encoding: base64 *standard* alphabet with padding (not URL-safe) for
//!   `salt`, `iv`, and `cipherText`.
//! - Plaintext is the UTF-8 JSON serialization of the value being encrypted.
//!
//! [`EncryptedPayload`]'s field names are part of the wire contract and must
//! match the web app / Go struct exactly, including the capital `T` in
//! `cipherText`. Do not rename them without updating every producer/consumer
//! of this format.

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rand::RngCore;
use serde::{Deserialize, Serialize};

/// PBKDF2 iteration count used by `encrypt_json` (matches the web app's
/// `crypto.subtle.deriveKey` call and the Go reference's
/// `webCryptoIterations`).
const ITERATIONS: u32 = 210_000;
/// Lower bound accepted by [`EncryptedPayload::validate`] for a payload's
/// `iterations` field (guards against a maliciously/accidentally weak KDF).
const MIN_ITERATIONS: u32 = 100_000;
/// Upper bound accepted by [`EncryptedPayload::validate`].
const MAX_ITERATIONS: u32 = 1_000_000;

const SALT_LEN: usize = 16;
const IV_LEN: usize = 12;
const KEY_LEN: usize = 32;

/// On-disk / on-wire envelope for an AES-256-GCM encrypted value, byte- and
/// field-compatible with the tc-storage web app and Go CLI.
///
/// Field names are serialized verbatim (no `rename_all`) so they must be
/// written in the exact casing expected by the interop contract, in
/// particular `cipherText` (capital `T`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedPayload {
    /// Format version; currently always `1`.
    pub version: u32,
    /// Cipher identifier; always `"AES-GCM"`.
    pub algorithm: String,
    /// KDF identifier; always `"PBKDF2-SHA256"`.
    pub kdf: String,
    /// PBKDF2 iteration count used to derive the key.
    pub iterations: u32,
    /// Base64 (standard, padded) encoded 16-byte salt.
    pub salt: String,
    /// Base64 (standard, padded) encoded 12-byte IV/nonce.
    pub iv: String,
    /// Base64 (standard, padded) encoded ciphertext with the GCM tag
    /// appended (`ciphertext || tag`).
    #[serde(rename = "cipherText")]
    pub cipher_text: String,
}

impl EncryptedPayload {
    /// Reject payloads that don't match this module's exact interop
    /// contract: known version/algorithm/kdf identifiers, an iteration count
    /// in the sane `[100_000, 1_000_000]` range, a 16-byte salt, a 12-byte
    /// IV, and non-empty ciphertext.
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 || self.algorithm != "AES-GCM" || self.kdf != "PBKDF2-SHA256" {
            bail!("unsupported encryption format");
        }
        if self.iterations < MIN_ITERATIONS || self.iterations > MAX_ITERATIONS {
            bail!("invalid encryption iterations");
        }
        let salt = STANDARD
            .decode(&self.salt)
            .context("invalid salt encoding")?;
        if salt.len() != SALT_LEN {
            bail!("invalid salt");
        }
        let iv = STANDARD.decode(&self.iv).context("invalid iv encoding")?;
        if iv.len() != IV_LEN {
            bail!("invalid iv");
        }
        let cipher_text = STANDARD
            .decode(&self.cipher_text)
            .context("invalid cipherText encoding")?;
        if cipher_text.is_empty() {
            bail!("invalid ciphertext");
        }
        Ok(())
    }
}

/// Derive a 32-byte AES-256 key from `passphrase` and `salt` via
/// PBKDF2-HMAC-SHA256 with the given iteration count.
fn derive_key(passphrase: &str, salt: &[u8], iterations: u32) -> [u8; KEY_LEN] {
    let mut key = [0u8; KEY_LEN];
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(passphrase.as_bytes(), salt, iterations, &mut key);
    key
}

/// Serialize `value` to JSON and encrypt it with AES-256-GCM, deriving the
/// key from `passphrase` via PBKDF2-HMAC-SHA256 (210000 iterations) with a
/// fresh random 16-byte salt and 12-byte IV.
///
/// Produces a payload that any tc-storage-compatible reader (the web app's
/// `SubtleCrypto` decrypt path, or the Go `DecryptJSON`) can decrypt given
/// the same passphrase.
pub fn encrypt_json<T: Serialize>(value: &T, passphrase: &str) -> Result<EncryptedPayload> {
    if passphrase.is_empty() {
        bail!("passphrase is required");
    }
    let plaintext = serde_json::to_vec(value).context("serializing value to JSON")?;

    let mut salt = [0u8; SALT_LEN];
    rand::thread_rng().fill_bytes(&mut salt);
    let mut iv = [0u8; IV_LEN];
    rand::thread_rng().fill_bytes(&mut iv);

    let key = derive_key(passphrase, &salt, ITERATIONS);
    let cipher = Aes256Gcm::new_from_slice(&key).context("initializing AES-256-GCM cipher")?;
    let nonce = Nonce::from_slice(&iv);
    let cipher_text = cipher
        .encrypt(nonce, plaintext.as_slice())
        .map_err(|_| anyhow::anyhow!("encryption failed"))?;

    Ok(EncryptedPayload {
        version: 1,
        algorithm: "AES-GCM".to_string(),
        kdf: "PBKDF2-SHA256".to_string(),
        iterations: ITERATIONS,
        salt: STANDARD.encode(salt),
        iv: STANDARD.encode(iv),
        cipher_text: STANDARD.encode(cipher_text),
    })
}

/// Decrypt an [`EncryptedPayload`] produced by `encrypt_json` (or by the
/// tc-storage web app / Go CLI) with `passphrase`, then deserialize the
/// resulting JSON into `T`.
pub fn decrypt_json<T: serde::de::DeserializeOwned>(
    payload: &EncryptedPayload,
    passphrase: &str,
) -> Result<T> {
    if passphrase.is_empty() {
        bail!("passphrase is required");
    }
    payload.validate()?;

    let salt = STANDARD.decode(&payload.salt).context("decoding salt")?;
    let iv = STANDARD.decode(&payload.iv).context("decoding iv")?;
    let cipher_text = STANDARD
        .decode(&payload.cipher_text)
        .context("decoding cipherText")?;

    let key = derive_key(passphrase, &salt, payload.iterations);
    let cipher = Aes256Gcm::new_from_slice(&key).context("initializing AES-256-GCM cipher")?;
    let nonce = Nonce::from_slice(&iv);
    let plaintext = cipher
        .decrypt(nonce, cipher_text.as_slice())
        .map_err(|_| anyhow::anyhow!("decryption failed (wrong passphrase or corrupt data)"))?;

    serde_json::from_slice(&plaintext).context("deserializing decrypted JSON")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        id: u64,
        name: String,
        tags: Vec<String>,
    }

    fn sample() -> Sample {
        Sample {
            id: 42,
            name: "hello, mistl".to_string(),
            tags: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        }
    }

    /// Small and large values in one test: the payload size is not something
    /// this module branches on, and each extra `encrypt_json`/`decrypt_json`
    /// pair costs two 210000-iteration PBKDF2 derivations. Both values go
    /// through a single key derivation each here rather than two tests'
    /// worth.
    #[test]
    fn roundtrip_preserves_small_and_large_values() {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct Big {
            entries: Vec<Sample>,
            blob: String,
        }

        let passphrase = "correct horse battery staple";

        let small = sample();
        let payload = encrypt_json(&small, passphrase).unwrap();
        let decrypted: Sample = decrypt_json(&payload, passphrase).unwrap();
        assert_eq!(decrypted, small);

        let big = Big {
            entries: (0..500)
                .map(|i| Sample {
                    id: i,
                    name: format!("entry-{i}"),
                    tags: vec![format!("tag-{i}"), "shared".to_string()],
                })
                .collect(),
            blob: "x".repeat(100_000),
        };
        let payload = encrypt_json(&big, passphrase).unwrap();
        let decrypted: Big = decrypt_json(&payload, passphrase).unwrap();
        assert_eq!(decrypted, big);
    }

    #[test]
    fn wrong_passphrase_fails_to_decrypt() {
        let value = sample();
        let payload = encrypt_json(&value, "right passphrase").unwrap();
        let result: Result<Sample> = decrypt_json(&payload, "wrong passphrase");
        assert!(result.is_err());
    }

    #[test]
    fn empty_passphrase_errors_on_encrypt_and_decrypt() {
        let value = sample();
        assert!(encrypt_json(&value, "").is_err());

        let payload = encrypt_json(&value, "some passphrase").unwrap();
        let result: Result<Sample> = decrypt_json(&payload, "");
        assert!(result.is_err());
    }

    /// A correctly-shaped payload for the `validate()` tests below.
    ///
    /// Built as a literal rather than via `encrypt_json`: `validate()` never
    /// decrypts anything -- it only checks identifiers, an iteration range
    /// and base64 field lengths -- so deriving a real key here would spend a
    /// 210000-iteration PBKDF2 per test on a value that is immediately
    /// mutated into an invalid one. `encrypt_json_produces_spec_compliant_payload`
    /// is what pins the real encryptor's output *to* this shape, so these
    /// literals cannot drift away from reality unnoticed.
    fn valid_payload() -> EncryptedPayload {
        EncryptedPayload {
            version: 1,
            algorithm: "AES-GCM".to_string(),
            kdf: "PBKDF2-SHA256".to_string(),
            iterations: ITERATIONS,
            salt: STANDARD.encode([0u8; SALT_LEN]),
            iv: STANDARD.encode([0u8; IV_LEN]),
            cipher_text: STANDARD.encode([0u8; 32]),
        }
    }

    #[test]
    fn valid_payload_fixture_passes_validation() {
        // Guards the fixture itself: if `validate()` grows a new rule, this
        // fails loudly instead of every `validate_rejects_*` test below
        // silently passing for the wrong reason.
        valid_payload().validate().unwrap();
    }

    #[test]
    fn validate_rejects_wrong_version() {
        let mut payload = valid_payload();
        payload.version = 2;
        assert!(payload.validate().is_err());
    }

    #[test]
    fn validate_rejects_wrong_algorithm() {
        let mut payload = valid_payload();
        payload.algorithm = "AES-CBC".to_string();
        assert!(payload.validate().is_err());
    }

    #[test]
    fn validate_rejects_wrong_kdf() {
        let mut payload = valid_payload();
        payload.kdf = "PBKDF2-SHA1".to_string();
        assert!(payload.validate().is_err());
    }

    #[test]
    fn validate_rejects_out_of_range_iterations() {
        let mut too_low = valid_payload();
        too_low.iterations = 99_999;
        assert!(too_low.validate().is_err());

        let mut too_high = valid_payload();
        too_high.iterations = 1_000_001;
        assert!(too_high.validate().is_err());
    }

    #[test]
    fn validate_rejects_wrong_length_salt() {
        let mut payload = valid_payload();
        payload.salt = STANDARD.encode([0u8; 15]);
        assert!(payload.validate().is_err());
    }

    #[test]
    fn validate_rejects_wrong_length_iv() {
        let mut payload = valid_payload();
        payload.iv = STANDARD.encode([0u8; 11]);
        assert!(payload.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_ciphertext() {
        let mut payload = valid_payload();
        payload.cipher_text = STANDARD.encode(Vec::<u8>::new());
        assert!(payload.validate().is_err());
    }

    #[test]
    fn encrypt_json_produces_spec_compliant_payload() {
        let payload = encrypt_json(&sample(), "spec check passphrase").unwrap();
        assert_eq!(payload.version, 1);
        assert_eq!(payload.algorithm, "AES-GCM");
        assert_eq!(payload.kdf, "PBKDF2-SHA256");
        assert_eq!(payload.iterations, 210_000);
        assert_eq!(STANDARD.decode(&payload.salt).unwrap().len(), 16);
        assert_eq!(STANDARD.decode(&payload.iv).unwrap().len(), 12);
    }

    /// Guards the on-wire field name spelling (in particular the capital
    /// `T` in `cipherText`) against accidental renames: a hand-written JSON
    /// document using the documented field names must deserialize, and a
    /// round trip through it must decrypt correctly.
    #[test]
    fn deserializes_hand_written_json_with_ciphertext_field() {
        let payload = encrypt_json(&sample(), "field name guard").unwrap();
        let json = format!(
            r#"{{"version":1,"algorithm":"AES-GCM","kdf":"PBKDF2-SHA256","iterations":210000,"salt":"{}","iv":"{}","cipherText":"{}"}}"#,
            payload.salt, payload.iv, payload.cipher_text
        );
        let parsed: EncryptedPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.cipher_text, payload.cipher_text);

        let decrypted: Sample = decrypt_json(&parsed, "field name guard").unwrap();
        assert_eq!(decrypted, sample());
    }
}
