//! Passphrase envelope encryption, byte-compatible with tc-storage's
//! `src/crypto/crypto.ts` (and the Go reimplementation in
//! `tools/storage-cli/internal/protocol/crypto.go`):
//!
//! ```json
//! {
//!   "version": 1,
//!   "algorithm": "AES-GCM",
//!   "kdf": "PBKDF2-SHA256",
//!   "iterations": 210000,
//!   "salt": "<base64 standard, 16 bytes>",
//!   "iv": "<base64 standard, 12 bytes>",
//!   "cipherText": "<base64 standard, AES-256-GCM ciphertext with the 16-byte tag appended>"
//! }
//! ```
//!
//! Key derivation: PBKDF2-HMAC-SHA256 over the UTF-8 passphrase bytes with a
//! 16-byte random salt, producing a 32-byte AES-256 key. The nonce/IV is 12
//! random bytes. `aes_gcm`'s `Aead::encrypt`/`decrypt` append/expect the
//! 16-byte GCM tag at the end of the ciphertext, matching Web Crypto's
//! `AES-GCM` and Go's `cipher.AEAD.Seal`.

// Interop API retained for tc-storage-compatible encrypted identity export;
// not yet wired to a CLI path, so allow it to sit unused for now.
#![allow(dead_code)]

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anyhow::{Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

const ITERATIONS: u32 = 210_000;
const MIN_ITERATIONS: u32 = 100_000;
const MAX_ITERATIONS: u32 = 1_000_000;
const SALT_LEN: usize = 16;
const IV_LEN: usize = 12;
const KEY_LEN: usize = 32;

#[derive(Debug, Serialize, Deserialize)]
struct Envelope {
    version: u32,
    algorithm: String,
    kdf: String,
    iterations: u32,
    salt: String,
    iv: String,
    #[serde(rename = "cipherText")]
    cipher_text: String,
}

fn derive_key(passphrase: &str, salt: &[u8], iterations: u32) -> [u8; KEY_LEN] {
    let mut key = [0u8; KEY_LEN];
    pbkdf2_hmac::<Sha256>(passphrase.as_bytes(), salt, iterations, &mut key);
    key
}

/// Encrypt `plaintext` under `passphrase`, returning the JSON envelope string.
pub fn encrypt(passphrase: &str, plaintext: &[u8]) -> Result<String> {
    let passphrase = passphrase.trim();
    if passphrase.is_empty() {
        bail!("passphrase is required");
    }

    let mut rng = rand::thread_rng();
    let mut salt = [0u8; SALT_LEN];
    let mut iv = [0u8; IV_LEN];
    rng.fill_bytes(&mut salt);
    rng.fill_bytes(&mut iv);

    let key = derive_key(passphrase, &salt, ITERATIONS);
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| anyhow!("invalid key length: {e}"))?;
    let nonce = Nonce::from_slice(&iv);
    let cipher_text = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| anyhow!("encryption failed"))?;

    let envelope = Envelope {
        version: 1,
        algorithm: "AES-GCM".into(),
        kdf: "PBKDF2-SHA256".into(),
        iterations: ITERATIONS,
        salt: BASE64.encode(salt),
        iv: BASE64.encode(iv),
        cipher_text: BASE64.encode(cipher_text),
    };
    Ok(serde_json::to_string(&envelope)?)
}

/// Decrypt a JSON envelope (as produced by [`encrypt`]) with `passphrase`.
pub fn decrypt(passphrase: &str, envelope_json: &str) -> Result<Vec<u8>> {
    let passphrase = passphrase.trim();
    if passphrase.is_empty() {
        bail!("passphrase is required");
    }

    let envelope: Envelope =
        serde_json::from_str(envelope_json).map_err(|e| anyhow!("invalid envelope JSON: {e}"))?;

    if envelope.version != 1 || envelope.algorithm != "AES-GCM" || envelope.kdf != "PBKDF2-SHA256" {
        bail!("unsupported encryption format");
    }
    if envelope.iterations < MIN_ITERATIONS || envelope.iterations > MAX_ITERATIONS {
        bail!("invalid encryption iterations");
    }

    let salt = BASE64
        .decode(&envelope.salt)
        .map_err(|e| anyhow!("invalid salt: {e}"))?;
    let iv = BASE64.decode(&envelope.iv).map_err(|e| anyhow!("invalid iv: {e}"))?;
    let cipher_text = BASE64
        .decode(&envelope.cipher_text)
        .map_err(|e| anyhow!("invalid ciphertext: {e}"))?;

    if salt.len() != SALT_LEN {
        bail!("invalid salt");
    }
    if iv.len() != IV_LEN {
        bail!("invalid iv");
    }
    if cipher_text.is_empty() {
        bail!("invalid ciphertext");
    }

    let key = derive_key(passphrase, &salt, envelope.iterations);
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| anyhow!("invalid key length: {e}"))?;
    let nonce = Nonce::from_slice(&iv);
    cipher
        .decrypt(nonce, cipher_text.as_ref())
        .map_err(|_| anyhow!("decryption failed (wrong passphrase, or corrupted data)"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let envelope = encrypt("correct horse battery staple", b"hello world").unwrap();
        let plain = decrypt("correct horse battery staple", &envelope).unwrap();
        assert_eq!(plain, b"hello world");
    }

    #[test]
    fn wrong_passphrase_fails() {
        let envelope = encrypt("right", b"secret data").unwrap();
        assert!(decrypt("wrong", &envelope).is_err());
    }

    #[test]
    fn envelope_shape_matches_tc_storage() {
        let envelope = encrypt("pw", b"data").unwrap();
        let value: serde_json::Value = serde_json::from_str(&envelope).unwrap();
        assert_eq!(value["version"], 1);
        assert_eq!(value["algorithm"], "AES-GCM");
        assert_eq!(value["kdf"], "PBKDF2-SHA256");
        assert_eq!(value["iterations"], 210_000);
        assert!(value["salt"].is_string());
        assert!(value["iv"].is_string());
        assert!(value["cipherText"].is_string());

        use base64::Engine as _;
        let salt = base64::engine::general_purpose::STANDARD
            .decode(value["salt"].as_str().unwrap())
            .unwrap();
        let iv = base64::engine::general_purpose::STANDARD
            .decode(value["iv"].as_str().unwrap())
            .unwrap();
        assert_eq!(salt.len(), 16);
        assert_eq!(iv.len(), 12);
    }

    #[test]
    fn empty_passphrase_rejected() {
        assert!(encrypt("", b"data").is_err());
        assert!(encrypt("   ", b"data").is_err());
    }
}
