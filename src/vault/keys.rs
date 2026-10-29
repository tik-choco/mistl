//! Owner keys derived from a separate recovery secret.
use crate::identity::Identity;
use anyhow::{Context, Result, ensure};
use ed25519_dalek::SigningKey;
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::Path;

/// The recovery secret plus everything derived from it.
pub struct OwnerKey {
    secret: [u8; 32],
}

impl OwnerKey {
    /// A fresh random owner key.
    pub fn generate() -> Self {
        let mut secret = [0; 32];
        OsRng.fill_bytes(&mut secret);
        Self { secret }
    }

    pub fn from_secret(secret: [u8; 32]) -> Self {
        Self { secret }
    }

    /// Parse mvk1-<64 hex>-<8 hex checksum>; whitespace and case are ignored.
    pub fn from_recovery_code(code: &str) -> Result<Self> {
        let mut normalized = String::with_capacity(78);
        for c in code.chars().filter(|c| !c.is_whitespace()) {
            ensure!(
                c.is_ascii() && normalized.len() < 78,
                "invalid recovery code"
            );
            normalized.push(c.to_ascii_lowercase());
        }
        ensure!(normalized.len() == 78, "invalid recovery code length");
        ensure!(
            normalized.starts_with("mvk1-") && &normalized[69..70] == "-",
            "invalid recovery code format"
        );
        let mut secret = [0; 32];
        hex::decode_to_slice(&normalized[5..69], &mut secret)
            .map_err(|_| anyhow::anyhow!("invalid recovery secret encoding"))?;
        let checksum = hex::encode(Sha256::digest(secret));
        ensure!(
            normalized[70..] == checksum[..8],
            "recovery code checksum mismatch"
        );
        Ok(Self { secret })
    }

    pub fn recovery_code(&self) -> String {
        let checksum = hex::encode(Sha256::digest(self.secret));
        format!("mvk1-{}-{}", hex::encode(self.secret), &checksum[..8])
    }

    pub fn secret(&self) -> &[u8; 32] {
        &self.secret
    }

    /// AES-256-GCM key for the catalog.
    pub fn enc_key(&self) -> [u8; 32] {
        derive(b"mistl-vault-enc-v1", &self.secret)
    }

    /// Signing identity (fromId = owner did:key) for heads.
    pub fn identity(&self) -> Identity {
        let seed = derive(b"mistl-vault-sign-v1", &self.secret);
        crate::identity::from_signing_key(SigningKey::from_bytes(&seed))
    }

    pub fn owner_did(&self) -> String {
        self.identity().did().to_owned()
    }

    /// Load <dir>/owner.key (recovery code text), if present.
    pub fn load(dir: &Path) -> Result<Option<Self>> {
        let path = dir.join("owner.key");
        let file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err).context("opening owner key"),
        };
        crate::statefile::restrict_existing(&path);
        let mut text = String::new();
        file.take(4097)
            .read_to_string(&mut text)
            .context("reading owner key")?;
        ensure!(text.len() <= 4096, "owner key file too large");
        Self::from_recovery_code(&text).map(Some)
    }

    /// Persist to <dir>/owner.key with private, atomic replacement.
    pub fn save(&self, dir: &Path) -> Result<()> {
        crate::statefile::write_private(&dir.join("owner.key"), self.recovery_code().as_bytes())
            .context("saving owner key")
    }
}

fn derive(domain: &[u8], secret: &[u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(secret);
    hash.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_roundtrip_and_validation() {
        let key = OwnerKey::from_secret([0xab; 32]);
        let code = key.recovery_code();
        assert_eq!(code.len(), 78);
        let spaced = code
            .to_uppercase()
            .chars()
            .map(|c| format!(" {c}\n"))
            .collect::<String>();
        assert_eq!(
            OwnerKey::from_recovery_code(&spaced).unwrap().secret(),
            key.secret()
        );
        for bad in [
            "".to_owned(),
            format!("{code}0"),
            code.replacen("mvk1", "mvk2", 1),
            code.replacen("ab", "ac", 1),
            "é".repeat(78),
        ] {
            assert!(OwnerKey::from_recovery_code(&bad).is_err());
        }
    }

    #[test]
    fn derivation_matches_domains_and_signs() {
        let key = OwnerKey::from_secret([7; 32]);
        let mut enc = b"mistl-vault-enc-v1".to_vec();
        enc.extend_from_slice(key.secret());
        let expected: [u8; 32] = Sha256::digest(&enc).into();
        assert_eq!(key.enc_key(), expected);
        let mut sign = b"mistl-vault-sign-v1".to_vec();
        sign.extend_from_slice(key.secret());
        let seed: [u8; 32] = Sha256::digest(sign).into();
        assert_eq!(
            key.identity().verifying_key(),
            SigningKey::from_bytes(&seed).verifying_key()
        );
        let identity = key.identity();
        assert_eq!(key.owner_did(), identity.did());
        assert!(crate::identity::verify(identity.did(), b"head", &identity.sign(b"head")).unwrap());
        assert_ne!(OwnerKey::generate().secret(), OwnerKey::generate().secret());
    }

    #[test]
    fn private_key_persistence_and_corruption() {
        let dir =
            std::env::temp_dir().join(format!("mistl-vault-key-{:016x}", rand::random::<u64>()));
        assert!(OwnerKey::load(&dir).unwrap().is_none());
        let key = OwnerKey::generate();
        key.save(&dir).unwrap();
        assert_eq!(
            OwnerKey::load(&dir).unwrap().unwrap().secret(),
            key.secret()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.join("owner.key"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        OwnerKey::from_secret([1; 32]).save(&dir).unwrap();
        assert_eq!(OwnerKey::load(&dir).unwrap().unwrap().secret(), &[1; 32]);
        std::fs::write(dir.join("owner.key"), b"invalid").unwrap();
        assert!(OwnerKey::load(&dir).is_err());
        std::fs::write(dir.join("owner.key"), vec![b' '; 4097]).unwrap();
        assert!(OwnerKey::load(&dir).is_err());
        std::fs::remove_file(dir.join("owner.key")).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }
}
