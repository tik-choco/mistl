//! tc-storage-compatible identity transfer and local replacement backups.

use super::*;
use crate::storage::crypto::{EncryptedPayload, decrypt_json_async, encrypt_json};

fn identity_document(identity: &Identity) -> Result<Value> {
    // WebCrypto exports RFC 8410's seed-only PKCS8 v1. Emit that form too;
    // some browser importers reject dalek's PKCS8 v2 public-key extension.
    let mut der = vec![
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ];
    der.extend_from_slice(&identity.signing_key.to_bytes());
    Ok(json!({
        "did": identity.did(),
        "method": "did:key",
        "keyType": "Ed25519",
        "publicKeyMultibase": identity.did().strip_prefix("did:key:").unwrap(),
        "privateKeyPkcs8": BASE64.encode(der),
        "createdAt": identity.created_at(),
    }))
}

pub(super) fn validate_export_passphrase(passphrase: &str) -> Result<()> {
    if passphrase.chars().count() < 8 {
        bail!("export passphrase must contain at least 8 characters");
    }
    Ok(())
}

pub(super) fn export(identity: &Identity, passphrase: &str) -> Result<Value> {
    validate_export_passphrase(passphrase)?;
    let envelope = encrypt_json(&identity_document(identity)?, passphrase)?;
    Ok(json!({"did": identity.did(), "envelope": envelope}))
}

fn validate_document(value: &Value) -> Result<Identity> {
    let encoded = value
        .get("privateKeyPkcs8")
        .and_then(Value::as_str)
        .context("missing `privateKeyPkcs8`")?;
    let der = BASE64
        .decode(encoded)
        .context("invalid privateKeyPkcs8 base64")?;
    // dalek accepts both WebCrypto's seed-only PKCS8 v1 and its own encoding.
    let signing_key = SigningKey::from_pkcs8_der(&der).context("invalid Ed25519 PKCS8 key")?;
    let did = did_from_pubkey(&signing_key.verifying_key().to_bytes());
    if value.get("did").and_then(Value::as_str) != Some(did.as_str()) {
        bail!("imported DID does not match the private key (expected {did})");
    }
    for (field, expected) in [
        ("method", "did:key"),
        ("keyType", "Ed25519"),
        ("publicKeyMultibase", did.strip_prefix("did:key:").unwrap()),
    ] {
        if value
            .get(field)
            .is_some_and(|v| v.as_str() != Some(expected))
        {
            bail!("imported {field} does not match the Ed25519 key");
        }
    }
    let created_at = value
        .get("createdAt")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| Utc::now().to_rfc3339());
    Ok(Identity {
        did,
        signing_key,
        created_at,
    })
}

pub(super) async fn decode_import(args: &Value) -> Result<Identity> {
    let data = args.get("data").context("missing `data`")?;
    let mut value = match data {
        Value::String(text) => serde_json::from_str(text).context("parsing identity JSON")?,
        value => value.clone(),
    };
    if value.get("cipherText").is_some() {
        let passphrase = args
            .get("passphrase")
            .and_then(Value::as_str)
            .filter(|p| !p.is_empty())
            .context("passphrase is required for encrypted identity")?;
        let envelope: EncryptedPayload =
            serde_json::from_value(value).context("invalid encrypted identity envelope")?;
        value = decrypt_json_async(envelope, passphrase.to_string()).await?;
    }
    validate_document(&value)
}

pub(super) fn import_into(
    dir: &Path,
    identity: &Identity,
    force: bool,
    dry_run: bool,
) -> Result<Value> {
    let previous = load_identity_in(dir)?;
    let previous_did = previous.as_ref().map(Identity::did);
    let changed = previous_did != Some(identity.did());
    if changed && previous.is_some() && !force && !dry_run {
        bail!(
            "replacing identity {} with {} requires force=true",
            previous_did.unwrap(),
            identity.did()
        );
    }
    let mut result = json!({
        "did": identity.did(), "previous_did": previous_did,
        "changed": changed, "restart_required": changed && !dry_run,
    });
    if !changed || dry_run {
        return Ok(result);
    }

    let der = identity
        .signing_key
        .to_pkcs8_der()
        .context("encoding PKCS8 key")?;
    let meta = IdentityMeta {
        did: identity.did().to_string(),
        method: "did:key".into(),
        key_type: "Ed25519".into(),
        public_key_multibase: identity.did().strip_prefix("did:key:").unwrap().to_string(),
        created_at: identity.created_at().to_string(),
    };
    let meta_bytes = serde_json::to_vec_pretty(&meta)?;
    if previous.is_some() {
        let backup = dir
            .join("backup")
            .join(Utc::now().format("%Y%m%dT%H%M%SZ").to_string());
        // Never replace a previous backup if two imports land in the same second.
        crate::statefile::create_private_dir(backup.parent().unwrap())?;
        let builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        let builder = {
            use std::os::unix::fs::DirBuilderExt;
            let mut builder = builder;
            builder.mode(0o700);
            builder
        };
        match builder.create(&backup) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                bail!("identity backup already exists; retry the import in one second");
            }
            Err(e) => return Err(e).context("creating identity backup"),
        }
        for name in ["key.pkcs8.b64", "meta.json"] {
            let bytes =
                std::fs::read(dir.join(name)).with_context(|| format!("reading old {name}"))?;
            crate::statefile::write_private(&backup.join(name), &bytes)?;
        }
        result["backup_dir"] = json!(backup);
    }
    crate::statefile::write_private(
        &dir.join("key.pkcs8.b64"),
        BASE64.encode(der.as_bytes()).as_bytes(),
    )?;
    crate::statefile::write_private(&dir.join("meta.json"), &meta_bytes)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "mistl-identity-import-{:016x}",
                rand::random::<u64>()
            )))
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn plain_identity_round_trip_preserves_key_and_created_at() {
        let original = for_test();
        let document = identity_document(&original).unwrap();
        assert_eq!(
            BASE64
                .decode(document["privateKeyPkcs8"].as_str().unwrap())
                .unwrap()
                .len(),
            48
        );
        let imported = decode_import(&json!({"data": document.to_string()}))
            .await
            .unwrap();
        let dir = TestDir::new();
        let result = import_into(&dir.0, &imported, false, false).unwrap();
        assert_eq!(result["did"], original.did());
        assert_eq!(result["previous_did"], Value::Null);
        assert_eq!(result["changed"], true);
        assert_eq!(result["restart_required"], true);
        assert!(result.get("backup_dir").is_none());
        let loaded = load_identity_in(&dir.0).unwrap().unwrap();
        assert_eq!(loaded.did(), original.did());
        assert_eq!(loaded.created_at(), original.created_at());
        assert_eq!(loaded.sign(b"roundtrip"), original.sign(b"roundtrip"));
    }

    #[tokio::test]
    async fn webcrypto_pkcs8_v1_seed_is_accepted() {
        let seed = [42; 32];
        let mut der = vec![
            0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22,
            0x04, 0x20,
        ];
        der.extend_from_slice(&seed);
        let document = json!({"did": did_for_seed(42), "privateKeyPkcs8": BASE64.encode(der)});
        let identity = decode_import(&json!({"data": document})).await.unwrap();
        assert_eq!(identity.did(), did_for_seed(42));
        assert_eq!(identity.signing_key.to_bytes(), seed);
        assert!(chrono::DateTime::parse_from_rfc3339(identity.created_at()).is_ok());
        let dalek_der = identity.signing_key.to_pkcs8_der().unwrap();
        let dalek =
            json!({"did": identity.did(), "privateKeyPkcs8": BASE64.encode(dalek_der.as_bytes())});
        assert_eq!(validate_document(&dalek).unwrap().did(), identity.did());
    }

    #[tokio::test]
    async fn envelope_round_trip_and_passphrase_errors() {
        let original = for_test();
        let exported = export(&original, "test passphrase").unwrap();
        assert!(exported.get("privateKeyPkcs8").is_none());
        assert!(exported["envelope"].get("privateKeyPkcs8").is_none());
        let data = &exported["envelope"];
        let imported = decode_import(&json!({"data": data, "passphrase": "test passphrase"}))
            .await
            .unwrap();
        let dir = TestDir::new();
        import_into(&dir.0, &imported, false, false).unwrap();
        assert_eq!(
            load_identity_in(&dir.0).unwrap().unwrap().did(),
            original.did()
        );
        assert!(
            decode_import(&json!({"data": data, "passphrase": "wrong passphrase"}))
                .await
                .is_err()
        );
        let error = decode_import(&json!({"data": data})).await.err().unwrap();
        assert!(error.to_string().contains("passphrase"));
        assert!(
            decode_import(&json!({"data": data, "passphrase": ""}))
                .await
                .is_err()
        );
    }

    #[test]
    fn mismatched_identity_fields_and_invalid_keys_are_rejected() {
        let original = for_test();
        let document = identity_document(&original).unwrap();
        for (field, bad) in [
            ("did", did_for_seed(123)),
            (
                "publicKeyMultibase",
                did_for_seed(123).trim_start_matches("did:key:").to_string(),
            ),
            ("method", "did:web".to_string()),
            ("keyType", "RSA".to_string()),
            ("privateKeyPkcs8", "invalid!".to_string()),
            ("privateKeyPkcs8", BASE64.encode([0; 48])),
        ] {
            let mut invalid = document.clone();
            invalid[field] = json!(bad);
            assert!(
                validate_document(&invalid).is_err(),
                "accepted invalid {field}"
            );
        }
    }

    #[test]
    fn force_creates_exact_backup_and_preserves_profile_and_delegations() {
        let dir = TestDir::new();
        let original = for_test();
        let replacement = for_test();
        import_into(&dir.0, &original, false, false).unwrap();
        let old_key = std::fs::read(dir.0.join("key.pkcs8.b64")).unwrap();
        let old_meta = std::fs::read(dir.0.join("meta.json")).unwrap();
        for name in ["profile.json", "delegations.json"] {
            crate::statefile::write_private(&dir.0.join(name), b"keep unchanged").unwrap();
        }
        let error = import_into(&dir.0, &replacement, false, false)
            .unwrap_err()
            .to_string();
        assert!(error.contains(original.did()));
        assert!(error.contains(replacement.did()));
        assert!(error.contains("force"));
        assert!(!dir.0.join("backup").exists());

        let dry = import_into(&dir.0, &replacement, false, true).unwrap();
        assert_eq!(dry["changed"], true);
        assert_eq!(dry["restart_required"], false);
        assert_eq!(std::fs::read(dir.0.join("key.pkcs8.b64")).unwrap(), old_key);
        assert_eq!(std::fs::read(dir.0.join("meta.json")).unwrap(), old_meta);
        assert!(!dir.0.join("backup").exists());

        let result = import_into(&dir.0, &replacement, true, false).unwrap();
        assert_eq!(result["previous_did"], original.did());
        assert_eq!(result["restart_required"], true);
        let backup = PathBuf::from(result["backup_dir"].as_str().unwrap());
        assert_eq!(backup.parent().unwrap(), dir.0.join("backup"));
        assert!(
            chrono::NaiveDateTime::parse_from_str(
                backup.file_name().unwrap().to_str().unwrap(),
                "%Y%m%dT%H%M%SZ"
            )
            .is_ok()
        );
        assert_eq!(
            std::fs::read(backup.join("key.pkcs8.b64")).unwrap(),
            old_key
        );
        assert_eq!(std::fs::read(backup.join("meta.json")).unwrap(), old_meta);
        assert_eq!(
            load_identity_in(&dir.0).unwrap().unwrap().did(),
            replacement.did()
        );
        for name in ["profile.json", "delegations.json"] {
            assert_eq!(std::fs::read(dir.0.join(name)).unwrap(), b"keep unchanged");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [backup.join("key.pkcs8.b64"), dir.0.join("key.pkcs8.b64")] {
                assert_eq!(
                    std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
        }
    }

    #[test]
    fn empty_dry_run_and_same_did_write_nothing() {
        let dir = TestDir::new();
        let original = for_test();
        let result = import_into(&dir.0, &original, false, true).unwrap();
        assert_eq!(result["changed"], true);
        assert_eq!(result["restart_required"], false);
        assert!(!dir.0.exists());
        import_into(&dir.0, &original, false, false).unwrap();
        let old_key = std::fs::read(dir.0.join("key.pkcs8.b64")).unwrap();
        let old_meta = std::fs::read(dir.0.join("meta.json")).unwrap();
        let mut same = validate_document(&identity_document(&original).unwrap()).unwrap();
        same.created_at = "2000-01-01T00:00:00Z".into();
        let result = import_into(&dir.0, &same, false, false).unwrap();
        assert_eq!(result["changed"], false);
        assert_eq!(result["restart_required"], false);
        assert_eq!(std::fs::read(dir.0.join("key.pkcs8.b64")).unwrap(), old_key);
        assert_eq!(std::fs::read(dir.0.join("meta.json")).unwrap(), old_meta);
        assert!(!dir.0.join("backup").exists());
    }

    #[test]
    fn export_rejects_short_passphrases() {
        let identity = for_test();
        for passphrase in ["", "1234567", "短い文字列"] {
            assert!(export(&identity, passphrase).is_err());
        }
        assert!(validate_export_passphrase("12345678").is_ok());
    }
}
