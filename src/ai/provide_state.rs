//! Cleanup for the retired global providing switch. Room flags in config.toml
//! are now the only persisted intent; old state never enables or blocks sharing.

use std::path::Path;

use anyhow::{Context, Result};

const FILE_NAME: &str = "ai-provide-state.json";

/// Remove even corrupt legacy JSON without reading or applying its flag.
/// Missing files are normal, and repeated migration is harmless.
pub fn migrate(data_dir: &Path) -> Result<()> {
    match std::fs::remove_file(data_dir.join(FILE_NAME)) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).context("ai: removing legacy provide state"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_discards_both_flag_values_and_corrupt_state_idempotently() {
        let dir = std::env::temp_dir().join(format!(
            "mistl-ai-provide-migration-{:016x}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        migrate(&dir).unwrap();
        let config_path = dir.join("config.toml");
        let config = "[[ai.providers]]\nbase_url = \"mist-network://room\"\nprovide = true\n";
        std::fs::write(&config_path, config).unwrap();
        for old_state in [
            r#"{"enabled":true}"#,
            r#"{"enabled":false}"#,
            "invalid JSON",
        ] {
            std::fs::write(dir.join(FILE_NAME), old_state).unwrap();
            migrate(&dir).unwrap();
            assert!(!dir.join(FILE_NAME).exists());
            assert_eq!(std::fs::read_to_string(&config_path).unwrap(), config);
            migrate(&dir).unwrap();
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn migration_reports_cleanup_failure() {
        let dir = std::env::temp_dir().join(format!(
            "mistl-ai-provide-migration-failure-{:016x}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(dir.join(FILE_NAME)).unwrap();
        assert!(migrate(&dir).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
