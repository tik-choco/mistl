//! Persistent user intent. Applying a change restarts only this daemon, so
//! native P2P threads and established sockets cannot survive an OFF transition.
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    schema: u32,
    enabled: bool,
}
struct State {
    enabled: bool,
    active: bool,
    transitioning: bool,
    error: Option<String>,
}
pub struct NetworkControl {
    path: PathBuf,
    state: Mutex<State>,
}

impl NetworkControl {
    pub fn load(data_dir: &Path) -> Self {
        let path = data_dir.join("network-state.json");
        let (enabled, error) = match read_saved(&path) {
            Ok(v) => (v, None),
            Err(e) => (false, Some(format!("{e:#}"))),
        };
        Self {
            path,
            state: Mutex::new(State {
                enabled,
                active: enabled,
                transitioning: false,
                error,
            }),
        }
    }
    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            path: PathBuf::new(),
            state: Mutex::new(State {
                enabled: true,
                active: true,
                transitioning: false,
                error: None,
            }),
        }
    }
    pub fn permitted(&self) -> bool {
        let s = self.state.lock().expect("network lock poisoned");
        s.active && !s.transitioning
    }
    pub fn require_online(&self) -> Result<()> {
        if !self.permitted() {
            bail!("external connections are OFF or switching; use mistl network on");
        }
        Ok(())
    }
    pub fn status(&self) -> Value {
        let s = self.state.lock().expect("network lock poisoned");
        json!({"enabled":s.enabled, "state":if s.transitioning { "restarting" } else if s.active { "on" } else { "off" },
            "saved":s.error.is_none(), "error":s.error, "scope":"external", "restart_required":s.transitioning})
    }
    /// Returns true when a restart is needed. No network is enabled before durable save.
    pub fn set(&self, enabled: bool) -> Result<bool> {
        let mut s = self.state.lock().expect("network lock poisoned");
        if s.transitioning {
            bail!("connection mode is switching; wait for daemon restart");
        }
        let was_active = s.active;
        if !enabled {
            s.active = false;
        }
        let bytes = serde_json::to_vec(&Saved { schema: 1, enabled })?;
        if let Err(err) = crate::statefile::write_bytes(&self.path, &bytes) {
            s.error = Some(format!("connection setting was not saved: {err:#}"));
            // OFF must stop current traffic even when persistence fails.
            s.transitioning = !enabled && was_active;
            return Err(err).context("connection setting was not saved");
        }
        s.error = None;
        let changed = s.enabled != enabled || was_active != enabled;
        s.enabled = enabled;
        s.transitioning = changed;
        Ok(changed)
    }
}

fn read_saved(path: &Path) -> Result<bool> {
    if !path.exists() {
        // First use and legacy upgrade both start offline until explicitly enabled.
        crate::statefile::write_bytes(
            path,
            &serde_json::to_vec(&Saved {
                schema: 1,
                enabled: false,
            })?,
        )?;
        return Ok(false);
    }
    let saved: Saved = serde_json::from_slice(&std::fs::read(path)?)
        .context("invalid connection state; staying offline")?;
    if saved.schema != 1 {
        bail!("unsupported connection state schema; staying offline");
    }
    Ok(saved.enabled)
}

pub fn offline_command_allowed(cmd: &str) -> bool {
    matches!(
        cmd,
        "daemon.status"
            | "daemon.stop"
            | "daemon.restart"
            | "network.status"
            | "network.set"
            | "config.show"
            | "config.set"
            | "logs.tail"
            | "logs.clear"
            | "install.status"
            | "install.install"
            | "install.uninstall"
            | "autostart.status"
            | "autostart.enable"
            | "autostart.disable"
            | "profile.show"
            | "profile.set"
            | "key.did"
            | "key.list"
            | "key.generate"
            | "key.delegations"
            | "key.delegate"
            | "ai.status"
            | "stream.status"
            | "tunnel.status"
            | "update.status"
            | "topology.status"
            | "consensus.status"
            | "chat.rooms"
            | "chat.log"
            | "bot.list"
            | "bot.status"
            | "bot.logs"
            | "bot.items"
            | "bot.options"
            | "sched.ls"
            | "sched.logs"
            | "sched.next"
            | "tunnel.room.list"
            | "store.put"
            | "store.get"
            | "store.ls"
            | "store.sandbox.import"
            | "store.sandbox.export"
            | "store.sandbox.ls"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn dir() -> PathBuf {
        let p = std::env::temp_dir().join(format!("mistl-network-{:016x}", rand::random::<u64>()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
    #[test]
    fn survives_restarts_and_keeps_intent_separate() {
        let dir = dir();
        let n = NetworkControl::load(&dir);
        assert!(!n.permitted());
        assert!(n.set(true).unwrap());
        assert!(!n.permitted());
        let n = NetworkControl::load(&dir);
        assert!(n.permitted());
        assert!(n.set(false).unwrap());
        assert!(!n.permitted());
        assert!(n.set(true).is_err());
        assert!(!NetworkControl::load(&dir).permitted());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn corrupt_state_stays_off() {
        let dir = dir();
        std::fs::write(dir.join("network-state.json"), b"garbage").unwrap();
        let n = NetworkControl::load(&dir);
        assert!(!n.permitted());
        assert!(!n.status()["saved"].as_bool().unwrap());
        assert!(n.set(true).unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn save_failure_never_enables_network() {
        let dir = dir();
        let n = NetworkControl::load(&dir);
        std::fs::remove_file(dir.join("network-state.json")).unwrap();
        std::fs::create_dir(dir.join("network-state.json")).unwrap();
        assert!(n.set(true).is_err());
        assert!(!n.permitted());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn offline_blocks_side_effects() {
        for cmd in [
            "key.pair.start",
            "ai.chat",
            "sched.run",
            "update.check",
            "update.apply",
            "store.connect",
            "stream.start",
            "bot.run",
        ] {
            assert!(!offline_command_allowed(cmd));
        }
        assert!(offline_command_allowed("store.ls"));
    }
}
