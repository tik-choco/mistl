//! Vault: encrypted datasets replicated across a trusted group of mistl
//! nodes. Owners encrypt locally; holders keep ciphertext blocks chosen by
//! weighted rendezvous placement and repair under-replicated data on their
//! own. See docs/vault.md.
#![allow(dead_code)]
mod blocks;
mod commands;
mod engine;
mod group;
mod head;
mod keys;
mod object;
mod placement;
mod provider;
mod replicate;
mod service;
mod state;
mod wire;

use crate::daemon::AppState;
use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;

/// Start replication when a group exists and the network is permitted.
pub fn spawn_background(state: Arc<AppState>) {
    engine::spawn_background(state);
}

/// `vault.*` IPC / dashboard commands.
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    engine::handle(cmd, args, state).await
}
