//! Tunnel integration: publish guest SSH as `vm-<server_id>-ssh`, client
//! connect forwards, and the owner gate consulted by the tunnel authorizer.
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};

use crate::daemon::{self, AppState};
use crate::tunnel::forward_args::{node_scoped_target, split_node_scope};

use super::store::VmRecord;
use super::store::valid_server_id;

static SSH_ACLS: LazyLock<Mutex<HashMap<String, Vec<String>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn ssh_target(server_id: &str) -> String {
    format!("vm-{server_id}-ssh")
}

/// Registers who may open `vm-<server_id>-ssh` on this executor.
pub fn register_acl(server_id: &str, dids: Vec<String>) {
    if let Ok(mut acls) = SSH_ACLS.lock() {
        acls.insert(server_id.to_owned(), dids);
    }
}

pub fn unregister_acl(server_id: &str) {
    if let Ok(mut acls) = SSH_ACLS.lock() {
        acls.remove(server_id);
    }
}

pub(super) fn register_record(record: &VmRecord) {
    let mut allowed = record.allowed_dids.clone();
    allowed.push(record.owner_did.clone());
    register_acl(&record.server_id, allowed);
}

fn ssh_server_id(target: &str) -> Option<&str> {
    let (base, _) = split_node_scope(target);
    base.strip_prefix("vm-")?.strip_suffix("-ssh")
}

/// Reserves the `vm-*-ssh` namespace for registered, verified DIDs.
pub fn authorize_vm_target(target: &str, verified_did: Option<&str>) -> Option<bool> {
    let server_id = ssh_server_id(target)?;
    if !valid_server_id(server_id) {
        return Some(false);
    }
    Some(verified_did.is_some_and(|did| {
        let Ok(acls) = SSH_ACLS.lock() else {
            return false;
        };
        acls.get(server_id)
            .is_some_and(|dids| dids.iter().any(|entry| entry == did))
    }))
}

async fn tunnel_call(state: &Arc<AppState>, cmd: &str, args: Value) -> Result<Value> {
    // The daemon dispatcher also handles vm.* calls, so break the future's size cycle.
    Box::pin(daemon::dispatch(cmd, args, state)).await
}

async fn add_ssh_forward(
    state: &Arc<AppState>,
    target: &str,
    direction: &str,
    port: u16,
    listen_port: i32,
) -> Result<()> {
    tunnel_call(
        state,
        "tunnel.forward.add",
        json!({
            "direction": direction,
            "proto": "tcp",
            "addr": format!("127.0.0.1:{port}"),
            "listen_port": listen_port,
            "target": target,
        }),
    )
    .await?;
    Ok(())
}

/// Adds the Serve forward when the tunnel is running. `Ok(false)` = tunnel
/// not running (the VM is then reachable only locally).
pub async fn publish_ssh(state: &Arc<AppState>, server_id: &str, ssh_port: u16) -> Result<bool> {
    let status = tunnel_call(state, "tunnel.status", json!({})).await?;
    if status["running"] != true {
        return Ok(false);
    }
    let target = ssh_target(server_id);
    if let Some(old) = status["forwards"].as_array().and_then(|rows| {
        rows.iter()
            .find(|r| r["target"] == target && r["direction"] == "serve")
    }) {
        if old["addr"] == format!("127.0.0.1:{ssh_port}") && old["state"] == "listening" {
            return Ok(true);
        }
        tunnel_call(state, "tunnel.forward.remove", json!({"target": target})).await?;
    }
    add_ssh_forward(state, &target, "serve", ssh_port, -1).await?;
    Ok(true)
}

pub async fn unpublish_ssh(state: &Arc<AppState>, server_id: &str) -> Result<()> {
    remove_ssh_forward(state, &ssh_target(server_id), "serve").await
}

async fn remove_ssh_forward(state: &Arc<AppState>, target: &str, direction: &str) -> Result<()> {
    let status = tunnel_call(state, "tunnel.status", json!({})).await?;
    if status["running"] == true
        && status["forwards"].as_array().is_some_and(|forwards| {
            forwards
                .iter()
                .any(|forward| forward["target"] == target && forward["direction"] == direction)
        })
    {
        tunnel_call(state, "tunnel.forward.remove", json!({ "target": target })).await?;
    }
    Ok(())
}

/// Creates a Connect forward on a free loopback port. Returns
/// `{"target": "...", "port": N}`.
pub async fn connect(state: &Arc<AppState>, server_id: &str, node: Option<&str>) -> Result<Value> {
    ensure!(valid_server_id(server_id), "invalid VM server id");
    let node = node.context("remote VM SSH requires --node <executor node id>")?;
    ensure!(
        !node.is_empty() && !node.contains('@') && !node.contains(char::is_whitespace),
        "invalid VM host node id"
    );
    let target = node_scoped_target(&ssh_target(server_id), node);
    let status = tunnel_call(state, "tunnel.status", json!({})).await?;
    if status["running"] != true {
        bail!("tunnel is not running (start it with `mistl tunnel start`)");
    }
    remove_ssh_forward(state, &target, "connect").await?;
    let probe = TcpListener::bind(("127.0.0.1", 0))
        .await
        .context("probing a free VM SSH port")?;
    let port = probe.local_addr()?.port();
    drop(probe);
    add_ssh_forward(state, &target, "connect", port, i32::from(port)).await?;

    let ready = tokio::time::timeout(Duration::from_secs(10), async {
        while TcpStream::connect(("127.0.0.1", port)).await.is_err() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    if ready.is_err() {
        if let Err(error) = disconnect(state, &target).await {
            tracing::warn!(%error, %target, "failed to remove VM SSH forward after readiness timeout");
        }
        bail!("VM SSH forward did not listen on 127.0.0.1:{port} within 10 seconds");
    }
    Ok(json!({ "target": target, "port": port }))
}

pub async fn disconnect(state: &Arc<AppState>, target: &str) -> Result<()> {
    if !ssh_server_id(target).is_some_and(valid_server_id) {
        bail!("invalid VM SSH target");
    }
    remove_ssh_forward(state, target, "connect").await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vm_target_requires_a_registered_verified_did() {
        let id = "0123456789abcdef";
        let target = ssh_target(id);
        let scoped = node_scoped_target(&target, "fedcba9876543210");
        let owner = "did:key:owner";
        let guest = "did:key:guest";
        register_acl(id, vec![owner.into(), guest.into()]);

        assert_eq!(authorize_vm_target(&target, Some(owner)), Some(true));
        assert_eq!(authorize_vm_target(&scoped, Some(guest)), Some(true));
        assert_eq!(authorize_vm_target(&target, None), Some(false));
        assert_eq!(
            authorize_vm_target(&scoped, Some("did:key:other")),
            Some(false)
        );

        register_acl(id, vec![guest.into()]);
        assert_eq!(authorize_vm_target(&target, Some(owner)), Some(false));
        unregister_acl(id);
        assert_eq!(authorize_vm_target(&scoped, Some(guest)), Some(false));
    }

    #[test]
    fn vm_namespace_fails_closed_and_other_targets_fall_through() {
        assert_eq!(
            authorize_vm_target("vm-invalid-ssh", Some("did:key:owner")),
            Some(false)
        );
        assert_eq!(authorize_vm_target("vm--ssh@node", None), Some(false));
        assert_eq!(
            authorize_vm_target("vm-0000000000000000-ssh", None),
            Some(false)
        );
        assert_eq!(authorize_vm_target("tcp:22@node", None), None);
        assert_eq!(authorize_vm_target("vm-0000000000000000-http", None), None);
    }
}
