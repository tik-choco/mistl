//! Service startup, command dispatch and independent maintenance/repair loops.
use super::service::{Core, Service, now_ms};
use crate::daemon::AppState;
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, OnceCell, Semaphore};

static SERVICE: OnceCell<Arc<Service>> = OnceCell::const_new();

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;

async fn local(state: &Arc<AppState>) -> Result<Arc<Service>> {
    Ok(SERVICE
        .get_or_try_init(|| async {
            let me = crate::identity::current(state).await?;
            let cfg = state.config().vault;
            let dir = crate::config::data_dir()?.join("vault");
            let core = tokio::task::spawn_blocking(move || Core::open(dir, cfg, me)).await??;
            let service = Arc::new(Service {
                core: Mutex::new(core),
                app: state.clone(),
                wake: Notify::new(),
                joined: Mutex::new(None),
                joining: tokio::sync::Mutex::new(()),
                provider: Mutex::new(None),
                inbound: Arc::new(Semaphore::new(64)),
                serving: Arc::new(Semaphore::new(4)),
            });
            service.register();
            start_loops(service.clone());
            Ok::<_, anyhow::Error>(service)
        })
        .await?
        .clone())
}

pub fn spawn_background(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(error) = local(&state).await {
            tracing::warn!(%error, "vault: startup failed");
        }
    });
}

fn start_loops(service: Arc<Service>) {
    let maintenance = service.clone();
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(5));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            if let Err(error) = maintain(&maintenance).await {
                let message = error.to_string();
                tracing::debug!(%error, "vault: maintenance deferred");
                let _ = maintenance
                    .work(move |core| {
                        core.last_error = Some(message);
                        Ok(())
                    })
                    .await;
            }
        }
    });
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(15));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = timer.tick() => {}, _ = service.wake.notified() => {} }
            if !service.app.network.permitted() {
                continue;
            }
            let ready = service
                .work(|core| {
                    Ok(core
                        .group
                        .as_ref()
                        .is_some_and(|g| g.is_member(core.me.did())))
                })
                .await
                .unwrap_or(false);
            if !ready || service.joined.lock().expect("vault room lock").is_none() {
                continue;
            }
            if let Err(error) = service.tick().await {
                tracing::debug!(%error, "vault: tick deferred");
                let message = error.to_string();
                let _ = service
                    .work(move |core| {
                        core.last_error = Some(message);
                        Ok(())
                    })
                    .await;
            }
        }
    });
}

async fn maintain(service: &Arc<Service>) -> Result<()> {
    let _joining = service.joining.lock().await;
    if !service.app.network.permitted() {
        return Ok(());
    }
    let room = service
        .work(|core| {
            Ok(core
                .group
                .as_ref()
                .filter(|g| g.is_member(core.me.did()) || core.join.is_some())
                .map(|g| g.room()))
        })
        .await?;
    let Some(room) = room else {
        return Ok(());
    };
    let joined = service.joined.lock().expect("vault room lock").as_deref() == Some(room.as_str());
    if !joined {
        crate::net::ensure_started(&service.app, room.clone()).await?;
        *service.joined.lock().expect("vault room lock") = Some(room.clone());
        tracing::info!(%room, "vault: room joined");
        service.wake.notify_one();
    }
    let messages = service
        .work(|core| {
            let mut messages = Vec::new();
            let now = now_ms();
            if core
                .join
                .as_ref()
                .is_some_and(|j| now.saturating_sub(j.started) >= 10 * 60 * 1000)
            {
                core.join = None;
                core.last_error = Some("join timed out before admission".into());
                core.save()?;
            }
            if let Some(join) = &mut core.join {
                if now.saturating_sub(join.last_sent) >= 5_000 {
                    join.last_sent = now;
                    let (invite, mac) = join.code.join_proof(core.me.did());
                    let to = crate::identity::node_id_for_did(&join.code.by);
                    messages.push(core.message(
                        Some(&to),
                        "join",
                        json!({"invite":invite,"mac":mac}),
                    )?);
                }
            }
            if now.saturating_sub(core.last_hello) >= 30_000 {
                core.last_hello = now;
                messages.extend(core.hello(None)?);
                if let Some(owner) = core.own_id().filter(|id| !core.owners.contains_key(id)) {
                    messages.push(core.message(None, "head_req", json!({"owner":owner}))?);
                }
            }
            Ok(messages)
        })
        .await?;
    service.send(messages).await;
    Ok(())
}

pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    let service = local(state).await?;
    let command = cmd.to_string();
    let running =
        state.network.permitted() && service.joined.lock().expect("vault room lock").is_some();
    let (mut result, messages) = service
        .work(move |core| core.command(&command, args, running))
        .await?;
    service.send(messages).await;
    if matches!(cmd, "vault.init" | "vault.join") {
        if let Err(error) = maintain(&service).await {
            tracing::debug!(%error, "vault: network start deferred");
        }
    }
    if !matches!(
        cmd,
        "vault.status" | "vault.ls" | "vault.get" | "vault.key.show"
    ) {
        service.wake.notify_one();
    }
    if cmd == "vault.restore" && result.get("seq").is_none() && state.network.permitted() {
        let owner = result["owner"].as_str().unwrap_or_default().to_string();
        let wait = async {
            loop {
                let id = owner.clone();
                if let Some(seq) = service
                    .work(move |core| Ok(core.owners.get(&id).map(|o| o.cur.seq)))
                    .await?
                {
                    return Ok::<_, anyhow::Error>(seq);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        };
        if let Ok(Ok(seq)) = tokio::time::timeout(Duration::from_secs(10), wait).await {
            result["seq"] = json!(seq);
        }
    }
    Ok(result)
}
