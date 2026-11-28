//! mistl web: loopback-only hosting of static Web Apps / Web Games.
//!
//! Authors publish a static folder (`mistl upload <dir>`); peers who have
//! mistl installed open it at `http://<siteId>.localhost:<port>/`, served by a
//! local gateway from blocks fetched over the p2p storage network. Nothing is
//! exposed outside loopback.
//!
//! - [`site`]: pure formats -- site identifiers, the file manifest, the signed
//!   site record, and the gateway's path / MIME rules.
//! - [`state`]: `<data_dir>/web/state.json` (known sites, trust).
//! - [`wire`]: the `mistl-web-v1` room protocol (`want` / `record`).
//! - [`host`]: the host core (installs, resolve, trust) the gateway uses.
//! - [`publish`]: `web.publish` (folder -> pinned files + signed record).

// TODO(web): drop once the gateway uses the whole host surface.
#![allow(dead_code)]

pub mod gateway;
pub mod host;
pub mod publish;
pub mod site;
pub mod state;
pub mod wire;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::daemon::AppState;
use host::{SiteView, WebHost};

/// How long `web.install` waits for a record from peers.
const INSTALL_RESOLVE_WAIT: Duration = Duration::from_secs(10);

/// Start the host when `[web] enabled`: loads state, opens the store so
/// installed blocks are served to peers after a restart, and joins
/// `mistl-web-v1` when the network is permitted, retrying failed joins.
pub fn spawn_background(state: Arc<AppState>) {
    if !state.config().web.enabled {
        return;
    }
    tokio::spawn(async move {
        if let Err(error) = crate::storage::store(&state).await {
            tracing::warn!(%error, "web: opening storage");
        }
        let mut delay = Duration::from_secs(5);
        loop {
            match host::host(&state).await {
                Ok(host) => {
                    if !state.network.permitted() || !state.config().web.enabled || host.ensure_joined().await {
                        break;
                    }
                }
                Err(error) => tracing::warn!(%error, "web: failed to start"),
            }
            if !state.network.permitted() || !state.config().web.enabled {
                break;
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(60));
        }
    });
}

fn site_arg(args: &Value) -> Result<String> {
    let raw = args
        .get("site")
        .and_then(Value::as_str)
        .context("missing `site` (a site id or http://<site>.localhost URL)")?;
    site::parse_site_ref(raw).with_context(|| format!("not a site id or site URL: {raw:?}"))
}

fn view_json(host: &WebHost, v: &SiteView) -> Value {
    let r = &v.record;
    json!({
        "site": r.site,
        "name": r.name,
        "url": host.site_url(&r.site),
        "did": r.did,
        "own": v.own,
        "trusted": v.trusted,
        "installed": v.ready_cid.is_some(),
        "installed_manifest": v.ready_cid,
        "installed_seq": v.installed_seq,
        "update_pending": v.ready_cid.is_some() && v.installed_seq.unwrap_or(0) < r.seq,
        "manifest": r.manifest,
        "size": r.size,
        "files": r.files,
        "seq": r.seq,
        "at": r.at,
        "progress": v.progress,
    })
}

/// `site` or `did` target for trust commands.
enum TrustTarget {
    Site(String),
    Did(String),
}

fn trust_target(args: &Value) -> Result<TrustTarget> {
    if let Some(did) = args.get("did").and_then(Value::as_str) {
        return Ok(TrustTarget::Did(did.to_string()));
    }
    let raw = args
        .get("site")
        .and_then(Value::as_str)
        .context("missing `site` or `did`")?;
    if raw.starts_with("did:") {
        return Ok(TrustTarget::Did(raw.to_string()));
    }
    site_arg(args).map(TrustTarget::Site)
}

/// IPC entry point for `web.*` commands.
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    let cfg = state.config().web;
    if cmd == "web.status" {
        let port = cfg
            .listen
            .parse::<std::net::SocketAddr>()
            .map(|a| a.port())
            .ok();
        let running = host::get();
        let sites = match &running {
            Some(h) => h.list().await.len(),
            None => 0,
        };
        return Ok(json!({
            "enabled": cfg.enabled,
            "listen": cfg.listen,
            "base_url": port.map(|p| format!("http://localhost:{p}/")),
            "network": state.network.permitted(),
            "room_joined": running.as_ref().is_some_and(|h| h.room_joined()),
            "sites": sites,
        }));
    }
    if !cfg.enabled {
        bail!("mistl web is disabled; enable it with `mistl config set web.enabled true`");
    }
    let host = host::host(state).await?;
    match cmd {
        "web.publish" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .context("missing `path`")?;
            let dir = PathBuf::from(path);
            let opts = publish::PublishOptions {
                name: args.get("name").and_then(Value::as_str).map(str::to_string),
                entry: args
                    .get("entry")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                spa: args.get("spa").and_then(Value::as_bool).unwrap_or(false),
                coi: args.get("coi").and_then(Value::as_bool),
                dry_run: args
                    .get("dry_run")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            };
            let identity = crate::identity::current(state).await?;
            publish::publish(&host, &identity, &dir, opts).await
        }
        "web.ls" => {
            let sites: Vec<Value> = host
                .list()
                .await
                .iter()
                .map(|v| view_json(&host, v))
                .collect();
            Ok(json!({ "sites": sites }))
        }
        "web.url" => {
            let site = site_arg(&args)?;
            Ok(json!({
                "site": site,
                "url": host.site_url(&site),
                "known": host.view(&site).await.is_some(),
            }))
        }
        "web.install" => {
            let site = site_arg(&args)?;
            if host.resolve(&site, INSTALL_RESOLVE_WAIT).await.is_none() {
                bail!(
                    "site {site} was not found: no peer answered (is the network on and someone who has it online?)"
                );
            }
            if args.get("trust").and_then(Value::as_bool).unwrap_or(false) {
                host.trust(&site, false).await?;
            }
            let view = host.view(&site).await.context("site disappeared")?;
            if !view.trusted {
                bail!(
                    "site {site} ({}, published by {}) is not trusted; re-run with --trust to install it",
                    view.record.name,
                    view.record.did
                );
            }
            host.start_install(&site).await;
            let view = host.view(&site).await.context("site disappeared")?;
            Ok(view_json(&host, &view))
        }
        "web.rm" => {
            let site = site_arg(&args)?;
            if !host.remove(&site).await? {
                bail!("unknown site {site}");
            }
            Ok(json!({ "removed": true, "site": site }))
        }
        "web.trust" | "web.untrust" => {
            let trusted = cmd == "web.trust";
            match trust_target(&args)? {
                TrustTarget::Site(site) => {
                    if trusted {
                        host.trust(&site, false).await?;
                        Ok(json!({ "site": site, "trusted": true }))
                    } else {
                        let still = host.untrust(&site, false).await?;
                        Ok(json!({ "site": site, "trusted": still, "via": still.then_some("publisher") }))
                    }
                }
                TrustTarget::Did(did) => {
                    host.set_publisher_trust(&did, trusted).await?;
                    Ok(json!({ "did": did, "trusted": trusted }))
                }
            }
        }
        _ => bail!("unknown command: {cmd}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn site_and_trust_args() {
        let id = site::derive_site_id(&crate::identity::did_for_seed(1), "g");
        assert_eq!(site_arg(&json!({"site": id})).unwrap(), id);
        let url = format!("http://{id}.localhost:6490/x");
        assert_eq!(site_arg(&json!({"site": url})).unwrap(), id);
        assert!(site_arg(&json!({"site": "nope"})).is_err());
        assert!(site_arg(&json!({})).is_err());

        let did = crate::identity::did_for_seed(2);
        assert!(matches!(
            trust_target(&json!({"did": did})).unwrap(),
            TrustTarget::Did(_)
        ));
        assert!(matches!(
            trust_target(&json!({"site": did})).unwrap(),
            TrustTarget::Did(_)
        ));
        assert!(matches!(
            trust_target(&json!({"site": id})).unwrap(),
            TrustTarget::Site(_)
        ));
    }
}
