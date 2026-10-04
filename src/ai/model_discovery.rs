//! Shared discovery gate for startup, config edits and dashboard calls.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::Value;

use crate::config::AiProviderConfig;
use crate::daemon::AppState;

#[derive(Default)]
pub(crate) struct ModelDiscovery {
    requests: Mutex<HashMap<String, Arc<Entry>>>,
}

#[derive(Default)]
struct Entry {
    generation: std::sync::atomic::AtomicU64,
    fetch: tokio::sync::Mutex<Fetch>,
}

#[derive(Default)]
struct Fetch {
    generation: u64,
    provider: Option<AiProviderConfig>,
    completed: Option<Instant>,
    result: Option<Value>,
}

pub(super) fn same_connection(a: &AiProviderConfig, b: &AiProviderConfig) -> bool {
    a.base_url == b.base_url && a.api_key == b.api_key && a.enabled == b.enabled
}

impl ModelDiscovery {
    fn invalidate(&self, id: &str) {
        if let Some(entry) = self.requests.lock().expect("ai discovery lock").get(id) {
            entry
                .generation
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub(super) async fn fetch(&self, state: &Arc<AppState>, id: &str) -> Result<Value> {
        let requested = Instant::now();
        let slot = {
            let mut requests = self.requests.lock().expect("ai discovery lock");
            let config = state.effective_config();
            requests.retain(|id, _| config.ai.providers.iter().any(|p| &p.id == id));
            requests.entry(id.to_string()).or_default().clone()
        };
        let mut fetch = slot.fetch.lock().await;
        let generation = slot.generation.load(std::sync::atomic::Ordering::Relaxed);
        let provider = state
            .effective_config()
            .ai
            .providers
            .into_iter()
            .find(|p| p.id == id)
            .ok_or_else(|| anyhow::anyhow!("provider not found in ai.providers"))?;
        if fetch.generation == generation
            && fetch
                .provider
                .as_ref()
                .is_some_and(|p| same_connection(p, &provider))
            && let (Some(completed), Some(result)) = (fetch.completed, &fetch.result)
            && (completed >= requested
                || (result["live"] == true && completed.elapsed() < Duration::from_secs(10)))
        {
            let mut result = result.clone();
            if provider.room().is_some() {
                result["models"] = serde_json::json!(provider.models);
            }
            return Ok(result);
        }
        let result = super::discover_models(state, &provider).await?;
        fetch.generation = generation;
        fetch.provider = Some(provider);
        fetch.completed = Some(Instant::now());
        fetch.result = Some(result.clone());
        Ok(result)
    }
}

/// Only connection edits invalidate discovery; labels, shared refs and caches do not.
pub fn spawn_http_refresh(state: Arc<AppState>, previous: Option<&crate::config::AiConfig>) {
    for provider in state.effective_config().ai.providers {
        if previous.is_some_and(|old| {
            old.providers
                .iter()
                .any(|p| p.id == provider.id && same_connection(p, &provider))
        }) {
            continue;
        }
        state.ai_model_discovery.invalidate(&provider.id);
        if !provider.enabled || provider.room().is_some() || !state.network.permitted() {
            continue;
        }
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = state.ai_model_discovery.fetch(&state, &provider.id).await {
                tracing::debug!(%err, "ai: automatic model discovery failed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn endpoint() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let count = Arc::new(AtomicUsize::new(0));
        let requests = count.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = vec![0; 8192];
                let size = socket.read(&mut bytes).await.unwrap();
                let request = String::from_utf8_lossy(&bytes[..size]);
                requests.fetch_add(1, Ordering::Relaxed);
                let (status, body) = if request.starts_with("GET /fail/models ") {
                    ("503 Unavailable", "try again".to_string())
                } else {
                    let model = if request.contains("Bearer changed") {
                        "changed-model"
                    } else {
                        "live-model"
                    };
                    ("200 OK", json!({"data":[{"id":model}]}).to_string())
                };
                // Both callers must be waiting before the first reply.
                tokio::time::sleep(Duration::from_millis(30)).await;
                socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        (url, count, task)
    }

    fn state(url: &str) -> Arc<AppState> {
        let state = AppState::for_test();
        let mut config = state.config();
        config.ai.providers = vec![AiProviderConfig {
            id: "discovery".into(),
            base_url: url.into(),
            models: vec!["cached-model".into()],
            ..Default::default()
        }];
        state.set_config(config);
        state
    }

    async fn wait_models(state: &AppState, model: &str) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while state.config().ai.providers[0].models != [model] {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn concurrent_calls_dedupe_and_success_expires_after_ten_seconds() {
        let (url, count, task) = endpoint().await;
        let state = state(&url);
        let (a, b) = tokio::join!(
            state.ai_model_discovery.fetch(&state, "discovery"),
            state.ai_model_discovery.fetch(&state, "discovery")
        );
        assert_eq!(a.unwrap(), b.unwrap());
        assert_eq!(count.load(Ordering::Relaxed), 1);
        assert_eq!(state.config().ai.providers[0].models, ["live-model"]);
        assert!(state.config().ai.providers[0].models_fetched_at.is_some());
        state
            .ai_model_discovery
            .fetch(&state, "discovery")
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 1);
        let slot = state.ai_model_discovery.requests.lock().unwrap()["discovery"].clone();
        slot.fetch.lock().await.completed = Some(Instant::now() - Duration::from_secs(11));
        state
            .ai_model_discovery
            .fetch(&state, "discovery")
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 2);
        task.abort();
    }

    #[tokio::test]
    async fn failure_keeps_cache_and_concurrent_errors_dedupe_without_throttling_retry() {
        let (url, count, task) = endpoint().await;
        let state = state(&url.replace("/v1", "/fail"));
        let (a, b) = tokio::join!(
            state.ai_model_discovery.fetch(&state, "discovery"),
            state.ai_model_discovery.fetch(&state, "discovery")
        );
        let a = a.unwrap();
        assert_eq!(a, b.unwrap());
        assert_eq!(a["live"], false);
        assert!(a["error"].as_str().unwrap().contains("503"));
        assert_eq!(count.load(Ordering::Relaxed), 1);
        assert_eq!(state.config().ai.providers[0].models, ["cached-model"]);
        assert!(state.config().ai.providers[0].models_fetched_at.is_none());
        state
            .ai_model_discovery
            .fetch(&state, "discovery")
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 2);
        task.abort();
    }

    #[tokio::test]
    async fn automatic_startup_and_connection_edits_refresh_but_cache_label_edits_do_not() {
        let (url, count, task) = endpoint().await;
        let state = state(&url);
        spawn_http_refresh(state.clone(), None);
        wait_models(&state, "live-model").await;
        let previous = state.config().ai;
        let mut config = state.config();
        config.ai.providers[0].label = "new label".into();
        state.set_config(config);
        spawn_http_refresh(state.clone(), Some(&previous));
        state
            .ai_model_discovery
            .fetch(&state, "discovery")
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 1);
        let previous = state.config().ai;
        let mut config = state.config();
        config.ai.providers[0].api_key = "changed".into();
        state.set_config(config);
        spawn_http_refresh(state.clone(), Some(&previous));
        wait_models(&state, "changed-model").await;
        assert_eq!(count.load(Ordering::Relaxed), 2);
        let previous = state.config().ai;
        let mut config = state.config();
        config.ai.providers[0].enabled = false;
        state.set_config(config);
        spawn_http_refresh(state.clone(), Some(&previous));
        assert_eq!(
            state
                .ai_model_discovery
                .fetch(&state, "discovery")
                .await
                .unwrap()["live"],
            false
        );
        assert_eq!(count.load(Ordering::Relaxed), 2);
        let previous = state.config().ai;
        let mut config = state.config();
        config.ai.providers[0].enabled = true;
        config.ai.providers[0].models = vec!["stale".into()];
        state.set_config(config);
        spawn_http_refresh(state.clone(), Some(&previous));
        wait_models(&state, "changed-model").await;
        assert_eq!(count.load(Ordering::Relaxed), 3);
        task.abort();
    }

    #[test]
    fn old_connection_response_cannot_overwrite_new_connection_cache() {
        let state = state("http://old/v1");
        let fetched = state.config().ai.providers[0].clone();
        let mut config = state.config();
        config.ai.providers[0].api_key = "new-key".into();
        state.set_config(config);
        state
            .cache_ai_models(&fetched, &["old-result".into()])
            .unwrap();
        assert_eq!(state.config().ai.providers[0].models, ["cached-model"]);
    }
}
