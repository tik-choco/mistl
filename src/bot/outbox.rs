//! Durable, per-sink delivery progress. A retry reuses transformed content
//! and skips acknowledged sinks. External sends and local disk writes cannot
//! be atomic: a crash between them can still cause a duplicate (at-least-once).
//! Sink configurations are read live; only their hashes, not credentials,
//! are saved alongside the content. Changing a sink makes it a new target.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{persist, source::Article, transform::TransformOutcome};
use crate::config::SinkConfig;
use persist::{DeliveredItem, SinkOutcome};

pub(super) const CAPACITY: usize = 100;
const DELIVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct PendingDelivery {
    pub source_id: String,
    pub article: Article,
    pub outcome: TransformOutcome,
    #[serde(default)]
    pub succeeded: HashMap<String, SinkOutcome>,
    #[serde(default)]
    pub last_attempt: i64,
}

fn filename(pipeline_id: &str) -> String {
    format!(
        "bot-outbox-{:x}.json",
        Sha256::digest(pipeline_id.as_bytes())
    )
}

pub(super) fn load(dir: &Path, pipeline_id: &str) -> Result<Vec<PendingDelivery>> {
    crate::statefile::read(dir, &filename(pipeline_id))
}

pub(super) fn save(dir: &Path, pipeline_id: &str, queue: &[PendingDelivery]) -> Result<()> {
    crate::statefile::write(dir, &filename(pipeline_id), &queue)
}

pub(super) fn enqueue(
    dir: &Path,
    pipeline_id: &str,
    queue: &mut Vec<PendingDelivery>,
    source_id: String,
    article: Article,
    outcome: TransformOutcome,
) -> Result<()> {
    if queue.iter().any(|entry| entry.source_id == source_id) {
        return Ok(());
    }
    if queue.len() >= CAPACITY {
        bail!(
            "bot delivery queue is full ({CAPACITY}); restore failing sinks before importing more items"
        );
    }
    queue.push(PendingDelivery {
        source_id,
        article,
        outcome,
        succeeded: HashMap::new(),
        last_attempt: 0,
    });
    save(dir, pipeline_id, queue)
}

/// Checkpoint after every successful sink, before attempting the next. Do
/// not mark the source processed until all currently configured sinks succeed.
pub(super) async fn attempt<F, Fut>(
    dir: &Path,
    pipeline_id: &str,
    queue: &mut Vec<PendingDelivery>,
    index: usize,
    sinks: &[SinkConfig],
    mut deliver: F,
) -> Result<DeliveredItem>
where
    F: FnMut(SinkConfig, Article, TransformOutcome) -> Fut,
    Fut: Future<Output = SinkOutcome>,
{
    if sinks.is_empty() {
        bail!("bot pipeline has no delivery sinks; pending content retained");
    }
    queue[index].last_attempt = chrono::Utc::now().timestamp_millis();
    save(dir, pipeline_id, queue)?;
    let mut outcomes = Vec::new();
    // Include an occurrence number so two intentionally identical sinks
    // remain two deliveries, while reordering distinct sinks preserves receipts.
    let mut occurrences = HashMap::<String, usize>::new();
    for sink in sinks {
        let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(sink)?));
        let occurrence = occurrences.entry(digest.clone()).or_default();
        let key = format!("{digest}:{}", *occurrence);
        *occurrence += 1;
        let outcome = if let Some(previous) = queue[index].succeeded.get(&key) {
            previous.clone()
        } else {
            let entry = &queue[index];
            let outcome = match tokio::time::timeout(
                DELIVERY_TIMEOUT,
                deliver(sink.clone(), entry.article.clone(), entry.outcome.clone()),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(_) => {
                    let (kind, target) = match sink {
                        SinkConfig::ChatPost { room } => ("chat-post", room),
                        SinkConfig::ArticlePublish { room } => ("article-publish", room),
                        SinkConfig::Webhook { url, .. } => ("webhook", url),
                    };
                    SinkOutcome {
                        kind: kind.into(),
                        target: target.trim().into(),
                        ok: false,
                        error: Some(
                            "delivery timed out after 120 seconds; retained for retry".into(),
                        ),
                    }
                }
            };
            if outcome.ok {
                queue[index].succeeded.insert(key, outcome.clone());
                save(dir, pipeline_id, queue)?;
            }
            outcome
        };
        outcomes.push(outcome);
    }
    let entry = &queue[index];
    let item = DeliveredItem {
        pipeline_id: pipeline_id.to_string(),
        item_id: entry.source_id.clone(),
        title: entry.article.title.clone(),
        delivered_at: chrono::Utc::now().to_rfc3339(),
        sinks: outcomes,
    };
    if item.sinks.iter().all(|sink| sink.ok) {
        persist::mark_processed(dir, pipeline_id, std::slice::from_ref(&entry.source_id)).await?;
        queue.remove(index);
        save(dir, pipeline_id, queue)?;
    }
    Ok(item)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn scratch() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("mistl-outbox-test-{:016x}", rand::random::<u64>()))
    }

    fn article() -> Article {
        Article {
            id: "body-id".into(),
            title: "title".into(),
            body: "original".into(),
            excerpt: "excerpt".into(),
            source_links: vec![],
            author_name: "author".into(),
            created_at: 0,
        }
    }

    fn receipt(target: String, ok: bool) -> SinkOutcome {
        SinkOutcome {
            kind: "chat-post".into(),
            target,
            ok,
            error: (!ok).then(|| "offline".into()),
        }
    }

    #[tokio::test]
    async fn partial_failure_survives_restart_and_only_retries_failed_sink() {
        let dir = scratch();
        let mut queue = vec![];
        let outcome = TransformOutcome {
            script: Some("already transformed".into()),
            ..Default::default()
        };
        enqueue(
            &dir,
            "pipeline",
            &mut queue,
            "wire-id".into(),
            article(),
            outcome,
        )
        .unwrap();
        let sinks = vec![
            SinkConfig::ChatPost { room: "a".into() },
            SinkConfig::ChatPost { room: "b".into() },
        ];
        let first = attempt(
            &dir,
            "pipeline",
            &mut queue,
            0,
            &sinks,
            |sink, _, _| async move {
                let SinkConfig::ChatPost { room } = sink else {
                    panic!()
                };
                let ok = room == "a";
                receipt(room, ok)
            },
        )
        .await
        .unwrap();
        assert!(!first.sinks[1].ok);
        assert!(
            persist::load_processed(&dir, "pipeline")
                .await
                .unwrap()
                .is_empty()
        );

        let mut reloaded = load(&dir, "pipeline").unwrap();
        let calls = RefCell::new(vec![]);
        // Reordering distinct sinks must not invalidate completed deliveries.
        let mut reordered = sinks.clone();
        reordered.reverse();
        let second = attempt(
            &dir,
            "pipeline",
            &mut reloaded,
            0,
            &reordered,
            |sink, _, outcome| {
                assert_eq!(outcome.script.as_deref(), Some("already transformed"));
                let SinkConfig::ChatPost { room } = sink else {
                    panic!()
                };
                calls.borrow_mut().push(room.clone());
                async move { receipt(room, true) }
            },
        )
        .await
        .unwrap();
        assert!(second.sinks.iter().all(|sink| sink.ok));
        assert_eq!(*calls.borrow(), vec!["b"]);
        assert!(load(&dir, "pipeline").unwrap().is_empty());
        assert!(
            persist::load_processed(&dir, "pipeline")
                .await
                .unwrap()
                .contains("wire-id")
        );
    }

    #[tokio::test]
    async fn all_failures_and_no_sinks_retain_content() {
        let dir = scratch();
        let mut queue = vec![];
        enqueue(
            &dir,
            "pipeline",
            &mut queue,
            "id".into(),
            article(),
            TransformOutcome::default(),
        )
        .unwrap();
        let sinks = vec![SinkConfig::ChatPost {
            room: "offline".into(),
        }];
        let item = attempt(&dir, "pipeline", &mut queue, 0, &sinks, |_, _, _| async {
            receipt("offline".into(), false)
        })
        .await
        .unwrap();
        assert!(!item.sinks[0].ok);
        assert_eq!(load(&dir, "pipeline").unwrap().len(), 1);
        assert!(
            persist::load_processed(&dir, "pipeline")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            attempt(&dir, "pipeline", &mut queue, 0, &[], |_, _, _| async {
                panic!()
            })
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn disk_failure_prevents_sending() {
        let dir = scratch();
        let mut queue = vec![];
        enqueue(
            &dir,
            "pipeline",
            &mut queue,
            "id".into(),
            article(),
            TransformOutcome::default(),
        )
        .unwrap();
        let bad_dir = dir.join("not-a-directory");
        std::fs::write(&bad_dir, b"file").unwrap();
        let sinks = vec![SinkConfig::ChatPost { room: "a".into() }];
        assert!(
            attempt(
                &bad_dir,
                "pipeline",
                &mut queue,
                0,
                &sinks,
                |_, _, _| async { panic!("must not send without a durable checkpoint") }
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn queue_full_is_an_error_and_does_not_evict_pending_content() {
        let dir = scratch();
        let mut queue = vec![];
        enqueue(
            &dir,
            "pipeline",
            &mut queue,
            "id".into(),
            article(),
            TransformOutcome::default(),
        )
        .unwrap();
        queue.resize(CAPACITY, queue[0].clone());
        assert!(
            enqueue(
                &dir,
                "pipeline",
                &mut queue,
                "new".into(),
                article(),
                TransformOutcome::default()
            )
            .is_err()
        );
        assert_eq!(queue.len(), CAPACITY);
    }

    #[tokio::test(start_paused = true)]
    async fn hung_sink_times_out_without_blocking_other_sinks() {
        let dir = scratch();
        let mut queue = vec![];
        enqueue(
            &dir,
            "pipeline",
            &mut queue,
            "id".into(),
            article(),
            TransformOutcome::default(),
        )
        .unwrap();
        let sinks = vec![
            SinkConfig::ChatPost {
                room: "hung".into(),
            },
            SinkConfig::ChatPost {
                room: "healthy".into(),
            },
        ];
        let item = attempt(
            &dir,
            "pipeline",
            &mut queue,
            0,
            &sinks,
            |sink, _, _| async move {
                let SinkConfig::ChatPost { room } = sink else {
                    panic!()
                };
                if room == "hung" {
                    std::future::pending::<()>().await;
                }
                receipt(room, true)
            },
        )
        .await
        .unwrap();
        assert!(
            item.sinks[0]
                .error
                .as_deref()
                .unwrap()
                .contains("timed out")
        );
        assert!(item.sinks[1].ok);
        assert_eq!(load(&dir, "pipeline").unwrap()[0].succeeded.len(), 1);
        assert!(
            persist::load_processed(&dir, "pipeline")
                .await
                .unwrap()
                .is_empty()
        );
    }
}
