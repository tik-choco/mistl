//! On-disk persistence for the bot pipeline engine: capped run/item logs and
//! per-pipeline idempotent "already handled" state. Mirrors
//! `crate::scheduler`'s persistence idioms closely (see its module doc) --
//! capped JSONL append-logs written via temp-file-then-rename, and a
//! `tokio::sync::Mutex` guarding each on-disk resource so concurrent tick
//! iterations (and the manual `bot.run`/`bot.logs`/`bot.items` IPC paths)
//! never race on the same file.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Most-recent run records kept in `bot-runs.jsonl`. Mirrors
/// `scheduler::RUN_LOG_CAP`.
const RUN_LOG_CAP: usize = 200;
/// Most-recent delivered items kept in `bot-items.jsonl`.
const ITEMS_LOG_CAP: usize = 200;
/// Processed-article-id set cap per pipeline (`bot-imported-<id>.json`).
/// The brief allows "no cap or a generously large one"; this bounds memory
/// and disk for a long-running pipeline while comfortably exceeding any
/// realistic backlog (tc-news's own per-room wire log caps at 300 replayed
/// articles -- see `newsWire.ts`'s `MAX_WIRE_LOG`).
const PROCESSED_IDS_CAP: usize = 5_000;

/// One pipeline execution: the exact `RunRecord` shape the Wave 2 brief
/// fixes for `bot.run`'s response, `bot.logs`' `runs` array, and
/// `bot.list`'s per-pipeline `last_run`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub pipeline_id: String,
    pub started_at: String,
    pub ended_at: String,
    pub ok: bool,
    pub error: Option<String>,
    pub fetched_count: u64,
    pub delivered_count: u64,
}

/// One sink's delivery outcome for one item, embedded in [`DeliveredItem`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkOutcome {
    pub kind: String,
    pub target: String,
    pub ok: bool,
    pub error: Option<String>,
}

/// One piece of content a pipeline finished processing and attempted to
/// deliver, listed (newest first) by `bot.items`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveredItem {
    pub pipeline_id: String,
    pub item_id: String,
    pub title: String,
    pub delivered_at: String,
    pub sinks: Vec<SinkOutcome>,
}

fn runs_path(data_dir: &Path) -> PathBuf {
    data_dir.join("bot-runs.jsonl")
}

fn items_path(data_dir: &Path) -> PathBuf {
    data_dir.join("bot-items.jsonl")
}

fn imported_path(data_dir: &Path, pipeline_id: &str) -> PathBuf {
    // Pipeline ids are user-chosen config keys; unlike
    // `mailbox::chat_relay`'s room ids there is no shape gate on them here
    // (a pipeline id is never used to join a network room), so a
    // filesystem-unsafe id would produce an unusable path -- acceptable for
    // v1 since pipeline ids are locally authored via `config.toml`/`config
    // set`, not attacker-controlled network input.
    data_dir.join(format!("bot-imported-{pipeline_id}.json"))
}

fn runs_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn items_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn imported_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn read_lines(path: &Path) -> Result<Vec<String>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    Ok(text
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

/// Overwrites `path` with `lines`, one per line: temp file in the same
/// directory then rename over the target, mirroring
/// `scheduler`/`mailbox::chat_relay`'s capped-log write idiom.
fn write_lines(path: &Path, lines: &[String]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let mut body = String::new();
    for line in lines {
        body.push_str(line);
        body.push('\n');
    }
    let tmp = path.with_extension("jsonl.tmp");
    std::fs::write(&tmp, &body).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} into {}", tmp.display(), path.display()))
}

/// Appends `record`, keeping only the [`RUN_LOG_CAP`] most-recent entries.
pub async fn append_run(data_dir: &Path, record: &RunRecord) -> Result<()> {
    let _guard = runs_lock().lock().await;
    let path = runs_path(data_dir);
    let mut lines = read_lines(&path)?;
    lines.push(serde_json::to_string(record)?);
    if lines.len() > RUN_LOG_CAP {
        let excess = lines.len() - RUN_LOG_CAP;
        lines.drain(0..excess);
    }
    write_lines(&path, &lines)
}

/// Reads every run record, oldest first (append order).
pub async fn read_runs(data_dir: &Path) -> Result<Vec<RunRecord>> {
    let _guard = runs_lock().lock().await;
    read_lines(&runs_path(data_dir))?
        .iter()
        .map(|line| serde_json::from_str(line).context("parsing bot run record"))
        .collect()
}

/// Appends `item`, keeping only the [`ITEMS_LOG_CAP`] most-recent entries.
pub async fn append_item(data_dir: &Path, item: &DeliveredItem) -> Result<()> {
    let _guard = items_lock().lock().await;
    let path = items_path(data_dir);
    let mut lines = read_lines(&path)?;
    lines.push(serde_json::to_string(item)?);
    if lines.len() > ITEMS_LOG_CAP {
        let excess = lines.len() - ITEMS_LOG_CAP;
        lines.drain(0..excess);
    }
    write_lines(&path, &lines)
}

/// Reads every delivered item, oldest first (append order).
pub async fn read_items(data_dir: &Path) -> Result<Vec<DeliveredItem>> {
    let _guard = items_lock().lock().await;
    read_lines(&items_path(data_dir))?
        .iter()
        .map(|line| serde_json::from_str(line).context("parsing bot delivered item"))
        .collect()
}

/// Loads `pipeline_id`'s idempotent "already handled" article-id set
/// (missing file reads as empty -- a brand-new pipeline has processed
/// nothing yet).
pub async fn load_processed(data_dir: &Path, pipeline_id: &str) -> Result<HashSet<String>> {
    let _guard = imported_lock().lock().await;
    let path = imported_path(data_dir, pipeline_id);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    if text.trim().is_empty() {
        return Ok(HashSet::new());
    }
    let ids: Vec<String> =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    Ok(ids.into_iter().collect())
}

/// Merges `new_ids` into `pipeline_id`'s processed set and persists it,
/// capped at [`PROCESSED_IDS_CAP`] (oldest -- by file order -- dropped
/// first). A no-op when `new_ids` is empty, avoiding a spurious rewrite on
/// every tick of an otherwise-idle pipeline.
pub async fn mark_processed(data_dir: &Path, pipeline_id: &str, new_ids: &[String]) -> Result<()> {
    if new_ids.is_empty() {
        return Ok(());
    }
    let _guard = imported_lock().lock().await;
    let path = imported_path(data_dir, pipeline_id);
    let mut ids: Vec<String> = match std::fs::read_to_string(&path) {
        Ok(text) if !text.trim().is_empty() => {
            serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?
        }
        _ => Vec::new(),
    };
    let mut seen: HashSet<String> = ids.iter().cloned().collect();
    for id in new_ids {
        if seen.insert(id.clone()) {
            ids.push(id.clone());
        }
    }
    if ids.len() > PROCESSED_IDS_CAP {
        let excess = ids.len() - PROCESSED_IDS_CAP;
        ids.drain(0..excess);
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_string(&ids)?)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("renaming {} into {}", tmp.display(), path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_dir(name: &str) -> PathBuf {
        let suffix: u64 = rand::random();
        let dir = std::env::temp_dir().join(format!("mistl-bot-persist-test-{name}-{suffix:016x}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_run(pipeline_id: &str, ok: bool) -> RunRecord {
        RunRecord {
            pipeline_id: pipeline_id.to_string(),
            started_at: "2026-07-12T00:00:00Z".to_string(),
            ended_at: "2026-07-12T00:00:01Z".to_string(),
            ok,
            error: if ok { None } else { Some("boom".to_string()) },
            fetched_count: 1,
            delivered_count: if ok { 1 } else { 0 },
        }
    }

    #[tokio::test]
    async fn run_log_appends_and_caps_at_200() {
        let dir = scratch_dir("runs-cap");
        for i in 0..210 {
            let mut record = sample_run("news-audio", true);
            record.started_at = format!("run-{i:04}");
            append_run(&dir, &record).await.unwrap();
        }
        let records = read_runs(&dir).await.unwrap();
        assert_eq!(records.len(), 200);
        // Oldest 10 (run-0000..run-0009) were dropped; the log keeps the
        // most recent 200.
        assert_eq!(records.first().unwrap().started_at, "run-0010");
        assert_eq!(records.last().unwrap().started_at, "run-0209");
    }

    #[tokio::test]
    async fn items_log_appends_and_caps_at_200() {
        let dir = scratch_dir("items-cap");
        for i in 0..210 {
            let item = DeliveredItem {
                pipeline_id: "news-audio".to_string(),
                item_id: format!("article-{i}"),
                title: "t".to_string(),
                delivered_at: "2026-07-12T00:00:00Z".to_string(),
                sinks: vec![SinkOutcome {
                    kind: "chat-post".to_string(),
                    target: "room".to_string(),
                    ok: true,
                    error: None,
                }],
            };
            append_item(&dir, &item).await.unwrap();
        }
        let items = read_items(&dir).await.unwrap();
        assert_eq!(items.len(), 200);
        assert_eq!(items.first().unwrap().item_id, "article-10");
        assert_eq!(items.last().unwrap().item_id, "article-209");
    }

    #[tokio::test]
    async fn processed_ids_round_trip_dedupe_and_persist_across_calls() {
        let dir = scratch_dir("processed");
        assert!(load_processed(&dir, "news-audio").await.unwrap().is_empty());

        mark_processed(&dir, "news-audio", &["a".to_string(), "b".to_string()])
            .await
            .unwrap();
        let processed = load_processed(&dir, "news-audio").await.unwrap();
        assert_eq!(processed.len(), 2);
        assert!(processed.contains("a") && processed.contains("b"));

        // Re-marking an already-processed id, alongside a genuinely new one,
        // must not duplicate the existing entry.
        mark_processed(&dir, "news-audio", &["a".to_string(), "c".to_string()])
            .await
            .unwrap();
        let processed = load_processed(&dir, "news-audio").await.unwrap();
        assert_eq!(processed.len(), 3);

        // A different pipeline id has entirely separate state.
        assert!(load_processed(&dir, "other-pipeline").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn mark_processed_is_a_noop_write_for_an_empty_batch() {
        let dir = scratch_dir("processed-empty");
        mark_processed(&dir, "news-audio", &[]).await.unwrap();
        assert!(load_processed(&dir, "news-audio").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn processed_ids_cap_drops_oldest_first() {
        let dir = scratch_dir("processed-cap");
        let ids: Vec<String> = (0..(PROCESSED_IDS_CAP + 10)).map(|i| format!("id-{i}")).collect();
        mark_processed(&dir, "news-audio", &ids).await.unwrap();
        let processed = load_processed(&dir, "news-audio").await.unwrap();
        assert_eq!(processed.len(), PROCESSED_IDS_CAP);
        assert!(!processed.contains("id-0"), "oldest entries must be dropped");
        assert!(processed.contains(&format!("id-{}", PROCESSED_IDS_CAP + 9)));
    }

    /// `bot::run_pipeline` now calls `mark_processed` once per article
    /// immediately after that article is delivered (or skipped), rather
    /// than batching every id from a run into one call at the end -- so a
    /// daemon crash mid-run doesn't redeliver whatever was already
    /// successfully handled. This reproduces that exact call pattern (one
    /// single-element slice per call, `std::slice::from_ref`-style) and
    /// asserts each id is durable on disk *before* the next call is made --
    /// i.e. `mark_processed` itself does no request coalescing/deferred
    /// writing that would undermine the crash-safety `run_pipeline` now
    /// relies on.
    #[tokio::test]
    async fn mark_processed_persists_each_single_item_call_immediately_before_the_next_one() {
        let dir = scratch_dir("processed-immediate");
        let delivered_ids = ["article-1", "article-2", "article-3"];

        for (i, id) in delivered_ids.iter().enumerate() {
            // One call per delivered item, exactly like `run_pipeline`'s
            // inner loop.
            mark_processed(&dir, "news-audio", std::slice::from_ref(&id.to_string()))
                .await
                .unwrap();

            // Immediately re-read from disk (a fresh `load_processed` call,
            // not the in-memory value `mark_processed` worked from) to
            // prove the write already landed -- everything up to and
            // including this item must be present, and nothing after it
            // (proving the loop hasn't secretly pre-committed later items).
            let processed = load_processed(&dir, "news-audio").await.unwrap();
            assert_eq!(processed.len(), i + 1, "expected exactly {} processed ids after item {i}", i + 1);
            for done in &delivered_ids[..=i] {
                assert!(processed.contains(*done), "{done} should already be durable");
            }
            for pending in &delivered_ids[i + 1..] {
                assert!(!processed.contains(*pending), "{pending} must not be durable yet");
            }
        }
    }
}
