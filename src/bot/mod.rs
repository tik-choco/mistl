//! Bot pipeline engine: source -> transform(s) -> sink(s) automation runs,
//! driven by an internal 1-second tick loop (see [`spawn_background`]) that
//! fires each `[[bot.pipelines]]` entry on its own `schedule` expression --
//! the same grammar `sched` uses (`crate::scheduler::schedule`, opened to
//! `pub(crate)` for this reuse).
//!
//! See `tc-docs/drafts/bot-pipeline-v1.md` for the design rationale ("bot is
//! just an ordinary DID peer; no new wire contracts, no new app coupling")
//! and the Wave 2 implementation brief for the exact IPC/config contracts
//! this module implements. Split across:
//! - [`persist`]: capped run/item JSONL logs + per-pipeline idempotent
//!   "already delivered" state (`bot-runs.jsonl` / `bot-items.jsonl` /
//!   `bot-imported-<id>.json`).
//! - [`source`]: the `global-articles` source (subscribes to a
//!   `tc-news`-compatible signed article room).
//! - [`transform`]: `summarize` (LLM chat completion) and `tts`
//!   (direct-upstream speech synthesis).
//! - [`sink`]: `chat-post` (signed `tc-chat:post` wires) and `webhook`
//!   (signed HTTP `POST`).
//!
//! ## Fatal vs. non-fatal errors (pipeline flow step 6)
//!
//! A source-level failure (can't join the configured room) or a
//! transform-level failure (preset unresolved, upstream error) fails the
//! whole run (`RunRecord.ok = false`) and stops processing further articles
//! this run -- config problems and upstream outages are exactly the things
//! `bot.status`'s `warnings` and a failed run's `error` should surface
//! clearly (see [`validate_pipeline`]). A single article's body failing to
//! *resolve* over p2p (a transient timeout, not a config problem) is
//! treated as "not yet available" rather than fatal: it's simply left
//! unprocessed and retried on a later run (see `source::poll_candidates`).
//! A sink failure never fails the run either -- it's recorded per-sink on
//! the delivered item (`persist::SinkOutcome`) so a chat-post success next
//! to a webhook failure (or vice versa) is visible without hiding the part
//! that worked.

mod persist;
mod sink;
mod source;
mod transform;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Local, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, OnceCell};
use tracing::{info, warn};

use crate::config::{Config, PipelineConfig, SinkConfig, SourceConfig, TransformConfig};
use crate::daemon::AppState;

/// Cap on new articles processed per pipeline run: bounds LLM/TTS cost and
/// run duration. Any backlog beyond this is picked up on a later run --
/// only successfully-handled (or intentionally-skipped, e.g.
/// language-filtered) articles are marked processed (see
/// `persist::mark_processed`), so nothing is lost, just deferred.
const MAX_ITEMS_PER_RUN: usize = 5;

/// In-memory per-pipeline schedule anchor, mirroring
/// `scheduler::JobState`'s `next_fire` caching: an interval schedule
/// (`@every ...`) is only re-anchored when the schedule expression itself
/// changes or after it fires, never on every tick (re-deriving
/// `next_after(now)` every tick would perpetually push a plain interval's
/// next fire further out and it would never actually come due).
struct PipelineTickState {
    schedule_expr: String,
    next_fire: Option<DateTime<Local>>,
}

struct BotEngine {
    data_dir: PathBuf,
    ticks: Mutex<HashMap<String, PipelineTickState>>,
    /// Pipeline ids currently mid-run, guarding against a slow/long-running
    /// pipeline being fired again by the next tick (or a concurrent manual
    /// `bot.run`) before the previous run has finished -- see
    /// [`try_start_running`](BotEngine::try_start_running) and
    /// [`RunningGuard`]. A plain `std::sync::Mutex`, not the `tokio::sync::
    /// Mutex` [`ticks`](BotEngine::ticks) uses, because every critical
    /// section here is a single, non-`.await`-ing insert/remove/contains --
    /// no risk of blocking the executor, and it lets [`RunningGuard::drop`]
    /// release the slot synchronously (async `Drop` doesn't exist), which
    /// is what makes the guard panic-safe: a panicking `run_pipeline` still
    /// unwinds through the guard's `Drop` and frees the pipeline id.
    running: std::sync::Mutex<HashSet<String>>,
}

/// RAII guard marking one pipeline id as running for as long as it's held;
/// dropping it (including via unwind on panic) removes the id from
/// [`BotEngine::running`]. Acquired via
/// [`BotEngine::try_start_running`].
struct RunningGuard {
    engine: Arc<BotEngine>,
    pipeline_id: String,
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        if let Ok(mut running) = self.engine.running.lock() {
            running.remove(&self.pipeline_id);
        }
        // A poisoned lock here means some other panic already happened
        // while holding it; leaving the (probably-about-to-be-torn-down)
        // set as is rather than panicking again mid-unwind is the safer
        // choice for a `Drop` impl.
    }
}

impl BotEngine {
    /// Marks `pipeline_id` as running, returning a guard that un-marks it on
    /// drop -- or `None` if it's already running (the caller should skip/
    /// reject this attempt rather than run two instances of the same
    /// pipeline concurrently).
    fn try_start_running(self: &Arc<Self>, pipeline_id: &str) -> Option<RunningGuard> {
        let mut running = self.running.lock().expect("bot running-set lock poisoned");
        if !running.insert(pipeline_id.to_string()) {
            return None; // already present -- already running
        }
        Some(RunningGuard { engine: self.clone(), pipeline_id: pipeline_id.to_string() })
    }

    fn is_running(&self, pipeline_id: &str) -> bool {
        self.running.lock().expect("bot running-set lock poisoned").contains(pipeline_id)
    }
    /// Applies one pipeline's current config to the in-memory tick cache:
    /// removes it (so re-enabling recomputes a fresh anchor) when disabled,
    /// else re-anchors only if `schedule` actually changed since last seen.
    async fn sync_pipeline(&self, pipeline: &PipelineConfig, now: DateTime<Local>) {
        let mut ticks = self.ticks.lock().await;
        if !pipeline.enabled {
            ticks.remove(&pipeline.id);
            return;
        }
        let needs_recompute = ticks
            .get(&pipeline.id)
            .is_none_or(|state| state.schedule_expr != pipeline.schedule);
        if needs_recompute {
            let next_fire = compute_next_fire(&pipeline.schedule, now);
            ticks.insert(
                pipeline.id.clone(),
                PipelineTickState { schedule_expr: pipeline.schedule.clone(), next_fire },
            );
        }
    }

    /// Reconciles the tick cache against `pipelines` (the just-read-live
    /// config), then returns every pipeline whose anchor has come due,
    /// advancing each one's anchor in the same pass so a slow tick can't
    /// re-collect the same pipeline twice. Pipelines no longer present (or
    /// now disabled) have their cached state dropped.
    ///
    /// A pipeline still mid-run (per [`Self::is_running`] -- e.g. its
    /// previous fire hasn't finished, or a manual `bot.run` is in flight) is
    /// skipped even when due, and its anchor is deliberately **not**
    /// advanced: it stays "due" and is re-checked (and re-skipped, or
    /// finally fired) on every subsequent tick until it's no longer
    /// running, rather than silently missing a fire window.
    async fn take_due(&self, pipelines: &[PipelineConfig], now: DateTime<Local>) -> Vec<PipelineConfig> {
        {
            let live_ids: BTreeSet<&str> =
                pipelines.iter().filter(|p| p.enabled).map(|p| p.id.as_str()).collect();
            let mut ticks = self.ticks.lock().await;
            ticks.retain(|id, _| live_ids.contains(id.as_str()));
        }
        for pipeline in pipelines {
            self.sync_pipeline(pipeline, now).await;
        }

        let mut ticks = self.ticks.lock().await;
        let mut due = Vec::new();
        for pipeline in pipelines.iter().filter(|p| p.enabled) {
            let Some(state) = ticks.get_mut(&pipeline.id) else { continue };
            if state.next_fire.is_some_and(|t| t <= now) {
                if self.is_running(&pipeline.id) {
                    continue; // still running a previous fire; retry next tick
                }
                due.push(pipeline.clone());
                state.next_fire = compute_next_fire(&pipeline.schedule, now);
            }
        }
        due
    }

    async fn next_fire_of(&self, id: &str) -> Option<DateTime<Local>> {
        self.ticks.lock().await.get(id).and_then(|state| state.next_fire)
    }
}

fn compute_next_fire(expr: &str, now: DateTime<Local>) -> Option<DateTime<Local>> {
    match crate::scheduler::schedule::parse(expr) {
        Ok(parsed) => parsed.next_after(now),
        Err(error) => {
            warn!(%error, expr, "bot: pipeline has an invalid schedule expression; it will not fire");
            None
        }
    }
}

static ENGINE: OnceCell<Arc<BotEngine>> = OnceCell::const_new();

async fn ensure_engine(state: &Arc<AppState>) -> Result<Arc<BotEngine>> {
    let engine = ENGINE.get_or_try_init(|| async { init_engine(state).await }).await?;
    Ok(engine.clone())
}

async fn init_engine(state: &Arc<AppState>) -> Result<Arc<BotEngine>> {
    let data_dir = crate::config::data_dir()?;
    let engine = Arc::new(BotEngine {
        data_dir,
        ticks: Mutex::new(HashMap::new()),
        running: std::sync::Mutex::new(HashSet::new()),
    });
    let now = Local::now();
    for pipeline in &state.config().bot.pipelines {
        engine.sync_pipeline(pipeline, now).await;
    }
    Ok(engine)
}

/// Daemon-lifetime background task: every second, run any pipeline whose
/// schedule has come due. A no-op (logged) when `[bot] enabled = false` --
/// like `scheduler.enabled`, this master switch is only checked once at
/// startup (see `crate::config::applies_when`'s `"bot.enabled" => "daemon
/// restart"` entry); `bot.pipelines` itself, in contrast, is re-read from
/// config on *every* tick below, so pipeline add/edit/remove/reschedule
/// takes effect on the very next tick with no daemon restart needed. Manual
/// `bot.run` still works regardless of this switch.
pub fn spawn_background(state: Arc<AppState>) {
    if !state.config().bot.enabled {
        info!("bot: disabled via config; background tick loop not started");
        return;
    }
    tokio::spawn(async move {
        if let Err(error) = run_background(state).await {
            warn!(%error, "bot: background task failed to start");
        }
    });
}

async fn run_background(state: Arc<AppState>) -> Result<()> {
    let engine = ensure_engine(&state).await.context("bot: initializing engine")?;

    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // swallow the immediate first tick

    loop {
        ticker.tick().await;
        let now = Local::now();
        let pipelines = state.config().bot.pipelines;
        for pipeline in engine.take_due(&pipelines, now).await {
            // `take_due` already excludes still-running pipelines, but
            // acquire the guard here too (not just trust that check) so the
            // running-set is the single source of truth shared with manual
            // `bot.run` (see `cmd_run`) -- a concurrent manual run started
            // between `take_due`'s check and here is still caught.
            let Some(guard) = engine.try_start_running(&pipeline.id) else {
                continue;
            };
            let state = state.clone();
            let engine = engine.clone();
            tokio::spawn(async move {
                let _guard = guard; // released (Drop) when this task ends, success or panic
                let record = run_pipeline(&state, &engine.data_dir, &pipeline).await;
                if let Err(error) = persist::append_run(&engine.data_dir, &record).await {
                    warn!(%error, pipeline_id = %pipeline.id, "bot: failed to append run record");
                }
            });
        }
    }
}

/// Runs one pipeline end to end (source -> transforms -> sinks) and returns
/// its [`persist::RunRecord`] -- never panics, every failure mode is
/// captured in the record's `ok`/`error` fields. See the module doc's
/// "Fatal vs. non-fatal errors" section for exactly which failures abort
/// the run vs. are recorded per-item/per-sink and retried later.
async fn run_pipeline(state: &Arc<AppState>, data_dir: &std::path::Path, pipeline: &PipelineConfig) -> persist::RunRecord {
    let started_at = Utc::now();
    let config = state.config();

    // Fail fast on a config-level precondition before touching the network:
    // every transform's preset must resolve. Mirrors `validate_pipeline`'s
    // check, but here it aborts the run with a clear, actionable error
    // (config path + fix command) instead of just warning.
    if let Err(error) = check_transforms_resolve(&config, &pipeline.transforms) {
        return persist::RunRecord {
            pipeline_id: pipeline.id.clone(),
            started_at: started_at.to_rfc3339(),
            ended_at: Utc::now().to_rfc3339(),
            ok: false,
            error: Some(error.to_string()),
            fetched_count: 0,
            delivered_count: 0,
        };
    }

    let mut fetched_count = 0u64;
    let mut delivered_count = 0u64;
    let mut run_error: Option<String> = None;

    let candidates = match &pipeline.source {
        SourceConfig::GlobalArticles { rooms, langs } => {
            source::poll_candidates(state, data_dir, &pipeline.id, rooms, langs).await
        }
        SourceConfig::ChatRoom { room } => {
            source::poll_chat_candidates(state, data_dir, &pipeline.id, room).await
        }
    };
    match candidates {
        Ok(candidates) => {
            for candidate in candidates.into_iter().take(MAX_ITEMS_PER_RUN) {
                match candidate.article {
                    Some(article) => {
                        fetched_count += 1;
                        match transform::run_chain(state, &config, &pipeline.id, &pipeline.transforms, &article).await
                        {
                            Ok(outcome) => {
                                let item =
                                    sink::deliver(state, &pipeline.id, &pipeline.sinks, &article, &outcome).await;
                                if let Err(error) = persist::append_item(data_dir, &item).await {
                                    warn!(%error, pipeline_id = %pipeline.id, "bot: failed to append delivered item");
                                }
                                delivered_count += 1;
                                // Persisted immediately (one id at a time),
                                // not batched to the end of the run: if the
                                // daemon crashes right after this point, the
                                // article is already durably marked
                                // delivered and won't be redelivered on the
                                // next run. The per-run cap
                                // (`MAX_ITEMS_PER_RUN`) keeps this to at most
                                // a handful of extra small disk writes.
                                if let Err(error) =
                                    persist::mark_processed(data_dir, &pipeline.id, std::slice::from_ref(&candidate.id))
                                        .await
                                {
                                    warn!(%error, pipeline_id = %pipeline.id, article_id = %candidate.id, "bot: failed to persist a processed article id");
                                }
                            }
                            Err(error) => {
                                // Fatal per the module doc: stop processing
                                // further articles this run. The article
                                // that failed is *not* marked processed, so
                                // it's retried on the next run.
                                run_error = Some(format!(
                                    "transform failed for article {:?}: {error:#}",
                                    candidate.id
                                ));
                                break;
                            }
                        }
                    }
                    None => {
                        // Resolved and understood, but excluded forever
                        // (language filter, authorDid mismatch, ...) --
                        // never worth retrying. Persisted immediately for
                        // the same crash-safety reason as the delivered
                        // branch above.
                        if let Err(error) =
                            persist::mark_processed(data_dir, &pipeline.id, std::slice::from_ref(&candidate.id)).await
                        {
                            warn!(%error, pipeline_id = %pipeline.id, article_id = %candidate.id, "bot: failed to persist a processed article id");
                        }
                    }
                }
            }
        }
        Err(error) => {
            run_error = Some(format!("source failed: {error:#}"));
        }
    }

    persist::RunRecord {
        pipeline_id: pipeline.id.clone(),
        started_at: started_at.to_rfc3339(),
        ended_at: Utc::now().to_rfc3339(),
        ok: run_error.is_none(),
        error: run_error,
        fetched_count,
        delivered_count,
    }
}

/// Checks every `summarize`/`tts` transform's `preset_id` resolves against
/// `config.ai` -- and, for a `tts` transform, that the resolved preset also
/// has a `voice` set (required by `crate::ai::tts::synthesize`; see
/// `transform::synthesize_audio`) -- returning a config-path-plus-fix-command
/// error for the first problem found (per the brief's B5 policy). Mirrors
/// [`validate_pipeline`]'s equivalent checks exactly, just as a hard
/// fail-fast instead of a collected warning.
fn check_transforms_resolve(config: &Config, transforms: &[TransformConfig]) -> Result<()> {
    for transform in transforms {
        let (kind, preset_id) = match transform {
            TransformConfig::Summarize { preset_id } => ("summarize", preset_id),
            TransformConfig::Tts { preset_id, .. } => ("tts", preset_id),
            TransformConfig::Translate { preset_id, .. } => ("translate", preset_id),
        };
        if preset_id.trim().is_empty() {
            bail!(
                "a transform's preset_id is not set; set it with \
                 `mistl config set bot.pipelines <json>`"
            );
        }
        if let TransformConfig::Translate { target_lang, .. } = transform
            && target_lang.trim().is_empty()
        {
            bail!(
                "a translate transform's target_lang is not set; set it with \
                 `mistl config set bot.pipelines <json>`"
            );
        }
        let resolved = crate::config::resolve_preset(&config.ai, Some(preset_id)).with_context(|| {
            format!(
                "preset {preset_id:?} not found in ai.presets; add it with \
                 `mistl config set ai.presets <json>` (and ai.providers -- see `mistl config show`)"
            )
        })?;
        if kind == "tts" {
            let voice_set = resolved.voice.as_deref().is_some_and(|voice| !voice.trim().is_empty());
            if !voice_set {
                bail!(
                    "ai.presets[id={preset_id:?}].voice is not set; set it with \
                     `mistl config set ai.presets <json>`"
                );
            }
        }
    }
    Ok(())
}

/// Non-fatal configuration issues for one pipeline, surfaced by `bot.list`
/// (per-pipeline) and `bot.status` (aggregated across every pipeline).
/// Unlike [`check_transforms_resolve`] (which aborts a run), this never
/// stops anything -- it's purely diagnostic, matching the brief's warning
/// examples (`preset "worker" not found in ai.presets`, an unset sink
/// target, ...), each paired with the config path and a fix command.
fn validate_pipeline(config: &Config, pipeline: &PipelineConfig) -> Vec<String> {
    let mut warnings = Vec::new();
    let prefix = format!("bot.pipelines[id={:?}]", pipeline.id);

    for transform in &pipeline.transforms {
        let (kind, preset_id) = match transform {
            TransformConfig::Summarize { preset_id } => ("summarize", preset_id),
            TransformConfig::Tts { preset_id, .. } => ("tts", preset_id),
            TransformConfig::Translate { preset_id, .. } => ("translate", preset_id),
        };
        if let TransformConfig::Translate { target_lang, .. } = transform
            && target_lang.trim().is_empty()
        {
            warnings.push(format!(
                "{prefix}.transforms[kind=\"translate\"].target_lang is not set; set it with \
                 `mistl config set bot.pipelines <json>`"
            ));
        }
        if preset_id.trim().is_empty() {
            warnings.push(format!(
                "{prefix}.transforms[kind={kind:?}].preset_id is not set; set it with \
                 `mistl config set bot.pipelines <json>`"
            ));
        } else if crate::config::resolve_preset(&config.ai, Some(preset_id)).is_none() {
            warnings.push(format!(
                "preset {preset_id:?} not found in ai.presets; add it with \
                 `mistl config set ai.presets <json>` (and ai.providers -- see `mistl config show`)"
            ));
        } else if kind == "tts" {
            let voice_set = config
                .ai
                .presets
                .iter()
                .find(|p| &p.id == preset_id)
                .and_then(|p| p.voice.as_ref())
                .is_some_and(|voice| !voice.trim().is_empty());
            if !voice_set {
                warnings.push(format!(
                    "ai.presets[id={preset_id:?}].voice is not set; set it with \
                     `mistl config set ai.presets <json>`"
                ));
            }
        }
    }

    for sink in &pipeline.sinks {
        match sink {
            SinkConfig::ChatPost { room } if room.trim().is_empty() => {
                warnings.push(format!(
                    "{prefix}.sinks[kind=\"chat-post\"].room is not set; set it with \
                     `mistl config set bot.pipelines <json>`"
                ));
            }
            SinkConfig::Webhook { url, method, body_template, include_audio, headers, .. } => {
                if url.trim().is_empty() {
                    warnings.push(format!(
                        "{prefix}.sinks[kind=\"webhook\"].url is not set; set it with \
                         `mistl config set bot.pipelines <json>`"
                    ));
                }
                if let Some(method) = method {
                    let is_known =
                        ["post", "put", "patch"].iter().any(|known| method.eq_ignore_ascii_case(known));
                    if !is_known {
                        warnings.push(format!(
                            "{prefix}.sinks[kind=\"webhook\"].method {method:?} is not one of \
                             post/put/patch; requests will be sent as POST"
                        ));
                    }
                }
                if headers.iter().any(|header| header.name.trim().is_empty()) {
                    warnings.push(format!(
                        "{prefix}.sinks[kind=\"webhook\"].headers has an entry with an empty name; \
                         it will be skipped"
                    ));
                }
                if body_template.as_deref().is_some_and(|template| !template.trim().is_empty()) && *include_audio {
                    warnings.push(format!(
                        "{prefix}.sinks[kind=\"webhook\"].include_audio is ignored because \
                         body_template is set (template mode never embeds audio)"
                    ));
                }
            }
            SinkConfig::ArticlePublish { room } if room.trim().is_empty() => {
                warnings.push(format!(
                    "{prefix}.sinks[kind=\"article-publish\"].room is not set; set it with \
                     `mistl config set bot.pipelines <json>`"
                ));
            }
            _ => {}
        }
    }

    warnings
}

/// Sets one pipeline's `enabled` flag in `config.toml` and persists it.
/// `bot.pipelines` is an array, so `crate::config::set_by_path` (which only
/// replaces one whole `section.field` slot) can't express an in-place
/// element update -- this is the dedicated helper the brief calls for.
async fn set_pipeline_enabled(state: &Arc<AppState>, id: &str, enabled: bool) -> Result<PipelineConfig> {
    let mut config = state.config();
    let pipeline = config
        .bot
        .pipelines
        .iter_mut()
        .find(|p| p.id == id)
        .with_context(|| {
            format!("no bot pipeline with id {id:?} (see `mistl config show` for configured pipeline ids)")
        })?;
    pipeline.enabled = enabled;
    let updated = pipeline.clone();
    config.save()?;
    state.set_config(config);
    Ok(updated)
}

#[derive(Deserialize, Default)]
struct IdArg {
    id: String,
}

#[derive(Deserialize, Default)]
struct ListArgs {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

/// IPC entry point for the `bot.*` namespace. See the module doc and the
/// Wave 2 brief for the exact response shapes:
/// - `bot.list` `{}` -> `{"pipelines": [...]}`
/// - `bot.run` `{"id"}` -> a [`persist::RunRecord`]
/// - `bot.enable` / `bot.disable` `{"id"}` -> `{"id", "enabled"}`
/// - `bot.logs` `{"id"?, "limit"?}` -> `{"runs": [...]}` (newest first)
/// - `bot.items` `{"id"?, "limit"?}` -> `{"items": [...]}` (newest first)
/// - `bot.status` `{}` -> `{"enabled", "pipeline_count", "rooms", "warnings"}`
/// - `bot.options` `{}` -> `{"rooms": [{"room", "joined"}], "presets":
///   [{"id", "label", "model", "has_voice"}]}` -- the selection lists the
///   dashboard's pipeline form offers instead of free-text input
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    let engine = ensure_engine(state).await?;
    match cmd {
        "bot.list" => cmd_list(&engine, state).await,
        "bot.run" => cmd_run(&engine, state, args).await,
        "bot.enable" => cmd_set_enabled(&engine, state, args, true).await,
        "bot.disable" => cmd_set_enabled(&engine, state, args, false).await,
        "bot.logs" => cmd_logs(&engine, args).await,
        "bot.items" => cmd_items(&engine, args).await,
        "bot.status" => cmd_status(&engine, state).await,
        "bot.options" => cmd_options(state).await,
        _ => bail!("unknown command: {cmd}"),
    }
}

async fn cmd_list(engine: &Arc<BotEngine>, state: &Arc<AppState>) -> Result<Value> {
    let config = state.config();
    let runs = persist::read_runs(&engine.data_dir).await?;
    let mut pipelines = Vec::with_capacity(config.bot.pipelines.len());
    for pipeline in &config.bot.pipelines {
        let next_run = engine.next_fire_of(&pipeline.id).await.map(|t| t.to_rfc3339());
        let last_run = runs.iter().rev().find(|r| r.pipeline_id == pipeline.id).cloned();
        pipelines.push(json!({
            "id": pipeline.id,
            "enabled": pipeline.enabled,
            "schedule": pipeline.schedule,
            "next_run": next_run,
            "last_run": last_run,
            "warnings": validate_pipeline(&config, pipeline),
        }));
    }
    Ok(json!({ "pipelines": pipelines }))
}

async fn cmd_run(engine: &Arc<BotEngine>, state: &Arc<AppState>, args: Value) -> Result<Value> {
    let args: IdArg = serde_json::from_value(args).context("bot.run: invalid arguments")?;
    let pipeline = state
        .config()
        .bot
        .pipelines
        .into_iter()
        .find(|p| p.id == args.id)
        .with_context(|| format!("no bot pipeline with id {:?}", args.id))?;

    // Shares `BotEngine::running` with the tick loop (see `run_background`):
    // a pipeline the tick loop is already running (or a second concurrent
    // `bot.run` for the same id) is rejected outright rather than run
    // twice in parallel.
    let Some(guard) = engine.try_start_running(&pipeline.id) else {
        bail!("bot pipeline {:?} is already running", pipeline.id);
    };
    let record = run_pipeline(state, &engine.data_dir, &pipeline).await;
    drop(guard);

    if let Err(error) = persist::append_run(&engine.data_dir, &record).await {
        warn!(%error, pipeline_id = %pipeline.id, "bot: failed to append run record");
    }
    info!(pipeline_id = %pipeline.id, ok = record.ok, "bot: pipeline run requested via IPC");
    Ok(serde_json::to_value(&record)?)
}

async fn cmd_set_enabled(engine: &Arc<BotEngine>, state: &Arc<AppState>, args: Value, enabled: bool) -> Result<Value> {
    let args: IdArg = serde_json::from_value(args).context("bot.enable/bot.disable: invalid arguments")?;
    let updated = set_pipeline_enabled(state, &args.id, enabled).await?;
    engine.sync_pipeline(&updated, Local::now()).await;
    info!(pipeline_id = %updated.id, enabled, "bot: pipeline enabled state changed");
    Ok(json!({ "id": updated.id, "enabled": updated.enabled }))
}

async fn cmd_logs(engine: &Arc<BotEngine>, args: Value) -> Result<Value> {
    let args: ListArgs = serde_json::from_value(args).context("bot.logs: invalid arguments")?;
    let mut runs = persist::read_runs(&engine.data_dir).await?;
    runs.reverse(); // newest first
    if let Some(id) = &args.id {
        runs.retain(|r| &r.pipeline_id == id);
    }
    runs.truncate(args.limit.unwrap_or(50));
    Ok(json!({ "runs": runs }))
}

async fn cmd_items(engine: &Arc<BotEngine>, args: Value) -> Result<Value> {
    let args: ListArgs = serde_json::from_value(args).context("bot.items: invalid arguments")?;
    let mut items = persist::read_items(&engine.data_dir).await?;
    items.reverse(); // newest first
    if let Some(id) = &args.id {
        items.retain(|i| &i.pipeline_id == id);
    }
    items.truncate(args.limit.unwrap_or(50));
    Ok(json!({ "items": items }))
}

async fn cmd_status(engine: &Arc<BotEngine>, state: &Arc<AppState>) -> Result<Value> {
    let _ = engine; // status is computed live from config; the engine isn't otherwise needed
    let config = state.config();
    let mut warnings = Vec::new();
    let mut rooms: BTreeSet<String> = BTreeSet::new();
    for pipeline in &config.bot.pipelines {
        warnings.extend(validate_pipeline(&config, pipeline));
        match &pipeline.source {
            SourceConfig::GlobalArticles { rooms: pipeline_rooms, .. } => {
                if pipeline_rooms.is_empty() {
                    rooms.insert(source::GLOBAL_ARTICLES_ROOM_ID.to_string());
                } else {
                    for room in pipeline_rooms {
                        let room = room.trim();
                        if !room.is_empty() {
                            rooms.insert(room.to_string());
                        }
                    }
                }
            }
            SourceConfig::ChatRoom { room } => {
                let room = room.trim();
                if !room.is_empty() {
                    rooms.insert(room.to_string());
                }
            }
        }
    }
    let joined = crate::net::joined_rooms().await;
    let rooms_json: Vec<Value> = rooms
        .iter()
        .map(|room| json!({ "room": room, "joined": joined.contains(room) }))
        .collect();
    Ok(json!({
        "enabled": config.bot.enabled,
        "pipeline_count": config.bot.pipelines.len(),
        "rooms": rooms_json,
        "warnings": warnings,
    }))
}

/// Collects every room id the daemon knows about -- the well-known
/// global-articles room, rooms referenced anywhere in config (bot
/// pipelines, `ai.room_id`, `storage.room_ids`, `mailbox.chat_rooms`), and
/// currently-joined rooms -- so the dashboard's pipeline form can offer a
/// picker instead of a free-text field. Purely advisory: a pipeline may
/// still name a room that isn't listed here.
async fn cmd_options(state: &Arc<AppState>) -> Result<Value> {
    let config = state.config();
    let mut rooms: BTreeSet<String> = BTreeSet::new();
    let add_room = |room: &str, rooms: &mut BTreeSet<String>| {
        let room = room.trim();
        if !room.is_empty() {
            rooms.insert(room.to_string());
        }
    };
    add_room(source::GLOBAL_ARTICLES_ROOM_ID, &mut rooms);
    if let Some(room) = &config.ai.room_id {
        add_room(room, &mut rooms);
    }
    for room in &config.storage.room_ids {
        add_room(room, &mut rooms);
    }
    for room in &config.mailbox.chat_rooms {
        add_room(room, &mut rooms);
    }
    for pipeline in &config.bot.pipelines {
        match &pipeline.source {
            SourceConfig::GlobalArticles { rooms: source_rooms, .. } => {
                for room in source_rooms {
                    add_room(room, &mut rooms);
                }
            }
            SourceConfig::ChatRoom { room } => add_room(room, &mut rooms),
        }
        for sink in &pipeline.sinks {
            match sink {
                SinkConfig::ChatPost { room } | SinkConfig::ArticlePublish { room } => add_room(room, &mut rooms),
                SinkConfig::Webhook { .. } => {}
            }
        }
    }
    let joined = crate::net::joined_rooms().await;
    for room in &joined {
        add_room(room, &mut rooms);
    }
    let rooms_json: Vec<Value> = rooms
        .iter()
        .map(|room| json!({ "room": room, "joined": joined.contains(room) }))
        .collect();

    let presets_json: Vec<Value> = config
        .ai
        .presets
        .iter()
        .map(|preset| {
            json!({
                "id": preset.id,
                "label": preset.label,
                "model": preset.model,
                "has_voice": preset.voice.as_deref().is_some_and(|voice| !voice.trim().is_empty()),
            })
        })
        .collect();

    Ok(json!({ "rooms": rooms_json, "presets": presets_json }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AiPresetConfig, AiProviderConfig};

    fn sample_pipeline(id: &str, schedule: &str) -> PipelineConfig {
        PipelineConfig {
            id: id.to_string(),
            enabled: true,
            schedule: schedule.to_string(),
            source: SourceConfig::GlobalArticles { rooms: vec![], langs: vec![] },
            transforms: vec![
                TransformConfig::Summarize { preset_id: "worker".to_string() },
                TransformConfig::Tts { preset_id: "tts-default".to_string(), format: None, speed: None },
            ],
            sinks: vec![SinkConfig::ChatPost { room: "chat-room".to_string() }],
        }
    }

    fn configured_ai() -> Config {
        let mut config = Config::default();
        config.ai.providers.push(AiProviderConfig {
            id: "openai".to_string(),
            label: "OpenAI".to_string(),
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: "sk-test".to_string(),
        });
        config.ai.presets.push(AiPresetConfig {
            id: "worker".to_string(),
            label: "Worker".to_string(),
            provider_id: "openai".to_string(),
            model: "gpt-4o-mini".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
        });
        config.ai.presets.push(AiPresetConfig {
            id: "tts-default".to_string(),
            label: "TTS".to_string(),
            provider_id: "openai".to_string(),
            model: "tts-1".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: Some("alloy".to_string()),
        });
        config
    }

    fn test_engine() -> Arc<BotEngine> {
        Arc::new(BotEngine {
            data_dir: PathBuf::new(),
            ticks: Mutex::new(HashMap::new()),
            running: std::sync::Mutex::new(HashSet::new()),
        })
    }

    // -- schedule anchoring (mirrors scheduler::tests's coverage of the
    // same anchoring idiom, applied to `BotEngine` instead of `Job`) --

    #[tokio::test]
    async fn take_due_fires_an_interval_pipeline_once_its_anchor_is_reached() {
        let engine = BotEngine {
            data_dir: PathBuf::new(),
            ticks: Mutex::new(HashMap::new()),
            running: std::sync::Mutex::new(HashSet::new()),
        };
        let pipeline = sample_pipeline("p1", "@every 1h");
        let now = Local::now();

        // First pass anchors next_fire ~1h out; not due yet.
        let due = engine.take_due(&[pipeline.clone()], now).await;
        assert!(due.is_empty());
        assert!(engine.next_fire_of("p1").await.unwrap() > now);

        // A later pass whose "now" has caught up to the anchor fires it.
        let anchor = engine.next_fire_of("p1").await.unwrap();
        let due = engine.take_due(&[pipeline], anchor).await;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, "p1");
        // Re-anchored to another ~1h out, strictly after the fire time --
        // an interval never re-derives from "now" on every tick (see the
        // struct doc), it advances from the fire point.
        assert!(engine.next_fire_of("p1").await.unwrap() > anchor);
    }

    #[tokio::test]
    async fn take_due_drops_cached_state_for_disabled_or_removed_pipelines() {
        let engine = BotEngine {
            data_dir: PathBuf::new(),
            ticks: Mutex::new(HashMap::new()),
            running: std::sync::Mutex::new(HashSet::new()),
        };
        let mut pipeline = sample_pipeline("p1", "@every 1h");
        let now = Local::now();
        engine.take_due(&[pipeline.clone()], now).await;
        assert!(engine.next_fire_of("p1").await.is_some());

        pipeline.enabled = false;
        engine.take_due(&[pipeline], now).await;
        assert!(engine.next_fire_of("p1").await.is_none());
    }

    #[tokio::test]
    async fn take_due_re_anchors_when_the_schedule_expression_changes() {
        let engine = BotEngine {
            data_dir: PathBuf::new(),
            ticks: Mutex::new(HashMap::new()),
            running: std::sync::Mutex::new(HashSet::new()),
        };
        let now = Local::now();
        engine.take_due(&[sample_pipeline("p1", "@every 1h")], now).await;
        let first_anchor = engine.next_fire_of("p1").await.unwrap();

        engine.take_due(&[sample_pipeline("p1", "@every 5m")], now).await;
        let second_anchor = engine.next_fire_of("p1").await.unwrap();
        assert_ne!(first_anchor, second_anchor);
    }

    #[tokio::test]
    async fn take_due_treats_an_invalid_schedule_as_never_firing() {
        let engine = BotEngine {
            data_dir: PathBuf::new(),
            ticks: Mutex::new(HashMap::new()),
            running: std::sync::Mutex::new(HashSet::new()),
        };
        let due = engine
            .take_due(&[sample_pipeline("p1", "not a valid schedule")], Local::now())
            .await;
        assert!(due.is_empty());
        assert!(engine.next_fire_of("p1").await.is_none());
    }

    // -- concurrent-run guard (`BotEngine::running` / `RunningGuard`) --

    #[tokio::test]
    async fn take_due_skips_a_pipeline_that_is_already_running_and_does_not_advance_its_anchor() {
        let engine = test_engine();
        let pipeline = sample_pipeline("p1", "@every 1h");

        // Anchor it, then advance "now" to the anchor so it's due.
        engine.take_due(&[pipeline.clone()], Local::now()).await;
        let anchor = engine.next_fire_of("p1").await.unwrap();

        // Mark it running, the way `run_background`'s tick loop (and
        // `cmd_run`) do before spawning/awaiting the actual run.
        let guard = engine.try_start_running("p1").expect("not yet running");
        assert!(engine.is_running("p1"));

        let due = engine.take_due(&[pipeline.clone()], anchor).await;
        assert!(due.is_empty(), "a running pipeline must be skipped even though its anchor is due");
        // The anchor must not have advanced -- it stays due so the very
        // next tick retries it instead of silently skipping a whole
        // schedule interval.
        assert_eq!(engine.next_fire_of("p1").await.unwrap(), anchor);

        // Once the run finishes (guard dropped), the still-due anchor fires.
        drop(guard);
        assert!(!engine.is_running("p1"));
        let due = engine.take_due(&[pipeline], anchor).await;
        assert_eq!(due.len(), 1, "no longer running -- the still-due pipeline now fires");
    }

    #[tokio::test]
    async fn try_start_running_refuses_a_second_concurrent_start_for_the_same_pipeline() {
        let engine = test_engine();
        let first = engine.try_start_running("p1").expect("first start must succeed");
        assert!(engine.try_start_running("p1").is_none(), "a second concurrent start must be refused");

        // A different pipeline id is unaffected.
        assert!(engine.try_start_running("p2").is_some());

        drop(first);
        assert!(
            engine.try_start_running("p1").is_some(),
            "after the guard drops, the same id can start again"
        );
    }

    #[tokio::test]
    async fn running_guard_releases_its_slot_even_when_the_holder_panics() {
        // Panic safety: the guard's `Drop` impl (not an explicit
        // "finally"-style cleanup) is what frees the slot, so it must run
        // even when the task holding it unwinds.
        let engine = test_engine();
        let engine_for_task = engine.clone();
        let handle = tokio::spawn(async move {
            let _guard = engine_for_task.try_start_running("p1").unwrap();
            panic!("simulated pipeline run panic");
        });
        assert!(handle.await.is_err(), "the spawned task must have panicked");
        assert!(!engine.is_running("p1"), "the running slot must be released after the panic unwound");
    }

    // -- validation warnings --

    #[test]
    fn validate_pipeline_is_clean_for_a_fully_resolved_pipeline() {
        let config = configured_ai();
        let pipeline = sample_pipeline("p1", "@every 1h");
        assert!(validate_pipeline(&config, &pipeline).is_empty());
    }

    #[test]
    fn validate_pipeline_flags_an_unresolved_transform_preset() {
        let config = Config::default(); // no providers/presets configured
        let pipeline = sample_pipeline("p1", "@every 1h");
        let warnings = validate_pipeline(&config, &pipeline);
        assert!(warnings.iter().any(|w| w.contains("worker") && w.contains("ai.presets")));
        assert!(warnings.iter().any(|w| w.contains("tts-default")));
    }

    #[test]
    fn validate_pipeline_flags_a_tts_preset_missing_a_voice() {
        let mut config = configured_ai();
        config.ai.presets.iter_mut().find(|p| p.id == "tts-default").unwrap().voice = None;
        let pipeline = sample_pipeline("p1", "@every 1h");
        let warnings = validate_pipeline(&config, &pipeline);
        assert!(warnings.iter().any(|w| w.contains("voice") && w.contains("tts-default")));
    }

    #[test]
    fn validate_pipeline_flags_an_unset_chat_post_room() {
        let config = configured_ai();
        let mut pipeline = sample_pipeline("p1", "@every 1h");
        pipeline.sinks = vec![SinkConfig::ChatPost { room: String::new() }];
        let warnings = validate_pipeline(&config, &pipeline);
        assert!(warnings.iter().any(|w| w.contains("sinks") && w.contains("room")));
    }

    #[test]
    fn validate_pipeline_flags_an_unset_webhook_url() {
        let config = configured_ai();
        let mut pipeline = sample_pipeline("p1", "@every 1h");
        pipeline.sinks = vec![SinkConfig::Webhook {
            url: String::new(),
            include_audio: false,
            max_audio_bytes: None,
            method: None,
            body_template: None,
            sign: true,
            include_body: false,
            headers: Vec::new(),
        }];
        let warnings = validate_pipeline(&config, &pipeline);
        assert!(warnings.iter().any(|w| w.contains("sinks") && w.contains("url")));
    }

    #[test]
    fn validate_pipeline_flags_an_unknown_webhook_method() {
        let config = configured_ai();
        let mut pipeline = sample_pipeline("p1", "@every 1h");
        pipeline.sinks = vec![SinkConfig::Webhook {
            url: "https://example.com/hook".to_string(),
            include_audio: false,
            max_audio_bytes: None,
            method: Some("DELETE".to_string()),
            body_template: None,
            sign: true,
            include_body: false,
            headers: Vec::new(),
        }];
        let warnings = validate_pipeline(&config, &pipeline);
        assert!(warnings.iter().any(|w| w.contains("method")), "got: {warnings:?}");
    }

    #[test]
    fn validate_pipeline_accepts_case_insensitive_known_webhook_methods() {
        let config = configured_ai();
        let mut pipeline = sample_pipeline("p1", "@every 1h");
        pipeline.sinks = vec![SinkConfig::Webhook {
            url: "https://example.com/hook".to_string(),
            include_audio: false,
            max_audio_bytes: None,
            method: Some("put".to_string()),
            body_template: None,
            sign: true,
            include_body: false,
            headers: Vec::new(),
        }];
        let warnings = validate_pipeline(&config, &pipeline);
        assert!(!warnings.iter().any(|w| w.contains("method")), "got: {warnings:?}");
    }

    #[test]
    fn validate_pipeline_flags_empty_header_name_and_body_template_with_include_audio() {
        let config = configured_ai();
        let mut pipeline = sample_pipeline("p1", "@every 1h");
        pipeline.sinks = vec![SinkConfig::Webhook {
            url: "https://example.com/hook".to_string(),
            include_audio: true,
            max_audio_bytes: None,
            method: None,
            body_template: Some("{{title}}".to_string()),
            sign: true,
            include_body: false,
            headers: vec![crate::config::WebhookHeader { name: String::new(), value: "v".to_string() }],
        }];
        let warnings = validate_pipeline(&config, &pipeline);
        assert!(warnings.iter().any(|w| w.contains("headers")), "got: {warnings:?}");
        assert!(warnings.iter().any(|w| w.contains("include_audio")), "got: {warnings:?}");
    }

    #[test]
    fn check_transforms_resolve_matches_validate_pipelines_preset_check() {
        let config = Config::default();
        let pipeline = sample_pipeline("p1", "@every 1h");
        let err = check_transforms_resolve(&config, &pipeline.transforms).expect_err("unresolved preset must error");
        assert!(err.to_string().contains("ai.presets"));

        let config = configured_ai();
        assert!(check_transforms_resolve(&config, &pipeline.transforms).is_ok());
    }

    #[test]
    fn check_transforms_resolve_fails_fast_when_a_tts_preset_has_no_voice() {
        // Mirrors `validate_pipeline_flags_a_tts_preset_missing_a_voice`,
        // but as `check_transforms_resolve`'s hard-fail path -- the two must
        // agree on what counts as "not usable", per this function's doc
        // comment ("mirrors validate_pipeline's equivalent checks exactly").
        let mut config = configured_ai();
        config.ai.presets.iter_mut().find(|p| p.id == "tts-default").unwrap().voice = None;
        let pipeline = sample_pipeline("p1", "@every 1h");

        let err = check_transforms_resolve(&config, &pipeline.transforms)
            .expect_err("a tts preset with no voice must fail fast");
        let message = err.to_string();
        assert!(message.contains("voice"), "{message}");
        assert!(message.contains("tts-default"), "{message}");
    }

    #[test]
    fn check_transforms_resolve_ignores_voice_for_a_summarize_only_pipeline() {
        // A summarize-only pipeline has no tts step, so a missing voice
        // anywhere in ai.presets must not block it.
        let mut config = configured_ai();
        config.ai.presets.iter_mut().find(|p| p.id == "tts-default").unwrap().voice = None;
        let summarize_only = vec![TransformConfig::Summarize { preset_id: "worker".to_string() }];
        assert!(check_transforms_resolve(&config, &summarize_only).is_ok());
    }
}
