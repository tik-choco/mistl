//! Cron-like job scheduler, ported from the Go tool `tc-sched`: jobs
//! (id/name/schedule expression/shell command/enabled) are persisted on
//! disk, executed by the daemon on their own schedule, and every execution
//! is recorded in a capped run log.
//!
//! - [`schedule`] is the pure schedule-expression parser/evaluator (`@every
//!   ...`, descriptors, cron) -- the unit-test core, no filesystem or clock
//!   dependency beyond the `DateTime<Local>` passed in.
//! - [`runner`] shells out a job's command and captures its outcome.
//! - This module ties both together: the on-disk jobs table
//!   (`<data_dir>/scheduler-jobs.json`, the folder_sync "table file" idiom),
//!   the run log (`<data_dir>/scheduler-runs.jsonl`, the chat_relay capped
//!   append-log idiom), the in-memory next-fire cache, the IPC surface
//!   (`sched.*`), and the daemon-lifetime background tick loop.

mod runner;
mod schedule;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;

use anyhow::{Context, Result, bail};
use chrono::{Local, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{OnceCell, RwLock};
use tracing::{info, warn};

use crate::daemon::AppState;

/// On-disk row == wire shape (serde snake_case as written): the fixed
/// contract the CLI and web dashboard code against.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub name: String,
    pub schedule: String,
    pub command: String,
    pub enabled: bool,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
}

/// One line in the run log per execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub job_id: String,
    pub job_name: String,
    pub started_at: String,
    pub ended_at: String,
    pub exit_code: i64,
    pub ok: bool,
    pub output: String,
}

const RUN_LOG_CAP: usize = 200;

// ---------------------------------------------------------------------
// Jobs table persistence: `<data_dir>/scheduler-jobs.json`. Mirrors
// `storage::folder_sync`'s table-file idiom (see its module doc around
// lines 475-556): a process-wide lock guarding a plain JSON array, with a
// transactional read-modify-write helper so a `bail!` inside the mutator
// leaves the file untouched.
// ---------------------------------------------------------------------

fn jobs_table_path(data_dir: &Path) -> PathBuf {
    data_dir.join("scheduler-jobs.json")
}

fn jobs_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn load_jobs_unlocked(data_dir: &Path) -> Result<Vec<Job>> {
    let path = jobs_table_path(data_dir);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn save_jobs_unlocked(data_dir: &Path, jobs: &[Job]) -> Result<()> {
    let path = jobs_table_path(data_dir);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let text = serde_json::to_string_pretty(jobs)?;
    std::fs::write(&path, text).with_context(|| format!("writing {}", path.display()))
}

/// Read-modify-write under [`jobs_lock`]: `f` sees the current table and may
/// mutate it in place; on `Ok`, the (possibly mutated) table is written back
/// before returning `f`'s result.
async fn with_jobs_at<F, R>(data_dir: &Path, f: F) -> Result<R>
where
    F: FnOnce(&mut Vec<Job>) -> Result<R>,
{
    let _guard = jobs_lock().lock().await;
    let mut jobs = load_jobs_unlocked(data_dir)?;
    let result = f(&mut jobs)?;
    save_jobs_unlocked(data_dir, &jobs)?;
    Ok(result)
}

// ---------------------------------------------------------------------
// Run log persistence: `<data_dir>/scheduler-runs.jsonl`, capped at
// `RUN_LOG_CAP` most-recent records. Mirrors `mailbox::chat_relay`'s capped
// JSONL append-log idiom (see its module doc around lines 374-446): atomic
// temp-file+rename rewrite on every append.
// ---------------------------------------------------------------------

fn runs_log_path(data_dir: &Path) -> PathBuf {
    data_dir.join("scheduler-runs.jsonl")
}

fn runs_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn read_run_lines(path: &Path) -> Result<Vec<String>> {
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
/// directory then rename over the target (atomic-ish on the same volume).
fn write_run_lines(path: &Path, lines: &[String]) -> Result<()> {
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

/// Appends `record`, keeping only the [`RUN_LOG_CAP`] most-recent entries
/// (oldest dropped first).
async fn append_run_record(data_dir: &Path, record: &RunRecord) -> Result<()> {
    let _guard = runs_lock().lock().await;
    let path = runs_log_path(data_dir);
    let mut lines = read_run_lines(&path)?;
    lines.push(serde_json::to_string(record)?);
    if lines.len() > RUN_LOG_CAP {
        let excess = lines.len() - RUN_LOG_CAP;
        lines.drain(0..excess);
    }
    write_run_lines(&path, &lines)
}

/// Reads every run record, oldest first (append order).
async fn read_run_records(data_dir: &Path) -> Result<Vec<RunRecord>> {
    let _guard = runs_lock().lock().await;
    let lines = read_run_lines(&runs_log_path(data_dir))?;
    lines
        .iter()
        .map(|line| serde_json::from_str(line).context("parsing run record"))
        .collect()
}

// ---------------------------------------------------------------------
// In-memory service: the persisted table is the source of truth, but each
// enabled job's next-fire time is tracked in memory only (never persisted
// across restarts -- matches tc-sched). Interval-style schedules
// (`Schedule::Interval`/`IntervalAtTime`) are "anchored": recomputed only on
// startup, on any mutation of that job, and immediately after each fire --
// never on every tick -- since re-deriving `next_after(now)` on every tick
// would perpetually push a plain interval's next fire another interval into
// the future and it would never actually come due.
// ---------------------------------------------------------------------

struct JobState {
    job: Job,
    /// `None` when the job is disabled or its schedule fails to parse.
    next_fire: Option<chrono::DateTime<Local>>,
}

struct SchedulerService {
    data_dir: PathBuf,
    jobs: RwLock<Vec<JobState>>,
}

impl SchedulerService {
    /// Applies a just-persisted `job` to the in-memory cache: recomputes its
    /// next-fire time and either updates the existing entry or inserts a new
    /// one. Used by every mutating IPC command (add/set/enable) and by
    /// [`run_background`] right after a job fires.
    async fn sync_job_state(&self, job: Job, now: chrono::DateTime<Local>) {
        let next_fire = compute_next_fire(&job, now);
        let mut jobs = self.jobs.write().await;
        if let Some(state) = jobs.iter_mut().find(|s| s.job.id == job.id) {
            state.job = job;
            state.next_fire = next_fire;
        } else {
            jobs.push(JobState { job, next_fire });
        }
    }

    async fn remove_job_state(&self, id: &str) {
        self.jobs.write().await.retain(|s| s.job.id != id);
    }

    /// Jobs whose next-fire has arrived, advancing each one's cached
    /// next-fire in the same pass (so a slow tick can't re-collect the same
    /// job twice).
    async fn take_due_jobs(&self, now: chrono::DateTime<Local>) -> Vec<Job> {
        let mut jobs = self.jobs.write().await;
        let mut due = Vec::new();
        for state in jobs.iter_mut() {
            if state.next_fire.is_some_and(|t| t <= now) {
                due.push(state.job.clone());
                state.next_fire = compute_next_fire(&state.job, now);
            }
        }
        due
    }
}

fn compute_next_fire(job: &Job, now: chrono::DateTime<Local>) -> Option<chrono::DateTime<Local>> {
    if !job.enabled {
        return None;
    }
    match schedule::parse(&job.schedule) {
        Ok(parsed) => parsed.next_after(now),
        Err(error) => {
            warn!(job_id = %job.id, %error, "scheduler: job has an invalid schedule; it will not fire");
            None
        }
    }
}

fn job_json(job: &Job, next_fire: Option<chrono::DateTime<Local>>) -> Value {
    let mut value = serde_json::to_value(job).expect("Job always serializes");
    value["next_run"] = match next_fire {
        Some(t) => json!(t.to_rfc3339()),
        None => Value::Null,
    };
    value
}

fn new_job_id(existing: &[Job]) -> String {
    loop {
        let n: u32 = rand::thread_rng().gen_range(0..1_000_000);
        let candidate = format!("job-{n:06}");
        if !existing.iter().any(|j| j.id == candidate) {
            return candidate;
        }
    }
}

static SERVICE: OnceCell<Arc<SchedulerService>> = OnceCell::const_new();

async fn ensure_service() -> Result<Arc<SchedulerService>> {
    let service = SERVICE.get_or_try_init(init_service).await?;
    Ok(service.clone())
}

async fn init_service() -> Result<Arc<SchedulerService>> {
    let data_dir = crate::config::data_dir()?;
    let jobs = load_jobs_unlocked(&data_dir)?;
    let now = Local::now();
    let states = jobs
        .into_iter()
        .map(|job| {
            let next_fire = compute_next_fire(&job, now);
            JobState { job, next_fire }
        })
        .collect();
    Ok(Arc::new(SchedulerService {
        data_dir,
        jobs: RwLock::new(states),
    }))
}

/// Daemon-lifetime background task: every second, fire any job whose
/// next-fire has arrived (each execution spawned independently, so
/// overlapping runs of the same job are allowed, like tc-sched). A no-op
/// (logged) when `[scheduler] enabled = false` -- manual `sched.*` commands
/// (including "run now") still work either way, only the automatic tick
/// loop is gated.
pub fn spawn_background(state: Arc<AppState>) {
    if !state.config().scheduler.enabled {
        info!("scheduler: disabled via config; background tick loop not started");
        return;
    }
    tokio::spawn(async move {
        if let Err(error) = run_background().await {
            warn!(%error, "scheduler: background task failed to start");
        }
    });
}

async fn run_background() -> Result<()> {
    let service = ensure_service()
        .await
        .context("scheduler: initializing service")?;

    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // swallow the immediate first tick

    loop {
        ticker.tick().await;
        let now = Local::now();
        for job in service.take_due_jobs(now).await {
            let data_dir = service.data_dir.clone();
            tokio::spawn(async move {
                runner::execute_and_record(&data_dir, &job).await;
            });
        }
    }
}

/// IPC entry point for the `sched.*` namespace.
pub async fn handle(cmd: &str, args: Value, _state: &Arc<AppState>) -> Result<Value> {
    let service = ensure_service().await?;
    match cmd {
        "sched.ls" => {
            let jobs = service.jobs.read().await;
            let mut states: Vec<&JobState> = jobs.iter().collect();
            states.sort_by(|a, b| a.job.name.cmp(&b.job.name));
            let list: Vec<Value> = states
                .iter()
                .map(|s| job_json(&s.job, s.next_fire))
                .collect();
            Ok(json!({ "jobs": list }))
        }

        "sched.add" => {
            let name = args
                .get("name")
                .and_then(Value::as_str)
                .context("missing `name`")?
                .to_string();
            let schedule_expr = args
                .get("schedule")
                .and_then(Value::as_str)
                .context("missing `schedule`")?
                .to_string();
            let command = args
                .get("command")
                .and_then(Value::as_str)
                .context("missing `command`")?
                .to_string();
            let enabled = args.get("enabled").and_then(Value::as_bool).unwrap_or(true);
            schedule::parse(&schedule_expr)
                .with_context(|| format!("invalid schedule expression {schedule_expr:?}"))?;

            let now_wire = Utc::now().to_rfc3339();
            let mut job = Job {
                id: String::new(),
                name,
                schedule: schedule_expr,
                command,
                enabled,
                created_at: now_wire.clone(),
                updated_at: now_wire,
            };
            with_jobs_at(&service.data_dir, |jobs| {
                job.id = new_job_id(jobs);
                jobs.push(job.clone());
                Ok(())
            })
            .await?;
            service.sync_job_state(job.clone(), Local::now()).await;
            info!(job_id = %job.id, name = %job.name, "scheduler: job added");
            Ok(serde_json::to_value(&job)?)
        }

        "sched.set" => {
            let id = args
                .get("id")
                .and_then(Value::as_str)
                .context("missing `id`")?
                .to_string();
            let name = args.get("name").and_then(Value::as_str).map(str::to_string);
            let schedule_expr = args
                .get("schedule")
                .and_then(Value::as_str)
                .map(str::to_string);
            let command = args
                .get("command")
                .and_then(Value::as_str)
                .map(str::to_string);
            let enabled = args.get("enabled").and_then(Value::as_bool);
            if let Some(expr) = &schedule_expr {
                schedule::parse(expr)
                    .with_context(|| format!("invalid schedule expression {expr:?}"))?;
            }

            let updated = with_jobs_at(&service.data_dir, |jobs| {
                let entry = jobs
                    .iter_mut()
                    .find(|j| j.id == id)
                    .with_context(|| format!("no job with id {id:?}"))?;
                if let Some(name) = name {
                    entry.name = name;
                }
                if let Some(expr) = schedule_expr {
                    entry.schedule = expr;
                }
                if let Some(command) = command {
                    entry.command = command;
                }
                if let Some(enabled) = enabled {
                    entry.enabled = enabled;
                }
                entry.updated_at = Utc::now().to_rfc3339();
                Ok(entry.clone())
            })
            .await?;
            service.sync_job_state(updated.clone(), Local::now()).await;
            info!(job_id = %updated.id, "scheduler: job updated");
            Ok(serde_json::to_value(&updated)?)
        }

        "sched.rm" => {
            let id = args
                .get("id")
                .and_then(Value::as_str)
                .context("missing `id`")?
                .to_string();
            with_jobs_at(&service.data_dir, |jobs| {
                let before = jobs.len();
                jobs.retain(|j| j.id != id);
                if jobs.len() == before {
                    bail!("no job with id {id:?}");
                }
                Ok(())
            })
            .await?;
            service.remove_job_state(&id).await;
            info!(job_id = %id, "scheduler: job removed");
            Ok(json!({ "removed": true }))
        }

        "sched.enable" => {
            let id = args
                .get("id")
                .and_then(Value::as_str)
                .context("missing `id`")?
                .to_string();
            let enabled = args
                .get("enabled")
                .and_then(Value::as_bool)
                .context("missing `enabled`")?;
            let updated = with_jobs_at(&service.data_dir, |jobs| {
                let entry = jobs
                    .iter_mut()
                    .find(|j| j.id == id)
                    .with_context(|| format!("no job with id {id:?}"))?;
                entry.enabled = enabled;
                entry.updated_at = Utc::now().to_rfc3339();
                Ok(entry.clone())
            })
            .await?;
            service.sync_job_state(updated.clone(), Local::now()).await;
            info!(job_id = %updated.id, enabled, "scheduler: job enabled state changed");
            Ok(serde_json::to_value(&updated)?)
        }

        "sched.run" => {
            let id = args
                .get("id")
                .and_then(Value::as_str)
                .context("missing `id`")?
                .to_string();
            let job = {
                let jobs = service.jobs.read().await;
                jobs.iter().find(|s| s.job.id == id).map(|s| s.job.clone())
            }
            .with_context(|| format!("no job with id {id:?}"))?;
            let data_dir = service.data_dir.clone();
            tokio::spawn(async move {
                runner::execute_and_record(&data_dir, &job).await;
            });
            Ok(json!({ "started": true, "job_id": id }))
        }

        "sched.logs" => {
            let job_id = args.get("id").and_then(Value::as_str).map(str::to_string);
            let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(50) as usize;
            let mut runs = read_run_records(&service.data_dir).await?;
            runs.reverse(); // newest first
            if let Some(job_id) = job_id {
                runs.retain(|r| r.job_id == job_id);
            }
            runs.truncate(limit);
            Ok(json!({ "runs": runs }))
        }

        "sched.next" => {
            let expr = args
                .get("schedule")
                .and_then(Value::as_str)
                .context("missing `schedule`")?;
            let n = args.get("n").and_then(Value::as_u64).unwrap_or(5) as usize;
            let parsed = schedule::parse(expr)
                .with_context(|| format!("invalid schedule expression {expr:?}"))?;
            let mut times = Vec::new();
            let mut cursor = Local::now();
            for _ in 0..n {
                match parsed.next_after(cursor) {
                    Some(t) => {
                        times.push(t.to_rfc3339());
                        cursor = t;
                    }
                    None => break,
                }
            }
            Ok(json!({ "times": times }))
        }

        _ => bail!("unknown command: {cmd}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_dir(name: &str) -> PathBuf {
        let suffix: u64 = rand::random();
        let dir = std::env::temp_dir().join(format!("mistl-scheduler-test-{name}-{suffix:016x}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_job(id: &str, name: &str) -> Job {
        Job {
            id: id.to_string(),
            name: name.to_string(),
            schedule: "@every 1h".to_string(),
            command: "echo hi".to_string(),
            enabled: true,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    #[tokio::test]
    async fn jobs_table_round_trips_add_update_remove() {
        let dir = scratch_dir("jobs-table");
        assert!(load_jobs_unlocked(&dir).unwrap().is_empty());

        with_jobs_at(&dir, |jobs| {
            jobs.push(sample_job("job-000001", "first"));
            Ok(())
        })
        .await
        .unwrap();
        let jobs = load_jobs_unlocked(&dir).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].name, "first");

        with_jobs_at(&dir, |jobs| {
            jobs.iter_mut().find(|j| j.id == "job-000001").unwrap().name = "renamed".to_string();
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(load_jobs_unlocked(&dir).unwrap()[0].name, "renamed");

        with_jobs_at(&dir, |jobs| {
            jobs.retain(|j| j.id != "job-000001");
            Ok(())
        })
        .await
        .unwrap();
        assert!(load_jobs_unlocked(&dir).unwrap().is_empty());
    }

    #[tokio::test]
    async fn with_jobs_at_leaves_the_file_untouched_when_the_mutator_bails() {
        let dir = scratch_dir("jobs-table-bail");
        with_jobs_at(&dir, |jobs| {
            jobs.push(sample_job("job-000001", "first"));
            Ok(())
        })
        .await
        .unwrap();

        let result: Result<()> = with_jobs_at(&dir, |jobs| {
            jobs.push(sample_job("job-000002", "second"));
            bail!("deliberate failure");
        })
        .await;
        assert!(result.is_err());
        // The in-progress push of job-000002 must not have been persisted.
        assert_eq!(load_jobs_unlocked(&dir).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn run_log_appends_and_caps_at_200() {
        let dir = scratch_dir("run-log");
        for i in 0..210 {
            let record = RunRecord {
                job_id: "job-000001".to_string(),
                job_name: "first".to_string(),
                started_at: format!("2026-01-01T00:{i:02}:00Z"),
                ended_at: format!("2026-01-01T00:{i:02}:01Z"),
                exit_code: 0,
                ok: true,
                output: format!("run {i}"),
            };
            append_run_record(&dir, &record).await.unwrap();
        }
        let records = read_run_records(&dir).await.unwrap();
        assert_eq!(records.len(), RUN_LOG_CAP);
        // Oldest 10 (run 0..9) were dropped; the log keeps the most recent.
        assert_eq!(records.first().unwrap().output, "run 10");
        assert_eq!(records.last().unwrap().output, "run 209");
    }

    #[test]
    fn new_job_id_has_the_expected_shape_and_avoids_collisions() {
        let existing = vec![sample_job("job-042117", "taken")];
        let id = new_job_id(&existing);
        assert!(id.starts_with("job-"));
        assert_eq!(id.len(), "job-000000".len());
        assert_ne!(id, "job-042117");
    }

    #[test]
    fn compute_next_fire_is_none_for_disabled_or_invalid_schedules() {
        let now = Local::now();
        let mut job = sample_job("job-000001", "first");
        job.enabled = false;
        assert!(compute_next_fire(&job, now).is_none());

        job.enabled = true;
        job.schedule = "not a valid schedule".to_string();
        assert!(compute_next_fire(&job, now).is_none());

        job.schedule = "@every 1h".to_string();
        assert!(compute_next_fire(&job, now).is_some());
    }
}
