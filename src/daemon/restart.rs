//! One daemon-lifetime waiter for configuration changes requiring a restart.
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::time::Instant;

use super::AppState;

const DEBOUNCE: Duration = Duration::from_secs(3);

pub(super) struct PendingRestart {
    since: String,
    deadline: Instant,
    waiting_idle: bool,
}

impl AppState {
    pub(super) fn schedule_config_restart(&self) {
        let mut pending = self.config_restart.lock().expect("config restart lock");
        let restart = pending.get_or_insert_with(|| PendingRestart {
            since: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            deadline: Instant::now() + DEBOUNCE,
            waiting_idle: false,
        });
        restart.deadline = Instant::now() + DEBOUNCE;
        restart.waiting_idle = false;
    }

    pub(super) fn rearm_config_restart(&self) {
        if let Some(restart) = self
            .config_restart
            .lock()
            .expect("config restart lock")
            .as_mut()
        {
            restart.deadline = Instant::now() + DEBOUNCE;
            restart.waiting_idle = false;
        }
    }

    pub(super) fn config_restart_status(&self) -> Value {
        match self
            .config_restart
            .lock()
            .expect("config restart lock")
            .as_ref()
        {
            Some(restart) => json!({
                "reason": "config changed",
                "since": restart.since,
                "waiting_idle": restart.waiting_idle,
            }),
            None => Value::Null,
        }
    }

    // Caller holds config_apply so a save cannot race the final idle decision.
    fn check_config_restart(&self, busy: bool) {
        let mut pending = self.config_restart.lock().expect("config restart lock");
        let Some(restart) = pending.as_mut() else {
            return;
        };
        if Instant::now() < restart.deadline {
            return;
        }
        restart.waiting_idle = busy;
        if !busy {
            tracing::info!("config: restarting to apply saved settings");
            self.request_restart();
            *pending = None;
        }
    }
}

/// Shared with the updater: a running stream pipeline or any tunnel connection
/// defers automatic restarts, even when no RTSP client is currently watching.
pub(crate) async fn is_idle() -> bool {
    !crate::stream::is_running().await && crate::tunnel::active_conns().await == 0
}

pub(super) fn spawn(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut shutdown = state.shutdown.subscribe();
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tick.tick() => {
                    let _apply = state.config_apply.lock().await;
                    let pending = state.config_restart.lock().expect("config restart lock").is_some();
                    if pending {
                        state.check_config_restart(!is_idle().await);
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_save_does_not_schedule_restart() {
        let state = AppState::for_test();
        assert!(
            super::super::dispatch(
                "config.set",
                json!({"path":"escrow.max_blob_bytes", "value":0}),
                &state,
            )
            .await
            .is_err()
        );
        assert!(state.config_restart_status().is_null());
        assert!(!state.wants_restart());
    }

    #[tokio::test(start_paused = true)]
    async fn saves_debounce_one_restart_and_wait_until_idle() {
        let state = AppState::for_test();
        assert!(state.config_restart_status().is_null());
        state.schedule_config_restart();
        let since = state.config_restart_status()["since"].clone();
        tokio::time::advance(Duration::from_secs(2)).await;
        state.schedule_config_restart();
        assert_eq!(state.config_restart_status()["since"], since);
        tokio::time::advance(Duration::from_secs(2)).await;
        state.check_config_restart(false);
        assert!(!state.wants_restart());
        tokio::time::advance(Duration::from_secs(1)).await;
        state.check_config_restart(true);
        assert!(!state.wants_restart());
        assert_eq!(state.config_restart_status()["waiting_idle"], true);
        state.check_config_restart(false);
        assert!(state.wants_restart());
        assert!(state.config_restart_status().is_null());
    }

    #[tokio::test(start_paused = true)]
    async fn other_saved_fields_extend_pending_restart_only() {
        let state = AppState::for_test();
        state.rearm_config_restart();
        assert!(state.config_restart_status().is_null());
        state.schedule_config_restart();
        tokio::time::advance(Duration::from_secs(3)).await;
        state.check_config_restart(true);
        state.rearm_config_restart();
        assert_eq!(state.config_restart_status()["waiting_idle"], false);
        state.check_config_restart(false);
        assert!(!state.wants_restart());
        tokio::time::advance(DEBOUNCE).await;
        state.check_config_restart(false);
        assert!(state.wants_restart());
    }
}
