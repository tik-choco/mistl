use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::watch;
use tokio::time::Instant;

/// Initial backoff before the first retry of a failed tunnel send. Doubles
/// after each subsequent failure, capped by the remaining
/// [`SEND_RETRY_BUDGET`].
pub const SEND_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(250);

/// Total time budget for retrying a single failed tunnel send before giving
/// up and closing the connection. mistlib tolerates ~5s of disruption
/// (`DISCONNECTED_GRACE_MS`) while it attempts an ICE restart, during which
/// `send_message_direct` can transiently fail (e.g. "no active session" /
/// "Channel not open"). 8s comfortably exceeds that window.
pub const SEND_RETRY_BUDGET: Duration = Duration::from_secs(8);

/// Retries `attempt` with exponential backoff (doubling each time, capped by
/// the remaining budget) until it succeeds, the total elapsed time exceeds
/// `SEND_RETRY_BUDGET`, or `shutdown` reports a shutdown.
///
/// Stays responsive to shutdown by racing each backoff sleep against the
/// shutdown watch (via `tokio::select!`) instead of sleeping blindly, so a
/// caller waiting on this doesn't block process shutdown for up to 8s.
pub async fn retry_with_backoff<F, Fut>(
    mut attempt: F,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let deadline = Instant::now() + SEND_RETRY_BUDGET;
    let mut backoff = SEND_RETRY_INITIAL_BACKOFF;

    loop {
        match attempt().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(e);
                }
                let remaining = deadline - now;
                let sleep_dur = backoff.min(remaining);

                tokio::select! {
                    _ = tokio::time::sleep(sleep_dur) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Err(e);
                        }
                    }
                }

                backoff = backoff.saturating_mul(2);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn succeeds_after_transient_failures_without_exhausting_budget() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (_tx, mut rx) = watch::channel(false);

        let a = attempts.clone();
        let result = retry_with_backoff(
            move || {
                let a = a.clone();
                async move {
                    let n = a.fetch_add(1, Ordering::SeqCst);
                    if n < 3 {
                        Err(anyhow::anyhow!("transient"))
                    } else {
                        Ok(())
                    }
                }
            },
            &mut rx,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_budget_exhausted() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (_tx, mut rx) = watch::channel(false);

        let a = attempts.clone();
        let result = retry_with_backoff(
            move || {
                let a = a.clone();
                async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    Err(anyhow::anyhow!("always fails"))
                }
            },
            &mut rx,
        )
        .await;

        assert!(result.is_err());
        // Backoff is 250ms, 500ms, 1s, 2s, 4s, ... within an 8s budget, so
        // there should be several attempts -- but not unbounded.
        let n = attempts.load(Ordering::SeqCst);
        assert!((2..=10).contains(&n), "unexpected attempt count: {n}");
    }

    #[tokio::test]
    async fn stops_promptly_on_shutdown_instead_of_blocking_for_full_budget() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = watch::channel(false);

        let a = attempts.clone();
        let handle = tokio::spawn(async move {
            retry_with_backoff(
                move || {
                    let a = a.clone();
                    async move {
                        a.fetch_add(1, Ordering::SeqCst);
                        Err(anyhow::anyhow!("always fails"))
                    }
                },
                &mut rx,
            )
            .await
        });

        // Let the first attempt happen and enter its backoff sleep, then
        // signal shutdown; the loop must return promptly rather than
        // sleeping out the whole 8s budget.
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(true).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("retry loop did not stop promptly after shutdown")
            .unwrap();
        assert!(result.is_err());
        assert!(attempts.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test(start_paused = true)]
    async fn succeeds_immediately_without_sleeping() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (_tx, mut rx) = watch::channel(false);

        let a = attempts.clone();
        let result = retry_with_backoff(
            move || {
                let a = a.clone();
                async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            &mut rx,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
