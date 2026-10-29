//! Bounded fire-and-forget work.
//!
//! The agent step must never wait on best-effort side effects (billing
//! emits, long-term memory ingest), so those are spawned detached. A bare
//! `tokio::spawn` per call is unbounded in both count and time: a slow or
//! unreachable sink piles up one task per turn, each holding its payload,
//! until the process runs out of memory. [`BackgroundGate`] caps how many run
//! at once and how long each may take; work that finds no free slot is dropped
//! with a warning, which is the right trade for a best-effort side effect.

use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::Semaphore;

/// Most background side effects in flight at once, per gate.
pub(crate) const MAX_BACKGROUND: usize = 64;

/// Longest one background side effect may run.
pub(crate) const BACKGROUND_TIMEOUT: Duration = Duration::from_secs(30);

/// A concurrency and time cap for detached tasks.
#[derive(Clone)]
pub(crate) struct BackgroundGate {
    permits: Arc<Semaphore>,
    timeout: Duration,
}

impl BackgroundGate {
    pub(crate) fn new(max_in_flight: usize, timeout: Duration) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_in_flight)),
            timeout,
        }
    }

    /// Spawn `fut` detached if a slot is free. Returns `false` (and drops
    /// `fut` unrun) when the gate is full. `what` names the work in logs.
    pub(crate) fn try_spawn<F>(&self, what: &'static str, fut: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let Ok(permit) = self.permits.clone().try_acquire_owned() else {
            tracing::warn!(what, "background work at its in-flight cap; dropping it");
            return false;
        };
        let timeout = self.timeout;
        tokio::spawn(async move {
            let _slot = permit;
            if tokio::time::timeout(timeout, fut).await.is_err() {
                tracing::warn!(what, ?timeout, "background work timed out; abandoned");
            }
        });
        true
    }
}

/// The process-wide gate for long-term memory ingest.
pub(crate) fn long_term_gate() -> &'static BackgroundGate {
    static GATE: OnceLock<BackgroundGate> = OnceLock::new();
    GATE.get_or_init(|| BackgroundGate::new(MAX_BACKGROUND, BACKGROUND_TIMEOUT))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn work_beyond_the_cap_is_dropped_not_queued() {
        let gate = BackgroundGate::new(2, Duration::from_secs(30));
        let release = Arc::new(tokio::sync::Notify::new());
        let ran = Arc::new(AtomicUsize::new(0));
        let spawn_blocked = |gate: &BackgroundGate| {
            let release = release.clone();
            let ran = ran.clone();
            gate.try_spawn("test", async move {
                ran.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
            })
        };
        assert!(spawn_blocked(&gate));
        assert!(spawn_blocked(&gate));
        assert!(!spawn_blocked(&gate), "a third task finds no slot");
        // Let the two admitted tasks start, then free them.
        tokio::task::yield_now().await;
        release.notify_waiters();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(ran.load(Ordering::SeqCst), 2, "the dropped task never ran");
        assert!(spawn_blocked(&gate), "slots are free again");
    }

    #[tokio::test(start_paused = true)]
    async fn a_task_that_overruns_is_abandoned_and_frees_its_slot() {
        let gate = BackgroundGate::new(1, Duration::from_secs(5));
        assert!(gate.try_spawn("slow", async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }));
        assert!(
            !gate.try_spawn("second", async {}),
            "the slot is held while it runs"
        );
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert!(
            gate.try_spawn("third", async {}),
            "slot freed by the timeout"
        );
    }
}
