//! Keep the calling agent's session lock alive while one tool call is pending.
//!
//! The loop refreshes its lock (TTL 90 s, `state_kv`/`state_redis`) at the top
//! of each iteration only. A `flow:` tool call can run a whole nested agent
//! turn, so a single call may outlast the TTL: a second message for the same
//! session would then take the lock and run concurrently with this turn, and
//! whichever saves last wins. [`keep_alive`] refreshes on an interval for as
//! long as the wrapped call is pending.

use std::future::Future;
use std::time::Duration;

use tracing::warn;

use crate::state::SessionLock;

/// How often a held lock is refreshed while a tool call is pending: a third
/// of the 90 s TTL, so two refreshes can fail before the lock lapses.
pub(crate) const LOCK_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Await `fut` to completion, refreshing `lock` every `every` meanwhile.
///
/// `fut` is never dropped or restarted: the refreshes run alongside it and
/// stop when it completes. A failed refresh is logged and does not affect the
/// call; a lock that is genuinely lost is handled where it always was, at the
/// next iteration.
pub(crate) async fn keep_alive<F: Future>(
    lock: &SessionLock,
    every: Duration,
    fut: F,
) -> F::Output {
    let refreshing = async {
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        loop {
            ticker.tick().await;
            if let Err(e) = lock.refresh().await {
                warn!(error = %e, "lock refresh during a pending tool call failed; continuing");
            }
        }
    };
    tokio::pin!(fut);
    tokio::select! {
        out = &mut fut => out,
        // The refresher never finishes; if it ever did, still finish the call.
        _ = refreshing => fut.await,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::StateError;
    use crate::state::SessionLockInner;

    struct Counting {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    impl SessionLockInner for Counting {
        fn refresh<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let fail = self.fail;
            Box::pin(async move {
                if fail {
                    Err(StateError::Redis("down".into()))
                } else {
                    Ok(())
                }
            })
        }

        fn release(&self) {}
    }

    fn lock(fail: bool) -> (SessionLock, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = Counting {
            calls: calls.clone(),
            fail,
        };
        (SessionLock::new(Box::new(inner)), calls)
    }

    #[tokio::test(start_paused = true)]
    async fn refreshes_on_the_interval_until_the_call_completes() {
        let (lock, calls) = lock(false);
        let out = keep_alive(&lock, Duration::from_millis(5), async {
            tokio::time::sleep(Duration::from_millis(23)).await;
            7
        })
        .await;
        assert_eq!(out, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 4, "at 5, 10, 15 and 20 ms");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 4, "none after completion");
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_refresh_does_not_abort_the_call() {
        let (lock, calls) = lock(true);
        let out = keep_alive(&lock, Duration::from_millis(5), async {
            tokio::time::sleep(Duration::from_millis(12)).await;
            "finished"
        })
        .await;
        assert_eq!(out, "finished");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_fast_call_is_never_refreshed() {
        let (lock, calls) = lock(false);
        assert_eq!(
            keep_alive(&lock, Duration::from_secs(30), async { 1 }).await,
            1
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
