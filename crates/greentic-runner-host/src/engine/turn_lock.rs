//! In-process, per-session turn lock.
//!
//! A parked flow is resumed by `fetch -> execute -> clear/save`, and `fetch`
//! does not consume the snapshot. Without a lock, two inbound messages (or a
//! user message and a late dispatch response) for one parked session both
//! fetch the same snapshot and both run its effects. [`TurnLocks::run`] holds
//! one async mutex per session key across that whole sequence.
//!
//! Properties:
//! - **Keyed**: different sessions never wait on each other.
//! - **Cancellation/panic safe**: the guard is an RAII value inside the
//!   future, so dropping the future (or unwinding) releases the lock.
//! - **No leak**: a map entry is removed when nobody holds or awaits it.
//! - **Re-entrant per task**: the keys a task holds are carried in a
//!   task-local, so a nested run for the SAME session on the same task (the
//!   resumer taking the lock and then calling `handle`, a handover) skips the
//!   lock instead of deadlocking. A `tokio::spawn` between the outer turn and
//!   the nested run does NOT inherit the marker; nothing on the call path
//!   spawns today and `a_spawned_task_does_not_inherit_the_marker` pins that.
//!
//! Limit: this is IN-PROCESS only. Replicas sharing a Redis session store
//! still need a store-level compare-and-set/lease to be exclusive across pods.

use std::future::Future;
use std::sync::Arc;

use dashmap::DashMap;

tokio::task_local! {
    static HELD: Vec<String>;
}

type Slot = Arc<tokio::sync::Mutex<()>>;

#[derive(Default)]
pub(crate) struct TurnLocks {
    slots: DashMap<String, Slot>,
}

struct Guard<'a> {
    locks: &'a DashMap<String, Slot>,
    key: String,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        // Release the mutex (and its Arc) before checking the count.
        drop(self.guard.take());
        self.locks
            .remove_if(&self.key, |_, slot| Arc::strong_count(slot) == 1);
    }
}

impl TurnLocks {
    /// Run `fut` while holding the turn lock for `key`, unless this task
    /// already holds it.
    pub(crate) async fn run<F: Future>(&self, key: String, fut: F) -> F::Output {
        let mut held = HELD.try_with(Clone::clone).unwrap_or_default();
        if held.contains(&key) {
            return fut.await;
        }
        let slot = {
            let entry = self
                .slots
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
            Arc::clone(entry.value())
        };
        // Build the guard BEFORE awaiting so a cancelled wait still sweeps.
        let mut guard = Guard {
            locks: &self.slots,
            key: key.clone(),
            guard: None,
        };
        guard.guard = Some(slot.lock_owned().await);
        held.push(key);
        let output = HELD.scope(held, fut).await;
        drop(guard);
        output
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.slots.len()
    }
}

/// The process-wide lock table. Global (not per store) so a hot-swapped
/// runtime and the one it replaces still exclude each other.
pub(crate) fn global() -> &'static TurnLocks {
    static LOCKS: std::sync::OnceLock<TurnLocks> = std::sync::OnceLock::new();
    LOCKS.get_or_init(TurnLocks::default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn same_key_is_serialised() {
        let locks = Arc::new(TurnLocks::default());
        let active = Arc::new(AtomicUsize::new(0));
        let max = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let (locks, active, max) = (locks.clone(), active.clone(), max.clone());
            tasks.push(tokio::spawn(async move {
                locks
                    .run("s".into(), async {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                    })
                    .await
            }));
        }
        for task in tasks {
            assert!(task.await.is_ok());
        }
        assert_eq!(max.load(Ordering::SeqCst), 1);
        assert_eq!(locks.len(), 0, "entries are swept");
    }

    #[tokio::test]
    async fn different_keys_run_in_parallel() {
        let locks = Arc::new(TurnLocks::default());
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let run = |key: &'static str| {
            let (locks, barrier) = (locks.clone(), barrier.clone());
            tokio::spawn(async move {
                locks
                    .run(key.into(), async {
                        barrier.wait().await;
                    })
                    .await
            })
        };
        let (a, b) = (run("a"), run("b"));
        let both = tokio::time::timeout(Duration::from_secs(5), async { (a.await, b.await) });
        assert!(both.await.is_ok(), "two sessions must not block each other");
    }

    #[tokio::test]
    async fn nested_same_key_does_not_deadlock() {
        let locks = TurnLocks::default();
        let out = tokio::time::timeout(
            Duration::from_secs(5),
            locks.run("s".into(), async {
                locks.run("s".into(), async { 7 }).await
            }),
        )
        .await;
        assert_eq!(out.ok(), Some(7));
        assert_eq!(locks.len(), 0);
    }

    #[tokio::test]
    async fn released_on_cancellation() {
        let locks = Arc::new(TurnLocks::default());
        let holder = {
            let locks = locks.clone();
            tokio::spawn(async move { locks.run("s".into(), std::future::pending::<()>()).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        holder.abort();
        let _ = holder.await;
        let got =
            tokio::time::timeout(Duration::from_secs(5), locks.run("s".into(), async { 1 })).await;
        assert_eq!(got.ok(), Some(1));
        assert_eq!(locks.len(), 0);
    }

    #[tokio::test]
    async fn released_on_panic() {
        let locks = Arc::new(TurnLocks::default());
        let l = locks.clone();
        let joined = tokio::spawn(async move {
            l.run("s".into(), async {
                if true {
                    panic!("boom");
                }
            })
            .await
        })
        .await;
        assert!(joined.is_err());
        let got =
            tokio::time::timeout(Duration::from_secs(5), locks.run("s".into(), async { 1 })).await;
        assert_eq!(got.ok(), Some(1));
        assert_eq!(locks.len(), 0);
    }

    #[tokio::test]
    async fn a_spawned_task_does_not_inherit_the_marker() {
        let locks = TurnLocks::default();
        let inherited = locks
            .run("s".into(), async {
                tokio::spawn(async { HELD.try_with(|h| h.len()).unwrap_or(0) })
                    .await
                    .unwrap_or(usize::MAX)
            })
            .await;
        assert_eq!(inherited, 0);
    }
}
