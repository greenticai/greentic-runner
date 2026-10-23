use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::cache::keys::ArtifactKey;

#[derive(Clone, Debug, Default)]
pub struct Singleflight {
    locks: Arc<DashMap<ArtifactKey, Arc<Mutex<()>>>>,
}

impl Singleflight {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
        }
    }

    #[cfg(test)]
    fn tracked_keys(&self) -> usize {
        self.locks.len()
    }

    pub async fn acquire(&self, key: ArtifactKey) -> SingleflightGuard {
        let lock = self
            .locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let guard = lock.clone().lock_owned().await;
        SingleflightGuard {
            key,
            _lock: lock,
            guard: Some(guard),
            locks: Arc::clone(&self.locks),
        }
    }
}

pub struct SingleflightGuard {
    key: ArtifactKey,
    /// Keeps this guard counted in the map entry's strong count (see `Drop`).
    _lock: Arc<Mutex<()>>,
    guard: Option<OwnedMutexGuard<()>>,
    locks: Arc<DashMap<ArtifactKey, Arc<Mutex<()>>>>,
}

impl Drop for SingleflightGuard {
    fn drop(&mut self) {
        // Release the mutex first so its owned `Arc` clone is dropped, then
        // remove the map entry only when nobody else holds the lock: the map
        // and `self._lock` account for 2 strong refs; any waiter or fresh
        // acquirer holds an additional clone. `remove_if` runs under the shard
        // write lock, and acquirers clone under the same shard lock, so this
        // cannot race with a new acquirer.
        self.guard = None;
        self.locks
            .remove_if(&self.key, |_, entry| Arc::strong_count(entry) <= 2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> ArtifactKey {
        ArtifactKey::new("profile".to_string(), name.to_string())
    }

    #[tokio::test]
    async fn entry_removed_after_guard_dropped() {
        let sf = Singleflight::new();
        let guard = sf.acquire(key("a")).await;
        assert_eq!(sf.tracked_keys(), 1);
        drop(guard);
        assert_eq!(sf.tracked_keys(), 0);
    }

    #[tokio::test]
    async fn entry_kept_while_waiter_pending_then_removed() {
        let sf = Singleflight::new();
        let first = sf.acquire(key("a")).await;
        let sf2 = sf.clone();
        let waiter = tokio::spawn(async move { sf2.acquire(key("a")).await });
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(first);
        let second = waiter.await.expect("waiter joined");
        assert_eq!(sf.tracked_keys(), 1);
        drop(second);
        assert_eq!(sf.tracked_keys(), 0);
    }
}
