//! In-process [`AwKv`] backed by a single mutex + per-key expiry timestamp.
//! Always compiled (no external dependency). Ephemeral: all data is lost on
//! process exit.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::{AwKv, KvFut, encode_u64};

struct Entry {
    bytes: Vec<u8>,
    expires_at: Instant,
}

/// Fewest writes between two sweeps of expired entries.
const MIN_WRITES_PER_SWEEP: usize = 256;

#[derive(Default)]
struct State {
    map: HashMap<String, Entry>,
    /// Writes since the last sweep of expired entries.
    writes_since_sweep: usize,
}

impl State {
    /// Count one write and, once at least `max(MIN_WRITES_PER_SWEEP, len)`
    /// writes have happened since the last sweep, drop every expired entry.
    ///
    /// Without this an entry nobody reads again (a finished session's
    /// `ConversationState`, kept 7 days) would stay forever. Scaling the
    /// interval with the map size keeps the O(n) sweep amortised O(1) per
    /// write.
    fn note_write(&mut self, now: Instant) {
        self.writes_since_sweep += 1;
        if self.writes_since_sweep >= MIN_WRITES_PER_SWEEP.max(self.map.len()) {
            self.writes_since_sweep = 0;
            self.map.retain(|_, entry| entry.expires_at > now);
        }
    }
}

/// Ephemeral in-process key-value store.
#[derive(Default)]
pub struct MemoryKv {
    state: Mutex<State>,
}

impl MemoryKv {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of stored entries, expired-but-unswept ones included.
    #[cfg(test)]
    fn stored_len(&self) -> usize {
        self.state.lock().map(|s| s.map.len()).unwrap_or(0)
    }

    /// Lock the map and run `f` with the current instant.
    ///
    /// Expiry is handled two ways: each accessor treats a touched key whose
    /// `expires_at` has passed as absent and removes just that key, and every
    /// write counts toward a periodic sweep of all expired entries (see
    /// [`State::note_write`]), so keys nobody touches again do not accumulate.
    /// Returns a `StateError::Redis` on mutex poisoning (kept as `Redis` so the
    /// loop's existing error handling is unchanged).
    fn with_map<R>(
        &self,
        f: impl FnOnce(&mut HashMap<String, Entry>, Instant) -> R,
    ) -> Result<R, crate::error::StateError> {
        let now = Instant::now();
        let mut guard = self
            .state
            .lock()
            .map_err(|e| crate::error::StateError::Redis(format!("memory kv poisoned: {e}")))?;
        Ok(f(&mut guard.map, now))
    }

    /// [`with_map`](Self::with_map) for a mutation that adds or replaces an
    /// entry: also drives the periodic expired-entry sweep.
    fn with_map_write<R>(
        &self,
        f: impl FnOnce(&mut HashMap<String, Entry>, Instant) -> R,
    ) -> Result<R, crate::error::StateError> {
        let now = Instant::now();
        let mut guard = self
            .state
            .lock()
            .map_err(|e| crate::error::StateError::Redis(format!("memory kv poisoned: {e}")))?;
        let out = f(&mut guard.map, now);
        guard.note_write(now);
        Ok(out)
    }
}

impl AwKv for MemoryKv {
    fn get<'a>(&'a self, key: &'a str) -> KvFut<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.with_map(|map, now| match map.get(key) {
                Some(entry) if entry.expires_at > now => Some(entry.bytes.clone()),
                Some(_) => {
                    // Expired: evict lazily and report absent.
                    map.remove(key);
                    None
                }
                None => None,
            })
        })
    }

    fn set_ex<'a>(&'a self, key: &'a str, val: Vec<u8>, ttl: Duration) -> KvFut<'a, ()> {
        Box::pin(async move {
            self.with_map_write(|map, now| {
                map.insert(
                    key.to_string(),
                    Entry {
                        bytes: val,
                        expires_at: now + ttl,
                    },
                );
            })
        })
    }

    fn del<'a>(&'a self, key: &'a str) -> KvFut<'a, ()> {
        Box::pin(async move {
            self.with_map(|map, _now| {
                map.remove(key);
            })
        })
    }

    fn set_nx<'a>(&'a self, key: &'a str, val: Vec<u8>, ttl: Duration) -> KvFut<'a, bool> {
        Box::pin(async move {
            self.with_map_write(|map, now| {
                // An expired entry counts as absent, so a new lock may be taken.
                let occupied = map.get(key).is_some_and(|entry| entry.expires_at > now);
                if occupied {
                    false
                } else {
                    map.insert(
                        key.to_string(),
                        Entry {
                            bytes: val,
                            expires_at: now + ttl,
                        },
                    );
                    true
                }
            })
        })
    }

    fn compare_refresh<'a>(
        &'a self,
        key: &'a str,
        expected: &'a [u8],
        ttl: Duration,
    ) -> KvFut<'a, bool> {
        Box::pin(async move {
            self.with_map(|map, now| {
                let (expired, matches) = match map.get(key) {
                    Some(entry) => (entry.expires_at <= now, entry.bytes == expected),
                    None => return false,
                };
                if expired {
                    map.remove(key);
                    return false;
                }
                if matches {
                    if let Some(entry) = map.get_mut(key) {
                        entry.expires_at = now + ttl;
                    }
                    true
                } else {
                    false
                }
            })
        })
    }

    fn compare_del<'a>(&'a self, key: &'a str, expected: &'a [u8]) -> KvFut<'a, bool> {
        Box::pin(async move {
            self.with_map(|map, now| {
                let (expired, matches) = match map.get(key) {
                    Some(entry) => (entry.expires_at <= now, entry.bytes == expected),
                    None => return false,
                };
                // Either way the key leaves the map; only an owner match succeeds.
                if expired || matches {
                    map.remove(key);
                }
                !expired && matches
            })
        })
    }

    fn incr_by<'a>(&'a self, key: &'a str, delta: u64, ttl: Duration) -> KvFut<'a, u64> {
        Box::pin(async move {
            self.with_map_write(|map, now| {
                let current = map
                    .get(key)
                    .filter(|e| e.expires_at > now)
                    .and_then(|e| e.bytes.as_slice().try_into().ok())
                    .map(u64::from_be_bytes)
                    .unwrap_or(0);
                let next = current.saturating_add(delta);
                map.insert(
                    key.to_string(),
                    Entry {
                        bytes: encode_u64(next),
                        expires_at: now + ttl,
                    },
                );
                next
            })
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn set_get_del_roundtrip() {
        let kv = MemoryKv::new();
        assert_eq!(kv.get("k").await.unwrap(), None);
        kv.set_ex("k", b"v".to_vec(), Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(kv.get("k").await.unwrap(), Some(b"v".to_vec()));
        kv.del("k").await.unwrap();
        assert_eq!(kv.get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn expired_key_reads_none() {
        let kv = MemoryKv::new();
        kv.set_ex("k", b"v".to_vec(), Duration::from_millis(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(kv.get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn expired_entries_nobody_reads_again_are_swept_by_writes() {
        let kv = MemoryKv::new();
        for i in 0..100 {
            kv.set_ex(&format!("dead{i}"), b"v".to_vec(), Duration::from_millis(1))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Never read again; only further writes can reclaim them.
        for i in 0..MIN_WRITES_PER_SWEEP {
            kv.set_ex(&format!("live{i}"), b"v".to_vec(), Duration::from_secs(60))
                .await
                .unwrap();
        }
        assert_eq!(
            kv.stored_len(),
            MIN_WRITES_PER_SWEEP,
            "the 100 expired entries must be gone"
        );
    }

    #[tokio::test]
    async fn sweep_keeps_live_entries() {
        let kv = MemoryKv::new();
        kv.set_ex("keep", b"v".to_vec(), Duration::from_secs(60))
            .await
            .unwrap();
        for i in 0..(MIN_WRITES_PER_SWEEP * 2) {
            kv.set_ex(&format!("k{i}"), b"v".to_vec(), Duration::from_millis(1))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Writes to one key do not grow the map, so the sweep (due once the
        // write count reaches the map size) must fire within these.
        for _ in 0..(MIN_WRITES_PER_SWEEP * 4) {
            kv.set_ex("tick", b"v".to_vec(), Duration::from_secs(60))
                .await
                .unwrap();
        }
        assert_eq!(kv.get("keep").await.unwrap(), Some(b"v".to_vec()));
        assert_eq!(kv.stored_len(), 2, "only `keep` and `tick` are live");
    }

    #[tokio::test]
    async fn set_nx_only_first_wins() {
        let kv = MemoryKv::new();
        assert!(
            kv.set_nx("lock", b"a".to_vec(), Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert!(
            !kv.set_nx("lock", b"b".to_vec(), Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert_eq!(kv.get("lock").await.unwrap(), Some(b"a".to_vec()));
    }

    #[tokio::test]
    async fn compare_del_and_refresh_check_ownership() {
        let kv = MemoryKv::new();
        kv.set_nx("lock", b"owner".to_vec(), Duration::from_secs(60))
            .await
            .unwrap();
        // Wrong token: no-op.
        assert!(
            !kv.compare_refresh("lock", b"other", Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert!(!kv.compare_del("lock", b"other").await.unwrap());
        assert_eq!(kv.get("lock").await.unwrap(), Some(b"owner".to_vec()));
        // Right token: succeeds.
        assert!(
            kv.compare_refresh("lock", b"owner", Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert!(kv.compare_del("lock", b"owner").await.unwrap());
        assert_eq!(kv.get("lock").await.unwrap(), None);
    }

    #[tokio::test]
    async fn incr_by_accumulates_and_get_u64_reads() {
        let kv = MemoryKv::new();
        assert_eq!(kv.get_u64("c").await.unwrap(), 0);
        assert_eq!(
            kv.incr_by("c", 5, Duration::from_secs(60)).await.unwrap(),
            5
        );
        assert_eq!(
            kv.incr_by("c", 3, Duration::from_secs(60)).await.unwrap(),
            8
        );
        assert_eq!(kv.get_u64("c").await.unwrap(), 8);
    }
}
