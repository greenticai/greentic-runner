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

/// Ephemeral in-process key-value store.
#[derive(Default)]
pub struct MemoryKv {
    map: Mutex<HashMap<String, Entry>>,
}

impl MemoryKv {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock the map and run `f` with the current instant.
    ///
    /// Expiry is handled lazily per key: each accessor treats a touched key
    /// whose `expires_at` has passed as absent and removes just that key. There
    /// is no global sweep (that was O(n) on every op), so an untouched but
    /// expired key may linger until it is next accessed — an accepted tradeoff
    /// for this ephemeral in-process backend. Returns a `StateError::Redis` on
    /// mutex poisoning (kept as `Redis` so the loop's existing error handling is
    /// unchanged).
    fn with_map<R>(
        &self,
        f: impl FnOnce(&mut HashMap<String, Entry>, Instant) -> R,
    ) -> Result<R, crate::error::StateError> {
        let now = Instant::now();
        let mut guard = self
            .map
            .lock()
            .map_err(|e| crate::error::StateError::Redis(format!("memory kv poisoned: {e}")))?;
        Ok(f(&mut guard, now))
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
            self.with_map(|map, now| {
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
            self.with_map(|map, now| {
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
            self.with_map(|map, now| {
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
