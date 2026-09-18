//! On-disk [`AwKv`] backed by embedded redb. A single table stores
//! `key -> (expiry_unix_ms: u64, value: bytes)`; reads lazily drop expired
//! rows, writes run in a single redb write transaction (which serialises the
//! compare/incr primitives). Single writer process per file — not for
//! multi-instance deployments.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableTable, TableDefinition};

use super::{AwKv, KvFut, decode_u64, encode_u64};
use crate::error::StateError;

// value layout: first 8 bytes = expiry unix-millis (big-endian), remainder = payload.
const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("aw_kv");

/// Embedded on-disk key-value store.
#[derive(Clone)]
pub struct RedbKv {
    db: Arc<Database>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn frame(expires_at_ms: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&expires_at_ms.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Returns the live payload for a stored frame, or `None` when expired/short.
fn live_payload(stored: &[u8], now: u64) -> Option<Vec<u8>> {
    if stored.len() < 8 {
        return None;
    }
    let expiry = u64::from_be_bytes(stored[..8].try_into().ok()?);
    if expiry <= now {
        None
    } else {
        Some(stored[8..].to_vec())
    }
}

fn backend(msg: impl std::fmt::Display) -> StateError {
    StateError::Redis(format!("redb: {msg}"))
}

impl RedbKv {
    /// Open (creating if absent) the redb database at `path`. The parent
    /// directory is created if missing.
    pub fn open(path: &Path) -> Result<Self, StateError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| backend(format!("mkdir: {e}")))?;
        }
        let db = Database::create(path).map_err(|e| backend(format!("open {path:?}: {e}")))?;
        // Ensure the table exists so read txns never fail on a fresh file.
        let write = db.begin_write().map_err(backend)?;
        {
            let _ = write.open_table(TABLE).map_err(backend)?;
        }
        write.commit().map_err(backend)?;
        Ok(Self { db: Arc::new(db) })
    }

    fn read_live(&self, key: &str) -> Result<Option<Vec<u8>>, StateError> {
        let now = now_ms();
        let read = self.db.begin_read().map_err(backend)?;
        let table = read.open_table(TABLE).map_err(backend)?;
        let stored = table.get(key).map_err(backend)?;
        Ok(stored.and_then(|v| live_payload(v.value(), now)))
    }

    /// Run `f` inside a single write transaction; `f` sees the live payload
    /// (post-expiry) for `key` and returns `(result, action)`.
    fn write_op<R>(
        &self,
        key: &str,
        f: impl FnOnce(Option<Vec<u8>>) -> (R, WriteAction),
    ) -> Result<R, StateError> {
        let now = now_ms();
        let write = self.db.begin_write().map_err(backend)?;
        let result;
        {
            let mut table = write.open_table(TABLE).map_err(backend)?;
            let live = table
                .get(key)
                .map_err(backend)?
                .and_then(|v| live_payload(v.value(), now));
            let (r, action) = f(live);
            result = r;
            match action {
                WriteAction::None => {}
                WriteAction::Remove => {
                    table.remove(key).map_err(backend)?;
                }
                WriteAction::Put {
                    expires_at_ms,
                    payload,
                } => {
                    table
                        .insert(key, frame(expires_at_ms, &payload).as_slice())
                        .map_err(backend)?;
                }
            }
        }
        write.commit().map_err(backend)?;
        Ok(result)
    }
}

enum WriteAction {
    None,
    Remove,
    Put {
        expires_at_ms: u64,
        payload: Vec<u8>,
    },
}

impl AwKv for RedbKv {
    fn get<'a>(&'a self, key: &'a str) -> KvFut<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.read_live(key) })
    }

    fn set_ex<'a>(&'a self, key: &'a str, val: Vec<u8>, ttl: Duration) -> KvFut<'a, ()> {
        Box::pin(async move {
            let expires_at_ms = now_ms() + ttl.as_millis() as u64;
            self.write_op(key, move |_live| {
                (
                    (),
                    WriteAction::Put {
                        expires_at_ms,
                        payload: val,
                    },
                )
            })
        })
    }

    fn del<'a>(&'a self, key: &'a str) -> KvFut<'a, ()> {
        Box::pin(async move { self.write_op(key, |_live| ((), WriteAction::Remove)) })
    }

    fn set_nx<'a>(&'a self, key: &'a str, val: Vec<u8>, ttl: Duration) -> KvFut<'a, bool> {
        Box::pin(async move {
            let expires_at_ms = now_ms() + ttl.as_millis() as u64;
            self.write_op(key, move |live| match live {
                Some(_) => (false, WriteAction::None),
                None => (
                    true,
                    WriteAction::Put {
                        expires_at_ms,
                        payload: val,
                    },
                ),
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
            let expires_at_ms = now_ms() + ttl.as_millis() as u64;
            self.write_op(key, move |live| match live {
                Some(payload) if payload == expected => (
                    true,
                    WriteAction::Put {
                        expires_at_ms,
                        payload,
                    },
                ),
                _ => (false, WriteAction::None),
            })
        })
    }

    fn compare_del<'a>(&'a self, key: &'a str, expected: &'a [u8]) -> KvFut<'a, bool> {
        Box::pin(async move {
            self.write_op(key, move |live| match live {
                Some(payload) if payload == expected => (true, WriteAction::Remove),
                _ => (false, WriteAction::None),
            })
        })
    }

    fn incr_by<'a>(&'a self, key: &'a str, delta: u64, ttl: Duration) -> KvFut<'a, u64> {
        Box::pin(async move {
            let expires_at_ms = now_ms() + ttl.as_millis() as u64;
            self.write_op(key, move |live| {
                let current = live
                    .as_deref()
                    .and_then(|b| decode_u64(b).ok())
                    .unwrap_or(0);
                let next = current.saturating_add(delta);
                (
                    next,
                    WriteAction::Put {
                        expires_at_ms,
                        payload: encode_u64(next),
                    },
                )
            })
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn temp_kv() -> (RedbKv, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aw-state.redb");
        let kv = RedbKv::open(&path).unwrap();
        (kv, dir)
    }

    #[tokio::test]
    async fn set_get_del_roundtrip() {
        let (kv, _dir) = temp_kv();
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
        let (kv, _dir) = temp_kv();
        kv.set_ex("k", b"v".to_vec(), Duration::from_millis(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(kv.get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn set_nx_and_compare_ops() {
        let (kv, _dir) = temp_kv();
        assert!(
            kv.set_nx("lock", b"owner".to_vec(), Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert!(
            !kv.set_nx("lock", b"other".to_vec(), Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert!(!kv.compare_del("lock", b"other").await.unwrap());
        assert!(
            kv.compare_refresh("lock", b"owner", Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert!(kv.compare_del("lock", b"owner").await.unwrap());
        assert_eq!(kv.get("lock").await.unwrap(), None);
    }

    #[tokio::test]
    async fn incr_by_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aw-state.redb");
        {
            let kv = RedbKv::open(&path).unwrap();
            assert_eq!(
                kv.incr_by("c", 5, Duration::from_secs(600)).await.unwrap(),
                5
            );
        }
        // Reopen the same file: durability across restart.
        let kv = RedbKv::open(&path).unwrap();
        assert_eq!(kv.get_u64("c").await.unwrap(), 5);
        assert_eq!(
            kv.incr_by("c", 2, Duration::from_secs(600)).await.unwrap(),
            7
        );
    }
}
