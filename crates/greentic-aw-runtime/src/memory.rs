//! Memory provider seam for the agentic-worker runtime.
//!
//! aw-runtime owns its own memory abstraction (it does not depend on
//! `greentic-dw-runtime`). A provider is bound per memory tier via
//! [`crate::config::MemorySettings`]; the runtime invokes [`MemoryProvider`]
//! to persist and retrieve records. This module ships the trait, the portable
//! record/query types, and an always-available in-memory provider used by
//! tests and the designer playground until the real extension-backed provider
//! lands.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::MemoryError;
use crate::tenant::TenantContext;

/// A single memory record exchanged with a provider.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub key: String,
    pub value: String,
}

/// Lookup query for [`MemoryProvider::recall`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryQuery {
    pub key: String,
}

/// Backend abstraction for a memory tier. Mirrors the async-trait idiom used
/// by [`crate::state::AgentStateStore`] and [`crate::llm::LlmBackend`]
/// (manual `Pin<Box<dyn Future>>` so the trait stays object-safe).
pub trait MemoryProvider: Send + Sync {
    fn remember<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        record: MemoryRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), MemoryError>> + Send + 'a>>;

    fn recall<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        query: &'a MemoryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Option<MemoryRecord>, MemoryError>> + Send + 'a>>;
}

/// Longest accepted memory key, in bytes.
pub const MAX_KEY_BYTES: usize = 256;
/// Longest accepted memory value, in bytes.
pub const MAX_VALUE_BYTES: usize = 16 * 1024;
/// Most distinct keys one `(tenant, session)` may hold.
pub const MAX_ENTRIES_PER_SESSION: usize = 100;
/// Most key + value bytes one `(tenant, session)` may hold.
pub const MAX_BYTES_PER_SESSION: usize = 256 * 1024;
/// Most `(tenant, session)` buckets kept; past this the least recently used
/// session is evicted whole.
pub const MAX_SESSIONS: usize = 10_000;

/// Caps of an [`InMemoryMemoryProvider`]. The defaults are the `MAX_*`
/// constants above; a model can call `remember` in a loop and sessions are
/// attacker-chosen, so nothing here may grow without bound.
#[derive(Clone, Copy, Debug)]
pub struct MemoryLimits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_entries_per_session: usize,
    pub max_bytes_per_session: usize,
    pub max_sessions: usize,
}

impl Default for MemoryLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: MAX_KEY_BYTES,
            max_value_bytes: MAX_VALUE_BYTES,
            max_entries_per_session: MAX_ENTRIES_PER_SESSION,
            max_bytes_per_session: MAX_BYTES_PER_SESSION,
            max_sessions: MAX_SESSIONS,
        }
    }
}

/// Everything one `(tenant, session)` remembers.
#[derive(Default)]
struct Bucket {
    entries: HashMap<String, MemoryRecord>,
    bytes: usize,
    /// Logical clock value of the last read or write (LRU order).
    last_used: u64,
}

#[derive(Default)]
struct Inner {
    sessions: HashMap<(String, String), Bucket>,
    clock: u64,
}

/// In-memory provider keyed by `(tenant, session, key)`. Always compiled (like
/// [`crate::config_provider::InMemoryConfigProvider`]); used by tests and the
/// designer playground. Bounded by [`MemoryLimits`]. Lock-poisoning maps to
/// [`MemoryError::Backend`] — no `unwrap`/`expect` in this non-test code.
#[derive(Default)]
pub struct InMemoryMemoryProvider {
    inner: Mutex<Inner>,
    limits: MemoryLimits,
}

impl InMemoryMemoryProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// A provider with explicit caps (tests use small ones).
    pub fn with_limits(limits: MemoryLimits) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            limits,
        }
    }

    fn store(
        &self,
        tenant: &TenantContext,
        session_id: &str,
        record: MemoryRecord,
    ) -> Result<(), MemoryError> {
        let limits = self.limits;
        if record.key.len() > limits.max_key_bytes {
            return Err(MemoryError::LimitExceeded(format!(
                "key is longer than {} bytes",
                limits.max_key_bytes
            )));
        }
        if record.value.len() > limits.max_value_bytes {
            return Err(MemoryError::LimitExceeded(format!(
                "value is longer than {} bytes",
                limits.max_value_bytes
            )));
        }
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| MemoryError::Backend("memory mutex poisoned".to_string()))?;
        let inner = &mut *guard;
        inner.clock += 1;
        let now = inner.clock;
        let bucket_key = (tenant.key_prefix(), session_id.to_string());
        if !inner.sessions.contains_key(&bucket_key) && inner.sessions.len() >= limits.max_sessions
        {
            // Evict the least recently used session whole.
            let oldest = inner
                .sessions
                .iter()
                .min_by_key(|(_, b)| b.last_used)
                .map(|(k, _)| k.clone());
            if let Some(oldest) = oldest {
                inner.sessions.remove(&oldest);
            }
        }
        let bucket = inner.sessions.entry(bucket_key).or_default();
        bucket.last_used = now;
        let new_size = record.key.len() + record.value.len();
        let existing = bucket
            .entries
            .get(&record.key)
            .map(|old| old.key.len() + old.value.len());
        if existing.is_none() && bucket.entries.len() >= limits.max_entries_per_session {
            return Err(MemoryError::LimitExceeded(format!(
                "at most {} entries per session",
                limits.max_entries_per_session
            )));
        }
        let old_size = existing.unwrap_or(0);
        if bucket.bytes.saturating_sub(old_size) + new_size > limits.max_bytes_per_session {
            return Err(MemoryError::LimitExceeded(format!(
                "at most {} bytes per session",
                limits.max_bytes_per_session
            )));
        }
        bucket.bytes = bucket.bytes.saturating_sub(old_size) + new_size;
        bucket.entries.insert(record.key.clone(), record);
        Ok(())
    }

    fn load(
        &self,
        tenant: &TenantContext,
        session_id: &str,
        key: &str,
    ) -> Result<Option<MemoryRecord>, MemoryError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| MemoryError::Backend("memory mutex poisoned".to_string()))?;
        let inner = &mut *guard;
        inner.clock += 1;
        let now = inner.clock;
        let bucket_key = (tenant.key_prefix(), session_id.to_string());
        Ok(inner.sessions.get_mut(&bucket_key).and_then(|bucket| {
            bucket.last_used = now;
            bucket.entries.get(key).cloned()
        }))
    }
}

impl MemoryProvider for InMemoryMemoryProvider {
    fn remember<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        record: MemoryRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), MemoryError>> + Send + 'a>> {
        let result = self.store(tenant, session_id, record);
        Box::pin(async move { result })
    }

    fn recall<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        query: &'a MemoryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Option<MemoryRecord>, MemoryError>> + Send + 'a>> {
        let result = self.load(tenant, session_id, &query.key);
        Box::pin(async move { result })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn tenant() -> TenantContext {
        TenantContext::new("acme", "prod")
    }

    #[tokio::test]
    async fn remember_then_recall_roundtrips() {
        let provider = InMemoryMemoryProvider::new();
        let t = tenant();
        provider
            .remember(
                &t,
                "sess-1",
                MemoryRecord {
                    key: "fav_color".to_string(),
                    value: "green".to_string(),
                },
            )
            .await
            .unwrap();

        let got = provider
            .recall(
                &t,
                "sess-1",
                &MemoryQuery {
                    key: "fav_color".to_string(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(MemoryRecord {
                key: "fav_color".to_string(),
                value: "green".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn recall_missing_key_returns_none() {
        let provider = InMemoryMemoryProvider::new();
        let got = provider
            .recall(
                &tenant(),
                "sess-1",
                &MemoryQuery {
                    key: "nope".to_string(),
                },
            )
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn recall_is_isolated_across_tenants() {
        let provider = InMemoryMemoryProvider::new();
        provider
            .remember(
                &TenantContext::new("acme", "prod"),
                "sess-1",
                MemoryRecord {
                    key: "fav_color".to_string(),
                    value: "green".to_string(),
                },
            )
            .await
            .unwrap();

        // Same session id + key, different tenant -> must not leak.
        let other = provider
            .recall(
                &TenantContext::new("globex", "prod"),
                "sess-1",
                &MemoryQuery {
                    key: "fav_color".to_string(),
                },
            )
            .await
            .unwrap();
        assert!(other.is_none());
    }

    fn rec(key: &str, value: &str) -> MemoryRecord {
        MemoryRecord {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn small() -> MemoryLimits {
        MemoryLimits {
            max_key_bytes: 8,
            max_value_bytes: 16,
            max_entries_per_session: 3,
            max_bytes_per_session: 40,
            max_sessions: 2,
        }
    }

    #[tokio::test]
    async fn oversized_key_or_value_is_refused() {
        let p = InMemoryMemoryProvider::with_limits(small());
        let t = tenant();
        let err = p
            .remember(&t, "s", rec("123456789", "v"))
            .await
            .unwrap_err();
        assert!(matches!(err, MemoryError::LimitExceeded(_)));
        let err = p
            .remember(&t, "s", rec("k", &"x".repeat(17)))
            .await
            .unwrap_err();
        assert!(matches!(err, MemoryError::LimitExceeded(_)));
    }

    #[tokio::test]
    async fn entry_count_per_session_is_capped_but_overwrite_is_allowed() {
        let p = InMemoryMemoryProvider::with_limits(small());
        let t = tenant();
        for k in ["a", "b", "c"] {
            p.remember(&t, "s", rec(k, "v")).await.unwrap();
        }
        let err = p.remember(&t, "s", rec("d", "v")).await.unwrap_err();
        assert!(matches!(err, MemoryError::LimitExceeded(_)));
        // Replacing an existing key does not add an entry.
        p.remember(&t, "s", rec("a", "w")).await.unwrap();
        // Another session is unaffected.
        p.remember(&t, "other", rec("d", "v")).await.unwrap();
    }

    #[tokio::test]
    async fn bytes_per_session_are_capped_and_replacement_frees_the_old_size() {
        let p = InMemoryMemoryProvider::with_limits(small());
        let t = tenant();
        // 2 + 16 = 18 bytes each: two fit under 40, a third does not.
        p.remember(&t, "s", rec("k1", &"x".repeat(16)))
            .await
            .unwrap();
        p.remember(&t, "s", rec("k2", &"x".repeat(16)))
            .await
            .unwrap();
        let err = p
            .remember(&t, "s", rec("k3", &"x".repeat(16)))
            .await
            .unwrap_err();
        assert!(matches!(err, MemoryError::LimitExceeded(_)));
        // Shrinking k1 frees room for k3.
        p.remember(&t, "s", rec("k1", "x")).await.unwrap();
        p.remember(&t, "s", rec("k3", &"x".repeat(16)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn least_recently_used_session_is_evicted_past_the_session_cap() {
        let p = InMemoryMemoryProvider::with_limits(small());
        let t = tenant();
        p.remember(&t, "s1", rec("k", "1")).await.unwrap();
        p.remember(&t, "s2", rec("k", "2")).await.unwrap();
        // Touch s1 so s2 is the least recently used.
        let q = MemoryQuery { key: "k".into() };
        assert!(p.recall(&t, "s1", &q).await.unwrap().is_some());
        p.remember(&t, "s3", rec("k", "3")).await.unwrap();
        assert!(p.recall(&t, "s2", &q).await.unwrap().is_none(), "evicted");
        assert!(p.recall(&t, "s1", &q).await.unwrap().is_some());
        assert!(p.recall(&t, "s3", &q).await.unwrap().is_some());
    }
}
