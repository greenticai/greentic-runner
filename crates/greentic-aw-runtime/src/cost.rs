//! Per-tenant daily token counter enforced at `step()` entry.
//!
//! Key: `aw:{tenant}:{env}:cost:tokens:{yyyymmdd}` with 86400s TTL.
//! The trait is pluggable (`MockTokenMeter` for unit tests); the
//! production impl reuses the `ConnectionManager` shared from
//! `RedisAgentStateStore`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::Utc;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::error::StateError;
use crate::kv::AwKv;
use crate::tenant::TenantContext;

/// Daily per-tenant token accounting. Dyn-safe (`Arc<dyn TokenMeter>`),
/// so methods return `Pin<Box<dyn Future>>` rather than `async fn`.
pub trait TokenMeter: Send + Sync {
    /// Tokens consumed by this tenant within the current UTC day.
    /// Returns `0` when no counter exists yet.
    fn current<'a>(
        &'a self,
        tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = Result<u64, StateError>> + Send + 'a>>;

    /// Add `tokens` to the daily counter; refresh TTL to 86400s
    /// (rolling 24h window).
    fn add<'a>(
        &'a self,
        tenant: &'a TenantContext,
        tokens: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>>;
}

/// Production token meter backed by a multiplexed `ConnectionManager`.
///
/// Shares the Redis instance with `RedisAgentStateStore` via
/// `RedisAgentStateStore::manager()`. The manager is `Clone` (cheap,
/// reference-counted) so per-call clones open no new connections.
pub struct RedisTokenMeter {
    manager: ConnectionManager,
}

impl RedisTokenMeter {
    pub fn new(manager: ConnectionManager) -> Self {
        Self { manager }
    }

    fn key(tenant: &TenantContext) -> String {
        let day = Utc::now().format("%Y%m%d").to_string();
        format!("{}:cost:tokens:{day}", tenant.key_prefix())
    }
}

impl TokenMeter for RedisTokenMeter {
    fn current<'a>(
        &'a self,
        tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = Result<u64, StateError>> + Send + 'a>> {
        Box::pin(async move {
            let key = Self::key(tenant);
            let mut conn = self.manager.clone();
            let value: Option<u64> = conn
                .get(&key)
                .await
                .map_err(|e| StateError::Redis(format!("get: {e}")))?;
            Ok(value.unwrap_or(0))
        })
    }

    fn add<'a>(
        &'a self,
        tenant: &'a TenantContext,
        tokens: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        Box::pin(async move {
            let key = Self::key(tenant);
            let mut conn = self.manager.clone();
            // INCRBY returns the new value; we ignore it.
            let _: i64 = conn
                .incr(&key, tokens as i64)
                .await
                .map_err(|e| StateError::Redis(format!("incrby: {e}")))?;
            let _: bool = conn
                .expire(&key, 86_400)
                .await
                .map_err(|e| StateError::Redis(format!("expire: {e}")))?;
            Ok(())
        })
    }
}

const TOKEN_TTL: std::time::Duration = std::time::Duration::from_secs(86_400); // 24h

/// Daily per-tenant token meter over [`AwKv`] (Redis-free). Same key format
/// and 24h window as [`RedisTokenMeter`].
pub struct KvTokenMeter {
    kv: Arc<dyn AwKv>,
}

impl KvTokenMeter {
    pub fn new(kv: Arc<dyn AwKv>) -> Self {
        Self { kv }
    }

    fn key(tenant: &TenantContext) -> String {
        let day = Utc::now().format("%Y%m%d").to_string();
        format!("{}:cost:tokens:{day}", tenant.key_prefix())
    }
}

impl TokenMeter for KvTokenMeter {
    fn current<'a>(
        &'a self,
        tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = Result<u64, StateError>> + Send + 'a>> {
        Box::pin(async move { self.kv.get_u64(&Self::key(tenant)).await })
    }

    fn add<'a>(
        &'a self,
        tenant: &'a TenantContext,
        tokens: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        Box::pin(async move {
            self.kv
                .incr_by(&Self::key(tenant), tokens, TOKEN_TTL)
                .await?;
            Ok(())
        })
    }
}

/// In-memory token meter for unit tests. Gated behind `test-mock`.
#[cfg(feature = "test-mock")]
pub struct MockTokenMeter {
    pub current_value: std::sync::Mutex<u64>,
}

#[cfg(feature = "test-mock")]
impl MockTokenMeter {
    pub fn new(current: u64) -> Self {
        Self {
            current_value: std::sync::Mutex::new(current),
        }
    }
}

#[cfg(feature = "test-mock")]
impl TokenMeter for MockTokenMeter {
    #[allow(clippy::expect_used)] // test-mock infra: mutex poisoning is unreachable
    fn current<'a>(
        &'a self,
        _t: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = Result<u64, StateError>> + Send + 'a>> {
        let v = *self
            .current_value
            .lock()
            .expect("mock token meter poisoned");
        Box::pin(async move { Ok(v) })
    }

    #[allow(clippy::expect_used)] // test-mock infra: mutex poisoning is unreachable
    fn add<'a>(
        &'a self,
        _t: &'a TenantContext,
        tokens: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        {
            let mut v = self
                .current_value
                .lock()
                .expect("mock token meter poisoned");
            *v += tokens;
        }
        Box::pin(async move { Ok(()) })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod kv_meter_tests {
    use super::*;
    use crate::kv::MemoryKv;
    use std::sync::Arc;

    #[tokio::test]
    async fn add_accumulates_within_day() {
        let meter = KvTokenMeter::new(Arc::new(MemoryKv::new()));
        let t = TenantContext::new("acme", "prod");
        assert_eq!(meter.current(&t).await.unwrap(), 0);
        meter.add(&t, 100).await.unwrap();
        meter.add(&t, 50).await.unwrap();
        assert_eq!(meter.current(&t).await.unwrap(), 150);
    }

    #[tokio::test]
    async fn counters_are_isolated_per_tenant() {
        let meter = KvTokenMeter::new(Arc::new(MemoryKv::new()));
        let a = TenantContext::new("a", "prod");
        let b = TenantContext::new("b", "prod");
        meter.add(&a, 10).await.unwrap();
        assert_eq!(meter.current(&b).await.unwrap(), 0);
    }
}
