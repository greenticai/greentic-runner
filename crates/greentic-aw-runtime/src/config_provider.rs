//! Agent config provider trait + a 60s TTL caching decorator.
//!
//! Per spec Decision 13: TTL-only invalidation for MVP. Multi-instance
//! runners may observe up to 60s propagation lag for tenant config
//! changes made via the admin designer.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::config::AgentConfig;
use crate::error::ConfigError;
use crate::tenant::TenantContext;

/// Resolves [`AgentConfig`] for a given tenant + agent pair.
///
/// Stored as `Arc<dyn ConfigProvider>` by [`crate::AgentRuntime`], so all
/// methods use `Pin<Box<dyn Future>>` return types to remain dyn-safe.
/// Implementors that do not need dynamic dispatch (concrete generic contexts)
/// may use native `async fn` internally and box at the trait boundary.
pub trait ConfigProvider: Send + Sync {
    fn agent_config<'a>(
        &'a self,
        tenant: &'a TenantContext,
        agent_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>>;
}

/// Composite cache key covering tenant + env + agent.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    tenant_id: String,
    env_id: String,
    agent_id: String,
}

/// In-process cache that wraps any [`ConfigProvider`] with a TTL.
///
/// Default TTL is 60 seconds (per spec Decision 13). Use
/// [`CachingConfigProvider::with_ttl`] for shorter values in tests.
/// Safe to share across threads via `Arc`.
pub struct CachingConfigProvider<P: ConfigProvider> {
    inner: P,
    ttl: Duration,
    cache: RwLock<HashMap<CacheKey, (Instant, AgentConfig)>>,
}

impl<P: ConfigProvider> CachingConfigProvider<P> {
    /// Wraps `inner` with the production 60s TTL.
    pub fn new(inner: P) -> Self {
        Self::with_ttl(inner, Duration::from_secs(60))
    }

    /// Wraps `inner` with a custom TTL (useful for tests).
    pub fn with_ttl(inner: P, ttl: Duration) -> Self {
        Self {
            inner,
            ttl,
            cache: RwLock::new(HashMap::new()),
        }
    }
}

impl<P: ConfigProvider> ConfigProvider for CachingConfigProvider<P> {
    fn agent_config<'a>(
        &'a self,
        tenant: &'a TenantContext,
        agent_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
        Box::pin(async move {
            let key = CacheKey {
                tenant_id: tenant.tenant_id.clone(),
                env_id: tenant.env_id.clone(),
                agent_id: agent_id.to_string(),
            };
            {
                let cache = self.cache.read().await;
                if let Some((stored_at, cfg)) = cache.get(&key)
                    && stored_at.elapsed() < self.ttl
                {
                    return Ok(cfg.clone());
                }
            }
            let fresh = self.inner.agent_config(tenant, agent_id).await?;
            let mut cache = self.cache.write().await;
            cache.insert(key, (Instant::now(), fresh.clone()));
            Ok(fresh)
        })
    }
}

/// In-memory provider for the designer playground and unit tests.
///
/// Resolves configs from an internal `HashMap`. Returns
/// [`ConfigError::AgentNotFound`] for unknown `agent_id` values.
pub struct InMemoryConfigProvider {
    entries: HashMap<(String, String, String), AgentConfig>,
}

impl InMemoryConfigProvider {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Register a config for a specific (tenant, env, agent_id) triple.
    pub fn insert(&mut self, tenant: &TenantContext, agent_id: &str, cfg: AgentConfig) {
        self.entries.insert(
            (
                tenant.tenant_id.clone(),
                tenant.env_id.clone(),
                agent_id.to_string(),
            ),
            cfg,
        );
    }
}

impl Default for InMemoryConfigProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigProvider for InMemoryConfigProvider {
    fn agent_config<'a>(
        &'a self,
        tenant: &'a TenantContext,
        agent_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
        let key = (
            tenant.tenant_id.clone(),
            tenant.env_id.clone(),
            agent_id.to_string(),
        );
        let entry = self.entries.get(&key).cloned();
        let agent_id_owned = agent_id.to_string();
        Box::pin(async move { entry.ok_or(ConfigError::AgentNotFound(agent_id_owned)) })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::{AgentLimits, LlmProviderRef};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn sample_cfg() -> AgentConfig {
        AgentConfig {
            agent_id: "a-1".into(),
            system_prompt: "sys".into(),
            tools: vec![],
            guardrails: vec![],
            llm: LlmProviderRef {
                provider: "openai".into(),
                model: "gpt-4o-mini".into(),
                credential_ref: None,
            },
            limits: AgentLimits::default(),
            memory: None,
            knowledge: None,
            conversational: false,
            opening_message: None,
        }
    }

    /// Test double that counts every call to `agent_config`.
    struct CountingProvider {
        calls: AtomicUsize,
        cfg: AgentConfig,
    }

    impl ConfigProvider for CountingProvider {
        fn agent_config<'a>(
            &'a self,
            _tenant: &'a TenantContext,
            _agent_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let cfg = self.cfg.clone();
            Box::pin(async move { Ok(cfg) })
        }
    }

    /// Newtype wrapper so `Arc<CountingProvider>` implements `ConfigProvider`
    /// (required because `Arc<T>` does not auto-impl `ConfigProvider` unless
    /// we add a blanket impl, which we deliberately avoid).
    struct Wrapper(Arc<CountingProvider>);

    impl ConfigProvider for Wrapper {
        fn agent_config<'a>(
            &'a self,
            tenant: &'a TenantContext,
            agent_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
            self.0.agent_config(tenant, agent_id)
        }
    }

    #[tokio::test]
    async fn caching_provider_hits_inner_once_within_ttl() {
        let inner = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            cfg: sample_cfg(),
        });
        let cache = CachingConfigProvider::new(Wrapper(inner.clone()));
        let tc = TenantContext::new("acme", "prod");
        let _ = cache.agent_config(&tc, "a-1").await.unwrap();
        let _ = cache.agent_config(&tc, "a-1").await.unwrap();
        let _ = cache.agent_config(&tc, "a-1").await.unwrap();
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn caching_provider_expires_after_ttl() {
        let inner = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            cfg: sample_cfg(),
        });
        let cache =
            CachingConfigProvider::with_ttl(Wrapper(inner.clone()), Duration::from_millis(50));
        let tc = TenantContext::new("acme", "prod");
        let _ = cache.agent_config(&tc, "a-1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let _ = cache.agent_config(&tc, "a-1").await.unwrap();
        assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn in_memory_provider_returns_not_found_for_missing_agent() {
        let p = InMemoryConfigProvider::new();
        let tc = TenantContext::new("acme", "prod");
        let result = p.agent_config(&tc, "missing").await;
        assert!(matches!(result, Err(ConfigError::AgentNotFound(_))));
    }
}
