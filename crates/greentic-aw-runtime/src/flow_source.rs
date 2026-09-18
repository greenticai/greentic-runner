//! Flow-as-agent-tool source. Mirrors `component_source` but a flow is the
//! whole tool unit, so the catalog is keyed by a single `flow_ref`. The
//! LLM-facing description + parameters are derived from the flow (supplied by
//! the host-side `FlowInvoker`), not from the agent config.
//!
//! Resilience contract (a flow tool must never break an agent step):
//! - Building a catalog is infallible: a [`FlowInvoker`] that surfaces no
//!   flows simply yields an empty catalog. [`FlowToolSource::catalog`]
//!   never returns or propagates an error.
//! - [`FlowToolCatalog::dispatch`] always returns a JSON [`serde_json::Value`]
//!   and never panics — an unknown `flow_ref` or an invoker failure becomes
//!   `{"error": "..."}` so the LLM observes it as a normal tool result.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde_json::json;

use crate::tenant::TenantContext;

/// How long a built catalog is reused before a rebuild is considered.
/// Mirrors `component_source::CATALOG_TTL`.
const CATALOG_TTL: Duration = Duration::from_secs(5 * 60);

/// One flow offered as an agent tool, with its LLM-facing contract.
/// Produced by [`FlowInvoker::list_flows`].
#[derive(Clone, Debug)]
pub struct FlowOperation {
    pub flow_ref: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// A resolved catalog entry (description + JSON-schema parameters).
/// Stored inside [`FlowToolCatalog`], keyed by `flow_ref`.
#[derive(Clone, Debug)]
pub struct FlowToolEntry {
    pub description: String,
    pub parameters: serde_json::Value,
}

/// Host boundary: enumerate + invoke flows. The concrete implementation lives
/// in runner-host (`PackRuntimeFlowInvoker`) and is injected at the edge;
/// this crate depends only on the trait + JSON so it need not pull in
/// `wasmtime`/runner-host.
///
/// Both methods are total: `list_flows` returns whatever is currently exposed
/// (possibly empty), and `invoke` reports failure via `Err(String)` which
/// [`FlowToolCatalog::dispatch`] wraps into an `{"error": ...}` value —
/// neither aborts an agent step.
pub trait FlowInvoker: Send + Sync {
    /// Describe every flow exposed as an agentic-worker tool.
    fn list_flows(&self) -> Vec<FlowOperation>;

    /// Invoke one flow with JSON `args_json`. Returns the raw flow output
    /// value on success, or a stringified error on any failure (bad args,
    /// dispatch failure, timeout).
    fn invoke<'a>(
        &'a self,
        flow_ref: &'a str,
        args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;
}

/// Immutable per-tenant view of the flow-tool surface. Carries the LLM-facing
/// schemas (list seam) plus the [`FlowInvoker`] handle needed to dispatch a
/// call.
pub struct FlowToolCatalog {
    /// `flow_ref` → LLM-facing tool schema.
    tools: HashMap<String, FlowToolEntry>,
    invoker: Arc<dyn FlowInvoker>,
    fetched_at: Instant,
}

impl FlowToolCatalog {
    /// Build a catalog by calling [`FlowInvoker::list_flows`] once.
    /// Used by [`FlowToolSource::catalog`]; also `pub` so tests can construct
    /// a catalog directly (mirrors `ComponentToolCatalog::from_invoker`).
    pub fn from_invoker(invoker: Arc<dyn FlowInvoker>) -> Self {
        let mut tools = HashMap::new();
        for op in invoker.list_flows() {
            tools.insert(
                op.flow_ref,
                FlowToolEntry {
                    description: op.description,
                    parameters: op.parameters,
                },
            );
        }
        Self {
            tools,
            invoker,
            fetched_at: Instant::now(),
        }
    }

    /// Iterate every `flow_ref` key with its schema.
    pub fn tools(&self) -> impl Iterator<Item = (&String, &FlowToolEntry)> {
        self.tools.iter()
    }

    /// Number of flows in the catalog.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Whether the catalog exposes no flows.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// LLM-facing schema for one flow, if present.
    pub fn tool_entry(&self, flow_ref: &str) -> Option<&FlowToolEntry> {
        self.tools.get(flow_ref)
    }

    /// Invoke one flow, always returning a JSON value. An unknown `flow_ref`
    /// or an invoker failure is surfaced as `{"error": "..."}` so the LLM
    /// observes it as a normal tool result.
    pub async fn dispatch(&self, flow_ref: &str, args_json: &str) -> serde_json::Value {
        if self.tool_entry(flow_ref).is_none() {
            return json!({ "error": format!("unknown flow tool '{flow_ref}'") });
        }
        match self.invoker.invoke(flow_ref, args_json).await {
            Ok(value) => value,
            Err(e) => json!({ "error": e }),
        }
    }
}

/// Per-tenant, TTL-gated source of agentic-worker flow tool catalogs.
///
/// Mirrors [`crate::component_source::ComponentToolSource`]: a built catalog
/// is cached per tenant behind a short TTL so the per-step resolution does
/// not re-enumerate flows on every iteration. The [`FlowInvoker`] is the
/// host-injected seam over the pack flow runtime.
pub struct FlowToolSource {
    invoker: Arc<dyn FlowInvoker>,
    cache: DashMap<String, Arc<FlowToolCatalog>>,
}

impl FlowToolSource {
    /// Construct a source over a host-provided flow invoker.
    pub fn new(invoker: Arc<dyn FlowInvoker>) -> Self {
        Self {
            invoker,
            cache: DashMap::new(),
        }
    }

    /// Stable per-tenant cache key — the same `(tenant_id, env_id)` pair
    /// that `ComponentToolSource::cache_key` uses. Mirrors it exactly.
    fn cache_key(tenant: &TenantContext) -> String {
        format!("{}:{}", tenant.tenant_id, tenant.env_id)
    }

    /// Return the tenant's flow tool catalog, rebuilding when stale or absent.
    /// Infallible by contract.
    pub async fn catalog(&self, tenant: &TenantContext) -> Arc<FlowToolCatalog> {
        let key = Self::cache_key(tenant);

        if let Some(entry) = self.cache.get(&key) {
            let snap = entry.value();
            if snap.fetched_at.elapsed() < CATALOG_TTL {
                return snap.clone();
            }
        }

        let built = Arc::new(FlowToolCatalog::from_invoker(self.invoker.clone()));
        self.cache.insert(key, built.clone());
        built
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct FakeInvoker;
    impl FlowInvoker for FakeInvoker {
        fn list_flows(&self) -> Vec<FlowOperation> {
            vec![FlowOperation {
                flow_ref: "lookup".into(),
                description: "Look things up".into(),
                parameters: serde_json::json!({ "type": "object" }),
            }]
        }
        fn invoke<'a>(
            &'a self,
            flow_ref: &'a str,
            args_json: &'a str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
        > {
            Box::pin(async move {
                if flow_ref == "lookup" {
                    Ok(serde_json::json!({ "echoed": args_json }))
                } else {
                    Err(format!("flow '{flow_ref}' not found"))
                }
            })
        }
    }

    #[tokio::test]
    async fn catalog_lists_and_dispatches_flows() {
        let cat = FlowToolCatalog::from_invoker(Arc::new(FakeInvoker));
        assert_eq!(cat.len(), 1);
        let entry = cat.tool_entry("lookup").expect("entry");
        assert_eq!(entry.description, "Look things up");
        let out = cat.dispatch("lookup", "{\"q\":1}").await;
        assert_eq!(out["echoed"], "{\"q\":1}");
    }

    #[tokio::test]
    async fn dispatch_missing_flow_returns_error_value_not_err() {
        let cat = FlowToolCatalog::from_invoker(Arc::new(FakeInvoker));
        let out = cat.dispatch("nope", "{}").await;
        assert!(
            out.get("error").is_some(),
            "missing flow must yield an error value"
        );
    }

    #[tokio::test]
    async fn source_caches_within_ttl_window() {
        let source = FlowToolSource::new(Arc::new(FakeInvoker));
        let tenant = TenantContext::new("acme", "prod");
        let first = source.catalog(&tenant).await;
        let second = source.catalog(&tenant).await;
        assert!(
            Arc::ptr_eq(&first, &second),
            "second call must hit TTL cache"
        );
    }

    #[tokio::test]
    async fn cache_key_is_tenant_and_env() {
        // Different env_id must NOT share a cache entry.
        let source = FlowToolSource::new(Arc::new(FakeInvoker));
        let prod = TenantContext::new("acme", "prod");
        let staging = TenantContext::new("acme", "staging");
        let prod_cat = source.catalog(&prod).await;
        let staging_cat = source.catalog(&staging).await;
        assert!(
            !Arc::ptr_eq(&prod_cat, &staging_cat),
            "different envs must have independent catalogs"
        );
    }

    #[tokio::test]
    async fn dispatch_unknown_flow_errors_without_invoking() {
        let cat = FlowToolCatalog::from_invoker(Arc::new(FakeInvoker));
        let out = cat.dispatch("no_such_flow", "{}").await;
        assert!(
            out.get("error").is_some(),
            "unknown flow must yield an error value"
        );
        assert!(
            out["error"].as_str().unwrap_or("").contains("no_such_flow"),
            "error message must name the flow, got: {out}"
        );
    }
}
