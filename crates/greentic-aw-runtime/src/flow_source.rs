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

    /// Invoke one flow that MAY park on the user (a card with a routed
    /// submit). A flow that completes answers [`FlowInvokeOutcome::Completed`];
    /// one that waits answers [`FlowInvokeOutcome::Waiting`] with an opaque
    /// snapshot to hand back to [`FlowInvoker::resume`] and the presentation
    /// (the card) to show the user meanwhile.
    ///
    /// The default keeps every existing implementor source-compatible: it runs
    /// the non-interactive [`FlowInvoker::invoke`], so an invoker that never
    /// learned to suspend still answers exactly as it did before.
    fn invoke_interactive<'a>(
        &'a self,
        flow_ref: &'a str,
        args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async move {
            self.invoke(flow_ref, args_json)
                .await
                .map(FlowInvokeOutcome::Completed)
        })
    }

    /// Resume a flow parked by [`FlowInvoker::invoke_interactive`], feeding it
    /// `input` (the user's submit). The default refuses: an invoker that never
    /// answers `Waiting` has nothing to resume.
    fn resume<'a>(
        &'a self,
        flow_ref: &'a str,
        _snapshot: serde_json::Value,
        _input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async move { Err(format!("flow tool '{flow_ref}': resume not supported")) })
    }
}

/// What an interactive flow-tool call produced.
#[derive(Clone, Debug, PartialEq)]
pub enum FlowInvokeOutcome {
    /// The flow ran to its end; the value is its output (the tool result).
    Completed(serde_json::Value),
    /// The flow parked awaiting the user. `snapshot` is opaque to this crate
    /// and goes back verbatim to [`FlowInvoker::resume`]; `presentation` is
    /// what the flow rendered at the park point (typically an Adaptive Card).
    Waiting {
        snapshot: serde_json::Value,
        presentation: serde_json::Value,
    },
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

    /// [`Self::dispatch`] for a flow that may park on the user. Failures fold
    /// into `Completed({"error": ...})` exactly as `dispatch` folds them, so a
    /// flow tool still never breaks an agent step.
    pub async fn dispatch_interactive(&self, flow_ref: &str, args_json: &str) -> FlowInvokeOutcome {
        if self.tool_entry(flow_ref).is_none() {
            return FlowInvokeOutcome::Completed(
                json!({ "error": format!("unknown flow tool '{flow_ref}'") }),
            );
        }
        match self.invoker.invoke_interactive(flow_ref, args_json).await {
            Ok(outcome) => outcome,
            Err(e) => FlowInvokeOutcome::Completed(json!({ "error": e })),
        }
    }

    /// Resume a parked flow tool with the user's `input`. Failures fold into
    /// `Completed({"error": ...})`. Deliberately NOT gated on the catalog
    /// listing the flow: the call was admitted when it started, and a TTL
    /// rebuild in between must not strand the user on a card.
    pub async fn resume(
        &self,
        flow_ref: &str,
        snapshot: serde_json::Value,
        input: serde_json::Value,
    ) -> FlowInvokeOutcome {
        match self.invoker.resume(flow_ref, snapshot, input).await {
            Ok(outcome) => outcome,
            Err(e) => FlowInvokeOutcome::Completed(json!({ "error": e })),
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

    /// An invoker that never learned to suspend keeps working on the
    /// interactive path: `invoke` is reported as `Completed`, and `resume` is
    /// refused (there is nothing it could have parked).
    #[tokio::test]
    async fn default_interactive_methods_wrap_invoke_and_refuse_resume() {
        let cat = FlowToolCatalog::from_invoker(Arc::new(FakeInvoker));
        let out = cat.dispatch_interactive("lookup", "{}").await;
        assert_eq!(
            out,
            FlowInvokeOutcome::Completed(serde_json::json!({ "echoed": "{}" }))
        );
        let resumed = cat
            .resume("lookup", serde_json::json!({}), serde_json::json!({}))
            .await;
        let v = match resumed {
            FlowInvokeOutcome::Completed(v) => v,
            FlowInvokeOutcome::Waiting { .. } => serde_json::Value::Null,
        };
        assert!(
            v["error"]
                .as_str()
                .unwrap_or("")
                .contains("resume not supported"),
            "got {v}"
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
