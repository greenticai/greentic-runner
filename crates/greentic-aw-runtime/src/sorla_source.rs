//! Per-tenant agentic-worker SoRLa (System-of-Record) tool catalog.
//!
//! Exposes `sorla:<pack>` operations — a SoR BusinessAction invoked over the
//! host's SoRX interact client — to an agentic worker as LLM tools, mirroring
//! [`crate::component_source`] for the Component surface. A worker's
//! `AgentConfig.tools` entry of the form
//! `ToolRef { extension_id: "sorla:<pack>", tool_name: "<action>" }` resolves
//! here: the catalog supplies the LLM-facing `description`/`parameters` for
//! the list seam and routes the call to a [`SorxInvoker`] for dispatch.
//!
//! The actual SoR BusinessAction invocation lives in the runner host (over
//! its SoRX interact client), behind the [`SorxInvoker`] trait, so this
//! crate stays free of any HTTP/SoRX/wasmtime dependency — it sees only the
//! trait and JSON.
//!
//! Resilience contract (a sorla tool must never break an agent step):
//! - Building a catalog is infallible: a [`SorxInvoker`] that surfaces no
//!   operations simply yields an empty catalog. [`SorlaToolSource::catalog`]
//!   never returns or propagates an error.
//! - [`SorlaToolCatalog::dispatch`] always returns a JSON [`serde_json::Value`]
//!   and never panics — an unknown `(pack, action)` or an invoker failure
//!   becomes `{"error": "..."}` so the LLM observes it as a normal tool
//!   result.
//!
//! Like the component source (and unlike the designer's `mcp__server__tool`
//! string mangling), every tool is keyed by a `(pack, action)` tuple.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde_json::json;

use crate::tenant::TenantContext;

/// How long a built catalog is reused before a rebuild is considered.
const CATALOG_TTL: Duration = Duration::from_secs(5 * 60);

/// LLM-facing schema for one sorla operation: enough to build an
/// `LlmToolSchema` in [`crate::tools::list_tools_for_llm`].
#[derive(Clone, Debug)]
pub struct SorlaToolEntry {
    pub description: String,
    pub parameters: serde_json::Value,
}

/// One SoR BusinessAction discoverable as an agentic-worker tool: the
/// `(pack, action)` identity plus its LLM-facing schema. Produced by
/// [`SorxInvoker::list_operations`].
#[derive(Clone, Debug)]
pub struct SorxOperation {
    pub pack: String,
    pub action: String,
    pub description: String,
    pub parameters: serde_json::Value,
    /// The SoRX capability string (e.g.
    /// `cap://greentic/business-functions/<pack>/<action>/v0.1.0`), carried
    /// so the invoker impl in runner-host can resolve it. The catalog itself
    /// ignores `cap_uri` for list/dispatch keying.
    pub cap_uri: String,
}

/// Host-side seam that resolves and invokes SoR BusinessActions over the
/// SoRX interact client. The concrete implementation lives in the runner
/// host; this crate depends only on the trait + JSON so it need not pull in
/// any HTTP/SoRX/wasmtime dependency.
///
/// Both methods are total: `list_operations` returns whatever is currently
/// exposed (possibly empty), and `invoke` reports failure via `Err(String)`
/// which [`SorlaToolCatalog::dispatch`] wraps into an `{"error": ...}` value
/// — neither aborts an agent step.
pub trait SorxInvoker: Send + Sync {
    /// Describe every SoR BusinessAction exposed to agentic-worker tools.
    fn list_operations(&self) -> Vec<SorxOperation>;

    /// Invoke one SoR BusinessAction with JSON `args_json`. Returns the raw
    /// action output value on success, or a stringified error on any
    /// failure (bad args, SoRX transport error, action failure, timeout).
    fn invoke<'a>(
        &'a self,
        pack: &'a str,
        action: &'a str,
        args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;
}

/// Immutable per-tenant view of the sorla-tool surface. Carries the
/// LLM-facing schemas (list seam) plus the [`SorxInvoker`] handle needed to
/// dispatch a call.
pub struct SorlaToolCatalog {
    /// `(pack, action)` → LLM-facing tool schema.
    tools: HashMap<(String, String), SorlaToolEntry>,
    invoker: Arc<dyn SorxInvoker>,
    fetched_at: Instant,
}

impl SorlaToolCatalog {
    fn from_invoker(invoker: Arc<dyn SorxInvoker>) -> Self {
        let mut tools = HashMap::new();
        for op in invoker.list_operations() {
            tools.insert(
                (op.pack, op.action),
                SorlaToolEntry {
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

    /// Iterate every `(pack, action)` key with its schema.
    pub fn tools(&self) -> impl Iterator<Item = (&(String, String), &SorlaToolEntry)> {
        self.tools.iter()
    }

    /// Number of operations in the catalog.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Whether the catalog exposes no operations.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// LLM-facing schema for one operation, if present.
    pub fn tool_entry(&self, pack: &str, action: &str) -> Option<&SorlaToolEntry> {
        self.tools.get(&(pack.to_string(), action.to_string()))
    }

    /// Invoke one SoR BusinessAction, always returning a JSON value. An
    /// unknown `(pack, action)` or an invoker failure is surfaced as
    /// `{"error": "..."}` so the LLM observes it as a normal tool result.
    pub async fn dispatch(&self, pack: &str, action: &str, args_json: &str) -> serde_json::Value {
        if self.tool_entry(pack, action).is_none() {
            return json!({
                "error": format!("unknown sorla tool '{pack}/{action}'")
            });
        }
        match self.invoker.invoke(pack, action, args_json).await {
            Ok(value) => value,
            Err(e) => json!({ "error": e }),
        }
    }

    /// Build a catalog directly from a tool map + invoker, bypassing
    /// [`SorlaToolSource`]. Test-only: lets other test modules exercise the
    /// list/dispatch seams without standing up a real invoker.
    #[cfg(test)]
    pub(crate) fn for_tests(
        tools: HashMap<(String, String), SorlaToolEntry>,
        invoker: Arc<dyn SorxInvoker>,
    ) -> Self {
        Self {
            tools,
            invoker,
            fetched_at: Instant::now(),
        }
    }
}

/// Per-tenant, TTL-gated source of agentic-worker sorla tool catalogs.
///
/// Mirrors [`crate::component_source::ComponentToolSource`]: a built catalog
/// is cached per tenant behind a short TTL so the per-step resolution in
/// [`crate::r#loop::run_step`] does not re-enumerate the SoR pack's
/// BusinessActions on every iteration. The [`SorxInvoker`] is the
/// host-injected seam over the SoRX interact client.
pub struct SorlaToolSource {
    invoker: Arc<dyn SorxInvoker>,
    cache: DashMap<String, Arc<SorlaToolCatalog>>,
}

impl SorlaToolSource {
    /// Construct a source over a host-provided SoRX invoker.
    pub fn new(invoker: Arc<dyn SorxInvoker>) -> Self {
        Self {
            invoker,
            cache: DashMap::new(),
        }
    }

    /// Stable per-tenant cache key — the same `(tenant_id, env_id)` pair
    /// `TenantContext::key_prefix` is built from.
    fn cache_key(tenant: &TenantContext) -> String {
        format!("{}:{}", tenant.tenant_id, tenant.env_id)
    }

    /// Return the tenant's sorla tool catalog, rebuilding when stale or
    /// absent. Infallible by contract.
    pub async fn catalog(&self, tenant: &TenantContext) -> Arc<SorlaToolCatalog> {
        let key = Self::cache_key(tenant);

        if let Some(entry) = self.cache.get(&key) {
            let snap = entry.value();
            if snap.fetched_at.elapsed() < CATALOG_TTL {
                return snap.clone();
            }
        }

        let built = Arc::new(SorlaToolCatalog::from_invoker(self.invoker.clone()));
        self.cache.insert(key, built.clone());
        built
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod test_support {
    //! Test-only fakes shared with other sorla-tool tests.
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A scriptable [`SorxInvoker`]: returns a fixed operation list and a
    /// fixed `invoke` result, and counts `list_operations` calls so the TTL
    /// cache can be asserted.
    pub(crate) struct FakeInvoker {
        ops: Vec<SorxOperation>,
        result: Result<serde_json::Value, String>,
        pub list_calls: AtomicUsize,
    }

    impl FakeInvoker {
        pub(crate) fn new(
            ops: Vec<SorxOperation>,
            result: Result<serde_json::Value, String>,
        ) -> Self {
            Self {
                ops,
                result,
                list_calls: AtomicUsize::new(0),
            }
        }
    }

    impl SorxInvoker for FakeInvoker {
        fn list_operations(&self) -> Vec<SorxOperation> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.ops.clone()
        }

        fn invoke<'a>(
            &'a self,
            _pack: &'a str,
            _action: &'a str,
            _args_json: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
            let result = self.result.clone();
            Box::pin(async move { result })
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::test_support::*;
    use super::*;

    fn test_tenant() -> TenantContext {
        TenantContext::new("acme", "prod")
    }

    #[tokio::test]
    async fn catalog_dispatch_routes_to_invoker() {
        let inv = Arc::new(FakeInvoker::new(
            vec![SorxOperation {
                pack: "landlord".into(),
                action: "record_rent_payment".into(),
                description: "Record a rent payment".into(),
                parameters: serde_json::json!({"type":"object"}),
                cap_uri: "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0"
                    .into(),
            }],
            Ok(serde_json::json!({"id":"pay-1"})),
        ));
        let src = SorlaToolSource::new(inv);
        let cat = src.catalog(&test_tenant()).await;
        let out = cat.dispatch("landlord", "record_rent_payment", "{}").await;
        assert_eq!(out, serde_json::json!({"id":"pay-1"}));
    }

    #[tokio::test]
    async fn catalog_dispatch_unknown_is_error_value_not_panic() {
        let src = SorlaToolSource::new(Arc::new(FakeInvoker::new(
            vec![],
            Ok(serde_json::json!({})),
        )));
        let cat = src.catalog(&test_tenant()).await;
        let out = cat.dispatch("nope", "nope", "{}").await;
        assert!(out.get("error").is_some());
    }

    #[tokio::test]
    async fn catalog_is_ttl_cached_per_tenant() {
        let inv = Arc::new(FakeInvoker::new(vec![], Ok(serde_json::json!({}))));
        let src = SorlaToolSource::new(inv.clone());
        let _ = src.catalog(&test_tenant()).await;
        let _ = src.catalog(&test_tenant()).await; // within TTL -> no re-list
        assert_eq!(inv.list_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn for_tests_builds_catalog_with_entry() {
        let invoker = Arc::new(FakeInvoker::new(
            vec![],
            Ok(serde_json::json!({ "ok": true })),
        ));
        let mut tools = HashMap::new();
        tools.insert(
            ("landlord".to_string(), "record_rent_payment".to_string()),
            SorlaToolEntry {
                description: "Record a rent payment".to_string(),
                parameters: serde_json::json!({ "type": "object" }),
            },
        );
        let catalog = SorlaToolCatalog::for_tests(tools, invoker);
        assert_eq!(catalog.len(), 1);
        let out = catalog
            .dispatch("landlord", "record_rent_payment", "{}")
            .await;
        assert_eq!(out, serde_json::json!({ "ok": true }), "got: {out}");
    }
}
