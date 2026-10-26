//! An HTTP provider that pulls a full [`GraphConfig`] from the
//! greentic-designer-admin agent-graph registry over HTTP, authed with a
//! tenant `gtc_live_*` bearer token.
//!
//! Mirrors [`crate::http_provider::HttpConfigProvider`] exactly — 10s timeout,
//! bearer auth, identical error taxonomy:
//!
//! | HTTP status | `ConfigError` variant |
//! |---|---|
//! | 200 + valid JSON  | `Ok(GraphConfig)` |
//! | 200 + invalid/unparseable body | `Misconfigured` (corrupt doc — do NOT silently fall back) |
//! | 401 / 403 | `Misconfigured` (bad token — operator-actionable, do NOT fall back) |
//! | 404 | `AgentNotFound(graph_id)` |
//! | other (network, 5xx, …) | `Internal` |
//!
//! **Corrupt-doc decision** (mirrors `HttpConfigProvider`): a 200 body that
//! fails `GraphConfig::from_json` — whether due to a JSON parse error, an
//! unsupported `schemaVersion`, or a structural graph validation failure — is
//! classified as `Misconfigured`, not `Internal`. This matches the agent
//! provider, which calls `.json::<AgentConfig>()` and maps *all* decode errors
//! to `Misconfigured`. The intent: a corrupt doc that the admin wrote is an
//! operator configuration problem, not a transient infrastructure failure.
//! Callers (`LayeredGraphProvider`) never fall back past a `Misconfigured`
//! error, so a bad graph document surfaces immediately instead of silently
//! hiding behind stale local pack data.

use crate::error::ConfigError;
use crate::graph::model::GraphConfig;
use crate::tenant::TenantContext;

/// Pulls [`GraphConfig`] from `{base}/api/v1/designer/agent-graphs/{graph_id}`.
///
/// Construct via [`HttpGraphProvider::new`]; call
/// [`HttpGraphProvider::graph_config`] per request. The inherent method
/// (rather than a trait) keeps this struct in `greentic-aw-runtime`, away from
/// the `GraphConfigSource` trait that lives in `runner-host`. The trait adapter
/// is a 5-line `impl GraphConfigSource for HttpGraphProvider` in `runner-host`'s
/// `graph_node.rs`.
pub struct HttpGraphProvider {
    base_url: String,
    token: String,
    client: reqwest::Client,
}

impl HttpGraphProvider {
    /// `base_url` is the admin origin (no trailing slash needed); `token` is a
    /// tenant `gtc_live_*` key.
    ///
    /// Mirrors [`crate::http_provider::HttpConfigProvider::new`] exactly:
    /// 10s per-request timeout, `unwrap_or_default` fallback on client build.
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            client,
        }
    }

    /// Fetch the graph document for `graph_id` from the admin registry.
    ///
    /// `tenant` is accepted for API symmetry with `ConfigProvider::agent_config`
    /// but not used in the request URL (the bearer token already scopes the
    /// request to the correct tenant, matching the agent-config provider's
    /// behaviour).
    pub async fn graph_config(
        &self,
        _tenant: &TenantContext,
        graph_id: &str,
    ) -> Result<GraphConfig, ConfigError> {
        // `..` stays a traversal step even percent-encoded: never send it.
        if crate::tenant::is_dot_segment(graph_id) {
            return Err(ConfigError::AgentNotFound(graph_id.to_string()));
        }
        let url = format!(
            "{}/api/v1/designer/agent-graphs/{}",
            self.base_url,
            crate::tenant::percent_encode_segment(graph_id)
        );
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| {
                ConfigError::Internal(format!(
                    "graph registry request failed: {}",
                    e.without_url()
                ))
            })?;

        match resp.status().as_u16() {
            200 => {
                // Read the full body as text, then parse via GraphConfig::from_json.
                // Any failure — invalid JSON, wrong schemaVersion, structural
                // validation error — is `Misconfigured` (mirrors HttpConfigProvider's
                // `.json::<AgentConfig>()` decode-error → Misconfigured mapping).
                let body = resp
                    .text()
                    .await
                    .map_err(|e| ConfigError::Misconfigured(format!("graph config read: {e}")))?;
                GraphConfig::from_json(&body)
                    .map_err(|e| ConfigError::Misconfigured(format!("graph config decode: {e}")))
            }
            404 => Err(ConfigError::AgentNotFound(graph_id.to_string())),
            // Auth failures are operator-actionable misconfig, not a
            // transient fault — surface them (Misconfigured is NOT swallowed
            // by LayeredGraphProvider) rather than masking a bad token behind
            // a local fallback. Mirrors HttpConfigProvider verbatim.
            401 | 403 => Err(ConfigError::Misconfigured(format!(
                "graph registry auth rejected (status {})",
                resp.status().as_u16()
            ))),
            other => Err(ConfigError::Internal(format!(
                "graph registry returned status {other}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// CachingGraphProvider
// ---------------------------------------------------------------------------

/// In-process TTL cache wrapping any `async fn graph_config(…)` provider.
///
/// Mirrors [`crate::config_provider::CachingConfigProvider`] for the graph
/// path: default TTL is 60 seconds (per spec Decision 13). Use
/// [`CachingGraphProvider::with_ttl`] for shorter values in tests.
///
/// The type parameter `P` must expose an inherent `async fn graph_config(…)`
/// matching the signature used by [`HttpGraphProvider`]. Concretely only
/// [`HttpGraphProvider`] needs wrapping today; a generic type parameter avoids
/// boxing and lets the compiler inline the inner call.
pub struct CachingGraphProvider<P> {
    inner: P,
    ttl: std::time::Duration,
    cache:
        tokio::sync::RwLock<std::collections::HashMap<CacheKey, (std::time::Instant, GraphConfig)>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    tenant_id: String,
    env_id: String,
    graph_id: String,
}

impl<P: Send + Sync> CachingGraphProvider<P> {
    /// Wrap `inner` with the production 60s TTL.
    pub fn new(inner: P) -> Self {
        Self::with_ttl(inner, std::time::Duration::from_secs(60))
    }

    /// Wrap `inner` with a custom TTL (useful for tests).
    pub fn with_ttl(inner: P, ttl: std::time::Duration) -> Self {
        Self {
            inner,
            ttl,
            cache: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl CachingGraphProvider<HttpGraphProvider> {
    /// Fetch with caching: serve a valid cached entry within the TTL, otherwise
    /// call the inner [`HttpGraphProvider`] and populate the cache on success.
    ///
    /// Only `Ok` responses are cached; errors are always forwarded to the
    /// caller — matching `CachingConfigProvider` behaviour.
    pub async fn graph_config(
        &self,
        tenant: &TenantContext,
        graph_id: &str,
    ) -> Result<GraphConfig, ConfigError> {
        let key = CacheKey {
            tenant_id: tenant.tenant_id.clone(),
            env_id: tenant.env_id.clone(),
            graph_id: graph_id.to_string(),
        };
        {
            let cache = self.cache.read().await;
            if let Some((stored_at, cfg)) = cache.get(&key)
                && stored_at.elapsed() < self.ttl
            {
                return Ok(cfg.clone());
            }
        }
        let fresh = self.inner.graph_config(tenant, graph_id).await?;
        let mut cache = self.cache.write().await;
        cache.insert(key, (std::time::Instant::now(), fresh.clone()));
        Ok(fresh)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Minimal valid graph JSON — mirrors the triage fixture used elsewhere.
    fn valid_graph_json() -> serde_json::Value {
        serde_json::json!({
            "schemaVersion": 1,
            "entry": "agent",
            "nodes": [
                {"id": "agent", "kind": "agent", "systemPrompt": "You triage.", "model": "gpt-4o-mini", "tools": []},
                {"id": "lookup", "kind": "tool", "toolName": "kb/search"},
                {"id": "router", "kind": "router", "maxIterations": 3},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "agent", "to": "lookup"},
                {"from": "lookup", "to": "router"},
                {"from": "router", "to": "agent", "branch": "loop"},
                {"from": "router", "to": "respond", "branch": "resolved"}
            ]
        })
    }

    fn tenant() -> TenantContext {
        TenantContext::new("t", "e")
    }

    // -----------------------------------------------------------------------
    // HttpGraphProvider tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn fetches_and_parses_graph_config() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/designer/agent-graphs/triage.graph"))
            .and(header("authorization", "Bearer gtc_live_x"))
            .respond_with(ResponseTemplate::new(200).set_body_json(valid_graph_json()))
            .mount(&server)
            .await;

        let provider = HttpGraphProvider::new(server.uri(), "gtc_live_x");
        let cfg = provider
            .graph_config(&tenant(), "triage.graph")
            .await
            .unwrap();
        assert_eq!(cfg.schema_version, 1);
        assert_eq!(cfg.graph.entry, "agent");
        assert_eq!(cfg.graph.nodes.len(), 4);
    }

    #[tokio::test]
    async fn namespaced_graph_id_with_dot_is_fetched_at_correct_url() {
        // graph_id of form "{worker}.graph" (as registered by store hand-off PR 3)
        // — '.' is valid in graph ids; only ':' is forbidden. The URL must
        // contain the literal dot, not a percent-encoded form.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/designer/agent-graphs/my-worker.graph"))
            .respond_with(ResponseTemplate::new(200).set_body_json(valid_graph_json()))
            .mount(&server)
            .await;

        let provider = HttpGraphProvider::new(server.uri(), "tok");
        let result = provider.graph_config(&tenant(), "my-worker.graph").await;
        assert!(result.is_ok(), "dotted graph_id must resolve: {:?}", result);
    }

    #[tokio::test]
    async fn maps_404_to_agent_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let provider = HttpGraphProvider::new(server.uri(), "gtc_live_x");
        let result = provider.graph_config(&tenant(), "ghost.graph").await;
        assert!(
            matches!(result, Err(ConfigError::AgentNotFound(_))),
            "404 must map to AgentNotFound: {result:?}"
        );
    }

    #[tokio::test]
    async fn maps_401_to_misconfigured() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let provider = HttpGraphProvider::new(server.uri(), "gtc_live_bad");
        let result = provider.graph_config(&tenant(), "triage.graph").await;
        assert!(
            matches!(result, Err(ConfigError::Misconfigured(_))),
            "401 must map to Misconfigured: {result:?}"
        );
    }

    #[tokio::test]
    async fn maps_403_to_misconfigured() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let provider = HttpGraphProvider::new(server.uri(), "gtc_live_bad");
        let result = provider.graph_config(&tenant(), "triage.graph").await;
        assert!(
            matches!(result, Err(ConfigError::Misconfigured(_))),
            "403 must map to Misconfigured: {result:?}"
        );
    }

    #[tokio::test]
    async fn maps_5xx_to_internal() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let provider = HttpGraphProvider::new(server.uri(), "gtc_live_x");
        let result = provider.graph_config(&tenant(), "triage.graph").await;
        assert!(
            matches!(result, Err(ConfigError::Internal(_))),
            "5xx must map to Internal: {result:?}"
        );
    }

    #[tokio::test]
    async fn maps_malformed_json_to_misconfigured() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{not json"))
            .mount(&server)
            .await;

        let provider = HttpGraphProvider::new(server.uri(), "gtc_live_x");
        let result = provider.graph_config(&tenant(), "triage.graph").await;
        assert!(
            matches!(result, Err(ConfigError::Misconfigured(_))),
            "invalid JSON body must map to Misconfigured: {result:?}"
        );
    }

    #[tokio::test]
    async fn maps_unsupported_schema_version_to_misconfigured() {
        let server = MockServer::start().await;
        let bad_doc = serde_json::json!({
            "schemaVersion": 99,
            "entry": "agent",
            "nodes": [
                {"id": "agent", "kind": "agent", "systemPrompt": "x", "model": "gpt-4o-mini", "tools": []},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [{"from": "agent", "to": "respond"}]
        });
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(bad_doc))
            .mount(&server)
            .await;

        let provider = HttpGraphProvider::new(server.uri(), "gtc_live_x");
        let result = provider.graph_config(&tenant(), "triage.graph").await;
        assert!(
            matches!(result, Err(ConfigError::Misconfigured(_))),
            "unsupported schemaVersion must map to Misconfigured: {result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // CachingGraphProvider tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn caching_provider_hits_inner_once_within_ttl() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(valid_graph_json()))
            .mount(&server)
            .await;

        let provider = CachingGraphProvider::new(HttpGraphProvider::new(server.uri(), "tok"));
        let tc = tenant();

        let _ = provider.graph_config(&tc, "g1").await.unwrap();
        let _ = provider.graph_config(&tc, "g1").await.unwrap();
        let _ = provider.graph_config(&tc, "g1").await.unwrap();

        // wiremock counts requests; only 1 should have reached the mock server.
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn caching_provider_expires_after_ttl() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(valid_graph_json()))
            .mount(&server)
            .await;

        let provider = CachingGraphProvider::with_ttl(
            HttpGraphProvider::new(server.uri(), "tok"),
            std::time::Duration::from_millis(50),
        );
        let tc = tenant();

        let _ = provider.graph_config(&tc, "g1").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let _ = provider.graph_config(&tc, "g1").await.unwrap();

        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn caching_provider_does_not_cache_errors() {
        // Errors must always hit the inner provider; a cached error would
        // permanently block a graph_id after a transient failure.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let provider = CachingGraphProvider::new(HttpGraphProvider::new(server.uri(), "tok"));
        let tc = tenant();

        let _ = provider.graph_config(&tc, "g1").await.unwrap_err();
        let _ = provider.graph_config(&tc, "g1").await.unwrap_err();

        // Both calls must have reached the server — errors are not stored.
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            2,
            "errors must not be cached; every error call hits the inner provider"
        );
    }

    #[tokio::test]
    async fn caching_provider_isolates_tenants() {
        // Two tenants with the same graph_id must receive independent cache
        // entries — serving tenant-A's graph to tenant-B would be a data leak.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(valid_graph_json()))
            .mount(&server)
            .await;

        let provider = CachingGraphProvider::new(HttpGraphProvider::new(server.uri(), "tok"));
        let tc_a = TenantContext::new("tenant-a", "prod");
        let tc_b = TenantContext::new("tenant-b", "prod");

        let _ = provider.graph_config(&tc_a, "g1").await.unwrap();
        let _ = provider.graph_config(&tc_b, "g1").await.unwrap();

        // Each tenant's first request must hit the inner provider independently.
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            2,
            "separate tenants must not share a cache entry for the same graph_id"
        );
    }

    #[tokio::test]
    async fn a_graph_id_cannot_steer_the_request_path() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let provider = HttpGraphProvider::new(server.uri(), "tok");
        let tenant = TenantContext::new("t", "e");
        let _ = provider
            .graph_config(&tenant, "../tenant/me/mcp-servers")
            .await;
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests[0].url.path(),
            "/api/v1/designer/agent-graphs/..%2Ftenant%2Fme%2Fmcp-servers"
        );
    }
}
