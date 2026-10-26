//! [`GuardrailPolicy`] that fetches the resolved mandatory guardrail list from
//! greentic-admin (`GET /api/v1/designer/guardrail-policy?env=`) per tenant+env,
//! with a 60s TTL cache. Serves the last-known list on transient admin failure
//! (fail-safe); returns `Unavailable` only when there is no cached entry (cold).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::config::GuardrailRef;
use crate::guardrail::{GuardrailPolicy, GuardrailPolicyError};
use crate::tenant::TenantContext;

const DEFAULT_TTL: Duration = Duration::from_secs(60);

#[derive(serde::Deserialize)]
struct PolicyResp {
    guardrails: Vec<GuardrailRef>,
}

/// Cache keyed by `(tenant_id, env_id)` → (fetched-at, resolved guardrails).
type PolicyCache = HashMap<(String, String), (Instant, Vec<GuardrailRef>)>;

/// Fetches mandatory guardrails from the admin, cached per `(tenant_id, env_id)`.
pub struct HttpGuardrailPolicy {
    base_url: String,
    token: String,
    client: reqwest::Client,
    ttl: Duration,
    cache: Mutex<PolicyCache>,
}

impl HttpGuardrailPolicy {
    /// `base_url` is the admin origin (no trailing slash needed); `token` is a
    /// tenant `gtc_live_*` key. Production 60s TTL.
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        Self::with_ttl(base_url, token, DEFAULT_TTL)
    }

    /// Custom TTL (tests).
    pub fn with_ttl(base_url: impl Into<String>, token: impl Into<String>, ttl: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            client,
            ttl,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Cached entry younger than the TTL, if any. Lock is dropped before return.
    fn cached_fresh(&self, key: &(String, String)) -> Option<Vec<GuardrailRef>> {
        let guard = self.cache.lock().ok()?;
        let (at, refs) = guard.get(key)?;
        (at.elapsed() < self.ttl).then(|| refs.clone())
    }

    /// Any cached entry regardless of age (for serve-stale on fetch failure).
    fn cached_any(&self, key: &(String, String)) -> Option<Vec<GuardrailRef>> {
        let guard = self.cache.lock().ok()?;
        guard.get(key).map(|(_, refs)| refs.clone())
    }

    fn store(&self, key: (String, String), refs: Vec<GuardrailRef>) {
        if let Ok(mut guard) = self.cache.lock() {
            guard.insert(key, (Instant::now(), refs));
        }
    }

    /// One HTTP fetch. Never holds the cache lock.
    async fn fetch(&self, env: &str) -> Result<Vec<GuardrailRef>, String> {
        let url = format!("{}/api/v1/designer/guardrail-policy", self.base_url);
        let resp = self
            .client
            .get(&url)
            .query(&[("env", env)])
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e.without_url()))?;
        match resp.status().as_u16() {
            200 => resp
                .json::<PolicyResp>()
                .await
                .map(|p| p.guardrails)
                .map_err(|e| format!("decode: {e}")),
            other => Err(format!("status {other}")),
        }
    }
}

impl GuardrailPolicy for HttpGuardrailPolicy {
    fn mandatory_guardrails<'a>(
        &'a self,
        tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GuardrailRef>, GuardrailPolicyError>> + Send + 'a>>
    {
        Box::pin(async move {
            let key = (tenant.tenant_id.clone(), tenant.env_id.clone());
            if let Some(fresh) = self.cached_fresh(&key) {
                return Ok(fresh);
            }
            match self.fetch(&key.1).await {
                Ok(refs) => {
                    self.store(key, refs.clone());
                    Ok(refs)
                }
                Err(reason) => match self.cached_any(&key) {
                    Some(stale) => {
                        tracing::warn!(error = %reason, "guardrail policy fetch failed; serving stale");
                        Ok(stale)
                    }
                    None => Err(GuardrailPolicyError::Unavailable(reason)),
                },
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn body(caps: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "guardrails": caps.iter().map(|c| serde_json::json!({
                "cap_id": c, "config": {}
            })).collect::<Vec<_>>()
        })
    }

    #[tokio::test]
    async fn fetches_resolved_guardrails() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/designer/guardrail-policy"))
            .and(query_param("env", "prod"))
            .and(header("authorization", "Bearer gtc_live_x"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(body(&["greentic:guardrail/pii"])),
            )
            .mount(&server)
            .await;
        let p = HttpGuardrailPolicy::new(server.uri(), "gtc_live_x");
        let t = TenantContext::new("acme", "prod");
        let got = p.mandatory_guardrails(&t).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].cap_id, "greentic:guardrail/pii");
    }

    #[tokio::test]
    async fn empty_policy_is_ok_empty() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body(&[])))
            .mount(&server)
            .await;
        let p = HttpGuardrailPolicy::new(server.uri(), "gtc_live_x");
        let t = TenantContext::new("acme", "prod");
        assert!(p.mandatory_guardrails(&t).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cache_hit_skips_second_http() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(body(&["greentic:guardrail/pii"])),
            )
            .expect(1)
            .mount(&server)
            .await;
        let p = HttpGuardrailPolicy::new(server.uri(), "gtc_live_x");
        let t = TenantContext::new("acme", "prod");
        p.mandatory_guardrails(&t).await.unwrap();
        p.mandatory_guardrails(&t).await.unwrap();
        // server's .expect(1) is verified on drop
    }

    #[tokio::test]
    async fn cold_failure_is_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let p = HttpGuardrailPolicy::new(server.uri(), "gtc_live_x");
        let t = TenantContext::new("acme", "prod");
        assert!(matches!(
            p.mandatory_guardrails(&t).await,
            Err(GuardrailPolicyError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn transient_failure_serves_stale() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(body(&["greentic:guardrail/pii"])),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let p = HttpGuardrailPolicy::with_ttl(server.uri(), "gtc_live_x", Duration::from_secs(0));
        let t = TenantContext::new("acme", "prod");
        let first = p.mandatory_guardrails(&t).await.unwrap();
        assert_eq!(first.len(), 1);
        let stale = p.mandatory_guardrails(&t).await.unwrap();
        assert_eq!(stale, first);
    }

    #[tokio::test]
    async fn malformed_body_cold_is_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;
        let p = HttpGuardrailPolicy::new(server.uri(), "gtc_live_x");
        let t = TenantContext::new("acme", "prod");
        assert!(matches!(
            p.mandatory_guardrails(&t).await,
            Err(GuardrailPolicyError::Unavailable(_))
        ));
    }
}
