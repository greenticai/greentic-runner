//! Billing-metering sink: fire-and-forget emit of live-worker LLM token usage
//! to the cloud-commerce metering API. Mirrors the `TokenMeter` pattern; the
//! default impl is a no-op so the runtime works without billing configured.
//!
//! Activation: set both `GREENTIC_BILLING_BASE_URL` and
//! `GREENTIC_BILLING_SERVICE_SECRET` in the environment; the runner host calls
//! [`HttpBillingMeter::from_env`] and installs it via
//! [`crate::AgentRuntime::with_billing_meter`].
//!
//! A deployed env-canvas unit instead records its usage at the admin's
//! per-unit ingest door through [`WorkerUsageMeter`], which the embedding host
//! constructs from the unit's staged metering block (see [`worker_usage`]).
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::tenant::TenantContext;

pub mod worker_usage;
pub use worker_usage::{WorkerUsageError, WorkerUsageMeter, WorkerUsageTarget};

const BUDGET_TTL: Duration = Duration::from_secs(30);

/// Errors the billing sink can return.
///
/// In practice both current implementations always return `Ok(())`; the error
/// variant exists so future implementations can propagate transport failures
/// without a breaking API change.
#[derive(Debug, thiserror::Error)]
pub enum BillingError {
    #[error("billing transport error: {0}")]
    Transport(String),
}

/// Dyn-safe billing sink (parallels `TokenMeter`).
///
/// Implementations MUST be fire-and-forget: `emit` should return as quickly as
/// possible and never block the agent step on the billing outcome. The
/// [`HttpBillingMeter`] achieves this by spawning the HTTP POST and returning
/// `Ok(())` immediately.
///
/// `over_budget` is a synchronous pre-call gate: it returns `true` ONLY when
/// cloud-commerce definitively reports `available <= 0`. Any error, timeout, or
/// parse failure must return `false` (fail-open) so a billing outage never
/// halts real work. Implementations should cache the result per tenant with a
/// short TTL to avoid a round-trip on every LLM call.
///
/// `model` is the LLM model id the call was made against, taken from the
/// agent's `LlmProviderRef` at the call site. It is part of the cross-emitter
/// metadata contract (see [`build_worker_batch`]) — cloud-commerce cannot
/// group worker spend by model unless every emitter reports it.
pub trait BillingMeter: Send + Sync {
    fn emit<'a>(
        &'a self,
        tenant: &'a TenantContext,
        input_tokens: u64,
        output_tokens: u64,
        agent_id: &'a str,
        model: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), BillingError>> + Send + 'a>>;

    fn over_budget<'a>(
        &'a self,
        tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
}

/// Returns `true` only when cloud-commerce DEFINITIVELY reports no credits.
/// Pure helper so it can be unit-tested without HTTP.
pub(crate) fn decide_over_budget(available: i64) -> bool {
    available <= 0
}

/// Default: drop everything — billing disabled. Used when
/// `GREENTIC_BILLING_BASE_URL` / `GREENTIC_BILLING_SERVICE_SECRET` are unset.
pub struct NoopBillingMeter;

impl BillingMeter for NoopBillingMeter {
    fn emit<'a>(
        &'a self,
        _tenant: &'a TenantContext,
        _input_tokens: u64,
        _output_tokens: u64,
        _agent_id: &'a str,
        _model: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), BillingError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn over_budget<'a>(
        &'a self,
        _tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(std::future::ready(false))
    }
}

// ---------------------------------------------------------------------------
// Batch builder (used by HttpBillingMeter; pub(crate) for unit tests)
// ---------------------------------------------------------------------------

const PRODUCT_ID: &str = "greentic-worker";
type QuantityFn = fn(u64, u64) -> u64;
const METERS: [(&str, QuantityFn); 3] = [
    ("llm_input_tokens", |i, _o| i),
    ("llm_output_tokens", |_i, o| o),
    ("llm_total_tokens", |i, o| i.saturating_add(o)),
];

/// One metering event in the POST body.
#[derive(Debug, Serialize)]
pub(crate) struct WorkerMeteringEvent {
    pub(crate) tenant_id: String,
    pub(crate) product_id: String,
    pub(crate) meter: String,
    pub(crate) quantity: u64,
    pub(crate) unit: String,
    pub(crate) idempotency_key: String,
    pub(crate) timestamp: String,
    pub(crate) metadata: serde_json::Value,
    pub(crate) rating_mode: String,
}

/// Batch payload sent to `/v1/metering/events/batch`.
#[derive(Debug, Serialize)]
pub(crate) struct WorkerMeteringBatch {
    pub(crate) events: Vec<WorkerMeteringEvent>,
}

/// Build the three-event (input / output / total tokens) metering batch for
/// one agent LLM iteration. Pure function; used by [`HttpBillingMeter::emit`]
/// and directly in unit tests.
///
/// # Metadata contract
///
/// cloud-commerce (greentic-billing) groups usage by whatever metadata keys a
/// `?group_by=` query names, so a dimension only exists in reporting if every
/// emitter writes the same key. These keys are shared with the
/// greentic-designer emitter (which meters authoring-time LLM calls):
///
/// | key          | worker value                       | designer value      |
/// |--------------|------------------------------------|---------------------|
/// | `model`      | model id of the call               | model id of the call|
/// | `env_id`     | deployed environment id            | `"design-time"`     |
/// | `project_id` | pack identity, omitted if unknown  | pack identity       |
/// | `surface`    | `"worker"`                         | LLM role name       |
/// | `user_email` | session user, omitted if unknown   | authoring user      |
///
/// `project_id` is the PACK identity — [`TenantContext::project_id`], which the
/// host fills from the deployed revision's `bundle_id`. That is the same value
/// greentic-designer records as `pack_name` in `published_workers`, so a
/// product's authoring spend and its runtime spend land on the same project row.
///
/// It is deliberately NOT the agent id. An agent id is the key inside
/// `manifest.agents` and is not unique across packs, so two packs each shipping
/// an `assistant` would collapse into one bogus project with summed credits.
/// When the pack identity is unknown the key is OMITTED rather than filled with
/// a fallback: cloud-commerce groups a missing key under `"unknown"`, which is
/// honest, whereas any fallback silently reintroduces that collision.
///
/// `agent_id` and `source` are unchanged and still carried alongside —
/// `agent_id` remains the right answer to "which agent ran".
pub(crate) fn build_worker_batch(
    tenant: &TenantContext,
    input_tokens: u64,
    output_tokens: u64,
    agent_id: &str,
    model: &str,
    idem_base: &str,
) -> WorkerMeteringBatch {
    let timestamp = chrono::Utc::now().to_rfc3339();
    let mut metadata = serde_json::json!({
        "model": model,
        "env_id": tenant.env_id,
        "surface": "worker",
        "agent_id": agent_id,
        "source": "worker"
    });
    // Both optional dimensions are omitted rather than nulled or defaulted when
    // unknown: the contract says they may be absent, a null would create a
    // bogus grouping bucket, and for `project_id` a fallback to `agent_id`
    // would collapse same-named agents from different packs into one project.
    if let Some(map) = metadata.as_object_mut() {
        if let Some(email) = tenant.user_email.as_deref() {
            map.insert("user_email".to_string(), email.into());
        }
        if let Some(project_id) = tenant.project_id.as_deref() {
            map.insert("project_id".to_string(), project_id.into());
        }
    }
    let events = METERS
        .iter()
        .map(|(meter, quantity_fn)| WorkerMeteringEvent {
            tenant_id: tenant.tenant_id.clone(),
            product_id: PRODUCT_ID.to_string(),
            meter: (*meter).to_string(),
            quantity: quantity_fn(input_tokens, output_tokens),
            unit: "token".to_string(),
            idempotency_key: format!("{idem_base}:{meter}"),
            timestamp: timestamp.clone(),
            metadata: metadata.clone(),
            rating_mode: "ingest_only".to_string(),
        })
        .collect();
    WorkerMeteringBatch { events }
}

// ---------------------------------------------------------------------------
// HTTP sink
// ---------------------------------------------------------------------------

/// HTTP sink → cloud-commerce `/v1/metering/events/batch`.
///
/// Fire-and-forget: the POST is spawned on a new Tokio task so `emit` returns
/// `Ok(())` immediately and never adds latency or blocks the agent step on a
/// billing outcome. Transport errors are logged at `WARN` level and swallowed.
///
/// `over_budget` performs a synchronous GET to `/v1/tenants/{id}/wallet` and
/// caches the result per tenant for [`BUDGET_TTL`]. Any error returns `false`
/// (fail-open). The same HTTP client (5s timeout) bounds blocking time.
pub struct HttpBillingMeter {
    base_url: String,
    bearer: String,
    http: reqwest::Client,
    /// Per-tenant TTL cache: tenant_id → (checked_at, is_over_budget).
    over_budget_cache: Mutex<HashMap<String, (Instant, bool)>>,
}

impl HttpBillingMeter {
    /// Return `None` (use Noop) when either env var is unset or blank.
    ///
    /// Reads:
    /// - `GREENTIC_BILLING_BASE_URL` — base URL of the cloud-commerce service
    /// - `GREENTIC_BILLING_SERVICE_SECRET` — bearer token for service auth
    pub fn from_env() -> Option<Self> {
        let base_url = std::env::var("GREENTIC_BILLING_BASE_URL").ok()?;
        let bearer = std::env::var("GREENTIC_BILLING_SERVICE_SECRET").ok()?;
        if base_url.trim().is_empty() || bearer.trim().is_empty() {
            return None;
        }
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .ok()?;
        Some(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            bearer,
            http,
            over_budget_cache: Mutex::new(HashMap::new()),
        })
    }

    /// GET `{base}/v1/tenants/{tenant_id}/wallet` and parse the `available`
    /// field (string) to i64. Returns `None` on any error (fail-open).
    async fn fetch_available(&self, tenant_id: &str) -> Option<i64> {
        let url = format!("{}/v1/tenants/{tenant_id}/wallet", self.base_url);
        let res = self
            .http
            .get(&url)
            .header("x-greentic-service-auth", format!("Bearer {}", self.bearer))
            .send()
            .await
            .ok()?;
        if !res.status().is_success() {
            return None;
        }
        let v: serde_json::Value = res.json().await.ok()?;
        v.get("available")?.as_str()?.parse::<i64>().ok()
    }
}

impl BillingMeter for HttpBillingMeter {
    fn emit<'a>(
        &'a self,
        tenant: &'a TenantContext,
        input_tokens: u64,
        output_tokens: u64,
        agent_id: &'a str,
        model: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), BillingError>> + Send + 'a>> {
        // Build idempotency key and batch synchronously so we own all data
        // before spawning — the spawned future must be 'static.
        let idem = uuid::Uuid::new_v4().to_string();
        let batch = build_worker_batch(tenant, input_tokens, output_tokens, agent_id, model, &idem);
        let url = format!("{}/v1/metering/events/batch", self.base_url);
        let http = self.http.clone();
        let bearer = self.bearer.clone();

        // Detach: spawn independently; never block the agent step on billing.
        tokio::spawn(async move {
            let res = http
                .post(&url)
                .header("x-greentic-service-auth", format!("Bearer {bearer}"))
                .header("x-greentic-metering-source", "worker")
                .json(&batch)
                .send()
                .await;
            match res {
                Ok(r) if r.status().is_success() => {}
                Ok(r) => tracing::warn!(
                    status = %r.status(),
                    "worker metering rejected; continuing"
                ),
                Err(e) => tracing::warn!(
                    error = %e,
                    "worker metering unreachable; continuing"
                ),
            }
        });

        // Return immediately — the agent step is never blocked on billing.
        Box::pin(async { Ok(()) })
    }

    fn over_budget<'a>(
        &'a self,
        tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            let tenant_id = &tenant.tenant_id;

            // Check cache first; guard dropped before any await point.
            {
                if let Ok(guard) = self.over_budget_cache.lock()
                    && let Some(&(at, over)) = guard.get(tenant_id.as_str())
                    && at.elapsed() < BUDGET_TTL
                {
                    return over;
                }
            }

            // Fetch wallet balance; fail-open on any error.
            let over = match self.fetch_available(tenant_id).await {
                Some(available) => decide_over_budget(available),
                None => false,
            };

            if let Ok(mut guard) = self.over_budget_cache.lock() {
                guard.insert(tenant_id.clone(), (Instant::now(), over));
            }
            over
        })
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]
mod tests {
    use super::*;
    use crate::tenant::TenantContext;

    #[tokio::test]
    async fn noop_meter_is_ok() {
        let m = NoopBillingMeter;
        let t = TenantContext::new("acme", "prod");
        assert!(m.emit(&t, 10, 5, "agent-1", "gpt-4o-mini").await.is_ok());
    }

    #[tokio::test]
    async fn noop_over_budget_is_always_false() {
        let m = NoopBillingMeter;
        let t = TenantContext::new("acme", "prod");
        assert!(!m.over_budget(&t).await);
    }

    #[test]
    fn decide_over_budget_positive_is_false() {
        assert!(!decide_over_budget(1));
        assert!(!decide_over_budget(100));
    }

    #[test]
    fn decide_over_budget_zero_is_true() {
        assert!(decide_over_budget(0));
    }

    #[test]
    fn decide_over_budget_negative_is_true() {
        assert!(decide_over_budget(-1));
        assert!(decide_over_budget(-999));
    }

    #[test]
    fn builds_three_worker_token_events() {
        let tenant = TenantContext::new("acme", "prod");
        let batch = build_worker_batch(&tenant, 10, 5, "agent-1", "gpt-4o-mini", "idem-1");
        assert_eq!(batch.events.len(), 3);
        let by = |m: &str| batch.events.iter().find(|e| e.meter == m).unwrap();
        assert_eq!(by("llm_input_tokens").quantity, 10);
        assert_eq!(by("llm_output_tokens").quantity, 5);
        assert_eq!(by("llm_total_tokens").quantity, 15);
        assert_eq!(by("llm_input_tokens").product_id, "greentic-worker");
        assert_eq!(
            by("llm_input_tokens").idempotency_key,
            "idem-1:llm_input_tokens"
        );
        assert_eq!(by("llm_input_tokens").metadata["agent_id"], "agent-1");
        assert_eq!(by("llm_input_tokens").metadata["env_id"], "prod");
    }

    #[test]
    fn builds_zero_token_batch() {
        let tenant = TenantContext::new("t", "e");
        let batch = build_worker_batch(&tenant, 0, 0, "a", "m", "base");
        let by = |m: &str| batch.events.iter().find(|e| e.meter == m).unwrap();
        assert_eq!(by("llm_input_tokens").quantity, 0);
        assert_eq!(by("llm_output_tokens").quantity, 0);
        assert_eq!(by("llm_total_tokens").quantity, 0);
    }

    // ── Cross-emitter metadata contract (shared with greentic-designer) ─────
    //
    // cloud-commerce groups by arbitrary metadata keys, so a `?group_by=model`
    // report only sees worker spend if these exact key names are emitted.

    #[test]
    fn metadata_carries_model_from_the_llm_call_site() {
        let tenant = TenantContext::new("acme", "prod");
        let batch = build_worker_batch(&tenant, 10, 5, "agent-1", "claude-3-haiku", "idem-1");
        for event in &batch.events {
            assert_eq!(event.metadata["model"], "claude-3-haiku");
        }
    }

    #[test]
    fn metadata_carries_project_id_from_the_pack_identity() {
        let tenant =
            TenantContext::new("acme", "prod").with_project_id(Some("customer.support".into()));
        let batch = build_worker_batch(&tenant, 10, 5, "agent-1", "m", "idem-1");
        for event in &batch.events {
            assert_eq!(event.metadata["project_id"], "customer.support");
            // The in-pack agent id is a DIFFERENT dimension and must not leak
            // into project_id — that conflation is the bug this test pins shut.
            assert_eq!(event.metadata["agent_id"], "agent-1");
        }
    }

    #[test]
    fn metadata_omits_project_id_when_pack_identity_is_unknown() {
        // Omitted, never a fallback to agent_id and never a placeholder string:
        // cloud-commerce groups a missing key under "unknown", which is the
        // honest answer. A fallback would reintroduce the cross-pack collision.
        let tenant = TenantContext::new("acme", "prod");
        let batch = build_worker_batch(&tenant, 10, 5, "agent-1", "m", "idem-1");
        for event in &batch.events {
            assert!(
                event.metadata.get("project_id").is_none(),
                "project_id must be ABSENT when the pack identity is unknown, got {:?}",
                event.metadata.get("project_id")
            );
        }
    }

    #[test]
    fn two_packs_sharing_an_agent_id_get_distinct_project_ids() {
        // Regression: before the pack identity existed, both packs emitted
        // `project_id = "assistant"` and cloud-commerce summed their credits
        // into one bogus project row.
        let shared_agent_id = "assistant";
        let sales = TenantContext::new("acme", "prod").with_project_id(Some("sales.bot".into()));
        let support =
            TenantContext::new("acme", "prod").with_project_id(Some("support.bot".into()));

        let sales_batch = build_worker_batch(&sales, 10, 5, shared_agent_id, "m", "idem-1");
        let support_batch = build_worker_batch(&support, 10, 5, shared_agent_id, "m", "idem-2");

        assert_eq!(sales_batch.events[0].metadata["project_id"], "sales.bot");
        assert_eq!(
            support_batch.events[0].metadata["project_id"],
            "support.bot"
        );
        assert_ne!(
            sales_batch.events[0].metadata["project_id"],
            support_batch.events[0].metadata["project_id"],
            "same in-pack agent id in two packs must NOT collapse to one project"
        );
        // ...while `agent_id` still answers "which agent ran" identically.
        assert_eq!(
            sales_batch.events[0].metadata["agent_id"],
            support_batch.events[0].metadata["agent_id"]
        );
    }

    #[test]
    fn metadata_carries_worker_surface() {
        let tenant = TenantContext::new("acme", "prod");
        let batch = build_worker_batch(&tenant, 10, 5, "agent-1", "m", "idem-1");
        for event in &batch.events {
            assert_eq!(event.metadata["surface"], "worker");
        }
    }

    #[test]
    fn metadata_carries_user_email_when_tenant_context_has_one() {
        let tenant = TenantContext::new("acme", "prod").with_user_email(Some("u@x.com".into()));
        let batch = build_worker_batch(&tenant, 10, 5, "agent-1", "m", "idem-1");
        for event in &batch.events {
            assert_eq!(event.metadata["user_email"], "u@x.com");
        }
    }

    #[test]
    fn metadata_omits_user_email_when_unknown() {
        let tenant = TenantContext::new("acme", "prod");
        let batch = build_worker_batch(&tenant, 10, 5, "agent-1", "m", "idem-1");
        for event in &batch.events {
            assert!(
                event.metadata.get("user_email").is_none(),
                "user_email must be absent, not null, when not known"
            );
        }
    }

    #[test]
    fn metadata_keeps_pre_existing_agent_id_and_source_keys() {
        // Additive only: existing consumers may already read these.
        let tenant = TenantContext::new("acme", "prod");
        let batch = build_worker_batch(&tenant, 10, 5, "agent-1", "m", "idem-1");
        for event in &batch.events {
            assert_eq!(event.metadata["agent_id"], "agent-1");
            assert_eq!(event.metadata["source"], "worker");
            assert_eq!(event.metadata["env_id"], "prod");
        }
    }

    #[test]
    fn rating_mode_stays_ingest_only() {
        // Out of scope for the dimensions work; asserted so a future change
        // to worker rating is a deliberate, visible decision.
        let tenant = TenantContext::new("acme", "prod");
        let batch = build_worker_batch(&tenant, 10, 5, "agent-1", "m", "idem-1");
        for event in &batch.events {
            assert_eq!(event.rating_mode, "ingest_only");
        }
    }

    // ── HttpBillingMeter tests ──────────────────────────────────────────────

    #[tokio::test]
    #[serial_test::serial]
    async fn http_billing_meter_from_env_returns_none_when_unset() {
        unsafe {
            std::env::remove_var("GREENTIC_BILLING_BASE_URL");
            std::env::remove_var("GREENTIC_BILLING_SERVICE_SECRET");
        }
        assert!(HttpBillingMeter::from_env().is_none());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn http_billing_meter_from_env_returns_none_when_blank() {
        unsafe {
            std::env::set_var("GREENTIC_BILLING_BASE_URL", "  ");
            std::env::set_var("GREENTIC_BILLING_SERVICE_SECRET", "secret");
        }
        assert!(HttpBillingMeter::from_env().is_none());
        unsafe {
            std::env::remove_var("GREENTIC_BILLING_BASE_URL");
            std::env::remove_var("GREENTIC_BILLING_SERVICE_SECRET");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn http_billing_meter_from_env_returns_some_when_configured() {
        unsafe {
            std::env::set_var("GREENTIC_BILLING_BASE_URL", "http://127.0.0.1:19999");
            std::env::set_var("GREENTIC_BILLING_SERVICE_SECRET", "secret");
        }
        assert!(HttpBillingMeter::from_env().is_some());
        unsafe {
            std::env::remove_var("GREENTIC_BILLING_BASE_URL");
            std::env::remove_var("GREENTIC_BILLING_SERVICE_SECRET");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn http_billing_meter_emit_fires_and_forgets() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/v1/metering/events/batch"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .mount(&server)
            .await;

        unsafe {
            std::env::set_var("GREENTIC_BILLING_BASE_URL", server.uri());
            std::env::set_var("GREENTIC_BILLING_SERVICE_SECRET", "secret");
        }
        let meter = HttpBillingMeter::from_env().unwrap();
        let t = TenantContext::new("acme", "prod");
        let result = meter.emit(&t, 100, 50, "agent-1", "gpt-4o-mini").await;
        assert!(result.is_ok());
        // Give the spawned fire-and-forget task time to deliver.
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        unsafe {
            std::env::remove_var("GREENTIC_BILLING_BASE_URL");
            std::env::remove_var("GREENTIC_BILLING_SERVICE_SECRET");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn http_billing_meter_over_budget_fail_open_on_unreachable() {
        // Point at a port that is not listening; fail-open must return false.
        unsafe {
            std::env::set_var("GREENTIC_BILLING_BASE_URL", "http://127.0.0.1:1");
            std::env::set_var("GREENTIC_BILLING_SERVICE_SECRET", "secret");
        }
        let meter = HttpBillingMeter::from_env().unwrap();
        let t = TenantContext::new("acme", "prod");
        assert!(
            !meter.over_budget(&t).await,
            "must fail-open when server is unreachable"
        );

        unsafe {
            std::env::remove_var("GREENTIC_BILLING_BASE_URL");
            std::env::remove_var("GREENTIC_BILLING_SERVICE_SECRET");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn http_billing_meter_over_budget_positive_available() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/v1/tenants/acme/wallet"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"available": "100"})),
            )
            .mount(&server)
            .await;

        unsafe {
            std::env::set_var("GREENTIC_BILLING_BASE_URL", server.uri());
            std::env::set_var("GREENTIC_BILLING_SERVICE_SECRET", "secret");
        }
        let meter = HttpBillingMeter::from_env().unwrap();
        let t = TenantContext::new("acme", "prod");
        assert!(
            !meter.over_budget(&t).await,
            "available=100 must not be over budget"
        );

        unsafe {
            std::env::remove_var("GREENTIC_BILLING_BASE_URL");
            std::env::remove_var("GREENTIC_BILLING_SERVICE_SECRET");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn http_billing_meter_over_budget_zero_available() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/v1/tenants/acme/wallet"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"available": "0"})),
            )
            .mount(&server)
            .await;

        unsafe {
            std::env::set_var("GREENTIC_BILLING_BASE_URL", server.uri());
            std::env::set_var("GREENTIC_BILLING_SERVICE_SECRET", "secret");
        }
        let meter = HttpBillingMeter::from_env().unwrap();
        let t = TenantContext::new("acme", "prod");
        assert!(
            meter.over_budget(&t).await,
            "available=0 must be over budget"
        );

        unsafe {
            std::env::remove_var("GREENTIC_BILLING_BASE_URL");
            std::env::remove_var("GREENTIC_BILLING_SERVICE_SECRET");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn http_billing_meter_over_budget_caches_result() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/v1/tenants/acme/wallet"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"available": "500"})),
            )
            .expect(1) // Only one HTTP call; second is served from cache.
            .mount(&server)
            .await;

        unsafe {
            std::env::set_var("GREENTIC_BILLING_BASE_URL", server.uri());
            std::env::set_var("GREENTIC_BILLING_SERVICE_SECRET", "secret");
        }
        let meter = HttpBillingMeter::from_env().unwrap();
        let t = TenantContext::new("acme", "prod");
        assert!(!meter.over_budget(&t).await);
        // Second call within BUDGET_TTL must hit cache (no extra HTTP request).
        assert!(!meter.over_budget(&t).await);

        unsafe {
            std::env::remove_var("GREENTIC_BILLING_BASE_URL");
            std::env::remove_var("GREENTIC_BILLING_SERVICE_SECRET");
        }
    }
}
