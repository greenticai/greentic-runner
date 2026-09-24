//! [`WorkerUsageMeter`]: a [`BillingMeter`] that records a DEPLOYED unit's
//! LLM token usage at the admin's per-unit ingest door.
//!
//! Wire contract: greentic-designer
//! `docs/superpowers/specs/2026-09-24-env-canvas-unit-monitoring-phase-2-design.md`
//! §4.1 — `POST {admin}/api/v1/ingest/worker-usage`, bearer = the unit's
//! worker-usage token, one event per LLM iteration, `surface: "turn"`, plus an
//! optional `model`. The body is the one greentic-start's interop reporter
//! already sends (`src/interop/metering/event.rs`), so both producers speak one
//! shape: a ULID `event_id` (the admin's primary key and idempotency key), an
//! RFC 3339 `occurred_at` with a literal `Z`, and no content of any kind.
//!
//! # Recording only
//!
//! The admin door has no wallet, so [`BillingMeter::over_budget`] always
//! answers `false` and never makes a request. Nothing here charges anyone.
//!
//! # Why every attribution field comes from the constructor
//!
//! The token pins `(workspace tenant, canvas env, unit)` on the admin side,
//! and the admin refuses a body whose `tenant_slug` disagrees with it. The
//! runtime's own [`TenantContext`] carries the runtime tenant (`"default"` in
//! every remote deployment, load-bearing for `secrets://` addressing) and the
//! runtime env (`"local"`), neither of which names the workspace. So the meter
//! is built per unit by the embedding host with the values that DO. It reads
//! nothing from the environment, and ignores the `TenantContext` it is handed.
//!
//! # Failure
//!
//! Fire-and-forget: `emit` spawns the POST and returns `Ok(())`. A failed
//! delivery is dropped, never retried in place, and never fails the turn. What
//! it does instead is stop asking for a while — a refused token (`401`/`403`)
//! suspends the endpoint for [`AUTH_SUSPENSION`], a `429` for its
//! `Retry-After` (capped), a transport error or `5xx` for
//! [`TRANSIENT_SUSPENSION`] — so a dead or misconfigured admin costs one
//! request per window rather than one per LLM iteration. Each suspension is
//! one `warn`, which also reports how many events the previous window dropped.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;

use super::{BillingError, BillingMeter};
use crate::tenant::TenantContext;

/// The `surface` every event from this meter carries: a deployed LLM turn
/// that is not an interop (A2A/MCP) turn.
pub const TURN_SURFACE: &str = "turn";

/// Budget for DNS + address walk + TLS handshake. Mirrors greentic-start's
/// interop sink: a stalled address family must not look like a slow admin.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Budget for the whole POST. Strictly larger than [`CONNECT_TIMEOUT`] so a
/// connect fault is reported as one rather than as a generic timeout.
const POST_TIMEOUT: Duration = Duration::from_secs(15);

/// How long a refused token stops this meter asking. Neither `401` nor `403`
/// is fixed without a redeploy; the window expiring only lets a briefly
/// misconfigured admin recover without one.
const AUTH_SUSPENSION: Duration = Duration::from_secs(300);

/// Longest a `429`'s `Retry-After` is honoured, so a hostile or broken value
/// cannot switch metering off for the life of the process.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

/// `429` with no readable `Retry-After`: the admin's own limiter window.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(60);

/// Pause after a transport error or a `5xx`.
const TRANSIENT_SUSPENSION: Duration = Duration::from_secs(30);

/// Where and as whom a unit's usage is recorded. Every field is required and
/// is the value the embedding host resolved for this ONE unit — greentic-start
/// takes them from the unit's staged `metering {endpoint, token}` block plus
/// the revision it is loading.
#[derive(Clone)]
pub struct WorkerUsageTarget {
    /// The full ingest URL (`{admin}/api/v1/ingest/worker-usage`). Must be
    /// `https`, or `http` on a loopback host.
    pub endpoint: String,
    /// The unit's worker-usage bearer. Only ever sent as a header.
    pub token: String,
    /// The workspace tenant the token belongs to. The admin refuses a body
    /// naming any other.
    pub tenant_slug: String,
    /// The deployment the unit runs under.
    pub deployment_id: String,
    /// The deployed unit (the revision's `bundle_id`).
    pub bundle_id: String,
}

impl std::fmt::Debug for WorkerUsageTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerUsageTarget")
            .field("endpoint", &self.endpoint)
            .field("token", &"<redacted>")
            .field("tenant_slug", &self.tenant_slug)
            .field("deployment_id", &self.deployment_id)
            .field("bundle_id", &self.bundle_id)
            .finish()
    }
}

/// Why a [`WorkerUsageTarget`] could not configure a meter. The message names
/// the field or the endpoint, never the token.
#[derive(Debug, thiserror::Error)]
pub enum WorkerUsageError {
    #[error("worker usage metering needs a non-blank `{0}`")]
    Blank(&'static str),
    #[error(
        "worker usage endpoint `{0}` is not https and not loopback http; refusing to send \
         a bearer token in cleartext"
    )]
    UnsafeEndpoint(String),
    #[error("worker usage metering has no HTTP client: {0}")]
    Client(String),
}

/// One recorded LLM iteration, serialised exactly as the admin's ingest door
/// reads it. Identifiers and counters only — no field can hold message text.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct WorkerUsageEvent {
    pub(crate) event_id: String,
    pub(crate) occurred_at: String,
    pub(crate) tenant_slug: String,
    pub(crate) deployment_id: String,
    pub(crate) bundle_id: String,
    pub(crate) agent_id: String,
    pub(crate) surface: &'static str,
    pub(crate) tokens_in: u64,
    pub(crate) tokens_out: u64,
    /// Always `1`: this meter is called once per LLM iteration.
    pub(crate) iterations: u64,
    /// Always `0`: [`BillingMeter::emit`] is not told how long the call took,
    /// and the admin defaults an unmeasured duration to zero anyway.
    pub(crate) duration_ms: u64,
    /// Omitted, never empty, when unknown — the admin forwards an absent
    /// model as `"unknown"`, and an empty string would be a bucket of its own.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) model: Option<String>,
}

/// What one delivery attempt decided. `pub(crate)` for tests.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Delivery {
    /// `2xx`, including the admin's idempotent `stored: false` duplicate.
    Accepted,
    /// A `4xx` about this event (other than auth / rate limit). Dropped,
    /// nothing suspended.
    Rejected,
    /// The endpoint refused or could not be reached; it is now suspended.
    Failed,
    /// Dropped without a request because the endpoint is suspended.
    Suspended,
}

/// Records a deployed unit's LLM usage at the admin's ingest door. See the
/// module doc for the contract; construct with [`WorkerUsageMeter::new`] and
/// install with [`crate::AgentRuntime::with_billing_meter`] (or hand it to the
/// runner host's revision loader, which does).
pub struct WorkerUsageMeter {
    inner: std::sync::Arc<Inner>,
}

struct Inner {
    endpoint: String,
    token: SecretString,
    tenant_slug: String,
    deployment_id: String,
    bundle_id: String,
    http: reqwest::Client,
    suspended_until: Mutex<Option<Instant>>,
    dropped_while_suspended: AtomicU64,
}

impl std::fmt::Debug for WorkerUsageMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerUsageMeter")
            .field("endpoint", &self.inner.endpoint)
            .field("token", &"<redacted>")
            .field("tenant_slug", &self.inner.tenant_slug)
            .field("deployment_id", &self.inner.deployment_id)
            .field("bundle_id", &self.inner.bundle_id)
            .finish()
    }
}

fn required(field: &'static str, value: &str) -> Result<String, WorkerUsageError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(WorkerUsageError::Blank(field));
    }
    Ok(trimmed.to_string())
}

/// `https` anywhere, or `http` to a loopback host (which cannot leave the
/// machine). Same rule greentic-start applies to the staged block.
fn endpoint_is_safe(endpoint: &str) -> bool {
    let Ok(url) = url::Url::parse(endpoint) else {
        return false;
    };
    match url.scheme() {
        "https" => url.host().is_some(),
        "http" => match url.host() {
            Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            None => false,
        },
        _ => false,
    }
}

impl WorkerUsageMeter {
    /// Build a meter for one unit. Refuses a blank field and an endpoint that
    /// would carry the token in cleartext off this host.
    pub fn new(target: WorkerUsageTarget) -> Result<Self, WorkerUsageError> {
        let endpoint = required("endpoint", &target.endpoint)?;
        let token = required("token", &target.token)?;
        let tenant_slug = required("tenant_slug", &target.tenant_slug)?;
        let deployment_id = required("deployment_id", &target.deployment_id)?;
        let bundle_id = required("bundle_id", &target.bundle_id)?;
        if !endpoint_is_safe(&endpoint) {
            return Err(WorkerUsageError::UnsafeEndpoint(endpoint));
        }
        let http = reqwest::Client::builder()
            .timeout(POST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|err| WorkerUsageError::Client(err.to_string()))?;
        Ok(Self {
            inner: std::sync::Arc::new(Inner {
                endpoint,
                token: SecretString::from(token),
                tenant_slug,
                deployment_id,
                bundle_id,
                http,
                suspended_until: Mutex::new(None),
                dropped_while_suspended: AtomicU64::new(0),
            }),
        })
    }

    /// Build the event for one LLM iteration. Pure apart from the fresh id
    /// and timestamp.
    pub(crate) fn build_event(
        &self,
        tokens_in: u64,
        tokens_out: u64,
        agent_id: &str,
        model: &str,
    ) -> WorkerUsageEvent {
        let model = model.trim();
        WorkerUsageEvent {
            event_id: ulid::Ulid::new().to_string(),
            occurred_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            tenant_slug: self.inner.tenant_slug.clone(),
            deployment_id: self.inner.deployment_id.clone(),
            bundle_id: self.inner.bundle_id.clone(),
            agent_id: agent_id.to_string(),
            surface: TURN_SURFACE,
            tokens_in,
            tokens_out,
            iterations: 1,
            duration_ms: 0,
            model: (!model.is_empty()).then(|| model.to_string()),
        }
    }

    /// Deliver one event now. `emit` spawns the same delivery; tests call
    /// this to observe its outcome.
    #[cfg(test)]
    pub(crate) async fn deliver(&self, event: WorkerUsageEvent) -> Delivery {
        self.inner.deliver(event).await
    }
}

impl Inner {
    fn is_suspended(&self) -> bool {
        match self.suspended_until.lock() {
            Ok(guard) => guard.is_some_and(|until| Instant::now() < until),
            Err(_) => false,
        }
    }

    fn suspend(&self, window: Duration, reason: &str) {
        if let Ok(mut guard) = self.suspended_until.lock() {
            *guard = Some(Instant::now() + window);
        }
        let dropped = self.dropped_while_suspended.swap(0, Ordering::Relaxed);
        tracing::warn!(
            endpoint = %self.endpoint,
            bundle_id = %self.bundle_id,
            reason,
            suspended_secs = window.as_secs(),
            dropped_previous_window = dropped,
            "worker usage metering suspended; usage events are dropped until it resumes"
        );
    }

    async fn deliver(&self, event: WorkerUsageEvent) -> Delivery {
        if self.is_suspended() {
            self.dropped_while_suspended.fetch_add(1, Ordering::Relaxed);
            return Delivery::Suspended;
        }
        let response = self
            .http
            .post(&self.endpoint)
            // The ONE place the token is used: a header, never the URL or
            // the body, and never a log line.
            .bearer_auth(self.token.expose_secret())
            .json(&event)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(err) => {
                let kind = if err.is_timeout() {
                    "timeout"
                } else if err.is_connect() {
                    "connect failed"
                } else {
                    "transport error"
                };
                self.suspend(TRANSIENT_SUSPENSION, kind);
                return Delivery::Failed;
            }
        };
        let status = response.status();
        if status.is_success() {
            return Delivery::Accepted;
        }
        match status.as_u16() {
            401 | 403 => {
                self.suspend(AUTH_SUSPENSION, "token refused (401/403)");
                Delivery::Failed
            }
            429 => {
                let window = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.trim().parse::<u64>().ok())
                    .map(Duration::from_secs)
                    .unwrap_or(DEFAULT_RETRY_AFTER)
                    .min(MAX_RETRY_AFTER);
                self.suspend(window, "rate limited (429)");
                Delivery::Failed
            }
            code if (400..500).contains(&code) => {
                tracing::warn!(
                    endpoint = %self.endpoint,
                    status = code,
                    "worker usage event refused by the admin; dropped"
                );
                Delivery::Rejected
            }
            _ => {
                self.suspend(TRANSIENT_SUSPENSION, "admin answered 5xx");
                Delivery::Failed
            }
        }
    }
}

impl BillingMeter for WorkerUsageMeter {
    fn emit<'a>(
        &'a self,
        _tenant: &'a TenantContext,
        input_tokens: u64,
        output_tokens: u64,
        agent_id: &'a str,
        model: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), BillingError>> + Send + 'a>> {
        let event = self.build_event(input_tokens, output_tokens, agent_id, model);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let inner = std::sync::Arc::clone(&self.inner);
                handle.spawn(async move {
                    inner.deliver(event).await;
                });
            }
            Err(_) => tracing::warn!(
                bundle_id = %self.inner.bundle_id,
                "worker usage event dropped: emitted outside a tokio runtime"
            ),
        }
        Box::pin(async { Ok(()) })
    }

    fn over_budget<'a>(
        &'a self,
        _tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        // Recording only: the admin door has no wallet to consult.
        Box::pin(std::future::ready(false))
    }
}

#[cfg(test)]
#[path = "worker_usage_tests.rs"]
mod tests;
