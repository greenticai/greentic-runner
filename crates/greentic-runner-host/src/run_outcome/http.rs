//! [`HttpRunOutcomeSink`]: posts one run-outcome event per flow turn to the
//! admin's per-unit ingest door.
//!
//! Wire contract: greentic-designer
//! `docs/superpowers/specs/2026-09-25-deployed-run-audit-design.md` §3.1/§3.3 —
//! `POST {admin}/api/v1/ingest/run-outcome`, bearer = the unit's
//! worker-usage token (the SAME staged `metering` credential the
//! `WorkerUsageMeter` presents; the admin takes tenant, env and unit from it).
//!
//! # Body
//!
//! JSON, `snake_case`, matching the worker-usage body's convention:
//!
//! ```json
//! {
//!   "event_id": "01J…",            // ULID, admin idempotency key
//!   "occurred_at": "2026-09-25T10:00:00.000Z",
//!   "tenant_slug": "acme", "deployment_id": "…", "bundle_id": "…", "revision_id": "…",
//!   "run_id": "01J…", "flow_id": "main",
//!   "kind": "flow" | "agentic",
//!   "status": "in_progress" | "completed" | "technical_error" | "agentic",
//!   "last_step": "ask_name",        // omitted when unknown / agentic
//!   "user_ref": "webchat:u-1",      // omitted when unknown
//!   "user_verified": false,
//!   "channel": "webchat",           // omitted when unknown
//!   "outcome_json": { … },          // omitted; ≤ 4 KB when present
//!   "error_code": "timeout",        // technical_error only
//!   "started_at": "2026-09-25T09:59:00.000Z"
//! }
//! ```
//!
//! `tenant_slug` rides for parity with the worker-usage body; the admin must
//! still take the tenant from the token and never from here.
//!
//! # Failure
//!
//! Same policy as `greentic_aw_runtime::billing::WorkerUsageMeter`, restated
//! here rather than shared because that crate is an OPTIONAL dependency of
//! this one (behind `agentic-worker`) while a flow run is not: `record` spawns
//! the POST and returns; a delivery is dropped, never retried in place, and
//! never fails or delays the turn. `401`/`403` suspend the endpoint for
//! [`AUTH_SUSPENSION`], `429` for its `Retry-After` (capped), a transport
//! error or `5xx` for [`TRANSIENT_SUSPENSION`], and [`REJECTION_STREAK`]
//! consecutive `4xx` (an admin without this door) for a doubling backoff. At
//! most [`MAX_IN_FLIGHT`] POSTs run at once; past that an event is dropped
//! and counted.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde_json::Value;

use super::{RunKind, RunOutcome, RunOutcomeSink, RunStatus, now_rfc3339};

/// Budget for DNS + connect + TLS handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Budget for the whole POST; strictly larger than [`CONNECT_TIMEOUT`].
const POST_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a refused token (`401`/`403`) stops this sink asking.
const AUTH_SUSPENSION: Duration = Duration::from_secs(300);
/// Longest a `429`'s `Retry-After` is honoured.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);
/// `429` with no readable `Retry-After`.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(60);
/// Pause after a transport error or a `5xx`.
const TRANSIENT_SUSPENSION: Duration = Duration::from_secs(30);
/// Consecutive `4xx` after which the endpoint is suspended.
const REJECTION_STREAK: u32 = 5;
/// First suspension after a streak of `4xx`; doubles per further streak.
const REJECTION_BASE: Duration = Duration::from_secs(60);
/// Ceiling on the `4xx` backoff.
const REJECTION_MAX: Duration = Duration::from_secs(1800);

/// Longest value accepted in any id / label field (the admin ingest doors'
/// `MAX_FIELD_LEN`). An over-long REQUIRED id drops the event; an over-long
/// optional label is omitted rather than truncated into a different value.
pub(crate) const MAX_FIELD_BYTES: usize = 256;

/// Largest serialised `outcome_json` sent. A bigger one is omitted.
pub(crate) const MAX_OUTCOME_JSON_BYTES: usize = 4096;

/// Outcome POSTs in flight at once, per sink.
pub(crate) const MAX_IN_FLIGHT: usize = 32;

fn sampled(count: u64) -> bool {
    count == 1 || count.is_multiple_of(100)
}

/// Where and as whom a unit's run outcomes are recorded. Every field is
/// required and is the value the embedding host resolved for ONE unit.
#[derive(Clone)]
pub struct RunOutcomeTarget {
    /// Full ingest URL (`{admin}/api/v1/ingest/run-outcome`). Must be `https`,
    /// or `http` on a loopback host.
    pub endpoint: String,
    /// The unit's worker-usage bearer. Only ever sent as a header.
    pub token: String,
    /// The workspace tenant the token belongs to.
    pub tenant_slug: String,
    /// The deployment the unit runs under.
    pub deployment_id: String,
    /// The deployed unit (the revision's `bundle_id`).
    pub bundle_id: String,
    /// The revision being served.
    pub revision_id: String,
}

impl std::fmt::Debug for RunOutcomeTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunOutcomeTarget")
            .field("endpoint", &self.endpoint)
            .field("token", &"<redacted>")
            .field("tenant_slug", &self.tenant_slug)
            .field("deployment_id", &self.deployment_id)
            .field("bundle_id", &self.bundle_id)
            .field("revision_id", &self.revision_id)
            .finish()
    }
}

/// Why a [`RunOutcomeTarget`] could not configure a sink. Never names the token.
#[derive(Debug, thiserror::Error)]
pub enum RunOutcomeSinkError {
    #[error("run outcome sink needs a non-blank `{0}`")]
    Blank(&'static str),
    #[error("run outcome `{0}` is longer than the admin accepts ({MAX_FIELD_BYTES} bytes)")]
    TooLong(&'static str),
    #[error(
        "run outcome endpoint `{0}` is not https and not loopback http; refusing to send \
         a bearer token in cleartext"
    )]
    UnsafeEndpoint(String),
    #[error("run outcome sink has no HTTP client: {0}")]
    Client(String),
}

/// The wire body. Identifiers, a status and a class — no field holds message
/// text.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct RunOutcomeEvent {
    pub(crate) event_id: String,
    pub(crate) occurred_at: String,
    pub(crate) tenant_slug: String,
    pub(crate) deployment_id: String,
    pub(crate) bundle_id: String,
    pub(crate) revision_id: String,
    pub(crate) run_id: String,
    pub(crate) flow_id: String,
    pub(crate) kind: RunKind,
    pub(crate) status: RunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_step: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) user_ref: Option<String>,
    pub(crate) user_verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) outcome_json: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error_code: Option<String>,
    pub(crate) started_at: String,
}

/// What one delivery attempt decided. `pub(crate)` for tests.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Delivery {
    Accepted,
    Rejected,
    Failed,
    Suspended,
}

/// Records a deployed unit's run outcomes at the admin's ingest door.
pub struct HttpRunOutcomeSink {
    inner: Arc<Inner>,
}

struct Inner {
    endpoint: String,
    token: SecretString,
    tenant_slug: String,
    deployment_id: String,
    bundle_id: String,
    revision_id: String,
    http: reqwest::Client,
    suspended_until: Mutex<Option<Instant>>,
    dropped_while_suspended: AtomicU64,
    in_flight: Arc<tokio::sync::Semaphore>,
    dropped_saturated: AtomicU64,
    skipped_invalid: AtomicU64,
    rejected_streak: AtomicU32,
}

impl std::fmt::Debug for HttpRunOutcomeSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRunOutcomeSink")
            .field("endpoint", &self.inner.endpoint)
            .field("token", &"<redacted>")
            .field("tenant_slug", &self.inner.tenant_slug)
            .field("deployment_id", &self.inner.deployment_id)
            .field("bundle_id", &self.inner.bundle_id)
            .field("revision_id", &self.inner.revision_id)
            .finish()
    }
}

fn required(field: &'static str, value: &str) -> Result<String, RunOutcomeSinkError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(RunOutcomeSinkError::Blank(field));
    }
    Ok(trimmed.to_string())
}

fn required_id(field: &'static str, value: &str) -> Result<String, RunOutcomeSinkError> {
    let value = required(field, value)?;
    if value.len() > MAX_FIELD_BYTES {
        return Err(RunOutcomeSinkError::TooLong(field));
    }
    Ok(value)
}

/// `https` anywhere, or `http` to a loopback host.
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

/// An optional label within the admin's field cap, else omitted.
fn bounded(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.is_empty() && v.len() <= MAX_FIELD_BYTES)
}

impl HttpRunOutcomeSink {
    /// Build a sink for one unit. Refuses a blank field and an endpoint that
    /// would carry the token in cleartext off this host.
    pub fn new(target: RunOutcomeTarget) -> Result<Self, RunOutcomeSinkError> {
        let endpoint = required("endpoint", &target.endpoint)?;
        let token = required("token", &target.token)?;
        let tenant_slug = required_id("tenant_slug", &target.tenant_slug)?;
        let deployment_id = required_id("deployment_id", &target.deployment_id)?;
        let bundle_id = required_id("bundle_id", &target.bundle_id)?;
        let revision_id = required_id("revision_id", &target.revision_id)?;
        if !endpoint_is_safe(&endpoint) {
            return Err(RunOutcomeSinkError::UnsafeEndpoint(endpoint));
        }
        let http = reqwest::Client::builder()
            .timeout(POST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|err| RunOutcomeSinkError::Client(err.to_string()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                endpoint,
                token: SecretString::from(token),
                tenant_slug,
                deployment_id,
                bundle_id,
                revision_id,
                http,
                suspended_until: Mutex::new(None),
                dropped_while_suspended: AtomicU64::new(0),
                in_flight: Arc::new(tokio::sync::Semaphore::new(MAX_IN_FLIGHT)),
                dropped_saturated: AtomicU64::new(0),
                skipped_invalid: AtomicU64::new(0),
                rejected_streak: AtomicU32::new(0),
            }),
        })
    }

    /// Build the wire event. Refuses an event the admin would refuse (empty or
    /// over-long `run_id` / `flow_id`); ids are never truncated.
    pub(crate) fn build_event(&self, outcome: RunOutcome) -> Result<RunOutcomeEvent, &'static str> {
        let run_id = outcome.run_id.trim();
        if run_id.is_empty() || run_id.len() > MAX_FIELD_BYTES {
            return Err("run_id empty or longer than 256 bytes");
        }
        let flow_id = outcome.flow_id.trim();
        if flow_id.is_empty() || flow_id.len() > MAX_FIELD_BYTES {
            return Err("flow_id empty or longer than 256 bytes");
        }
        let outcome_json = outcome.outcome_json.filter(|value| {
            serde_json::to_vec(value)
                .map(|bytes| bytes.len() <= MAX_OUTCOME_JSON_BYTES)
                .unwrap_or(false)
        });
        Ok(RunOutcomeEvent {
            event_id: ulid::Ulid::new().to_string(),
            occurred_at: now_rfc3339(),
            tenant_slug: self.inner.tenant_slug.clone(),
            deployment_id: self.inner.deployment_id.clone(),
            bundle_id: self.inner.bundle_id.clone(),
            revision_id: self.inner.revision_id.clone(),
            run_id: run_id.to_string(),
            flow_id: flow_id.to_string(),
            kind: outcome.kind,
            status: outcome.status,
            last_step: bounded(outcome.last_step),
            user_ref: bounded(outcome.user_ref),
            user_verified: outcome.user_verified,
            channel: bounded(outcome.channel),
            outcome_json,
            error_code: bounded(outcome.error_code),
            started_at: outcome.started_at,
        })
    }

    #[cfg(test)]
    pub(crate) async fn deliver(&self, event: RunOutcomeEvent) -> Delivery {
        self.inner.deliver(event).await
    }

    #[cfg(test)]
    pub(crate) fn suspended_for(&self) -> Option<Duration> {
        let until = (*self.inner.suspended_until.lock())?;
        until.checked_duration_since(Instant::now())
    }

    #[cfg(test)]
    pub(crate) fn dropped_saturated(&self) -> u64 {
        self.inner.dropped_saturated.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn in_flight_semaphore(&self) -> Arc<tokio::sync::Semaphore> {
        Arc::clone(&self.inner.in_flight)
    }
}

impl Inner {
    fn is_suspended(&self) -> bool {
        self.suspended_until
            .lock()
            .is_some_and(|until| Instant::now() < until)
    }

    /// Suspend for `window`; a suspension already running is only extended,
    /// silently, so a burst of failures is one warning.
    fn suspend(&self, window: Duration, reason: &str) {
        let now = Instant::now();
        let already_suspended = {
            let mut guard = self.suspended_until.lock();
            let running = guard.is_some_and(|until| now < until);
            let until = now + window;
            *guard = Some(match *guard {
                Some(existing) if running && existing > until => existing,
                _ => until,
            });
            running
        };
        if already_suspended {
            return;
        }
        let dropped = self.dropped_while_suspended.swap(0, Ordering::Relaxed);
        tracing::warn!(
            endpoint = %self.endpoint,
            bundle_id = %self.bundle_id,
            reason,
            suspended_secs = window.as_secs(),
            dropped_previous_window = dropped,
            "run outcome reporting suspended; outcome events are dropped until it resumes"
        );
    }

    async fn deliver(&self, event: RunOutcomeEvent) -> Delivery {
        if self.is_suspended() {
            self.dropped_while_suspended.fetch_add(1, Ordering::Relaxed);
            return Delivery::Suspended;
        }
        let response = self
            .http
            .post(&self.endpoint)
            // The ONE place the token is used: a header, never URL/body/log.
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
            self.rejected_streak.store(0, Ordering::Relaxed);
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
                let streak = self.rejected_streak.fetch_add(1, Ordering::Relaxed) + 1;
                if streak == 1 {
                    tracing::warn!(
                        endpoint = %self.endpoint,
                        status = code,
                        "run outcome event refused by the admin; dropped"
                    );
                }
                if streak.is_multiple_of(REJECTION_STREAK) {
                    let doublings = (streak / REJECTION_STREAK).saturating_sub(1).min(16);
                    let window = REJECTION_BASE
                        .saturating_mul(1u32 << doublings)
                        .min(REJECTION_MAX);
                    self.suspend(
                        window,
                        "admin refused consecutive events (4xx); an admin older than this \
                         runtime may not have the run-outcome ingest door",
                    );
                }
                Delivery::Rejected
            }
            _ => {
                self.suspend(TRANSIENT_SUSPENSION, "admin answered 5xx");
                Delivery::Failed
            }
        }
    }
}

impl RunOutcomeSink for HttpRunOutcomeSink {
    fn record(&self, outcome: RunOutcome) {
        let event = match self.build_event(outcome) {
            Ok(event) => event,
            Err(reason) => {
                let skipped = self.inner.skipped_invalid.fetch_add(1, Ordering::Relaxed) + 1;
                if sampled(skipped) {
                    tracing::warn!(
                        bundle_id = %self.inner.bundle_id,
                        reason,
                        skipped_total = skipped,
                        "run outcome event not sent: the admin would refuse it"
                    );
                }
                return;
            }
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                bundle_id = %self.inner.bundle_id,
                "run outcome event dropped: recorded outside a tokio runtime"
            );
            return;
        };
        let Ok(permit) = Arc::clone(&self.inner.in_flight).try_acquire_owned() else {
            let dropped = self.inner.dropped_saturated.fetch_add(1, Ordering::Relaxed) + 1;
            if sampled(dropped) {
                tracing::warn!(
                    bundle_id = %self.inner.bundle_id,
                    in_flight = MAX_IN_FLIGHT,
                    dropped_total = dropped,
                    "run outcome event dropped: too many outcome POSTs already in flight"
                );
            }
            return;
        };
        let inner = Arc::clone(&self.inner);
        handle.spawn(async move {
            inner.deliver(event).await;
            drop(permit);
        });
    }
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
