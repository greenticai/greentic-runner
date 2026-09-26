//! [`WorkerUsageMeter`]: a [`BillingMeter`] that records a DEPLOYED unit's
//! LLM token usage at the admin's per-unit ingest door.
//!
//! Wire contract: greentic-designer
//! `docs/superpowers/specs/2026-09-24-env-canvas-unit-monitoring-phase-2-design.md`
//! §4.1 — `POST {admin}/api/v1/ingest/worker-usage`, bearer = the unit's
//! worker-usage token, one event per LLM iteration, `surface: "turn"`, plus an
//! optional `model` and (audit wire contract v2 §5) an optional `run_id`. The
//! body is the one greentic-start's interop reporter already sends (`src/interop/metering/event.rs`), so both producers speak one
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
//! [`TRANSIENT_SUSPENSION`], and [`REJECTION_STREAK`] consecutive `400`s (an
//! admin older than this contract) for a doubling backoff — so a dead or
//! misconfigured admin costs one request per window rather than one per LLM
//! iteration. Each suspension is one `warn` (concurrent failures extend it
//! silently), which also reports how many events the previous window dropped.
//! At most [`MAX_IN_FLIGHT`] POSTs run at once; past that an event is dropped
//! and counted. An event the admin would refuse (empty id, id over
//! [`MAX_FIELD_BYTES`]) is never sent.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
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

/// Longest id the admin's ingest door accepts in any field (its
/// `MAX_FIELD_LEN`). A longer one is a `400`, so it is never sent.
pub(crate) const MAX_FIELD_BYTES: usize = 256;

/// Usage POSTs in flight at once, per meter. A slow admin that still answers
/// `2xx` would otherwise accumulate one task per LLM iteration for up to
/// [`POST_TIMEOUT`] each. Past the bound an event is dropped and counted.
pub(crate) const MAX_IN_FLIGHT: usize = 32;

/// Consecutive `400`s after which the endpoint is suspended. An admin that
/// predates `surface: "turn"` refuses EVERY event; without this it would be
/// asked, and a warning written, once per LLM iteration forever.
const REJECTION_STREAK: u32 = 5;

/// First suspension after a streak of `400`s; doubles per further streak.
const REJECTION_BASE: Duration = Duration::from_secs(60);

/// Ceiling on the `400` backoff.
const REJECTION_MAX: Duration = Duration::from_secs(1800);

/// Warn on the first occurrence of a repeated condition and every 100th after,
/// always carrying the running count.
fn sampled(count: u64) -> bool {
    count == 1 || count.is_multiple_of(100)
}

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
    #[error("worker usage `{0}` is longer than the admin accepts ({MAX_FIELD_BYTES} bytes)")]
    TooLong(&'static str),
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
    /// The deployed run this LLM iteration belongs to
    /// ([`super::run_scope::current_run_id`]); omitted outside a run and when
    /// longer than [`MAX_FIELD_BYTES`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
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
    /// Bounds concurrent POSTs; see [`MAX_IN_FLIGHT`].
    in_flight: std::sync::Arc<tokio::sync::Semaphore>,
    /// Events dropped because [`MAX_IN_FLIGHT`] POSTs were already running.
    dropped_saturated: AtomicU64,
    /// Events never sent because an id field was empty or too long.
    skipped_invalid: AtomicU64,
    /// Consecutive `400`s; reset by any accepted event.
    rejected_streak: AtomicU32,
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

/// [`required`] plus the admin's per-field length cap, for the ids that ride
/// in every event body.
fn required_id(field: &'static str, value: &str) -> Result<String, WorkerUsageError> {
    let value = required(field, value)?;
    if value.len() > MAX_FIELD_BYTES {
        return Err(WorkerUsageError::TooLong(field));
    }
    Ok(value)
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
        let tenant_slug = required_id("tenant_slug", &target.tenant_slug)?;
        let deployment_id = required_id("deployment_id", &target.deployment_id)?;
        let bundle_id = required_id("bundle_id", &target.bundle_id)?;
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
                in_flight: std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_IN_FLIGHT)),
                dropped_saturated: AtomicU64::new(0),
                skipped_invalid: AtomicU64::new(0),
                rejected_streak: AtomicU32::new(0),
            }),
        })
    }

    /// Build the event for one LLM iteration. Pure apart from the fresh id
    /// and timestamp.
    ///
    /// Refuses (with the reason) an event the admin would refuse: an empty
    /// `agent_id`, or an `agent_id` / `model` over [`MAX_FIELD_BYTES`]. Ids are
    /// never truncated — a truncated id is a different id, attributed to
    /// nothing — and an over-long model is not silently dropped from an event
    /// that otherwise claims to say which model ran.
    pub(crate) fn build_event(
        &self,
        tokens_in: u64,
        tokens_out: u64,
        agent_id: &str,
        model: &str,
    ) -> Result<WorkerUsageEvent, &'static str> {
        let agent_id = agent_id.trim();
        if agent_id.is_empty() {
            return Err("empty agent_id");
        }
        if agent_id.len() > MAX_FIELD_BYTES {
            return Err("agent_id longer than 256 bytes");
        }
        let model = model.trim();
        if model.len() > MAX_FIELD_BYTES {
            return Err("model longer than 256 bytes");
        }
        Ok(WorkerUsageEvent {
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
            run_id: super::run_scope::current_run_id()
                .filter(|id| !id.is_empty() && id.len() <= MAX_FIELD_BYTES),
        })
    }

    /// Deliver one event now. `emit` spawns the same delivery; tests call
    /// this to observe its outcome.
    #[cfg(test)]
    pub(crate) async fn deliver(&self, event: WorkerUsageEvent) -> Delivery {
        self.inner.deliver(event).await
    }

    /// How much longer the endpoint stays suspended, if it is.
    #[cfg(test)]
    pub(crate) fn suspended_for(&self) -> Option<Duration> {
        let until = (*self.inner.suspended_until.lock().ok()?)?;
        until.checked_duration_since(Instant::now())
    }
}

impl Inner {
    fn is_suspended(&self) -> bool {
        match self.suspended_until.lock() {
            Ok(guard) => guard.is_some_and(|until| Instant::now() < until),
            Err(_) => false,
        }
    }

    /// Suspend the endpoint for `window`. Concurrent failures (a burst of
    /// in-flight POSTs all answered `429`) produce ONE suspension and ONE
    /// warning: a suspension already running is only extended, silently, and
    /// its dropped-event count is left to the warning that started it.
    fn suspend(&self, window: Duration, reason: &str) {
        let now = Instant::now();
        let already_suspended = match self.suspended_until.lock() {
            Ok(mut guard) => {
                let running = guard.is_some_and(|until| now < until);
                let until = now + window;
                *guard = Some(match *guard {
                    Some(existing) if running && existing > until => existing,
                    _ => until,
                });
                running
            }
            Err(_) => false,
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
                        "worker usage event refused by the admin; dropped"
                    );
                }
                if streak.is_multiple_of(REJECTION_STREAK) {
                    // Backoff doubles per streak: 60 s, 120 s, … capped.
                    let doublings = (streak / REJECTION_STREAK).saturating_sub(1).min(16);
                    let window = REJECTION_BASE
                        .saturating_mul(1u32 << doublings)
                        .min(REJECTION_MAX);
                    self.suspend(
                        window,
                        "admin refused consecutive events (4xx); an admin older than this \
                         runtime may not accept `surface: \"turn\"`",
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

impl BillingMeter for WorkerUsageMeter {
    fn emit<'a>(
        &'a self,
        _tenant: &'a TenantContext,
        input_tokens: u64,
        output_tokens: u64,
        agent_id: &'a str,
        model: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), BillingError>> + Send + 'a>> {
        let event = match self.build_event(input_tokens, output_tokens, agent_id, model) {
            Ok(event) => event,
            Err(reason) => {
                let skipped = self.inner.skipped_invalid.fetch_add(1, Ordering::Relaxed) + 1;
                if sampled(skipped) {
                    tracing::warn!(
                        bundle_id = %self.inner.bundle_id,
                        reason,
                        skipped_total = skipped,
                        "worker usage event not sent: the admin would refuse it"
                    );
                }
                return Box::pin(async { Ok(()) });
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let Ok(permit) = std::sync::Arc::clone(&self.inner.in_flight).try_acquire_owned()
                else {
                    let dropped = self.inner.dropped_saturated.fetch_add(1, Ordering::Relaxed) + 1;
                    if sampled(dropped) {
                        tracing::warn!(
                            bundle_id = %self.inner.bundle_id,
                            in_flight = MAX_IN_FLIGHT,
                            dropped_total = dropped,
                            "worker usage event dropped: too many usage POSTs already in flight"
                        );
                    }
                    return Box::pin(async { Ok(()) });
                };
                let inner = std::sync::Arc::clone(&self.inner);
                handle.spawn(async move {
                    inner.deliver(event).await;
                    drop(permit);
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
