//! The HTTP approval rail: human approvals on lanes that have no NATS broker.
//!
//! Design: greentic-designer
//! `docs/superpowers/specs/2026-09-29-approval-rail-http-design.md` §4 (and
//! its §13 amendments, which override the body). Wire contract: [`wire`].
//!
//! An `approval.call` node that needs a human is a REMOTE DISPATCH. Where no
//! broker is configured (the designer's env-canvas Cloud Run and k8s lanes),
//! [`HttpApprovalDispatcher`]:
//!
//! 1. **Sends** the request to the admin's ingest door with the `decision_token`
//!    STRIPPED — the token never leaves the worker on this transport — and
//!    keeps it in memory, keyed by the nonced correlation id. A send that the
//!    admin did not record fails the node rather than parking a gate nobody
//!    will ever see.
//! 2. **Polls** for the decision from one task per runtime (5 s for the first
//!    minute, then 15 s; 60 s after a transient failure).
//! 3. **Resumes exactly once.** The first `200` ends polling for that id,
//!    whatever happens next. The resume goes through a STRICT
//!    [`RuntimeSessionResumer`](crate::runner::runtime_session_resumer::RuntimeSessionResumer::strict):
//!    a decision for a gate that is no longer parked under exactly this nonced
//!    id is dropped, and NEVER starts a fresh run from the flow's entrypoint.
//!    The worker's own held token is injected at `output.decision_token`, so
//!    the gate's usual token check runs unchanged and spends it.
//! 4. **Watches the deadline** locally: at the node's `deadline_ms` (a default
//!    of 7 days when the node sets none, and never more than 7 days), it stops
//!    polling, withdraws the request, and resumes with the same `timeout` body
//!    the NATS watchdog publishes. One task, one outcome — a real decision
//!    cancels the timeout and vice versa.
//!
//! It goes into the engine's own `approval_dispatch_handler` slot, never the
//! shared `remote_dispatch_handler` one, so `sorla.call` keeps failing with
//! `sorla_route_missing` on these lanes (spec §4.1).
//!
//! # Lifetime
//!
//! The poller, every held token and the resumer are released by
//! [`HttpApprovalDispatcher::shutdown`], which the owning `TenantRuntime`
//! calls on drop: a superseded revision stops polling and cannot resume into a
//! session store it no longer owns. Shutdown also drops the resumer, which
//! breaks the `resumer → runtime → engine → dispatcher` reference cycle.
//!
//! # Not solved here
//!
//! Held tokens live in memory: a restart of the process loses the poller even
//! when the park itself survives in a durable session store (spec §12).
//!
//! Neither token — the worker-usage bearer nor a `decision_token` — is ever
//! logged, at any level.

pub mod wire;

mod poller;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use greentic_types::{DispatchMode, EnvId, TenantCtx, TenantId};
use parking_lot::Mutex;
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::dispatch_listener::SessionResumer;
use super::remote_dispatch::{RemoteDispatch, RemoteDispatchAction, RemoteDispatchHandler};

pub use wire::{
    APPROVAL_RAIL_HEADER, APPROVAL_RAIL_USER_AGENT, APPROVAL_RAIL_VERSION, ApprovalDecisionBody,
    ApprovalDecisionKind, ApprovalRequestBody, ApprovalWithdrawBody, WithdrawReason,
};

/// The longest any approval waits on this rail, and the deadline applied to
/// one whose node sets none (spec §4.3).
pub const MAX_APPROVAL_WAIT: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Budget for DNS + connect + TLS handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Budget for one whole request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Where and as whom one unit's approvals are filed and fetched. Every field
/// is the value the embedding host (greentic-start) resolved for ONE unit from
/// its staged `metering` block.
#[derive(Clone)]
pub struct ApprovalInboxTarget {
    /// `{admin}/api/v1/ingest/approval-request`.
    pub request_url: String,
    /// `{admin}/api/v1/ingest/approval-decision`.
    pub decision_url: String,
    /// `{admin}/api/v1/ingest/approval-withdraw`.
    pub withdraw_url: String,
    /// The unit's `gtm_` worker-usage bearer (with the `approvals` purpose).
    /// Only ever sent as a header; never logged.
    pub token: String,
    /// The workspace tenant the token belongs to.
    pub tenant_slug: String,
}

impl std::fmt::Debug for ApprovalInboxTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApprovalInboxTarget")
            .field("request_url", &self.request_url)
            .field("decision_url", &self.decision_url)
            .field("withdraw_url", &self.withdraw_url)
            .field("token", &"<redacted>")
            .field("tenant_slug", &self.tenant_slug)
            .finish()
    }
}

/// Why an [`ApprovalInboxTarget`] could not configure an inbox. Never names
/// the token.
#[derive(Debug, thiserror::Error)]
pub enum ApprovalInboxError {
    #[error("approval inbox needs a non-blank `{0}`")]
    Blank(&'static str),
    #[error(
        "approval inbox `{0}` is not https and not loopback http; refusing to send a bearer \
         token in cleartext"
    )]
    UnsafeUrl(String),
    #[error("approval inbox has no HTTP client: {0}")]
    Client(String),
}

/// Poll and retry cadence. Production uses [`Timing::default`]; tests shrink it.
#[derive(Clone, Debug)]
pub(crate) struct Timing {
    /// Delays before each RETRY of a send (the first send is immediate).
    pub(crate) send_retry_delays: Vec<Duration>,
    /// Poll interval while an approval is younger than `fast_window`.
    pub(crate) fast_interval: Duration,
    /// How long an approval is polled at `fast_interval`.
    pub(crate) fast_window: Duration,
    /// Poll interval afterwards.
    pub(crate) slow_interval: Duration,
    /// Poll interval after a transient failure.
    pub(crate) backoff: Duration,
    /// Deadline applied when the node sets none, and the hard cap.
    pub(crate) max_wait: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            send_retry_delays: vec![
                Duration::from_secs(1),
                Duration::from_secs(4),
                Duration::from_secs(10),
            ],
            fast_interval: Duration::from_secs(5),
            fast_window: Duration::from_secs(60),
            slow_interval: Duration::from_secs(15),
            backoff: Duration::from_secs(60),
            max_wait: MAX_APPROVAL_WAIT,
        }
    }
}

/// One approval this worker is waiting on.
pub(crate) struct Pending {
    /// The dispatch's own tenant context — the resume runs under it.
    pub(crate) tenant: TenantCtx,
    pub(crate) env: String,
    /// The token this gate was issued, held until the one resume.
    pub(crate) decision_token: Option<SecretString>,
    pub(crate) registered_at: Instant,
    pub(crate) deadline_at: Instant,
    /// The deadline as the node sees it, for the `timeout` message.
    pub(crate) deadline_ms: u64,
    /// What the withdrawal at `deadline_at` says.
    pub(crate) deadline_reason: WithdrawReason,
    pub(crate) next_poll_at: Instant,
}

pub(crate) struct Inner {
    pub(crate) request_url: String,
    pub(crate) decision_url: url::Url,
    pub(crate) withdraw_url: String,
    pub(crate) token: SecretString,
    pub(crate) tenant_slug: String,
    pub(crate) http: reqwest::Client,
    pub(crate) timing: Timing,
    pub(crate) pending: Mutex<HashMap<String, Pending>>,
    pub(crate) resumer: Mutex<Option<Arc<dyn SessionResumer>>>,
    pub(crate) cancel: CancellationToken,
    pub(crate) wake: Notify,
    pub(crate) poller_started: Mutex<bool>,
}

/// The HTTP approval inbox, as a [`RemoteDispatchHandler`] for the engine's
/// approval slot. Cheap to clone; clones share one poller and one set of held
/// tokens.
#[derive(Clone)]
pub struct HttpApprovalDispatcher {
    pub(crate) inner: Arc<Inner>,
}

impl std::fmt::Debug for HttpApprovalDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpApprovalDispatcher")
            .field("request_url", &self.inner.request_url)
            .field("token", &"<redacted>")
            .field("tenant_slug", &self.inner.tenant_slug)
            .field("pending", &self.inner.pending.lock().len())
            .finish()
    }
}

fn required(field: &'static str, value: &str) -> Result<String, ApprovalInboxError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApprovalInboxError::Blank(field));
    }
    Ok(trimmed.to_string())
}

/// `https` anywhere, or `http` to a loopback host.
fn safe_url(field: &'static str, value: &str) -> Result<url::Url, ApprovalInboxError> {
    let value = required(field, value)?;
    let unsafe_url = || ApprovalInboxError::UnsafeUrl(value.clone());
    let url = url::Url::parse(&value).map_err(|_| unsafe_url())?;
    let safe = match url.scheme() {
        "https" => url.host().is_some(),
        "http" => match url.host() {
            Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            None => false,
        },
        _ => false,
    };
    if safe { Ok(url) } else { Err(unsafe_url()) }
}

impl HttpApprovalDispatcher {
    /// Build an inbox for one unit. Refuses a blank field and any URL that
    /// would carry the token in cleartext off this host. Nothing is sent and
    /// no task is spawned until the first approval is dispatched.
    pub fn new(target: ApprovalInboxTarget) -> Result<Self, ApprovalInboxError> {
        Self::with_timing(target, Timing::default())
    }

    pub(crate) fn with_timing(
        target: ApprovalInboxTarget,
        timing: Timing,
    ) -> Result<Self, ApprovalInboxError> {
        let request_url = safe_url("request_url", &target.request_url)?;
        let decision_url = safe_url("decision_url", &target.decision_url)?;
        let withdraw_url = safe_url("withdraw_url", &target.withdraw_url)?;
        let token = required("token", &target.token)?;
        let tenant_slug = required("tenant_slug", &target.tenant_slug)?;
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .user_agent(APPROVAL_RAIL_USER_AGENT)
            .build()
            .map_err(|err| ApprovalInboxError::Client(err.to_string()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                request_url: request_url.to_string(),
                decision_url,
                withdraw_url: withdraw_url.to_string(),
                token: SecretString::from(token),
                tenant_slug,
                http,
                timing,
                pending: Mutex::new(HashMap::new()),
                resumer: Mutex::new(None),
                cancel: CancellationToken::new(),
                wake: Notify::new(),
                poller_started: Mutex::new(false),
            }),
        })
    }

    /// Hand the inbox the resumer its decisions are delivered through. The
    /// runtime installs a STRICT `RuntimeSessionResumer` here right after the
    /// state machine exists. Ignored once [`Self::shutdown`] has run.
    pub fn attach_resumer(&self, resumer: Arc<dyn SessionResumer>) {
        if self.inner.cancel.is_cancelled() {
            return;
        }
        *self.inner.resumer.lock() = Some(resumer);
    }

    /// Stop polling, forget every held token and drop the resumer. Called by
    /// the owning `TenantRuntime` on drop. Idempotent. Pending approvals are
    /// NOT withdrawn: the admin console refuses a row whose worker stopped
    /// polling (spec §6.1.5).
    pub fn shutdown(&self) {
        self.inner.cancel.cancel();
        self.inner.pending.lock().clear();
        self.inner.resumer.lock().take();
    }

    /// A clone of the token that [`Self::shutdown`] cancels.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancel.clone()
    }

    /// How many approvals are being polled for.
    pub fn pending_count(&self) -> usize {
        self.inner.pending.lock().len()
    }

    fn authed(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        builder
            .bearer_auth(self.inner.token.expose_secret())
            .header(APPROVAL_RAIL_HEADER, APPROVAL_RAIL_VERSION)
    }

    /// POST the request, retrying transport errors, `5xx` and any `429` but
    /// the pending cap. `Ok` only when the admin recorded it.
    async fn send(&self, body: &ApprovalRequestBody) -> Result<()> {
        let delays = self.inner.timing.send_retry_delays.clone();
        let mut last = String::new();
        for attempt in 0..=delays.len() {
            if attempt > 0 {
                tokio::time::sleep(delays[attempt - 1]).await;
            }
            let sent = self
                .authed(self.inner.http.post(&self.inner.request_url))
                .json(body)
                .send()
                .await;
            let response = match sent {
                Ok(response) => response,
                Err(error) => {
                    last = format!("transport error: {}", error.without_url());
                    continue;
                }
            };
            let status = response.status();
            if status.is_success() {
                return Ok(());
            }
            let text = response.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                if text.contains(wire::PENDING_CAP_CODE) {
                    bail!(
                        "approval inbox refused the request: this worker already has the maximum \
                         number of pending approvals ({})",
                        wire::PENDING_CAP_CODE
                    );
                }
                last = format!("HTTP {status}");
                continue;
            }
            if status.is_server_error() {
                last = format!("HTTP {status}");
                continue;
            }
            bail!("approval inbox refused the request with HTTP {status}");
        }
        Err(anyhow!(
            "approval inbox did not record the request after {} attempts ({last})",
            delays.len() + 1
        ))
    }

    fn register(&self, correlation_id: String, pending: Pending) {
        self.inner.pending.lock().insert(correlation_id, pending);
        self.ensure_poller();
        self.inner.wake.notify_one();
    }

    fn ensure_poller(&self) {
        let mut started = self.inner.poller_started.lock();
        if *started || self.inner.cancel.is_cancelled() {
            return;
        }
        *started = true;
        tokio::spawn(poller::run(Arc::clone(&self.inner)));
    }
}

/// The effective deadline and what its expiry is called (spec §4.3): the
/// node's own `deadline_ms` when it is within the cap (`timeout`), else the
/// cap (`poll_cap`).
fn effective_deadline(node_deadline_ms: Option<u64>, cap: Duration) -> (u64, WithdrawReason) {
    let cap_ms = u64::try_from(cap.as_millis()).unwrap_or(u64::MAX);
    match node_deadline_ms {
        Some(ms) if ms <= cap_ms => (ms, WithdrawReason::Timeout),
        _ => (cap_ms, WithdrawReason::PollCap),
    }
}

#[async_trait]
impl RemoteDispatchHandler for HttpApprovalDispatcher {
    async fn dispatch(&self, request: RemoteDispatch) -> Result<RemoteDispatchAction> {
        if request.runtime != crate::runner::engine::APPROVAL_RUNTIME {
            bail!(
                "{}.call dispatched to the approval inbox, which serves only approval.call",
                request.runtime
            );
        }
        if self.inner.cancel.is_cancelled() {
            bail!("approval inbox is shut down; this revision is no longer serving");
        }
        let tenant = TenantCtx::new(
            EnvId::try_from(request.env.as_str())
                .map_err(|error| anyhow!("invalid env for approval dispatch: {error}"))?,
            TenantId::try_from(request.tenant.as_str())
                .map_err(|error| anyhow!("invalid tenant for approval dispatch: {error}"))?,
        );
        let (deadline_ms, deadline_reason) =
            effective_deadline(request.deadline_ms, self.inner.timing.max_wait);
        let body = wire::request_body(
            &self.inner.tenant_slug,
            &request.correlation_id,
            &request.target,
            &request.operation,
            request.mode,
            request.input,
            deadline_ms,
        );
        self.send(&body).await?;

        match request.mode {
            DispatchMode::FireAndForget => Ok(RemoteDispatchAction::Dispatched),
            DispatchMode::Await => {
                let now = Instant::now();
                self.register(
                    request.correlation_id.clone(),
                    Pending {
                        tenant,
                        env: request.env,
                        decision_token: request.decision_token.map(SecretString::from),
                        registered_at: now,
                        deadline_at: now + Duration::from_millis(deadline_ms),
                        deadline_ms,
                        deadline_reason,
                        next_poll_at: now + self.inner.timing.fast_interval,
                    },
                );
                Ok(RemoteDispatchAction::AwaitingResponse {
                    correlation_id: request.correlation_id,
                })
            }
        }
    }
}

/// Whether a runtime should install the HTTP approval inbox: only when a
/// target was handed over AND no NATS broker connected. NATS stays
/// authoritative where configured (spec §2.5, §4.6).
pub(crate) fn inbox_to_install(
    nats_connected: bool,
    target: Option<ApprovalInboxTarget>,
) -> Option<ApprovalInboxTarget> {
    if nats_connected { None } else { target }
}
