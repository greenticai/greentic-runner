//! The HTTP approval rail's wire contract, in ONE place.
//!
//! greentic-designer `docs/superpowers/specs/2026-09-29-approval-rail-http-design.md`
//! §3 (and its §13 amendments). greentic-designer-admin implements the other
//! side of every type here; change nothing without changing that spec.
//!
//! Three doors, siblings of `/api/v1/ingest/worker-usage`, all authenticated
//! by the unit's `gtm_` worker-usage token (`Authorization: Bearer gtm_…`),
//! which must carry the `approvals` purpose. The admin takes tenant, env and
//! unit from the TOKEN, never from a body:
//!
//! | door | method | body / query | answers |
//! |---|---|---|---|
//! | `/api/v1/ingest/approval-request` | `POST` | [`ApprovalRequestBody`] | `201 {id}` new, `200 {id}` idempotent repeat, `409 approval_conflict`, `429 approval_pending_cap`, `400`/`413`, `404` (admin predates the door) |
//! | `/api/v1/ingest/approval-decision` | `GET` | `?correlation_id=<urlencoded>` | `204` pending, `200` [`ApprovalDecisionBody`], `410 approval_withdrawn` / `approval_expired`, `404` for every miss |
//! | `/api/v1/ingest/approval-withdraw` | `POST` | [`ApprovalWithdrawBody`] | `204` for a scoped hit or miss |
//!
//! Every request also carries [`APPROVAL_RAIL_HEADER`]` = `[`APPROVAL_RAIL_VERSION`]
//! and a `User-Agent` naming the same transport; the admin stamps
//! `worker_usage_tokens.approvals_seen_at` from it (spec §4.7).
//!
//! **No `decision_token` crosses this wire, in either direction.** The worker
//! mints, holds and verifies it; a decision fetched from the admin is resumed
//! with the worker's OWN held token injected (spec §3.2, §4.4, §10).

use greentic_types::{DispatchMode, RuntimeDispatchRequest};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Header identifying the transport on every request to the three doors.
pub const APPROVAL_RAIL_HEADER: &str = "X-Greentic-Approval-Rail";

/// Value of [`APPROVAL_RAIL_HEADER`]: the HTTP approval rail, version 1.
pub const APPROVAL_RAIL_VERSION: &str = "http/1";

/// `User-Agent` sent on every request to the three doors.
pub const APPROVAL_RAIL_USER_AGENT: &str = concat!(
    "greentic-runner-host/",
    env!("CARGO_PKG_VERSION"),
    " approval-rail/http/1"
);

/// Query parameter of the decision door.
pub const DECISION_QUERY_PARAM: &str = "correlation_id";

/// Admin error code for "this token already has 50 pending approvals" (`429`).
/// Unlike any other `429` it is NOT retried: another attempt cannot succeed
/// until a human resolves something.
pub const PENDING_CAP_CODE: &str = "approval_pending_cap";

/// `POST /api/v1/ingest/approval-request`.
///
/// ```json
/// {
///   "tenant_slug": "acme",
///   "correlation_id": "<bare hint>::pack=<p>::flow=<f>[::thread=…][::reply=…]::n=<32 hex>",
///   "request": { "target": "...", "operation": "...", "mode": "await",
///                "input": { ... }, "deadline_ms": 604800000 }
/// }
/// ```
///
/// Idempotent by construction: the admin upserts on `(tenant, correlation_id)`
/// and the id carries a per-dispatch nonce, so a retried send is a repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequestBody {
    /// Must equal the token's tenant slug or the admin answers
    /// `403 tenant_mismatch`. Never used to pick a tenant.
    pub tenant_slug: String,
    /// The nonced correlation id the flow parked under. The admin's
    /// idempotency key, and the key the decision is later fetched by.
    pub correlation_id: String,
    /// The dispatch request exactly as the NATS rail publishes it
    /// ([`RuntimeDispatchRequest`]: `target`, `operation`, `mode`, `input`,
    /// `deadline_ms`) MINUS `routing.decision_token`. The type has no
    /// `routing` field at all, so the token cannot ride here by construction.
    pub request: RuntimeDispatchRequest,
}

/// `200` answer of `POST /api/v1/ingest/approval-request` (`201` or `200`).
/// Informational only: the runner keys everything by `correlation_id`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequestAccepted {
    /// The admin's row id.
    pub id: Value,
}

/// A human's decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecisionKind {
    Approved,
    Denied,
}

impl ApprovalDecisionKind {
    /// The value the approval gate reads at `output.decision`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Denied => "denied",
        }
    }
}

/// `200` answer of `GET /api/v1/ingest/approval-decision`, once resolved:
///
/// ```json
/// { "decision": "approved" | "denied", "resolved_by": "...", "note": "..." }
/// ```
///
/// Carries NO `decision_token` (the admin holds none for a poll row). An
/// unknown field is ignored so the admin can add one without breaking a
/// deployed runner.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApprovalDecisionBody {
    pub decision: ApprovalDecisionKind,
    /// Who the admin says decided. "The admin asserted", never proof of a
    /// person (contract §5).
    #[serde(default)]
    pub resolved_by: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

/// Why the runner withdraws a pending approval.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WithdrawReason {
    /// The node's own `deadline_ms` elapsed.
    Timeout,
    /// The runtime stopped waiting for another reason.
    Cancelled,
    /// The 7-day poll cap elapsed before any node deadline.
    PollCap,
}

/// `POST /api/v1/ingest/approval-withdraw`: marks the row `expired` only while
/// it is still `pending`. `204` whether or not it matched.
///
/// ```json
/// { "tenant_slug": "acme", "correlation_id": "…", "reason": "timeout" | "cancelled" | "poll_cap" }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApprovalWithdrawBody {
    pub tenant_slug: String,
    pub correlation_id: String,
    pub reason: WithdrawReason,
}

/// Build the request door's body. `mode` and `deadline_ms` are the EFFECTIVE
/// ones (the inbox applies a default deadline, spec §4.3).
pub(crate) fn request_body(
    tenant_slug: &str,
    correlation_id: &str,
    target: &str,
    operation: &str,
    mode: DispatchMode,
    input: Value,
    deadline_ms: u64,
) -> ApprovalRequestBody {
    ApprovalRequestBody {
        tenant_slug: tenant_slug.to_string(),
        correlation_id: correlation_id.to_string(),
        request: RuntimeDispatchRequest {
            target: target.to_string(),
            operation: operation.to_string(),
            mode,
            input,
            deadline_ms: Some(deadline_ms),
        },
    }
}
