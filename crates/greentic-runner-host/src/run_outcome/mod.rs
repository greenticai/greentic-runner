//! Deployed run audit: one outcome event per flow TURN, so the admin can list
//! the runs of a deployed unit as *completed*, *in progress* (read back as
//! *dropped off* once its response is overdue, or once idle) or *technical
//! error* — for plain flows and agentic runs alike.
//!
//! Design: greentic-designer
//! `docs/superpowers/specs/2026-09-25-deployed-run-audit-design.md` §3.1 (v1)
//! and `docs/superpowers/specs/2026-09-26-operate-audit-v1-1-design.md` with
//! the Slice B wire contract v2 (`seq`, `wait_kind`, `response_due_at`,
//! `worker_*`, `error_ref`, the `error` excerpt).
//!
//! # Why the runner emits it
//!
//! `PackFlowAdapter::call_traced` (`engine/runtime.rs`) is the one place that
//! sees all three terminal shapes of a turn — `FlowStatus::Completed`,
//! `FlowStatus::Waiting` (with the node the user is parked on) and the engine
//! error. By the time the embedding host sees the reply, "waiting" has been
//! folded into it. A session also never announces that it ENDED (waits expire
//! silently), so every turn writes its status as it happens and "dropped off"
//! is derived at read time from an `in_progress` row whose `response_due_at`
//! passed (or, for a v1 row with no deadline, that went idle).
//!
//! # A run spans turns
//!
//! A `run_id` (ULID) is minted when a turn starts with NO resume snapshot, and
//! stored beside the parked snapshot (`FlowResumeRecord.run`) so every resume
//! of that run reports under the same id. A snapshot parked by a runtime that
//! predates this field carries none, and its resume mints a fresh id — the
//! only cost is that such an in-flight run is split in two, once.
//!
//! Every event carries `seq`: 1 on a run's first event, +1 on each later one.
//! The count is stored in the same marker (`RunMarker.seq`; a marker written
//! before the field decodes as 0, so its next event is 1). The admin applies
//! an event only when its `seq` is greater than the stored one.
//!
//! Journey identity inside one turn: a `flow.call` sub-flow and a `flow:` tool
//! a worker invokes run INSIDE the caller's turn (`FlowEngine::execute` /
//! `PackRuntime::run_flow_for_tool`, never through `call_traced`), so they
//! mint no run and emit nothing of their own. A `flow.goto` is a jump in the
//! SAME walk: the run continues under the same `run_id`, and the event
//! reports the flow the walk ended in (a parked target's resumes carry it as
//! `next_flow`).
//!
//! Card-driven packs whose pause points are rendered cards (they re-enter via
//! `entry_node` and never park) leave no snapshot, so each of their turns is a
//! run of its own. So does an agent-forward flow (`start → dw.agent`): it
//! reaches its end every turn, so each turn is its own journey. Both are
//! properties of those packs, not of this module.
//!
//! # Agentic runs
//!
//! A run that executed an agentic node carries `kind: agentic` (sticky across
//! its turns) and otherwise the SAME status rules as a flow — `completed`,
//! `in_progress` (+ `wait_kind`) or `technical_error`, with `last_step` and
//! `error_code`. v1 reported such a run as `status: agentic` with nothing
//! else; v2 never emits that status.
//!
//! # What never leaves the process
//!
//! Message text and node payloads. `error_code` is a short class (`timeout`,
//! `secret_missing`, …) computed here. The error's own text leaves ONLY in the
//! technical-error `error` excerpt, redacted and capped
//! ([`error`]) — `safe_summary` is built from identifiers alone. `outcome_json`
//! is reserved for a flow's DECLARED output map; flows do not declare one
//! today, so it is always absent rather than filled with the reply (which is
//! user-visible text).
//!
//! # No sink, no change
//!
//! Nothing here runs unless the embedding host installs a sink through
//! [`crate::runtime::RevisionHostOptions::with_run_outcome_sink`]. Without one
//! no id is minted, no observer is installed and the persisted wait is
//! byte-identical to before.

pub(crate) mod error;
pub mod http;
pub(crate) mod turn;
pub(crate) mod wait;

use serde::Serialize;
use serde_json::Value;

pub use error::ErrorExcerpt;
pub use http::{HttpRunOutcomeSink, RunOutcomeSinkError, RunOutcomeTarget};

/// What kind of run an event describes. Serialised `snake_case`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    /// A plain flow run.
    Flow,
    /// A run that executed an agentic node (`dw.agent`, `dw.agent_graph`,
    /// `operala.call` deep worker, `agentic.call`). Sticky across the run's
    /// turns; the status follows the same rules as a flow's.
    Agentic,
}

/// A turn's outcome. Serialised `snake_case`. `dropped_off` is deliberately
/// absent: it is derived by the reader from an idle `in_progress` run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// The flow parked and is waiting for the user.
    InProgress,
    /// The flow reached its end.
    Completed,
    /// The engine failed, or a node failed and the flow ended on it.
    TechnicalError,
}

/// What an `in_progress` run is waiting for. Serialised `snake_case`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitKind {
    /// A card, a channel message or any other await-input park.
    UserInput,
    /// An `approval.call` gate waiting for a human decision.
    Approval,
    /// A remote dispatch (operala / NATS / another runtime) in flight.
    Processing,
}

/// The wait an `in_progress` event reports.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WaitState {
    pub kind: WaitKind,
    /// RFC 3339 (ms, `Z`); present iff `kind` is [`WaitKind::UserInput`].
    pub response_due_at: Option<String>,
}

/// Who did the work: for an agentic run the agent, for a flow run the pack.
/// Every field is optional on the wire.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WorkerIdentity {
    /// Agentic: the agent node's canonical id. Flow: the manifest `pack_id`.
    pub id: Option<String>,
    /// Flow: the manifest name. Agentic: omitted (an agent config carries no
    /// display name).
    pub name: Option<String>,
    /// The pack manifest version.
    pub version: Option<String>,
}

/// One turn's outcome, as the runtime hands it to a [`RunOutcomeSink`]. The
/// sink adds the attribution (tenant, deployment, unit, revision) and the
/// event id; nothing here is message content.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct RunOutcome {
    /// Stable across every turn of one run.
    pub run_id: String,
    /// The flow the run executes (after resume redirection).
    pub flow_id: String,
    pub kind: RunKind,
    pub status: RunStatus,
    /// Completed: the last node executed. In progress: the node the flow
    /// resumes at. Technical error: the failing node when known.
    pub last_step: Option<String>,
    /// `extensions.caller.sub` when the provider stamped one, else
    /// `<provider>:<envelope user>`.
    pub user_ref: Option<String>,
    /// `true` only when the provider's caller block says `user_verified`.
    pub user_verified: bool,
    /// The messaging provider the turn arrived on.
    pub channel: Option<String>,
    /// The flow's declared output map. Always `None` today — see module doc.
    pub outcome_json: Option<Value>,
    /// A short class for a technical error, never raw error text.
    pub error_code: Option<String>,
    /// When the run's FIRST turn started (RFC 3339, UTC, `Z`).
    pub started_at: String,
    /// 1 on the run's first event, +1 on every later event of the run.
    pub seq: u64,
    /// `in_progress` only: what the run waits for, and until when.
    pub wait: Option<WaitState>,
    pub worker: WorkerIdentity,
    /// `technical_error` only: the support reference (ULID).
    pub error_ref: Option<String>,
    /// `technical_error` only, when an error chain was available.
    pub error: Option<ErrorExcerpt>,
}

/// Receives one [`RunOutcome`] per flow turn.
///
/// `record` is called on the turn's own task, after the flow has run and
/// before the reply is returned: it must not block, must not fail, and must
/// not wait on the network. [`HttpRunOutcomeSink`] spawns its POST.
pub trait RunOutcomeSink: Send + Sync {
    fn record(&self, outcome: RunOutcome);
}

/// RFC 3339 with millisecond precision and a literal `Z`, the shape the
/// admin's ingest doors parse (same as the worker-usage meter).
pub(crate) fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
