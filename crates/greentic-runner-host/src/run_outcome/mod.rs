//! Deployed run audit: one outcome event per flow TURN, so the admin can list
//! the runs of a deployed unit as *completed*, *in progress* (read back as
//! *dropped off* once idle), *technical error* or *agentic*.
//!
//! Design: greentic-designer
//! `docs/superpowers/specs/2026-09-25-deployed-run-audit-design.md` §3.1.
//!
//! # Why the runner emits it
//!
//! `PackFlowAdapter::call_traced` (`engine/runtime.rs`) is the one place that
//! sees all three terminal shapes of a turn — `FlowStatus::Completed`,
//! `FlowStatus::Waiting` (with the node the user is parked on) and the engine
//! error. By the time the embedding host sees the reply, "waiting" has been
//! folded into it. A session also never announces that it ENDED (waits expire
//! silently), so every turn writes its status as it happens and "dropped off"
//! is derived at read time from an `in_progress` row that went idle.
//!
//! # A run spans turns
//!
//! A `run_id` (ULID) is minted when a turn starts with NO resume snapshot, and
//! stored beside the parked snapshot (`FlowResumeRecord.run`) so every resume
//! of that run reports under the same id. A snapshot parked by a runtime that
//! predates this field carries none, and its resume mints a fresh id — the
//! only cost is that such an in-flight run is split in two, once.
//!
//! Card-driven packs whose pause points are rendered cards (they re-enter via
//! `entry_node` and never park) leave no snapshot, so each of their turns is a
//! run of its own. That is a property of those packs, not of this module.
//!
//! # What never leaves the process
//!
//! Message text, node payloads and raw error text. `error_code` is a short
//! class (`timeout`, `secret_missing`, …) computed here; the error's own text
//! can name internal hosts and is never sent. `outcome_json` is reserved for a
//! flow's DECLARED output map; flows do not declare one today, so it is always
//! absent rather than filled with the reply (which is user-visible text).
//!
//! # No sink, no change
//!
//! Nothing here runs unless the embedding host installs a sink through
//! [`crate::runtime::RevisionHostOptions::with_run_outcome_sink`]. Without one
//! no id is minted, no observer is installed and the persisted wait is
//! byte-identical to before.

pub mod http;
pub(crate) mod turn;

use serde::Serialize;
use serde_json::Value;

pub use http::{HttpRunOutcomeSink, RunOutcomeSinkError, RunOutcomeTarget};

/// What kind of run an event describes. Serialised `snake_case`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    /// A plain flow run.
    Flow,
    /// A run that executed an agentic node (`dw.agent`, `dw.agent_graph`,
    /// `operala.call` deep worker, `agentic.call`). Reported with
    /// [`RunStatus::Agentic`] and nothing else: an agent's turn has no
    /// business "step" to stop at.
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
    /// Any turn of an agentic run.
    Agentic,
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
