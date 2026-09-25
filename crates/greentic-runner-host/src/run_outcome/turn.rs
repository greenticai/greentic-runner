//! Turn-side half of the run audit: the run marker persisted with a parked
//! snapshot, the observer that remembers which node ran (trace mode or not),
//! and the pure functions that turn one finished turn into a [`RunOutcome`].

use std::error::Error as StdError;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{RunKind, RunOutcome, RunOutcomeSink, RunStatus, now_rfc3339};
use crate::engine::runtime::IngressEnvelope;
use crate::runner::engine::{ExecutionObserver, NodeEvent};
use crate::validate::ValidationIssue;

/// The placeholder `IngressEnvelope::canonicalize` writes when a producer
/// named no provider. It names nothing, so it is not reported as a channel.
const PLACEHOLDER_PROVIDER: &str = "provider";

/// Longest `error_code` accepted from a node's own `error.kind`.
const MAX_ERROR_CODE_LEN: usize = 64;

/// Error class used when nothing narrower can be said.
const FLOW_EXECUTION_FAILED: &str = "flow_execution_failed";

/// Stored beside a parked snapshot (`FlowResumeRecord.run`) so every resume of
/// a run reports under the id its first turn minted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RunMarker {
    pub(crate) run_id: String,
    pub(crate) started_at: String,
    /// Sticky: once a run has executed an agentic node, every later event for
    /// it is agentic, so the admin row never flips back to a flow status.
    #[serde(default)]
    pub(crate) agentic: bool,
}

impl RunMarker {
    pub(crate) fn mint() -> Self {
        Self {
            run_id: ulid::Ulid::new().to_string(),
            started_at: now_rfc3339(),
            agentic: false,
        }
    }
}

/// What the observer saw during one turn.
#[derive(Clone, Debug, Default)]
pub(crate) struct ObservedTurn {
    /// The most recent node started or finished.
    pub(crate) last_node: Option<String>,
    /// The node whose dispatch returned an error, if any.
    pub(crate) failed_node: Option<String>,
    /// A short class computed from that error — never its text.
    pub(crate) failed_class: Option<&'static str>,
    /// Whether an agentic node started.
    pub(crate) saw_agentic: bool,
}

/// An [`ExecutionObserver`] that records [`ObservedTurn`] and forwards every
/// callback to the trace recorder when one is installed. Installed only when a
/// sink is, so a host without one runs exactly as before.
pub(crate) struct RunOutcomeObserver<'a> {
    inner: Option<&'a dyn ExecutionObserver>,
    seen: Mutex<ObservedTurn>,
}

impl<'a> RunOutcomeObserver<'a> {
    pub(crate) fn new(inner: Option<&'a dyn ExecutionObserver>) -> Self {
        Self {
            inner,
            seen: Mutex::new(ObservedTurn::default()),
        }
    }

    pub(crate) fn observed(&self) -> ObservedTurn {
        self.seen.lock().clone()
    }
}

impl ExecutionObserver for RunOutcomeObserver<'_> {
    fn on_node_start(&self, event: &NodeEvent<'_>) {
        {
            let mut seen = self.seen.lock();
            seen.last_node = Some(event.node_id.to_string());
            if event.node.is_agentic() {
                seen.saw_agentic = true;
            }
        }
        if let Some(inner) = self.inner {
            inner.on_node_start(event);
        }
    }

    fn on_node_end(&self, event: &NodeEvent<'_>, output: &Value) {
        self.seen.lock().last_node = Some(event.node_id.to_string());
        if let Some(inner) = self.inner {
            inner.on_node_end(event, output);
        }
    }

    fn on_node_error(&self, event: &NodeEvent<'_>, error: &dyn StdError) {
        {
            let mut seen = self.seen.lock();
            seen.failed_node = Some(event.node_id.to_string());
            seen.failed_class = Some(classify_error_text(&error.to_string()));
        }
        if let Some(inner) = self.inner {
            inner.on_node_error(event, error);
        }
    }

    fn on_validation(&self, event: &NodeEvent<'_>, issues: &[ValidationIssue]) {
        if let Some(inner) = self.inner {
            inner.on_validation(event, issues);
        }
    }
}

/// Map an error's text onto a short class. The text itself is read here and
/// nowhere downstream.
pub(crate) fn classify_error_text(text: &str) -> &'static str {
    let lower = text.to_ascii_lowercase();
    if lower.contains("timed out") || lower.contains("timeout") || lower.contains("deadline") {
        "timeout"
    } else if lower.contains("secret") {
        "secret_missing"
    } else if lower.contains("rate limit") || lower.contains("too many requests") {
        "rate_limited"
    } else if lower.contains("component") {
        "component_failed"
    } else {
        "node_failed"
    }
}

/// A node-declared `error.kind` is accepted as a code only if it already looks
/// like one: lowercase ASCII letters, digits and `_`, at most 64 bytes.
fn sanitize_code(raw: &str) -> Option<String> {
    let valid = !raw.is_empty()
        && raw.len() <= MAX_ERROR_CODE_LEN
        && raw
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    valid.then(|| raw.to_string())
}

/// Hands one turn's outcome to the installed sink.
#[derive(Clone)]
pub(crate) struct RunOutcomeReporter {
    sink: Arc<dyn RunOutcomeSink>,
}

/// Everything about a turn that is fixed before the flow runs.
#[derive(Clone, Debug)]
pub(crate) struct TurnContext {
    pub(crate) marker: RunMarker,
    pub(crate) flow_id: String,
    pub(crate) user_ref: Option<String>,
    pub(crate) user_verified: bool,
    pub(crate) channel: Option<String>,
}

impl TurnContext {
    /// Build the turn's context. `resumed` is the marker stored with the
    /// snapshot this turn resumes; absent (a fresh run, or a snapshot parked
    /// before markers existed) mints a new run.
    pub(crate) fn begin(
        envelope: &IngressEnvelope,
        caller_block: Option<&Value>,
        resumed: Option<RunMarker>,
        flow_id: &str,
    ) -> Self {
        let (user_ref, user_verified) = user_ref(envelope, caller_block);
        Self {
            marker: resumed.unwrap_or_else(RunMarker::mint),
            flow_id: flow_id.to_string(),
            user_ref,
            user_verified,
            channel: envelope
                .provider
                .as_deref()
                .map(str::trim)
                .filter(|p| !p.is_empty() && *p != PLACEHOLDER_PROVIDER)
                .map(str::to_string),
        }
    }

    /// The marker to persist with the next parked snapshot.
    pub(crate) fn marker_after(&self, observed: &ObservedTurn) -> RunMarker {
        RunMarker {
            agentic: self.marker.agentic || observed.saw_agentic,
            ..self.marker.clone()
        }
    }

    fn outcome(&self, observed: &ObservedTurn, status: RunStatus) -> RunOutcome {
        let agentic = self.marker.agentic || observed.saw_agentic;
        RunOutcome {
            run_id: self.marker.run_id.clone(),
            flow_id: self.flow_id.clone(),
            kind: if agentic {
                RunKind::Agentic
            } else {
                RunKind::Flow
            },
            status: if agentic { RunStatus::Agentic } else { status },
            last_step: None,
            user_ref: self.user_ref.clone(),
            user_verified: self.user_verified,
            channel: self.channel.clone(),
            outcome_json: None,
            error_code: None,
            started_at: self.marker.started_at.clone(),
        }
    }

    /// An agentic outcome carries status and identity only.
    fn with_detail(
        &self,
        mut outcome: RunOutcome,
        last_step: Option<String>,
        error_code: Option<String>,
    ) -> RunOutcome {
        if outcome.kind == RunKind::Flow {
            outcome.last_step = last_step;
            outcome.error_code = error_code;
        }
        outcome
    }

    /// The flow reached its end. A session flow's terminal failure also ends
    /// here — the engine folds it into a completed output carrying
    /// `metadata.error_kind` so the provider can render it — and is reported
    /// as the technical error it is.
    pub(crate) fn completed(&self, observed: &ObservedTurn, output: &Value) -> RunOutcome {
        let metadata = output.get("metadata");
        let error_kind = metadata
            .and_then(|m| m.get("error_kind"))
            .and_then(Value::as_str);
        match error_kind {
            Some(kind) => {
                let node = metadata
                    .and_then(|m| m.get("node_id"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| observed.failed_node.clone())
                    .or_else(|| observed.last_node.clone());
                let code = observed
                    .failed_class
                    .map(str::to_string)
                    .or_else(|| sanitize_code(kind))
                    .unwrap_or_else(|| FLOW_EXECUTION_FAILED.to_string());
                let outcome = self.outcome(observed, RunStatus::TechnicalError);
                self.with_detail(outcome, node, Some(code))
            }
            None => {
                let outcome = self.outcome(observed, RunStatus::Completed);
                self.with_detail(outcome, observed.last_node.clone(), None)
            }
        }
    }

    /// The flow parked at `next_node`.
    pub(crate) fn waiting(&self, observed: &ObservedTurn, next_node: &str) -> RunOutcome {
        let outcome = self.outcome(observed, RunStatus::InProgress);
        self.with_detail(outcome, Some(next_node.to_string()), None)
    }

    /// The engine returned an error.
    pub(crate) fn failed(&self, observed: &ObservedTurn, error_text: &str) -> RunOutcome {
        let node = observed
            .failed_node
            .clone()
            .or_else(|| observed.last_node.clone());
        let code = observed
            .failed_class
            .unwrap_or_else(|| match classify_error_text(error_text) {
                "node_failed" => FLOW_EXECUTION_FAILED,
                other => other,
            });
        let outcome = self.outcome(observed, RunStatus::TechnicalError);
        self.with_detail(outcome, node, Some(code.to_string()))
    }
}

/// `extensions.caller.sub` when the provider stamped a caller block naming
/// one, else `<provider>:<user>` from the envelope.
fn user_ref(envelope: &IngressEnvelope, caller_block: Option<&Value>) -> (Option<String>, bool) {
    if let Some(block) = caller_block {
        let sub = block
            .get("sub")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(sub) = sub {
            let verified = block
                .get("user_verified")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            return (Some(sub.to_string()), verified);
        }
    }
    let user = envelope
        .user
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty());
    let provider = envelope
        .provider
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .unwrap_or(PLACEHOLDER_PROVIDER);
    (user.map(|u| format!("{provider}:{u}")), false)
}

/// One inbound turn, across the retries that wrap it.
///
/// `engine::state_machine` retries a failed adapter call (up to 5 attempts),
/// so a failing turn re-enters `PackFlowAdapter::call_traced` several times.
/// Without this scope a FRESH run would mint a new `run_id` per attempt and
/// report five technical errors for one message. Inside it, every attempt
/// reuses the first attempt's marker, a failure is held as PENDING, and the
/// scope's owner reports it once after the last attempt
/// ([`RunOutcomeReporter::scoped`]). A success reports immediately and clears
/// any pending failure from an earlier attempt.
#[derive(Debug, Default)]
struct TurnScope {
    marker: Option<RunMarker>,
    pending_failure: Option<RunOutcome>,
}

tokio::task_local! {
    static TURN_SCOPE: Arc<Mutex<TurnScope>>;
}

fn with_scope<R>(f: impl FnOnce(&mut TurnScope) -> R) -> Option<R> {
    TURN_SCOPE.try_with(|scope| f(&mut scope.lock())).ok()
}

impl RunOutcomeReporter {
    pub(crate) fn new(sink: Arc<dyn RunOutcomeSink>) -> Self {
        Self { sink }
    }

    /// Run one turn (all of its retries) inside a [`TurnScope`], then report
    /// the failure the last attempt left pending, if the turn failed.
    pub(crate) async fn scoped<T, E, F>(&self, turn: F) -> Result<T, E>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        let scope = Arc::new(Mutex::new(TurnScope::default()));
        let result = TURN_SCOPE.scope(Arc::clone(&scope), turn).await;
        let pending = scope.lock().pending_failure.take();
        if result.is_err()
            && let Some(outcome) = pending
        {
            self.sink.record(outcome);
        }
        result
    }

    /// The marker an EARLIER attempt of this same turn minted, if any. A
    /// retried attempt must not mint a second run.
    pub(crate) fn retried_marker(&self) -> Option<RunMarker> {
        with_scope(|scope| scope.marker.clone()).flatten()
    }

    /// Remember this attempt's marker for any retry of the same turn.
    pub(crate) fn remember(&self, marker: &RunMarker) {
        with_scope(|scope| scope.marker = Some(marker.clone()));
    }

    /// Report a turn that reached a flow status.
    pub(crate) fn record(&self, outcome: RunOutcome) {
        with_scope(|scope| scope.pending_failure = None);
        self.sink.record(outcome);
    }

    /// Report a failed attempt: held until the turn's retries are exhausted
    /// when inside a scope, reported at once otherwise.
    pub(crate) fn record_failure(&self, outcome: RunOutcome) {
        let held = with_scope(|scope| scope.pending_failure = Some(outcome.clone()));
        if held.is_none() {
            self.sink.record(outcome);
        }
    }
}

impl std::fmt::Debug for RunOutcomeReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunOutcomeReporter").finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "turn_tests.rs"]
mod tests;
