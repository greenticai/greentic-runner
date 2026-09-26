//! Turn-side half of the run audit: the run marker persisted with a parked
//! snapshot, the observer that remembers which node ran (trace mode or not),
//! and the pure functions that turn one finished turn into a [`RunOutcome`].
//!
//! The error chain a technical error carries is held here UNREDACTED and in
//! memory only; it is redacted and capped by [`super::error`] as the outcome
//! is built, before anything reaches a sink.

use std::error::Error as StdError;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::error::{ErrorExcerpt, anyhow_chain_text, chain_text, new_error_ref};
use super::wait::{ParkedNode, wait_state};
use super::{RunKind, RunOutcome, RunOutcomeSink, RunStatus, WorkerIdentity, now_rfc3339};
use crate::engine::runtime::IngressEnvelope;
use crate::runner::engine::FlowWait;
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
    /// it carries `kind: agentic`, so the admin badge never flips back.
    #[serde(default)]
    pub(crate) agentic: bool,
    /// How many events this run has emitted so far; the next one carries
    /// `seq + 1`. A marker written before the field decodes as 0.
    #[serde(default)]
    pub(crate) seq: u64,
    /// The first agentic node's worker id, so a later turn of an agentic run
    /// that runs no agent node still reports which worker it belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) agent_ref: Option<String>,
}

impl RunMarker {
    pub(crate) fn mint() -> Self {
        Self {
            run_id: ulid::Ulid::new().to_string(),
            started_at: now_rfc3339(),
            agentic: false,
            seq: 0,
            agent_ref: None,
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
    /// The failing node's error chain, UNREDACTED. In memory only; it leaves
    /// the process solely through [`ErrorExcerpt::build`].
    pub(crate) failed_chain: Option<String>,
    /// The first agentic node's worker id.
    pub(crate) agent_ref: Option<String>,
    /// The last node that started — the node a park happened at.
    pub(crate) parked: Option<ParkedNode>,
    /// The flow of the latest node event: after a `flow.goto` it is the
    /// target flow. A `flow.call` sub-flow's events move it, and the call
    /// node's own end / error event moves it back to the caller.
    pub(crate) flow_id: Option<String>,
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
            seen.flow_id = Some(event.context.flow_id.to_string());
            seen.parked = Some(ParkedNode {
                approval: event.node.is_approval(),
                response_timeout_secs: event.node.response_timeout_secs(),
            });
            if event.node.is_agentic() {
                seen.saw_agentic = true;
                if seen.agent_ref.is_none() {
                    seen.agent_ref = event.node.agent_ref().map(str::to_string);
                }
            }
        }
        if let Some(inner) = self.inner {
            inner.on_node_start(event);
        }
    }

    fn on_node_end(&self, event: &NodeEvent<'_>, output: &Value) {
        {
            let mut seen = self.seen.lock();
            seen.last_node = Some(event.node_id.to_string());
            seen.flow_id = Some(event.context.flow_id.to_string());
        }
        if let Some(inner) = self.inner {
            inner.on_node_end(event, output);
        }
    }

    fn on_node_error(&self, event: &NodeEvent<'_>, error: &dyn StdError) {
        {
            let mut seen = self.seen.lock();
            seen.failed_node = Some(event.node_id.to_string());
            seen.flow_id = Some(event.context.flow_id.to_string());
            let chain = chain_text(error);
            seen.failed_class = Some(classify_error_text(&chain));
            seen.failed_chain = Some(chain);
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
    /// The pack the turn runs: a flow run's worker identity, and the version
    /// an agentic run reports.
    pub(crate) pack: WorkerIdentity,
    /// Turn-level retries before this attempt (`TurnScope`), for the error
    /// excerpt.
    pub(crate) retries: u32,
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
            pack: WorkerIdentity::default(),
            retries: 0,
        }
    }

    /// Attach the pack's manifest identity (`pack_id`, name, version).
    pub(crate) fn with_pack(
        mut self,
        pack_id: String,
        name: Option<String>,
        version: String,
    ) -> Self {
        self.pack = WorkerIdentity {
            id: Some(pack_id),
            name,
            version: Some(version),
        };
        self
    }

    /// The `seq` this turn's event carries.
    fn seq(&self) -> u64 {
        self.marker.seq.saturating_add(1)
    }

    fn agent_ref(&self, observed: &ObservedTurn) -> Option<String> {
        self.marker
            .agent_ref
            .clone()
            .or_else(|| observed.agent_ref.clone())
    }

    /// The marker to persist with the next parked snapshot.
    /// The marker to persist once THIS turn's event is emitted: it carries
    /// the event's `seq`, so the next turn continues from it.
    pub(crate) fn marker_after(&self, observed: &ObservedTurn) -> RunMarker {
        RunMarker {
            agentic: self.marker.agentic || observed.saw_agentic,
            seq: self.seq(),
            agent_ref: self.agent_ref(observed),
            ..self.marker.clone()
        }
    }

    fn outcome(&self, observed: &ObservedTurn, status: RunStatus) -> RunOutcome {
        let agentic = self.marker.agentic || observed.saw_agentic;
        let worker = if agentic {
            WorkerIdentity {
                id: self.agent_ref(observed),
                name: None,
                version: self.pack.version.clone(),
            }
        } else {
            self.pack.clone()
        };
        RunOutcome {
            run_id: self.marker.run_id.clone(),
            flow_id: observed
                .flow_id
                .clone()
                .unwrap_or_else(|| self.flow_id.clone()),
            kind: if agentic {
                RunKind::Agentic
            } else {
                RunKind::Flow
            },
            status,
            last_step: None,
            user_ref: self.user_ref.clone(),
            user_verified: self.user_verified,
            channel: self.channel.clone(),
            outcome_json: None,
            error_code: None,
            started_at: self.marker.started_at.clone(),
            seq: self.seq(),
            wait: None,
            worker,
            error_ref: None,
            error: None,
        }
    }

    /// A technical error: `last_step`, `error_code`, a fresh `error_ref`, and
    /// the redacted excerpt when an error chain is known.
    fn technical_error(
        &self,
        observed: &ObservedTurn,
        node: Option<String>,
        code: String,
        chain: Option<&str>,
    ) -> RunOutcome {
        let error_ref = new_error_ref();
        let error = chain.map(|chain| {
            ErrorExcerpt::build(&error_ref, &code, node.as_deref(), self.retries, chain)
        });
        RunOutcome {
            last_step: node,
            error_code: Some(code),
            error_ref: Some(error_ref),
            error,
            ..self.outcome(observed, RunStatus::TechnicalError)
        }
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
                // The node's own chain when the observer saw it, else the
                // message the engine folded into the output. Redacted below.
                let chain = observed.failed_chain.clone().or_else(|| {
                    metadata
                        .and_then(|m| m.get("error_message"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                });
                self.technical_error(observed, node, code, chain.as_deref())
            }
            None => RunOutcome {
                last_step: observed.last_node.clone(),
                ..self.outcome(observed, RunStatus::Completed)
            },
        }
    }

    /// The flow parked: `last_step` is the node it resumes at, `flow_id` the
    /// flow it resumes in (a `flow.goto` target), and `wait` says what for.
    pub(crate) fn waiting(&self, observed: &ObservedTurn, wait: &FlowWait) -> RunOutcome {
        let snapshot = &wait.snapshot;
        RunOutcome {
            flow_id: snapshot
                .next_flow
                .clone()
                .unwrap_or_else(|| snapshot.flow_id.clone()),
            last_step: Some(snapshot.next_node.clone()),
            wait: Some(wait_state(wait.reason.as_deref(), observed.parked)),
            ..self.outcome(observed, RunStatus::InProgress)
        }
    }

    /// The engine returned an error. Its text picks the class and, redacted,
    /// becomes the excerpt; it is never the `error_code`.
    pub(crate) fn failed(&self, observed: &ObservedTurn, error: &anyhow::Error) -> RunOutcome {
        let chain = anyhow_chain_text(error);
        let node = observed
            .failed_node
            .clone()
            .or_else(|| observed.last_node.clone());
        let code = observed
            .failed_class
            .unwrap_or_else(|| match classify_error_text(&chain) {
                "node_failed" => FLOW_EXECUTION_FAILED,
                other => other,
            });
        self.technical_error(observed, node, code.to_string(), Some(&chain))
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
    /// Attempts of this turn so far ([`RunOutcomeReporter::remember`]).
    attempts: u32,
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

    /// Remember this attempt's marker for any retry of the same turn, and
    /// return how many attempts of this turn ran BEFORE this one.
    pub(crate) fn remember(&self, marker: &RunMarker) -> u32 {
        with_scope(|scope| {
            scope.marker = Some(marker.clone());
            let before = scope.attempts;
            scope.attempts = scope.attempts.saturating_add(1);
            before
        })
        .unwrap_or(0)
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
