#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;

use super::*;
use crate::runner::engine::{FlowContext, HostNode, RetryConfig};

fn envelope() -> IngressEnvelope {
    IngressEnvelope {
        tenant: "demo".into(),
        env: Some("local".into()),
        pack_id: Some("pack.demo".into()),
        flow_id: "flow.main".into(),
        flow_type: None,
        action: Some("messaging".into()),
        session_hint: Some("demo:webchat:chan:conv:u-7".into()),
        provider: Some("webchat".into()),
        messaging_endpoint_id: None,
        channel: Some("chan".into()),
        conversation: Some("conv".into()),
        user: Some("u-7".into()),
        entry_node: None,
        activity_id: Some("act-1".into()),
        timestamp: None,
        payload: json!({ "text": "my password is hunter2" }),
        metadata: None,
        reply_scope: None,
    }
}

fn fresh_turn() -> TurnContext {
    TurnContext::begin(&envelope(), None, None, "flow.main")
}

fn observed(last: &str) -> ObservedTurn {
    ObservedTurn {
        last_node: Some(last.to_string()),
        ..ObservedTurn::default()
    }
}

#[test]
fn a_fresh_turn_mints_a_ulid_run_and_a_resumed_turn_keeps_its_marker() {
    let fresh = fresh_turn();
    assert_eq!(fresh.marker.run_id.len(), 26, "ULID string");
    assert!(fresh.marker.started_at.ends_with('Z'));

    let stored = RunMarker {
        run_id: "01HZZZZZZZZZZZZZZZZZZZZZZZ".into(),
        started_at: "2026-09-25T09:00:00.000Z".into(),
        agentic: false,
    };
    let resumed = TurnContext::begin(&envelope(), None, Some(stored.clone()), "flow.main");
    assert_eq!(resumed.marker, stored);
    assert_ne!(fresh.marker.run_id, fresh_turn().marker.run_id);
}

#[test]
fn a_marker_written_before_the_agentic_flag_decodes() {
    let marker: RunMarker =
        serde_json::from_value(json!({ "run_id": "r1", "started_at": "t" })).unwrap();
    assert!(!marker.agentic);
}

#[test]
fn completed_reports_the_last_node() {
    let outcome = fresh_turn().completed(&observed("done"), &json!({ "text": "bye" }));
    assert_eq!(outcome.kind, RunKind::Flow);
    assert_eq!(outcome.status, RunStatus::Completed);
    assert_eq!(outcome.last_step.as_deref(), Some("done"));
    assert_eq!(outcome.error_code, None);
    assert_eq!(outcome.outcome_json, None);
    assert_eq!(outcome.flow_id, "flow.main");
}

#[test]
fn waiting_reports_the_node_the_flow_resumes_at() {
    let outcome = fresh_turn().waiting(&observed("ask"), "collect_name");
    assert_eq!(outcome.status, RunStatus::InProgress);
    assert_eq!(outcome.last_step.as_deref(), Some("collect_name"));
}

#[test]
fn a_session_failure_folded_into_a_completed_output_is_a_technical_error() {
    let output = json!({
        "text": "Something went wrong",
        "metadata": {
            "error_kind": "flow_execution_failed",
            "error_message": "component http.fetch failed: GET https://internal.corp/secret-api 500",
        }
    });
    let seen = ObservedTurn {
        last_node: Some("fetch".into()),
        failed_node: Some("fetch".into()),
        failed_class: Some("component_failed"),
        saw_agentic: false,
    };
    let outcome = fresh_turn().completed(&seen, &output);
    assert_eq!(outcome.status, RunStatus::TechnicalError);
    assert_eq!(outcome.last_step.as_deref(), Some("fetch"));
    assert_eq!(outcome.error_code.as_deref(), Some("component_failed"));
}

#[test]
fn a_lifted_node_error_uses_its_node_id_and_a_sanitised_kind() {
    let output = json!({
        "metadata": { "error_kind": "upstream_503", "error_message": "raw", "node_id": "call_api" }
    });
    let outcome = fresh_turn().completed(&observed("reply"), &output);
    assert_eq!(outcome.status, RunStatus::TechnicalError);
    assert_eq!(outcome.last_step.as_deref(), Some("call_api"));
    assert_eq!(outcome.error_code.as_deref(), Some("upstream_503"));

    // A kind that is not already a short token is never forwarded.
    let output = json!({ "metadata": { "error_kind": "Upstream said: no way" } });
    let outcome = fresh_turn().completed(&ObservedTurn::default(), &output);
    assert_eq!(outcome.error_code.as_deref(), Some("flow_execution_failed"));
}

#[test]
fn an_engine_error_is_classified_never_forwarded() {
    let text = "flow execution failed: secret openai/api_key not found at https://vault.internal";
    let outcome = fresh_turn().failed(&ObservedTurn::default(), text);
    assert_eq!(outcome.status, RunStatus::TechnicalError);
    assert_eq!(outcome.error_code.as_deref(), Some("secret_missing"));
    assert_eq!(outcome.last_step, None);

    let outcome = fresh_turn().failed(&ObservedTurn::default(), "flow x has no node `y`");
    assert_eq!(outcome.error_code.as_deref(), Some("flow_execution_failed"));

    let seen = ObservedTurn {
        last_node: Some("a".into()),
        failed_node: Some("b".into()),
        failed_class: Some("timeout"),
        saw_agentic: false,
    };
    let outcome = fresh_turn().failed(&seen, "whatever");
    assert_eq!(outcome.last_step.as_deref(), Some("b"));
    assert_eq!(outcome.error_code.as_deref(), Some("timeout"));
}

#[test]
fn classes_cover_the_documented_set() {
    assert_eq!(classify_error_text("request TIMED OUT"), "timeout");
    assert_eq!(classify_error_text("missing secret foo"), "secret_missing");
    assert_eq!(classify_error_text("429 Too Many Requests"), "rate_limited");
    assert_eq!(
        classify_error_text("component foo trapped"),
        "component_failed"
    );
    assert_eq!(classify_error_text("boom"), "node_failed");
}

#[test]
fn an_agentic_turn_reports_status_agentic_and_nothing_else() {
    let seen = ObservedTurn {
        last_node: Some("agent".into()),
        failed_node: Some("agent".into()),
        failed_class: Some("timeout"),
        saw_agentic: true,
    };
    let turn = fresh_turn();
    for outcome in [
        turn.completed(&seen, &json!({ "metadata": { "error_kind": "x" } })),
        turn.waiting(&seen, "agent"),
        turn.failed(&seen, "boom"),
    ] {
        assert_eq!(outcome.kind, RunKind::Agentic);
        assert_eq!(outcome.status, RunStatus::Agentic);
        assert_eq!(outcome.last_step, None);
        assert_eq!(outcome.error_code, None);
        assert_eq!(outcome.outcome_json, None);
    }
    // Sticky across turns: the next marker stays agentic.
    let marker = turn.marker_after(&seen);
    assert!(marker.agentic);
    let next = TurnContext::begin(&envelope(), None, Some(marker), "flow.main");
    let outcome = next.completed(&observed("card"), &json!({}));
    assert_eq!(outcome.status, RunStatus::Agentic);
}

#[test]
fn user_ref_prefers_the_callers_subject_and_its_verification() {
    let caller = json!({ "user_verified": true, "sub": " u-1@acme " });
    let turn = TurnContext::begin(&envelope(), Some(&caller), None, "f");
    assert_eq!(turn.user_ref.as_deref(), Some("u-1@acme"));
    assert!(turn.user_verified);

    let claim = json!({ "sub": "u-2" });
    let turn = TurnContext::begin(&envelope(), Some(&claim), None, "f");
    assert_eq!(turn.user_ref.as_deref(), Some("u-2"));
    assert!(!turn.user_verified);

    let turn = TurnContext::begin(&envelope(), None, None, "f");
    assert_eq!(turn.user_ref.as_deref(), Some("webchat:u-7"));
    assert!(!turn.user_verified);
    assert_eq!(turn.channel.as_deref(), Some("webchat"));
}

#[test]
fn the_placeholder_provider_is_not_a_channel() {
    let mut env = envelope();
    env.provider = Some("provider".into());
    let turn = TurnContext::begin(&env, None, None, "f");
    assert_eq!(turn.channel, None);
    assert_eq!(turn.user_ref.as_deref(), Some("provider:u-7"));
}

#[derive(Default)]
struct Counting {
    starts: AtomicUsize,
    ends: AtomicUsize,
    errors: AtomicUsize,
}

impl ExecutionObserver for Counting {
    fn on_node_start(&self, _event: &NodeEvent<'_>) {
        self.starts.fetch_add(1, Ordering::Relaxed);
    }
    fn on_node_end(&self, _event: &NodeEvent<'_>, _output: &Value) {
        self.ends.fetch_add(1, Ordering::Relaxed);
    }
    fn on_node_error(&self, _event: &NodeEvent<'_>, _error: &dyn StdError) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn the_observer_records_nodes_and_forwards_every_callback() {
    let inner = Counting::default();
    let observer = RunOutcomeObserver::new(Some(&inner));
    let ctx = FlowContext {
        tenant: "demo",
        pack_id: "p",
        flow_id: "f",
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        reply_scope: None,
        retry_config: RetryConfig {
            max_attempts: 1,
            base_delay_ms: 1,
        },
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    };
    let payload = json!({ "text": "hunter2" });
    let plain = HostNode::for_test("comp.a", None);
    let agent = HostNode::for_test_dw_agent("helper");

    let first = NodeEvent {
        context: &ctx,
        node_id: "first",
        node: &plain,
        payload: &payload,
    };
    observer.on_node_start(&first);
    observer.on_node_end(&first, &payload);
    let seen = observer.observed();
    assert_eq!(seen.last_node.as_deref(), Some("first"));
    assert!(!seen.saw_agentic);

    let second = NodeEvent {
        context: &ctx,
        node_id: "second",
        node: &agent,
        payload: &payload,
    };
    observer.on_node_start(&second);
    let error = std::io::Error::other("request timed out after 30s");
    observer.on_node_error(&second, &error);
    let seen = observer.observed();
    assert!(seen.saw_agentic);
    assert_eq!(seen.failed_node.as_deref(), Some("second"));
    assert_eq!(seen.failed_class, Some("timeout"));

    assert_eq!(inner.starts.load(Ordering::Relaxed), 2);
    assert_eq!(inner.ends.load(Ordering::Relaxed), 1);
    assert_eq!(inner.errors.load(Ordering::Relaxed), 1);
}

#[test]
fn the_observer_works_without_a_trace_recorder() {
    let observer = RunOutcomeObserver::new(None);
    assert!(observer.observed().last_node.is_none());
}

#[derive(Default)]
struct Collect(parking_lot::Mutex<Vec<RunOutcome>>);

impl RunOutcomeSink for Collect {
    fn record(&self, outcome: RunOutcome) {
        self.0.lock().push(outcome);
    }
}

#[tokio::test]
async fn retries_of_one_turn_share_a_run_and_report_one_failure() {
    let sink = Arc::new(Collect::default());
    let reporter = RunOutcomeReporter::new(Arc::clone(&sink) as Arc<dyn RunOutcomeSink>);
    let result: Result<(), &str> = reporter
        .scoped(async {
            let mut ids = Vec::new();
            for _ in 0..3 {
                let resumed = reporter.retried_marker();
                let turn = TurnContext::begin(&envelope(), None, resumed, "flow.main");
                reporter.remember(&turn.marker);
                ids.push(turn.marker.run_id.clone());
                reporter.record_failure(turn.failed(&ObservedTurn::default(), "boom"));
            }
            assert!(ids.windows(2).all(|w| w[0] == w[1]), "one run: {ids:?}");
            Err("exhausted")
        })
        .await;
    assert!(result.is_err());
    let recorded = sink.0.lock();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].status, RunStatus::TechnicalError);
}

#[tokio::test]
async fn a_retry_that_succeeds_drops_the_earlier_failure() {
    let sink = Arc::new(Collect::default());
    let reporter = RunOutcomeReporter::new(Arc::clone(&sink) as Arc<dyn RunOutcomeSink>);
    let result: Result<(), &str> = reporter
        .scoped(async {
            let turn = fresh_turn();
            reporter.record_failure(turn.failed(&ObservedTurn::default(), "boom"));
            reporter.record(turn.completed(&observed("done"), &json!({})));
            Ok(())
        })
        .await;
    assert!(result.is_ok());
    let recorded = sink.0.lock();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].status, RunStatus::Completed);
}

#[test]
fn outside_a_scope_a_failure_is_reported_at_once() {
    let sink = Arc::new(Collect::default());
    let reporter = RunOutcomeReporter::new(Arc::clone(&sink) as Arc<dyn RunOutcomeSink>);
    assert!(reporter.retried_marker().is_none());
    reporter.record_failure(fresh_turn().failed(&ObservedTurn::default(), "boom"));
    assert_eq!(sink.0.lock().len(), 1);
}
