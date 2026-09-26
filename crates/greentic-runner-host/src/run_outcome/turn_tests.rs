#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;

use super::*;
use crate::run_outcome::WaitKind;
use crate::runner::engine::{ExecutionState, FlowContext, FlowSnapshot, HostNode, RetryConfig};

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

fn wait_at(node: &str, reason: Option<&str>, next_flow: Option<&str>) -> FlowWait {
    let state: ExecutionState = serde_json::from_value(json!({
        "input": {},
        "nodes": {},
        "egress": []
    }))
    .expect("state");
    FlowWait {
        reason: reason.map(str::to_string),
        snapshot: FlowSnapshot {
            pack_id: "pack.demo".into(),
            flow_id: "flow.main".into(),
            next_flow: next_flow.map(str::to_string),
            next_node: node.into(),
            awaiting_submit: false,
            state,
        },
    }
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
        seq: 4,
        agent_ref: None,
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
    let outcome = fresh_turn().waiting(&observed("ask"), &wait_at("collect_name", None, None));
    assert_eq!(outcome.status, RunStatus::InProgress);
    assert_eq!(outcome.last_step.as_deref(), Some("collect_name"));
    assert_eq!(outcome.flow_id, "flow.main");
    let wait = outcome.wait.expect("an in_progress event carries its wait");
    assert_eq!(wait.kind, WaitKind::UserInput);
    assert!(wait.response_due_at.is_some());
    assert_eq!(outcome.error_ref, None);
}

#[test]
fn a_parked_goto_target_is_the_flow_the_event_reports() {
    let outcome = fresh_turn().waiting(
        &observed("ask"),
        &wait_at(
            "ask",
            Some("awaiting user submit at node `ask`"),
            Some("support"),
        ),
    );
    assert_eq!(outcome.flow_id, "support");
    // A completed walk reports the flow its last node ran in.
    let seen = ObservedTurn {
        last_node: Some("bye".into()),
        flow_id: Some("support".into()),
        ..ObservedTurn::default()
    };
    assert_eq!(fresh_turn().completed(&seen, &json!({})).flow_id, "support");
}

#[test]
fn seq_starts_at_one_and_the_stored_marker_carries_it() {
    let turn = fresh_turn();
    let first = turn.waiting(&observed("ask"), &wait_at("ask", None, None));
    assert_eq!(first.seq, 1);
    let marker = turn.marker_after(&observed("ask"));
    assert_eq!(marker.seq, 1);
    let next = TurnContext::begin(&envelope(), None, Some(marker), "flow.main");
    let second = next.completed(&observed("done"), &json!({}));
    assert_eq!(
        (second.seq, second.run_id.as_str()),
        (2, first.run_id.as_str())
    );
}

#[test]
fn a_marker_written_before_seq_starts_its_next_event_at_one() {
    let marker: RunMarker =
        serde_json::from_value(json!({ "run_id": "r1", "started_at": "t", "agentic": true }))
            .unwrap();
    assert_eq!(marker.seq, 0);
    let turn = TurnContext::begin(&envelope(), None, Some(marker), "flow.main");
    assert_eq!(turn.completed(&observed("x"), &json!({})).seq, 1);
    // A marker without an agent ref serialises without the key.
    let fresh = serde_json::to_value(RunMarker::mint()).unwrap();
    assert!(fresh.get("agent_ref").is_none(), "{fresh}");
}

#[test]
fn a_technical_error_carries_a_ref_and_a_redacted_excerpt() {
    let err = anyhow::anyhow!("401 Bearer abc from secrets://default/acme/_/crm/token")
        .context("component crm.lookup failed");
    let seen = ObservedTurn {
        failed_node: Some("crm".into()),
        failed_class: Some("component_failed"),
        ..ObservedTurn::default()
    };
    let mut turn = fresh_turn();
    turn.retries = 2;
    let outcome = turn.failed(&seen, &err);
    let error_ref = outcome.error_ref.clone().expect("error_ref");
    assert_eq!(error_ref.len(), 26, "ULID");
    let excerpt = outcome.error.expect("excerpt");
    assert_eq!(excerpt.error_ref, error_ref);
    assert_eq!(excerpt.error_code, "component_failed");
    assert_eq!(excerpt.node_id.as_deref(), Some("crm"));
    assert_eq!(excerpt.retry_count, Some(2));
    assert_eq!(
        excerpt.safe_summary,
        "Node `crm` failed after 2 retries: component_failed"
    );
    assert!(
        excerpt
            .redacted_excerpt
            .starts_with("component crm.lookup failed\ncaused by: ")
    );
    assert!(!excerpt.redacted_excerpt.contains("secrets://"));
    assert!(!excerpt.redacted_excerpt.contains("abc"));

    // A completed-with-error turn uses the observed chain, else the message
    // the engine folded into the output.
    let output = json!({ "metadata": { "error_kind": "flow_execution_failed",
        "error_message": "GET https://api.example/x?key=s3cr3t failed" } });
    let outcome = fresh_turn().completed(&ObservedTurn::default(), &output);
    let excerpt = outcome.error.expect("excerpt");
    assert!(
        !excerpt.redacted_excerpt.contains("s3cr3t"),
        "{}",
        excerpt.redacted_excerpt
    );
    assert!(outcome.error_ref.is_some());

    // A completed run carries neither.
    let done = fresh_turn().completed(&observed("done"), &json!({}));
    assert!(done.error_ref.is_none() && done.error.is_none() && done.wait.is_none());
}

#[test]
fn the_worker_is_the_pack_for_a_flow_and_the_agent_for_an_agentic_run() {
    let turn = fresh_turn().with_pack("pack.demo".into(), Some("Demo".into()), "1.2.0".into());
    let flow = turn.completed(&observed("done"), &json!({}));
    assert_eq!(flow.worker.id.as_deref(), Some("pack.demo"));
    assert_eq!(flow.worker.name.as_deref(), Some("Demo"));
    assert_eq!(flow.worker.version.as_deref(), Some("1.2.0"));

    let seen = ObservedTurn {
        saw_agentic: true,
        agent_ref: Some("helper".into()),
        ..ObservedTurn::default()
    };
    let agentic = turn.completed(&seen, &json!({}));
    assert_eq!(agentic.worker.id.as_deref(), Some("helper"));
    assert_eq!(agentic.worker.name, None);
    assert_eq!(agentic.worker.version.as_deref(), Some("1.2.0"));
    // The agent survives into later turns that run no agent node.
    let next = TurnContext::begin(
        &envelope(),
        None,
        Some(turn.marker_after(&seen)),
        "flow.main",
    );
    let later = next.completed(&observed("card"), &json!({}));
    assert_eq!(later.kind, RunKind::Agentic);
    assert_eq!(later.worker.id.as_deref(), Some("helper"));
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
        ..ObservedTurn::default()
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
    let outcome = fresh_turn().failed(&ObservedTurn::default(), &anyhow::anyhow!(text));
    assert_eq!(outcome.status, RunStatus::TechnicalError);
    assert_eq!(outcome.error_code.as_deref(), Some("secret_missing"));
    assert_eq!(outcome.last_step, None);

    let outcome = fresh_turn().failed(
        &ObservedTurn::default(),
        &anyhow::anyhow!("flow x has no node `y`"),
    );
    assert_eq!(outcome.error_code.as_deref(), Some("flow_execution_failed"));

    let seen = ObservedTurn {
        last_node: Some("a".into()),
        failed_node: Some("b".into()),
        failed_class: Some("timeout"),
        saw_agentic: false,
        ..ObservedTurn::default()
    };
    let outcome = fresh_turn().failed(&seen, &anyhow::anyhow!("whatever"));
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
fn an_agentic_turn_reports_real_statuses_steps_and_codes() {
    let seen = ObservedTurn {
        last_node: Some("agent".into()),
        failed_node: Some("agent".into()),
        failed_class: Some("timeout"),
        saw_agentic: true,
        ..ObservedTurn::default()
    };
    let turn = fresh_turn();
    let failed = turn.completed(&seen, &json!({ "metadata": { "error_kind": "x" } }));
    assert_eq!(failed.status, RunStatus::TechnicalError);
    assert_eq!(failed.last_step.as_deref(), Some("agent"));
    assert_eq!(failed.error_code.as_deref(), Some("timeout"));
    let parked = turn.waiting(&seen, &wait_at("agent", None, None));
    assert_eq!(parked.status, RunStatus::InProgress);
    assert_eq!(parked.last_step.as_deref(), Some("agent"));
    let errored = turn.failed(&seen, &anyhow::anyhow!("boom"));
    assert_eq!(errored.status, RunStatus::TechnicalError);
    for outcome in [&failed, &parked, &errored] {
        assert_eq!(outcome.kind, RunKind::Agentic);
        assert_eq!(outcome.outcome_json, None);
    }
    // Sticky across turns: the next marker stays agentic.
    let marker = turn.marker_after(&seen);
    assert!(marker.agentic);
    let next = TurnContext::begin(&envelope(), None, Some(marker), "flow.main");
    let outcome = next.completed(&observed("card"), &json!({}));
    assert_eq!(outcome.kind, RunKind::Agentic);
    assert_eq!(outcome.status, RunStatus::Completed);
    assert_eq!(outcome.last_step.as_deref(), Some("card"));
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
    assert_eq!(seen.agent_ref.as_deref(), Some("helper"));
    assert_eq!(seen.failed_node.as_deref(), Some("second"));
    assert_eq!(seen.failed_class, Some("timeout"));
    assert_eq!(
        seen.failed_chain.as_deref(),
        Some("request timed out after 30s")
    );
    assert_eq!(seen.flow_id.as_deref(), Some("f"));

    // The last node that started is the park point, with its timeout.
    let gate = HostNode::for_test_approval("hitl");
    let card = HostNode::for_test("card", None).with_response_timeout(90);
    for (node, approval, timeout) in [(&gate, true, None), (&card, false, Some(90))] {
        observer.on_node_start(&NodeEvent {
            context: &ctx,
            node_id: "park",
            node,
            payload: &payload,
        });
        let parked = observer.observed().parked.expect("parked");
        assert_eq!(
            (parked.approval, parked.response_timeout_secs),
            (approval, timeout)
        );
    }

    assert_eq!(inner.starts.load(Ordering::Relaxed), 4);
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
            for attempt in 0..3 {
                let resumed = reporter.retried_marker();
                let mut turn = TurnContext::begin(&envelope(), None, resumed, "flow.main");
                turn.retries = reporter.remember(&turn.marker);
                assert_eq!(turn.retries, attempt);
                ids.push(turn.marker.run_id.clone());
                reporter.record_failure(
                    turn.failed(&ObservedTurn::default(), &anyhow::anyhow!("boom")),
                );
            }
            assert!(ids.windows(2).all(|w| w[0] == w[1]), "one run: {ids:?}");
            Err("exhausted")
        })
        .await;
    assert!(result.is_err());
    let recorded = sink.0.lock();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].status, RunStatus::TechnicalError);
    assert_eq!(recorded[0].seq, 1, "retries of one turn are one event");
    let excerpt = recorded[0].error.as_ref().expect("excerpt");
    assert_eq!(excerpt.retry_count, Some(2));
}

#[tokio::test]
async fn a_retry_that_succeeds_drops_the_earlier_failure() {
    let sink = Arc::new(Collect::default());
    let reporter = RunOutcomeReporter::new(Arc::clone(&sink) as Arc<dyn RunOutcomeSink>);
    let result: Result<(), &str> = reporter
        .scoped(async {
            let turn = fresh_turn();
            reporter
                .record_failure(turn.failed(&ObservedTurn::default(), &anyhow::anyhow!("boom")));
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
    reporter
        .record_failure(fresh_turn().failed(&ObservedTurn::default(), &anyhow::anyhow!("boom")));
    assert_eq!(sink.0.lock().len(), 1);
}
