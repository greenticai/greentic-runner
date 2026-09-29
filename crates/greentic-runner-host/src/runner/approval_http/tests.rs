#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use greentic_types::{DispatchMode, TenantCtx};
use parking_lot::Mutex;
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use super::*;
use crate::runner::dispatch_listener::SessionResumer;

const BEARER: &str = "gtm_unit_bearer_secret";
const REQUEST_PATH: &str = "/api/v1/ingest/approval-request";
const DECISION_PATH: &str = "/api/v1/ingest/approval-decision";
const WITHDRAW_PATH: &str = "/api/v1/ingest/approval-withdraw";
const ID: &str = "acme:webchat:chan:conv:user::pack=p::flow=f::n=0123456789abcdef0123456789abcdef";
const HELD: &str = "held-decision-token-value";

#[derive(Default)]
struct RecordingResumer {
    calls: Mutex<Vec<(TenantCtx, String, Value)>>,
}

#[async_trait]
impl SessionResumer for RecordingResumer {
    async fn resume(
        &self,
        tenant: TenantCtx,
        correlation_id: &str,
        output: Value,
    ) -> anyhow::Result<()> {
        self.calls
            .lock()
            .push((tenant, correlation_id.to_string(), output));
        Ok(())
    }
}

fn fast() -> Timing {
    Timing {
        send_retry_delays: vec![Duration::from_millis(10), Duration::from_millis(10)],
        fast_interval: Duration::from_millis(20),
        fast_window: Duration::from_secs(60),
        slow_interval: Duration::from_millis(20),
        backoff: Duration::from_millis(20),
        max_wait: MAX_APPROVAL_WAIT,
    }
}

fn target(server: &MockServer) -> ApprovalInboxTarget {
    ApprovalInboxTarget {
        request_url: format!("{}{REQUEST_PATH}", server.uri()),
        decision_url: format!("{}{DECISION_PATH}", server.uri()),
        withdraw_url: format!("{}{WITHDRAW_PATH}", server.uri()),
        token: BEARER.to_string(),
        tenant_slug: "acme".to_string(),
    }
}

fn inbox(server: &MockServer, timing: Timing) -> (HttpApprovalDispatcher, Arc<RecordingResumer>) {
    let dispatcher = HttpApprovalDispatcher::with_timing(target(server), timing).unwrap();
    let resumer = Arc::new(RecordingResumer::default());
    dispatcher.attach_resumer(resumer.clone());
    (dispatcher, resumer)
}

fn approval(deadline_ms: Option<u64>) -> RemoteDispatch {
    RemoteDispatch {
        tenant: "acme".into(),
        env: "local".into(),
        runtime: "approval".into(),
        target: "".into(),
        operation: "approve".into(),
        mode: DispatchMode::Await,
        correlation_id: ID.into(),
        input: json!({"mode": "always", "title": "Refund"}),
        deadline_ms,
        decision_token: Some(HELD.into()),
    }
}

async fn accept_requests(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(REQUEST_PATH))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": "row-1"})))
        .mount(server)
        .await;
}

async fn requests_to(server: &MockServer, wanted: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.url.path() == wanted)
        .collect()
}

async fn eventually(mut check: impl FnMut() -> bool) {
    for _ in 0..200 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached");
}

#[tokio::test]
async fn the_request_carries_no_decision_token_and_names_the_transport() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(REQUEST_PATH))
        .and(header("authorization", format!("Bearer {BEARER}").as_str()))
        .and(header(APPROVAL_RAIL_HEADER, APPROVAL_RAIL_VERSION))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": "row-1"})))
        .expect(1)
        .mount(&server)
        .await;
    let (dispatcher, _) = inbox(&server, fast());

    let action = dispatcher.dispatch(approval(None)).await.unwrap();
    assert!(
        matches!(action, RemoteDispatchAction::AwaitingResponse { ref correlation_id } if correlation_id == ID)
    );

    let sent = requests_to(&server, REQUEST_PATH).await;
    let raw = String::from_utf8(sent[0].body.clone()).unwrap();
    assert!(
        !raw.contains(HELD),
        "the decision_token must never leave the worker"
    );
    assert!(!raw.contains("decision_token"));
    assert!(!raw.contains("routing"));
    let body: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(body["tenant_slug"], "acme");
    assert_eq!(body["correlation_id"], ID);
    assert_eq!(body["request"]["mode"], "await");
    assert_eq!(body["request"]["input"]["title"], "Refund");
    assert_eq!(
        body["request"]["deadline_ms"],
        json!(MAX_APPROVAL_WAIT.as_millis() as u64),
        "a node with no deadline gets the 7-day default"
    );
    let agent = sent[0].headers.get("user-agent").unwrap().to_str().unwrap();
    assert!(agent.contains("approval-rail/http/1"), "got {agent}");
    dispatcher.shutdown();
}

#[tokio::test]
async fn a_transient_send_failure_is_retried_and_a_refusal_fails_the_node() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(REQUEST_PATH))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    accept_requests(&server).await;
    let (dispatcher, _) = inbox(&server, fast());
    dispatcher.dispatch(approval(None)).await.unwrap();
    assert_eq!(requests_to(&server, REQUEST_PATH).await.len(), 2);
    dispatcher.shutdown();

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(REQUEST_PATH))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;
    let (dispatcher, _) = inbox(&server, fast());
    let err = dispatcher.dispatch(approval(None)).await.unwrap_err();
    assert!(err.to_string().contains("400"), "got {err}");
    assert_eq!(dispatcher.pending_count(), 0, "a refused send never parks");
    assert_eq!(requests_to(&server, REQUEST_PATH).await.len(), 1);
    assert!(!err.to_string().contains(BEARER));
}

#[tokio::test]
async fn the_pending_cap_is_not_retried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(REQUEST_PATH))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_json(json!({"error": {"code": "approval_pending_cap"}})),
        )
        .mount(&server)
        .await;
    let (dispatcher, _) = inbox(&server, fast());
    let err = dispatcher.dispatch(approval(None)).await.unwrap_err();
    assert!(
        err.to_string().contains("approval_pending_cap"),
        "got {err}"
    );
    assert_eq!(requests_to(&server, REQUEST_PATH).await.len(), 1);
}

#[tokio::test]
async fn exhausted_retries_fail_the_node() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(REQUEST_PATH))
        .respond_with(ResponseTemplate::new(502))
        .mount(&server)
        .await;
    let (dispatcher, _) = inbox(&server, fast());
    assert!(dispatcher.dispatch(approval(None)).await.is_err());
    assert_eq!(requests_to(&server, REQUEST_PATH).await.len(), 3);
    assert_eq!(dispatcher.pending_count(), 0);
}

#[tokio::test]
async fn a_decision_resumes_once_with_the_held_token_injected() {
    let server = MockServer::start().await;
    accept_requests(&server).await;
    Mock::given(method("GET"))
        .and(path(DECISION_PATH))
        .and(query_param("correlation_id", ID))
        .respond_with(ResponseTemplate::new(204))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(DECISION_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"decision": "approved", "resolved_by": "ops@acme", "note": "ok"}),
        ))
        .mount(&server)
        .await;
    let (dispatcher, resumer) = inbox(&server, fast());
    dispatcher.dispatch(approval(None)).await.unwrap();

    eventually(|| !resumer.calls.lock().is_empty()).await;
    // Give a duplicate a chance to happen: the decision door keeps answering 200.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let calls = resumer.calls.lock().clone();
    assert_eq!(calls.len(), 1, "a decision resumes exactly once");
    let (tenant, id, output) = &calls[0];
    assert_eq!(id, ID);
    assert_eq!(tenant.tenant_id.as_str(), "acme");
    assert_eq!(tenant.env.as_str(), "local");
    assert_eq!(output["ok"], true);
    assert_eq!(output["output"]["decision"], "approved");
    assert_eq!(output["output"]["resolved_by"], "ops@acme");
    assert_eq!(output["output"]["decision_token"], HELD);
    assert_eq!(output["error"], Value::Null);
    assert_eq!(dispatcher.pending_count(), 0, "the held token is dropped");
    let polls = requests_to(&server, DECISION_PATH).await.len();
    assert_eq!(polls, 3, "the first 200 ends polling");
    for poll in requests_to(&server, DECISION_PATH).await {
        assert_eq!(
            poll.headers.get(APPROVAL_RAIL_HEADER).unwrap(),
            APPROVAL_RAIL_VERSION
        );
    }
    dispatcher.shutdown();
}

#[tokio::test]
async fn a_denial_resumes_as_denied() {
    let server = MockServer::start().await;
    accept_requests(&server).await;
    Mock::given(method("GET"))
        .and(path(DECISION_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"decision": "denied"})))
        .mount(&server)
        .await;
    let (dispatcher, resumer) = inbox(&server, fast());
    dispatcher.dispatch(approval(None)).await.unwrap();
    eventually(|| !resumer.calls.lock().is_empty()).await;
    let output = resumer.calls.lock()[0].2.clone();
    assert_eq!(output["output"]["decision"], "denied");
    assert_eq!(output["output"]["decision_token"], HELD);
    dispatcher.shutdown();
}

#[tokio::test]
async fn a_withdrawn_or_refused_id_stops_polling_without_resuming() {
    for status in [410u16, 401, 403, 404] {
        let server = MockServer::start().await;
        accept_requests(&server).await;
        Mock::given(method("GET"))
            .and(path(DECISION_PATH))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        let (dispatcher, resumer) = inbox(&server, fast());
        dispatcher.dispatch(approval(None)).await.unwrap();
        eventually(|| dispatcher.pending_count() == 0).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            resumer.calls.lock().is_empty(),
            "{status} must never resume"
        );
        assert_eq!(
            requests_to(&server, DECISION_PATH).await.len(),
            1,
            "{status} stops polling"
        );
        dispatcher.shutdown();
    }
}

#[tokio::test]
async fn a_transient_poll_failure_keeps_waiting() {
    let server = MockServer::start().await;
    accept_requests(&server).await;
    Mock::given(method("GET"))
        .and(path(DECISION_PATH))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(DECISION_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"decision": "approved"})))
        .mount(&server)
        .await;
    let (dispatcher, resumer) = inbox(&server, fast());
    dispatcher.dispatch(approval(None)).await.unwrap();
    eventually(|| !resumer.calls.lock().is_empty()).await;
    assert_eq!(resumer.calls.lock().len(), 1);
    dispatcher.shutdown();
}

#[tokio::test]
async fn the_deadline_withdraws_and_resumes_locally_with_the_token() {
    let server = MockServer::start().await;
    accept_requests(&server).await;
    Mock::given(method("GET"))
        .and(path(DECISION_PATH))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(WITHDRAW_PATH))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let (dispatcher, resumer) = inbox(&server, fast());
    dispatcher.dispatch(approval(Some(120))).await.unwrap();

    eventually(|| !resumer.calls.lock().is_empty()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let calls = resumer.calls.lock().clone();
    assert_eq!(calls.len(), 1, "one timeout, never a second");
    let output = &calls[0].2;
    assert_eq!(output["ok"], false);
    assert_eq!(output["error"]["code"], "timeout");
    assert_eq!(output["output"]["decision_token"], HELD);

    let withdrawn = requests_to(&server, WITHDRAW_PATH).await;
    let body: Value = serde_json::from_slice(&withdrawn[0].body).unwrap();
    assert_eq!(
        body,
        json!({"tenant_slug": "acme", "correlation_id": ID, "reason": "timeout"})
    );
    let request: Value =
        serde_json::from_slice(&requests_to(&server, REQUEST_PATH).await[0].body).unwrap();
    assert_eq!(request["request"]["deadline_ms"], 120);
    dispatcher.shutdown();
}

#[tokio::test]
async fn a_deadline_past_the_cap_is_withdrawn_as_poll_cap() {
    let server = MockServer::start().await;
    accept_requests(&server).await;
    Mock::given(method("GET"))
        .and(path(DECISION_PATH))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(WITHDRAW_PATH))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let mut timing = fast();
    timing.max_wait = Duration::from_millis(100);
    let (dispatcher, resumer) = inbox(&server, timing);
    dispatcher.dispatch(approval(Some(60_000))).await.unwrap();
    eventually(|| !resumer.calls.lock().is_empty()).await;
    let withdrawn = requests_to(&server, WITHDRAW_PATH).await;
    let body: Value = serde_json::from_slice(&withdrawn[0].body).unwrap();
    assert_eq!(body["reason"], "poll_cap");
    assert_eq!(resumer.calls.lock()[0].2["error"]["code"], "timeout");
    dispatcher.shutdown();
}

#[tokio::test]
async fn a_decision_cancels_the_timeout() {
    let server = MockServer::start().await;
    accept_requests(&server).await;
    Mock::given(method("GET"))
        .and(path(DECISION_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"decision": "approved"})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(WITHDRAW_PATH))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;
    let (dispatcher, resumer) = inbox(&server, fast());
    dispatcher.dispatch(approval(Some(200))).await.unwrap();
    eventually(|| !resumer.calls.lock().is_empty()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let calls = resumer.calls.lock().clone();
    assert_eq!(calls.len(), 1, "the decision is the one outcome");
    assert_eq!(calls[0].2["output"]["decision"], "approved");
    dispatcher.shutdown();
}

#[tokio::test]
async fn shutdown_stops_polling_and_releases_the_resumer() {
    let server = MockServer::start().await;
    accept_requests(&server).await;
    Mock::given(method("GET"))
        .and(path(DECISION_PATH))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let (dispatcher, resumer) = inbox(&server, fast());
    dispatcher.dispatch(approval(None)).await.unwrap();
    for _ in 0..200 {
        if requests_to(&server, DECISION_PATH).await.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    dispatcher.shutdown();
    assert!(dispatcher.cancellation_token().is_cancelled());
    assert_eq!(dispatcher.pending_count(), 0, "held tokens are forgotten");
    eventually(|| Arc::strong_count(&resumer) == 1).await;
    let polls = requests_to(&server, DECISION_PATH).await.len();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        requests_to(&server, DECISION_PATH).await.len(),
        polls,
        "a shut-down runtime never polls again"
    );
    assert!(resumer.calls.lock().is_empty());
    assert!(
        dispatcher.dispatch(approval(None)).await.is_err(),
        "a shut-down inbox files nothing"
    );
}

#[tokio::test]
async fn a_non_approval_dispatch_is_refused() {
    let server = MockServer::start().await;
    let (dispatcher, _) = inbox(&server, fast());
    let mut dispatch = approval(None);
    dispatch.runtime = "sorla".into();
    assert!(dispatcher.dispatch(dispatch).await.is_err());
    assert!(requests_to(&server, REQUEST_PATH).await.is_empty());
}

#[test]
fn an_unsafe_or_blank_target_is_refused() {
    let mut t = ApprovalInboxTarget {
        request_url: "http://admin.example.com/api/v1/ingest/approval-request".into(),
        decision_url: "https://admin.example.com/api/v1/ingest/approval-decision".into(),
        withdraw_url: "https://admin.example.com/api/v1/ingest/approval-withdraw".into(),
        token: BEARER.into(),
        tenant_slug: "acme".into(),
    };
    assert!(matches!(
        HttpApprovalDispatcher::new(t.clone()),
        Err(ApprovalInboxError::UnsafeUrl(_))
    ));
    t.request_url = "https://admin.example.com/api/v1/ingest/approval-request".into();
    assert!(HttpApprovalDispatcher::new(t.clone()).is_ok());
    t.token = " ".into();
    assert!(matches!(
        HttpApprovalDispatcher::new(t.clone()),
        Err(ApprovalInboxError::Blank("token"))
    ));
    assert!(!format!("{t:?}").contains(BEARER));
}

#[test]
fn nats_stays_authoritative_over_the_inbox() {
    let t = ApprovalInboxTarget {
        request_url: "https://a/x".into(),
        decision_url: "https://a/y".into(),
        withdraw_url: "https://a/z".into(),
        token: BEARER.into(),
        tenant_slug: "acme".into(),
    };
    assert!(inbox_to_install(true, Some(t.clone())).is_none());
    assert!(inbox_to_install(false, Some(t)).is_some());
    assert!(inbox_to_install(false, None).is_none());
}

#[test]
fn the_decision_body_decodes_and_ignores_unknown_fields() {
    let body: ApprovalDecisionBody = serde_json::from_value(
        json!({"decision": "approved", "resolved_by": "a", "note": null, "extra": 1}),
    )
    .unwrap();
    assert_eq!(body.decision, ApprovalDecisionKind::Approved);
    assert!(serde_json::from_value::<ApprovalDecisionBody>(json!({"decision": "maybe"})).is_err());
}
