#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

const TOKEN: &str = "wut_super_secret_token_value";
const INGEST_PATH: &str = "/api/v1/ingest/run-outcome";

fn target(endpoint: &str) -> RunOutcomeTarget {
    RunOutcomeTarget {
        endpoint: endpoint.to_string(),
        token: TOKEN.to_string(),
        tenant_slug: "acme".to_string(),
        deployment_id: "dep-1".to_string(),
        bundle_id: "support-bot".to_string(),
        revision_id: "rev-1".to_string(),
    }
}

fn ingest_url(server: &MockServer) -> String {
    format!("{}{INGEST_PATH}", server.uri())
}

fn outcome() -> RunOutcome {
    RunOutcome {
        run_id: "01J8ZZZZZZZZZZZZZZZZZZZZZZ".into(),
        flow_id: "main".into(),
        kind: RunKind::Flow,
        status: RunStatus::TechnicalError,
        last_step: Some("call_api".into()),
        user_ref: Some("webchat:u-7".into()),
        user_verified: false,
        channel: Some("webchat".into()),
        outcome_json: None,
        error_code: Some("timeout".into()),
        started_at: "2026-09-25T09:00:00.000Z".into(),
    }
}

async fn wait_for_requests(server: &MockServer, n: usize) -> Vec<wiremock::Request> {
    for _ in 0..60 {
        let got = server.received_requests().await.unwrap_or_default();
        if got.len() >= n {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    server.received_requests().await.unwrap_or_default()
}

#[test]
fn construction_refuses_blank_fields_and_cleartext_endpoints() {
    let mut t = target("https://admin.example/api/v1/ingest/run-outcome");
    t.revision_id = " ".into();
    assert!(matches!(
        HttpRunOutcomeSink::new(t),
        Err(RunOutcomeSinkError::Blank("revision_id"))
    ));
    assert!(matches!(
        HttpRunOutcomeSink::new(target("http://admin.example/x")),
        Err(RunOutcomeSinkError::UnsafeEndpoint(_))
    ));
    assert!(HttpRunOutcomeSink::new(target("http://127.0.0.1:9/x")).is_ok());
    let mut t = target("https://admin.example/x");
    t.bundle_id = "b".repeat(MAX_FIELD_BYTES + 1);
    assert!(matches!(
        HttpRunOutcomeSink::new(t),
        Err(RunOutcomeSinkError::TooLong("bundle_id"))
    ));
}

#[test]
fn debug_never_prints_the_token() {
    let sink = HttpRunOutcomeSink::new(target("https://admin.example/x")).unwrap();
    assert!(!format!("{sink:?}").contains(TOKEN));
    assert!(!format!("{:?}", target("https://admin.example/x")).contains(TOKEN));
}

#[test]
fn the_wire_body_is_snake_case_with_attribution_from_the_target() {
    let sink = HttpRunOutcomeSink::new(target("https://admin.example/x")).unwrap();
    let event = sink.build_event(outcome()).unwrap();
    let body = serde_json::to_value(&event).unwrap();
    assert_eq!(body["tenant_slug"], "acme");
    assert_eq!(body["deployment_id"], "dep-1");
    assert_eq!(body["bundle_id"], "support-bot");
    assert_eq!(body["revision_id"], "rev-1");
    assert_eq!(body["run_id"], "01J8ZZZZZZZZZZZZZZZZZZZZZZ");
    assert_eq!(body["kind"], "flow");
    assert_eq!(body["status"], "technical_error");
    assert_eq!(body["last_step"], "call_api");
    assert_eq!(body["error_code"], "timeout");
    assert_eq!(body["user_verified"], false);
    assert_eq!(body["event_id"].as_str().unwrap().len(), 26);
    assert!(body["occurred_at"].as_str().unwrap().ends_with('Z'));
    assert!(body.get("outcome_json").is_none(), "absent, never null");
    assert!(!body.to_string().contains(TOKEN));
}

#[test]
fn over_long_ids_drop_the_event_and_over_long_labels_are_omitted() {
    let sink = HttpRunOutcomeSink::new(target("https://admin.example/x")).unwrap();
    let mut bad = outcome();
    bad.run_id = String::new();
    assert!(sink.build_event(bad).is_err());
    let mut bad = outcome();
    bad.flow_id = "f".repeat(MAX_FIELD_BYTES + 1);
    assert!(sink.build_event(bad).is_err());

    let mut long = outcome();
    long.user_ref = Some("u".repeat(MAX_FIELD_BYTES + 1));
    long.outcome_json = Some(json!({ "blob": "x".repeat(MAX_OUTCOME_JSON_BYTES) }));
    let event = sink.build_event(long).unwrap();
    assert_eq!(event.user_ref, None);
    assert_eq!(event.outcome_json, None);

    let mut small = outcome();
    small.outcome_json = Some(json!({ "ticket": 42 }));
    assert_eq!(
        sink.build_event(small).unwrap().outcome_json,
        Some(json!({ "ticket": 42 }))
    );
}

#[tokio::test]
async fn record_posts_with_the_bearer_and_returns_immediately() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(INGEST_PATH))
        .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
        .respond_with(ResponseTemplate::new(202).set_delay(Duration::from_millis(300)))
        .mount(&server)
        .await;
    let sink = HttpRunOutcomeSink::new(target(&ingest_url(&server))).unwrap();

    let started = std::time::Instant::now();
    sink.record(outcome());
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "record must not wait on the network"
    );
    let requests = wait_for_requests(&server, 1).await;
    assert_eq!(requests.len(), 1);
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["status"], "technical_error");
    assert_eq!(body["run_id"], "01J8ZZZZZZZZZZZZZZZZZZZZZZ");
}

#[tokio::test]
async fn each_event_carries_a_fresh_event_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let sink = HttpRunOutcomeSink::new(target(&ingest_url(&server))).unwrap();
    sink.record(outcome());
    sink.record(outcome());
    let requests = wait_for_requests(&server, 2).await;
    let ids: Vec<String> = requests
        .iter()
        .map(|r| {
            serde_json::from_slice::<Value>(&r.body).unwrap()["event_id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
}

#[tokio::test]
async fn a_refused_token_suspends_the_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;
    let sink = HttpRunOutcomeSink::new(target(&ingest_url(&server))).unwrap();
    let first = sink.build_event(outcome()).unwrap();
    assert_eq!(sink.deliver(first).await, Delivery::Failed);
    assert!(sink.suspended_for().unwrap() > Duration::from_secs(200));
    let second = sink.build_event(outcome()).unwrap();
    assert_eq!(sink.deliver(second).await, Delivery::Suspended);
}

#[tokio::test]
async fn a_429_honours_retry_after_capped() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "7"))
        .mount(&server)
        .await;
    let sink = HttpRunOutcomeSink::new(target(&ingest_url(&server))).unwrap();
    assert_eq!(
        sink.deliver(sink.build_event(outcome()).unwrap()).await,
        Delivery::Failed
    );
    let left = sink.suspended_for().unwrap();
    assert!(left <= Duration::from_secs(7) && left > Duration::from_secs(5));
}

#[tokio::test]
async fn a_5xx_suspends_briefly() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let sink = HttpRunOutcomeSink::new(target(&ingest_url(&server))).unwrap();
    assert_eq!(
        sink.deliver(sink.build_event(outcome()).unwrap()).await,
        Delivery::Failed
    );
    let left = sink.suspended_for().unwrap();
    assert!(left <= Duration::from_secs(30) && left > Duration::from_secs(25));
}

#[tokio::test]
async fn a_streak_of_4xx_suspends_but_a_single_one_does_not() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let sink = HttpRunOutcomeSink::new(target(&ingest_url(&server))).unwrap();
    assert_eq!(
        sink.deliver(sink.build_event(outcome()).unwrap()).await,
        Delivery::Rejected
    );
    assert!(sink.suspended_for().is_none());
    for _ in 0..4 {
        sink.deliver(sink.build_event(outcome()).unwrap()).await;
    }
    assert!(sink.suspended_for().is_some());
}

#[tokio::test]
async fn a_full_in_flight_queue_drops_rather_than_waits() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let sink = HttpRunOutcomeSink::new(target(&ingest_url(&server))).unwrap();
    let semaphore = sink.in_flight_semaphore();
    let held = semaphore.acquire_many(MAX_IN_FLIGHT as u32).await.unwrap();
    sink.record(outcome());
    assert_eq!(sink.dropped_saturated(), 1);
    drop(held);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
    );
}

#[test]
fn record_outside_a_runtime_does_not_panic() {
    let sink = HttpRunOutcomeSink::new(target("https://admin.example/x")).unwrap();
    sink.record(outcome());
}
