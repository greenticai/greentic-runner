#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;
use crate::billing::BillingMeter;
use crate::tenant::TenantContext;

const TOKEN: &str = "wut_super_secret_token_value";

fn target(endpoint: &str) -> WorkerUsageTarget {
    WorkerUsageTarget {
        endpoint: endpoint.to_string(),
        token: TOKEN.to_string(),
        tenant_slug: "acme".to_string(),
        deployment_id: "dep-1".to_string(),
        bundle_id: "support-bot".to_string(),
    }
}

fn ingest_url(server: &MockServer) -> String {
    format!("{}/api/v1/ingest/worker-usage", server.uri())
}

/// Poll the mock until it has seen `n` requests, or give up after 3 s. The
/// POST is spawned, so `emit` returning says nothing about delivery yet.
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

// ── construction ────────────────────────────────────────────────────────────

#[test]
fn refuses_a_blank_field() {
    for blank in [
        "endpoint",
        "token",
        "tenant_slug",
        "deployment_id",
        "bundle_id",
    ] {
        let mut t = target("https://admin.example/api/v1/ingest/worker-usage");
        match blank {
            "endpoint" => t.endpoint = "  ".into(),
            "token" => t.token = "".into(),
            "tenant_slug" => t.tenant_slug = " ".into(),
            "deployment_id" => t.deployment_id = "".into(),
            _ => t.bundle_id = "\t".into(),
        }
        let err = WorkerUsageMeter::new(t).expect_err("a blank field must be refused");
        assert!(
            err.to_string().contains(blank),
            "the refusal names the field: {err}"
        );
    }
}

#[test]
fn refuses_a_cleartext_endpoint_off_this_host() {
    let err = WorkerUsageMeter::new(target("http://admin.example/ingest"))
        .expect_err("cleartext bearer off-host must be refused");
    assert!(err.to_string().contains("https"), "{err}");
    // The refusal never carries the token.
    assert!(!err.to_string().contains(TOKEN));
}

#[test]
fn accepts_https_and_loopback_http() {
    assert!(WorkerUsageMeter::new(target("https://admin.example/ingest")).is_ok());
    assert!(WorkerUsageMeter::new(target("http://127.0.0.1:9/ingest")).is_ok());
    assert!(WorkerUsageMeter::new(target("http://localhost:9/ingest")).is_ok());
    assert!(WorkerUsageMeter::new(target("http://[::1]:9/ingest")).is_ok());
}

#[test]
fn debug_never_prints_the_token() {
    let meter = WorkerUsageMeter::new(target("https://admin.example/ingest")).unwrap();
    let rendered = format!("{meter:?}");
    assert!(
        !rendered.contains(TOKEN),
        "Debug leaked the token: {rendered}"
    );
    assert!(rendered.contains("redacted"));
    let rendered_target = format!("{:?}", target("https://admin.example/ingest"));
    assert!(!rendered_target.contains(TOKEN), "{rendered_target}");
}

// ── the event body (designer spec §4.1) ────────────────────────────────────

#[test]
fn the_body_carries_the_ingest_fields_and_nothing_else() {
    let meter = WorkerUsageMeter::new(target("https://admin.example/ingest")).unwrap();
    let body = meter.build_event(120, 30, "assistant", "gpt-4o-mini");
    let value = serde_json::to_value(&body).unwrap();
    let mut keys: Vec<&str> = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "agent_id",
            "bundle_id",
            "deployment_id",
            "duration_ms",
            "event_id",
            "iterations",
            "model",
            "occurred_at",
            "surface",
            "tenant_slug",
            "tokens_in",
            "tokens_out",
        ],
        "no content-bearing or unexpected key may reach the admin"
    );
    assert_eq!(value["surface"], "turn");
    assert_eq!(value["tenant_slug"], "acme");
    assert_eq!(value["deployment_id"], "dep-1");
    assert_eq!(value["bundle_id"], "support-bot");
    assert_eq!(value["agent_id"], "assistant");
    assert_eq!(value["model"], "gpt-4o-mini");
    assert_eq!(value["tokens_in"], 120);
    assert_eq!(value["tokens_out"], 30);
    assert_eq!(value["iterations"], 1);
    assert_eq!(value["duration_ms"], 0);
    let occurred = value["occurred_at"].as_str().unwrap();
    assert!(
        occurred.ends_with('Z'),
        "RFC 3339 with a literal Z: {occurred}"
    );
    assert!(chrono::DateTime::parse_from_rfc3339(occurred).is_ok());
    assert!(!value["event_id"].as_str().unwrap().is_empty());
}

#[test]
fn every_event_gets_its_own_id() {
    let meter = WorkerUsageMeter::new(target("https://admin.example/ingest")).unwrap();
    let a = meter.build_event(1, 1, "a", "m");
    let b = meter.build_event(1, 1, "a", "m");
    assert_ne!(
        a.event_id, b.event_id,
        "event_id is the admin's idempotency key"
    );
}

#[test]
fn a_blank_model_is_omitted_rather_than_sent_empty() {
    // The admin records an absent model as "unknown"; an empty string would
    // become a bogus grouping bucket of its own.
    let meter = WorkerUsageMeter::new(target("https://admin.example/ingest")).unwrap();
    let value = serde_json::to_value(meter.build_event(1, 1, "a", "  ")).unwrap();
    assert!(value.get("model").is_none(), "{value}");
}

#[test]
fn the_bundle_comes_from_the_constructor_not_the_tenant_context() {
    // The token pins (tenant, env, unit) server-side; the runtime's own
    // TenantContext (tenant "default", env "local") must not leak in.
    let meter = WorkerUsageMeter::new(target("https://admin.example/ingest")).unwrap();
    let value = serde_json::to_value(meter.build_event(1, 1, "a", "m")).unwrap();
    assert_eq!(value["bundle_id"], "support-bot");
    assert_eq!(value["tenant_slug"], "acme");
}

// ── HTTP behaviour ─────────────────────────────────────────────────────────

#[tokio::test]
async fn emit_posts_one_event_with_the_bearer() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/ingest/worker-usage"))
        .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&server)
        .await;

    let meter = WorkerUsageMeter::new(target(&ingest_url(&server))).unwrap();
    let tenant = TenantContext::new("default", "local").with_project_id(Some("ignored".into()));
    meter
        .emit(&tenant, 11, 4, "assistant", "claude-3-haiku")
        .await
        .expect("emit is infallible");

    let requests = wait_for_requests(&server, 1).await;
    assert_eq!(requests.len(), 1, "one LLM iteration → one POST");
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["tokens_in"], 11);
    assert_eq!(body["tokens_out"], 4);
    assert_eq!(body["model"], "claude-3-haiku");
    assert_eq!(body["agent_id"], "assistant");
    assert_eq!(body["surface"], "turn");
    assert_eq!(body["bundle_id"], "support-bot");
    // The token is only ever a header — never in the URL or the body.
    assert!(!requests[0].url.as_str().contains(TOKEN));
    assert!(!String::from_utf8_lossy(&requests[0].body).contains(TOKEN));
}

#[tokio::test]
async fn a_rejected_or_unreachable_post_never_fails_the_turn() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let meter = WorkerUsageMeter::new(target(&ingest_url(&server))).unwrap();
    let tenant = TenantContext::new("default", "local");
    assert!(meter.emit(&tenant, 1, 1, "a", "m").await.is_ok());

    let dead = WorkerUsageMeter::new(target("http://127.0.0.1:1/ingest")).unwrap();
    assert!(dead.emit(&tenant, 1, 1, "a", "m").await.is_ok());
    assert_eq!(
        dead.deliver(dead.build_event(1, 1, "a", "m")).await,
        Delivery::Failed
    );
}

#[tokio::test]
async fn over_budget_is_always_false_and_asks_nobody() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let meter = WorkerUsageMeter::new(target(&ingest_url(&server))).unwrap();
    assert!(
        !meter
            .over_budget(&TenantContext::new("default", "local"))
            .await
    );
}

#[tokio::test]
async fn a_refused_token_suspends_further_posts() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;
    let meter = WorkerUsageMeter::new(target(&ingest_url(&server))).unwrap();

    assert_eq!(
        meter.deliver(meter.build_event(1, 1, "a", "m")).await,
        Delivery::Failed
    );
    // A second event inside the suspension window is dropped without a
    // request: a revoked token is not fixed by asking again every iteration.
    assert_eq!(
        meter.deliver(meter.build_event(1, 1, "a", "m")).await,
        Delivery::Suspended
    );
}

#[tokio::test]
async fn a_rate_limit_honours_retry_after() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "120"))
        .expect(1)
        .mount(&server)
        .await;
    let meter = WorkerUsageMeter::new(target(&ingest_url(&server))).unwrap();
    assert_eq!(
        meter.deliver(meter.build_event(1, 1, "a", "m")).await,
        Delivery::Failed
    );
    assert_eq!(
        meter.deliver(meter.build_event(1, 1, "a", "m")).await,
        Delivery::Suspended
    );
}

#[tokio::test]
async fn a_bad_request_drops_that_event_only() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400))
        .expect(2)
        .mount(&server)
        .await;
    let meter = WorkerUsageMeter::new(target(&ingest_url(&server))).unwrap();
    assert_eq!(
        meter.deliver(meter.build_event(1, 1, "a", "m")).await,
        Delivery::Rejected
    );
    assert_eq!(
        meter.deliver(meter.build_event(1, 1, "a", "m")).await,
        Delivery::Rejected,
        "a 400 is about this event, not the endpoint"
    );
}

#[tokio::test]
async fn an_accepted_duplicate_is_a_success() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(202)
                .set_body_json(serde_json::json!({"event_id": "x", "stored": false})),
        )
        .mount(&server)
        .await;
    let meter = WorkerUsageMeter::new(target(&ingest_url(&server))).unwrap();
    assert_eq!(
        meter.deliver(meter.build_event(1, 1, "a", "m")).await,
        Delivery::Accepted
    );
}

#[test]
fn emit_outside_a_tokio_runtime_drops_rather_than_panics() {
    let meter = WorkerUsageMeter::new(target("https://admin.example/ingest")).unwrap();
    let tenant = TenantContext::new("default", "local");
    let result = futures::executor::block_on(meter.emit(&tenant, 1, 1, "a", "m"));
    assert!(result.is_ok());
}
