//! Gated NATS round-trip integration test for `aw-event-bridge`.
//!
//! Self-skips when `GREENTIC_TEST_NATS_URL` is unset — no broker required for
//! CI. Set the env var to a live broker to run the full round-trip:
//!
//! ```text
//! GREENTIC_TEST_NATS_URL=nats://127.0.0.1:4222 \
//!   cargo test -p aw-event-bridge --test nats_roundtrip
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use aw_event_bridge::{AgentDispatchInvoker, InvokeOutcome, run_bridge};
use futures_util::StreamExt;
use greentic_types::{DispatchMode, RuntimeDispatchRequest, RuntimeDispatchResponse};
use serde_json::json;
use tokio::time::{Duration, timeout};

const REQUEST_SUBJECT: &str = "greentic.agentic.request.v1";
const RESPONSE_SUBJECT: &str = "greentic.agentic.response.v1";
const NATS_URL_ENV: &str = "GREENTIC_TEST_NATS_URL";

/// Canned-reply stub mimicking an agentic step (no LLM/Redis).
struct CannedReplyInvoker;

#[async_trait]
impl AgentDispatchInvoker for CannedReplyInvoker {
    async fn invoke(
        &self,
        _tenant: &str,
        _env: &str,
        target: &str,
        _operation: &str,
        input: serde_json::Value,
        _idempotency_key: Option<&str>,
    ) -> anyhow::Result<InvokeOutcome> {
        let user_text = input
            .get("user_text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        Ok(InvokeOutcome {
            ok: true,
            output: json!({
                "reply": format!("{target} heard: {user_text}"),
                "trail": [],
                "terminated_by": "final_reply",
            }),
            events: vec![],
            error: None,
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn nats_roundtrip_agentic_request_response() {
    let nats_url = match std::env::var(NATS_URL_ENV) {
        Ok(url) if !url.trim().is_empty() => url,
        _ => {
            println!("SKIP nats_roundtrip: {NATS_URL_ENV} not set — skipping (no broker)");
            return;
        }
    };

    let bridge_client = async_nats::connect(&nats_url)
        .await
        .expect("connect bridge client to NATS");
    let harness_client = async_nats::connect(&nats_url)
        .await
        .expect("connect harness client to NATS");

    let mut response_subscriber = harness_client
        .subscribe(RESPONSE_SUBJECT)
        .await
        .expect("subscribe to response subject");

    let bridge_handle = tokio::spawn(run_bridge(bridge_client, Arc::new(CannedReplyInvoker)));

    tokio::time::sleep(Duration::from_millis(50)).await;

    let request = RuntimeDispatchRequest {
        target: "greeter".into(),
        operation: String::new(),
        mode: DispatchMode::Await,
        input: json!({ "user_text": "ping" }),
        deadline_ms: Some(5_000),
    };
    let request_bytes: bytes::Bytes = serde_json::to_vec(&request).expect("serialize").into();

    // The runner encodes resume markers in the correlation id; verify the bridge
    // echoes them VERBATIM (the runner's resume path depends on it).
    let correlation = "sess-1::pack=p::flow=f::thread=T::reply=R";
    let mut request_headers = async_nats::HeaderMap::new();
    request_headers.insert("Greentic-Correlation-Id", correlation);
    request_headers.insert("Greentic-Idempotency-Key", correlation);
    request_headers.insert("Greentic-Tenant", "acme");
    request_headers.insert("Greentic-Env", "prod");

    harness_client
        .publish_with_headers(REQUEST_SUBJECT, request_headers, request_bytes)
        .await
        .expect("publish request");
    harness_client.flush().await.expect("flush publish");

    let response_message = timeout(Duration::from_secs(5), response_subscriber.next())
        .await
        .expect("timed out waiting for response (5s)")
        .expect("response subscriber closed before receiving a message");

    let response_headers = response_message
        .headers
        .as_ref()
        .expect("response must carry headers");
    assert_eq!(
        response_headers
            .get("Greentic-Correlation-Id")
            .expect("correlation header missing")
            .as_str(),
        correlation,
        "correlation id must be echoed verbatim (resume markers preserved)"
    );

    let response: RuntimeDispatchResponse =
        serde_json::from_slice(&response_message.payload).expect("deserialize response");
    assert!(response.ok, "response must indicate success");
    assert_eq!(
        response.output["reply"], "greeter heard: ping",
        "reply must reflect the dispatched agent + user text"
    );
    assert_eq!(response.output["terminated_by"], "final_reply");

    bridge_handle.abort();
}
