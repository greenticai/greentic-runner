//! Subscribes to runtime response messages on `greentic.<runtime>.response.v1`
//! and resumes the paused flow session whose correlation id matches.
//!
//! The production `SessionResumer` (built separately) synthesizes an ingress
//! envelope and feeds the runtime's resume path; here we only define the seam.

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use greentic_types::{EnvId, RuntimeDispatchResponse, TenantCtx, TenantId, response_topic};
use serde_json::Value;
use std::sync::Arc;

/// Decoded resume input extracted from a response message.
pub struct ResumeInput {
    pub tenant: TenantCtx,
    pub correlation_id: String,
    pub output: Value,
}

/// Resume a waiting flow session identified by the opaque `correlation_id`
/// (= canonical session hint), feeding it `output`.
#[async_trait]
pub trait SessionResumer: Send + Sync {
    async fn resume(&self, tenant: TenantCtx, correlation_id: &str, output: Value) -> Result<()>;
}

/// Pure: turn response headers + JSON body into a `ResumeInput`. No I/O.
///
/// # Arguments
/// * `correlation_id` - value of the `Greentic-Correlation-Id` header
/// * `tenant` - value of the `Greentic-Tenant` header (falls back to `"default"`)
/// * `env` - value of the `Greentic-Env` header (falls back to `"default"`)
/// * `payload` - raw JSON bytes of a [`RuntimeDispatchResponse`]
///
/// # Errors
/// Returns an error if `correlation_id` is missing/empty, if the tenant/env
/// identifiers are invalid, or if the payload cannot be parsed.
pub fn decode_response(
    correlation_id: Option<&str>,
    tenant: Option<&str>,
    env: Option<&str>,
    payload: &[u8],
) -> Result<ResumeInput> {
    let correlation_id = correlation_id
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("dispatch response missing Greentic-Correlation-Id"))?
        .to_string();

    let tenant_str = tenant.filter(|s| !s.is_empty()).unwrap_or("default");
    let env_str = env.filter(|s| !s.is_empty()).unwrap_or("default");

    let tenant_id = TenantId::try_from(tenant_str)
        .map_err(|error| anyhow!("invalid Greentic-Tenant header value: {error}"))?;
    let env_id = EnvId::try_from(env_str)
        .map_err(|error| anyhow!("invalid Greentic-Env header value: {error}"))?;

    let response: RuntimeDispatchResponse = serde_json::from_slice(payload)?;

    let output = serde_json::json!({
        "ok": response.ok,
        "output": response.output,
        "events": response.events,
        "error": response.error,
    });

    Ok(ResumeInput {
        tenant: TenantCtx::new(env_id, tenant_id),
        correlation_id,
        output,
    })
}

/// Forward a decoded input to the resumer, logging failures.
pub async fn dispatch_to_resumer(resumer: Arc<dyn SessionResumer>, input: ResumeInput) {
    if let Err(error) = resumer
        .resume(input.tenant, &input.correlation_id, input.output)
        .await
    {
        tracing::error!(
            %error,
            correlation_id = %input.correlation_id,
            "failed to resume session on dispatch response"
        );
    }
}

/// Subscribe to the runtime's response subject and resume sessions forever.
///
/// Runs until the NATS subscription is closed (e.g. on server shutdown).
/// Malformed messages are logged and dropped; the listener does not stop.
///
/// # Errors
/// Returns an error only if subscribing to the NATS subject fails.
pub async fn run_response_listener(
    client: async_nats::Client,
    runtime: String,
    resumer: Arc<dyn SessionResumer>,
) -> Result<()> {
    let subject = response_topic(&runtime);
    let mut subscription = client.subscribe(subject).await?;
    while let Some(msg) = subscription.next().await {
        let headers = msg.headers.as_ref();
        let get_header = |name: &str| headers.and_then(|h| h.get(name)).map(|v| v.as_str());
        match decode_response(
            get_header("Greentic-Correlation-Id"),
            get_header("Greentic-Tenant"),
            get_header("Greentic-Env"),
            &msg.payload,
        ) {
            Ok(input) => dispatch_to_resumer(resumer.clone(), input).await,
            Err(error) => tracing::warn!(%error, "bad dispatch response; dropping"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_types::{DispatchError, RuntimeDispatchResponse};
    use serde_json::json;
    use std::sync::Mutex;

    // -----------------------------------------------------------------------
    // Broker-gated round-trip test
    // -----------------------------------------------------------------------

    /// A [`SessionResumer`] that records every `(correlation_id, output)` pair
    /// it receives, and notifies a [`tokio::sync::Notify`] on each record so
    /// waiting tasks can wake up without polling.
    struct RecordingResumerNotify {
        seen: Mutex<Vec<(String, serde_json::Value)>>,
        notify: tokio::sync::Notify,
    }

    impl RecordingResumerNotify {
        fn new() -> Self {
            Self {
                seen: Mutex::new(vec![]),
                notify: tokio::sync::Notify::new(),
            }
        }

        fn snapshot(&self) -> Vec<(String, serde_json::Value)> {
            self.seen.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SessionResumer for RecordingResumerNotify {
        async fn resume(
            &self,
            _tenant: TenantCtx,
            correlation_id: &str,
            output: Value,
        ) -> Result<()> {
            self.seen
                .lock()
                .unwrap()
                .push((correlation_id.to_string(), output));
            self.notify.notify_one();
            Ok(())
        }
    }

    /// End-to-end NATS dispatch → listen → resume round-trip test.
    ///
    /// Skips automatically when `GREENTIC_TEST_NATS_URL` is not set so that
    /// regular CI (which has no broker) stays green.
    ///
    /// Run against a live broker:
    /// ```text
    /// GREENTIC_TEST_NATS_URL=nats://127.0.0.1:4222 \
    ///   cargo test -p greentic-runner-host --lib \
    ///   dispatch_listener::tests::nats_dispatch_and_resume_roundtrip_over_real_broker \
    ///   -- --nocapture
    /// ```
    #[tokio::test]
    async fn nats_dispatch_and_resume_roundtrip_over_real_broker() {
        let nats_url = match std::env::var("GREENTIC_TEST_NATS_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!("skipping: GREENTIC_TEST_NATS_URL not set");
                return;
            }
        };

        // ── 1. Connect three separate clients (fake bridge, listener, dispatcher) ──
        let bridge_client = async_nats::connect(&nats_url)
            .await
            .expect("broker client for fake bridge");
        let listener_client = async_nats::connect(&nats_url)
            .await
            .expect("broker client for response listener");
        let dispatcher_client = async_nats::connect(&nats_url)
            .await
            .expect("broker client for dispatcher");

        // ── 2. Fake bridge: subscribe to request topic, echo back a response ──
        use futures::StreamExt as _;
        use greentic_types::request_topic;

        let request_subject = request_topic("sorla");
        let response_subject = greentic_types::response_topic("sorla");

        let mut request_subscription = bridge_client
            .subscribe(request_subject)
            .await
            .expect("fake bridge subscription");

        let bridge_response_client = bridge_client.clone();
        let response_subject_clone = response_subject.clone();
        tokio::spawn(async move {
            while let Some(msg) = request_subscription.next().await {
                // Read the correlation and tenant/env headers to echo them back.
                let headers = msg.headers.as_ref();
                let get_header = |name: &str| {
                    headers
                        .and_then(|h| h.get(name))
                        .map(|v| v.as_str().to_owned())
                        .unwrap_or_default()
                };
                let correlation_id = get_header("Greentic-Correlation-Id");
                let tenant = get_header("Greentic-Tenant");
                let env = get_header("Greentic-Env");

                // Deserialize the request body to capture `input` for the echo.
                let request_input: serde_json::Value = serde_json::from_slice(&msg.payload)
                    .map(|v: serde_json::Value| {
                        v.get("input").cloned().unwrap_or(serde_json::Value::Null)
                    })
                    .unwrap_or(serde_json::Value::Null);

                // Build the response payload echoing the request input.
                let response_payload = RuntimeDispatchResponse {
                    ok: true,
                    output: json!({ "echo": request_input }),
                    events: vec![],
                    error: None,
                };
                let response_bytes =
                    serde_json::to_vec(&response_payload).expect("serialize response");

                // Build response headers echoing the correlation / tenant / env.
                let mut response_headers = async_nats::HeaderMap::new();
                response_headers.insert("Greentic-Correlation-Id", correlation_id.as_str());
                response_headers.insert("Greentic-Tenant", tenant.as_str());
                response_headers.insert("Greentic-Env", env.as_str());

                bridge_response_client
                    .publish_with_headers(
                        response_subject_clone.clone(),
                        response_headers,
                        response_bytes.into(),
                    )
                    .await
                    .expect("fake bridge: publish response");
            }
        });

        // ── 3. Start run_response_listener with a recording resumer ──
        let recording_resumer = Arc::new(RecordingResumerNotify::new());
        let resumer_for_listener = recording_resumer.clone();
        tokio::spawn(async move {
            run_response_listener(listener_client, "sorla".to_owned(), resumer_for_listener)
                .await
                .expect("response listener exited unexpectedly");
        });

        // Give subscriptions a moment to be acknowledged by the broker.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // ── 4. Dispatch via NatsDispatcher ──
        use crate::runner::remote_dispatch::{
            NatsDispatcher, RemoteDispatch, RemoteDispatchHandler,
        };
        use greentic_types::DispatchMode;

        let dispatcher = NatsDispatcher::new(dispatcher_client);
        let dispatch_action = dispatcher
            .dispatch(RemoteDispatch {
                tenant: "t1".into(),
                env: "default".into(),
                runtime: "sorla".into(),
                target: "dep-1".into(),
                operation: "create".into(),
                mode: DispatchMode::Await,
                correlation_id: "corr-runner-e2e".into(),
                input: json!({"a": 1}),
                deadline_ms: None,
                decision_token: None,
            })
            .await
            .expect("dispatch should succeed");

        assert!(
            matches!(
                dispatch_action,
                crate::runner::remote_dispatch::RemoteDispatchAction::AwaitingResponse { .. }
            ),
            "expected AwaitingResponse action"
        );

        // ── 5. Wait (up to 5 s) for the resumer to record the resume call ──
        let wait_result = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            recording_resumer.notify.notified(),
        )
        .await;

        assert!(
            wait_result.is_ok(),
            "timed out waiting for the resumer to be called — check NATS connectivity"
        );

        // ── 6. Assert the captured resume payload ──
        let captured = recording_resumer.snapshot();
        assert_eq!(
            captured.len(),
            1,
            "resumer should have been called exactly once"
        );

        let (captured_correlation_id, captured_output) = &captured[0];
        assert_eq!(
            captured_correlation_id, "corr-runner-e2e",
            "correlation_id must be echoed verbatim"
        );
        assert_eq!(
            captured_output["ok"],
            json!(true),
            "response ok flag must be true"
        );
        assert_eq!(
            captured_output["output"]["echo"],
            json!({"a": 1}),
            "echo output must reflect the dispatched input"
        );

        eprintln!("PASSED: NATS dispatch→listen→resume round-trip verified");
    }

    #[test]
    fn decode_response_extracts_tenant_correlation_and_output() {
        let body = serde_json::to_vec(&RuntimeDispatchResponse {
            ok: true,
            output: json!({"reply": "hi"}),
            events: vec![],
            error: None,
        })
        .unwrap();
        let decoded = decode_response(
            Some("t1:web:c:conv:u::pack=p"),
            Some("t1"),
            Some("default"),
            &body,
        )
        .unwrap();
        assert_eq!(decoded.correlation_id, "t1:web:c:conv:u::pack=p");
        assert_eq!(decoded.tenant.tenant_id.as_str(), "t1");
        assert_eq!(decoded.output["ok"], json!(true));
        assert_eq!(decoded.output["output"], json!({"reply": "hi"}));
    }

    #[test]
    fn decode_response_errors_without_correlation_id() {
        let body = serde_json::to_vec(&RuntimeDispatchResponse {
            ok: true,
            output: json!(null),
            events: vec![],
            error: None,
        })
        .unwrap();
        assert!(decode_response(None, Some("t1"), Some("default"), &body).is_err());
    }

    struct RecordingResumer {
        seen: Mutex<Vec<(String, serde_json::Value)>>,
    }

    #[async_trait]
    impl SessionResumer for RecordingResumer {
        async fn resume(
            &self,
            _tenant: TenantCtx,
            correlation_id: &str,
            output: Value,
        ) -> Result<()> {
            self.seen
                .lock()
                .unwrap()
                .push((correlation_id.to_string(), output));
            Ok(())
        }
    }

    #[tokio::test]
    async fn dispatch_to_resumer_forwards_decoded_input() {
        let resumer = Arc::new(RecordingResumer {
            seen: Mutex::new(vec![]),
        });
        let body = serde_json::to_vec(&RuntimeDispatchResponse {
            ok: false,
            output: json!(null),
            events: vec![],
            error: Some(DispatchError {
                code: "timeout".into(),
                message: "x".into(),
            }),
        })
        .unwrap();
        let decoded = decode_response(Some("sess-9"), Some("t1"), Some("default"), &body).unwrap();
        dispatch_to_resumer(resumer.clone(), decoded).await;
        let seen = resumer.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, "sess-9");
        assert_eq!(seen[0].1["ok"], json!(false));
        assert_eq!(seen[0].1["error"]["code"], json!("timeout"));
    }
}
