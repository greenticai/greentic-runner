//! Generic async dispatch of flow work to a separate runtime over NATS pub/sub.
//! The `RemoteDispatchHandler` trait isolates the transport so it can be swapped
//! (e.g. to greentic-events EventBus) without touching the flow engine.

use anyhow::Result;
use async_nats::HeaderMap;
use async_trait::async_trait;
use greentic_types::{
    DispatchError, DispatchMode, RuntimeDispatchRequest, RuntimeDispatchResponse, request_topic,
    response_topic,
};
use serde_json::Value;

/// Everything needed to dispatch one unit of work to a runtime.
pub struct RemoteDispatch {
    pub tenant: String,
    pub env: String,
    pub runtime: String,
    pub target: String,
    pub operation: String,
    pub mode: DispatchMode,
    /// Opaque correlation id (= canonical session hint); echoed by the response.
    pub correlation_id: String,
    pub input: Value,
    pub deadline_ms: Option<u64>,
    /// The single-use `decision_token` an `approval` dispatch is issued
    /// (approval rail contract v2 §4; see [`crate::runner::approval_token`]).
    /// Published at `routing.decision_token` and echoed by the runner's own
    /// deadline watchdog at `output.decision_token`. `None` for every other
    /// runtime, whose request body is unchanged.
    ///
    /// A bearer credential: never log it.
    pub decision_token: Option<String>,
}

/// Outcome of a dispatch: the flow waits for a response, or it was fire-and-forget.
#[derive(Debug)]
pub enum RemoteDispatchAction {
    AwaitingResponse { correlation_id: String },
    Dispatched,
}

/// A built NATS message (broker-free; unit-testable).
pub struct BuiltRequest {
    pub subject: String,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
    pub action: RemoteDispatchAction,
}

/// Pure builder: turn a [`RemoteDispatch`] into the subject/headers/body to
/// publish plus the resulting action. No I/O — unit-testable without a broker.
pub fn build_request(req: &RemoteDispatch) -> Result<BuiltRequest> {
    let body_struct = RuntimeDispatchRequest {
        target: req.target.clone(),
        operation: req.operation.clone(),
        mode: req.mode,
        input: req.input.clone(),
        deadline_ms: req.deadline_ms,
    };
    let body = match req.decision_token.as_deref() {
        None => serde_json::to_vec(&body_struct)?,
        Some(token) => {
            // Contract §2: the token rides in a top-level `routing` object
            // beside `target` / `operation` / `input`. greentic-designer-admin
            // seals it from exactly `routing.decision_token`.
            let mut value = serde_json::to_value(&body_struct)?;
            if let Value::Object(map) = &mut value {
                map.insert(
                    "routing".to_string(),
                    serde_json::json!({ "decision_token": token }),
                );
            }
            serde_json::to_vec(&value)?
        }
    };

    let mut headers = HeaderMap::new();
    headers.insert("Greentic-Correlation-Id", req.correlation_id.as_str());
    headers.insert("Greentic-Tenant", req.tenant.as_str());
    headers.insert("Greentic-Env", req.env.as_str());
    headers.insert("Greentic-Idempotency-Key", req.correlation_id.as_str());

    let action = match req.mode {
        DispatchMode::Await => RemoteDispatchAction::AwaitingResponse {
            correlation_id: req.correlation_id.clone(),
        },
        DispatchMode::FireAndForget => RemoteDispatchAction::Dispatched,
    };

    Ok(BuiltRequest {
        subject: request_topic(&req.runtime),
        headers,
        body,
        action,
    })
}

/// Build the subject, headers, and serialized body for a timeout error response.
///
/// This is a pure function (no I/O) that constructs the message a spawned
/// timeout task should publish when `deadline_ms` elapses before the real
/// runtime responds.
///
/// # Parameters
/// - `runtime` - the runtime name (e.g. `"sorla"`)
/// - `correlation_id` - echoed verbatim in the `Greentic-Correlation-Id` header
/// - `tenant` - echoed in `Greentic-Tenant`
/// - `env` - echoed in `Greentic-Env`
/// - `deadline_ms` - the timeout that expired; used in the error message
///
/// # Returns
/// A tuple of `(subject, headers, body_bytes)` ready to pass to
/// `async_nats::Client::publish_with_headers`.
pub fn build_timeout_response_message(
    runtime: &str,
    correlation_id: &str,
    tenant: &str,
    env: &str,
    deadline_ms: u64,
) -> (String, HeaderMap, Vec<u8>) {
    build_timeout_response_message_with_token(
        runtime,
        correlation_id,
        tenant,
        env,
        deadline_ms,
        None,
    )
}

/// [`build_timeout_response_message`] for a dispatch that was issued a
/// `decision_token`: the watchdog is a sender like any other, so its `timeout`
/// must carry the token at `output.decision_token` or the parked gate refuses
/// it and the `timeout` branch never fires.
pub fn build_timeout_response_message_with_token(
    runtime: &str,
    correlation_id: &str,
    tenant: &str,
    env: &str,
    deadline_ms: u64,
    decision_token: Option<&str>,
) -> (String, HeaderMap, Vec<u8>) {
    let subject = response_topic(runtime);

    let mut headers = HeaderMap::new();
    headers.insert("Greentic-Correlation-Id", correlation_id);
    headers.insert("Greentic-Tenant", tenant);
    headers.insert("Greentic-Env", env);

    let output = match decision_token {
        Some(token) => serde_json::json!({ "decision_token": token }),
        None => Value::Null,
    };
    let response = RuntimeDispatchResponse {
        ok: false,
        output,
        events: vec![],
        error: Some(DispatchError {
            code: "timeout".into(),
            message: format!("no response within {deadline_ms}ms"),
        }),
    };
    // Serialization of well-formed types is infallible in practice.
    let body = serde_json::to_vec(&response).unwrap_or_default();

    (subject, headers, body)
}

/// Transport-agnostic publisher for runtime dispatch requests. Implemented over
/// raw `async-nats` today; the trait lets a greentic-events EventBus be swapped
/// in later without changing the flow engine.
#[async_trait]
pub trait RemoteDispatchHandler: Send + Sync {
    async fn dispatch(&self, request: RemoteDispatch) -> Result<RemoteDispatchAction>;
}

/// Publishes dispatch requests to NATS via raw `async-nats`.
pub struct NatsDispatcher {
    client: async_nats::Client,
}

impl NatsDispatcher {
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RemoteDispatchHandler for NatsDispatcher {
    async fn dispatch(&self, request: RemoteDispatch) -> Result<RemoteDispatchAction> {
        // Capture fields needed by the timeout task BEFORE moving `request`
        // into `build_request`.
        let maybe_timeout = match (request.mode, request.deadline_ms) {
            (DispatchMode::Await, Some(deadline_ms)) => Some((
                deadline_ms,
                request.runtime.clone(),
                request.correlation_id.clone(),
                request.tenant.clone(),
                request.env.clone(),
                request.decision_token.clone(),
            )),
            _ => None,
        };

        let built = build_request(&request)?;
        self.client
            .publish_with_headers(built.subject, built.headers, built.body.into())
            .await?;

        // Spawn the deadline watchdog AFTER the request is on the wire.
        if let Some((deadline_ms, runtime, correlation_id, tenant, env, decision_token)) =
            maybe_timeout
        {
            let timeout_client = self.client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(deadline_ms)).await;
                let (subject, headers, body) = build_timeout_response_message_with_token(
                    &runtime,
                    &correlation_id,
                    &tenant,
                    &env,
                    deadline_ms,
                    decision_token.as_deref(),
                );
                if let Err(publish_error) = timeout_client
                    .publish_with_headers(subject, headers, body.into())
                    .await
                {
                    tracing::warn!(
                        %publish_error,
                        %correlation_id,
                        deadline_ms,
                        "failed to publish timeout response for awaited dispatch"
                    );
                }
            });
        }

        Ok(built.action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_types::{DispatchMode, RuntimeDispatchRequest, RuntimeDispatchResponse};
    use serde_json::json;

    fn sample(mode: DispatchMode) -> RemoteDispatch {
        RemoteDispatch {
            tenant: "t1".into(),
            env: "default".into(),
            runtime: "sorla".into(),
            target: "dep-1".into(),
            operation: "create".into(),
            mode,
            correlation_id: "t1:web:chan:conv:user::pack=p".into(), // opaque hint, contains ':'
            input: json!({"a": 1}),
            deadline_ms: Some(1000),
            decision_token: None,
        }
    }

    #[test]
    fn a_request_without_a_token_has_no_routing_block() {
        let built = build_request(&sample(DispatchMode::Await)).unwrap();
        let body: Value = serde_json::from_slice(&built.body).unwrap();
        assert!(body.get("routing").is_none());
    }

    #[test]
    fn an_approval_token_rides_at_routing_decision_token() {
        let mut dispatch = sample(DispatchMode::Await);
        dispatch.runtime = "approval".into();
        dispatch.decision_token = Some("tok-abc".into());
        let built = build_request(&dispatch).unwrap();
        let body: Value = serde_json::from_slice(&built.body).unwrap();
        assert_eq!(body["routing"]["decision_token"], "tok-abc");
        // The v1 half is untouched, so a v1 consumer still decodes it.
        let decoded: RuntimeDispatchRequest = serde_json::from_slice(&built.body).unwrap();
        assert_eq!(decoded.target, "dep-1");
        assert_eq!(decoded.input, json!({"a": 1}));
    }

    #[test]
    fn the_watchdog_timeout_carries_the_token_at_output() {
        let (_s, _h, body) = build_timeout_response_message_with_token(
            "approval",
            "corr-1",
            "t1",
            "default",
            200,
            Some("tok-abc"),
        );
        let response: RuntimeDispatchResponse = serde_json::from_slice(&body).unwrap();
        assert!(!response.ok);
        assert_eq!(response.output["decision_token"], "tok-abc");
        assert_eq!(response.error.expect("error").code, "timeout");
    }

    #[test]
    fn build_request_targets_request_topic_with_headers_and_body() {
        let built = build_request(&sample(DispatchMode::Await)).unwrap();
        assert_eq!(built.subject, "greentic.sorla.request.v1");
        assert_eq!(
            built
                .headers
                .get("Greentic-Correlation-Id")
                .map(|v| v.as_str()),
            Some("t1:web:chan:conv:user::pack=p")
        );
        assert_eq!(
            built.headers.get("Greentic-Tenant").map(|v| v.as_str()),
            Some("t1")
        );
        let body: RuntimeDispatchRequest = serde_json::from_slice(&built.body).unwrap();
        assert_eq!(body.operation, "create");
        assert_eq!(body.mode, DispatchMode::Await);
        assert!(matches!(
            built.action,
            RemoteDispatchAction::AwaitingResponse { .. }
        ));
    }

    #[test]
    fn fire_and_forget_action_is_dispatched() {
        let built = build_request(&sample(DispatchMode::FireAndForget)).unwrap();
        assert!(matches!(built.action, RemoteDispatchAction::Dispatched));
    }

    #[test]
    fn build_request_targets_telco_x_request_topic() {
        // `telco-x.call` uses runtime name "telco-x" → subject
        // `greentic.telco-x.request.v1` (the runner now also registers a
        // matching response listener for this runtime).
        let mut dispatch = sample(DispatchMode::Await);
        dispatch.runtime = "telco-x".into();
        let built = build_request(&dispatch).unwrap();
        assert_eq!(built.subject, "greentic.telco-x.request.v1");
    }

    #[test]
    fn build_timeout_response_message_telco_x_response_topic() {
        let (subject, _headers, _body) =
            build_timeout_response_message("telco-x", "corr-1", "t1", "default", 500);
        assert_eq!(subject, "greentic.telco-x.response.v1");
    }

    #[test]
    fn build_timeout_response_message_uses_response_topic() {
        let (subject, _headers, _body) =
            build_timeout_response_message("sorla", "corr-1", "t1", "default", 500);
        assert_eq!(subject, "greentic.sorla.response.v1");
    }

    #[test]
    fn build_timeout_response_message_echoes_correlation_header() {
        let (_subject, headers, _body) =
            build_timeout_response_message("sorla", "my-corr-id", "t1", "default", 1000);
        assert_eq!(
            headers.get("Greentic-Correlation-Id").map(|v| v.as_str()),
            Some("my-corr-id")
        );
    }

    #[test]
    fn build_timeout_response_message_body_deserializes_to_timeout_error() {
        let (_subject, _headers, body) =
            build_timeout_response_message("sorla", "corr-1", "t1", "default", 200);
        let response: RuntimeDispatchResponse = serde_json::from_slice(&body).unwrap();
        assert!(!response.ok, "ok must be false");
        assert_eq!(response.output, serde_json::Value::Null);
        let error = response.error.expect("error must be present");
        assert_eq!(error.code, "timeout");
        assert!(
            error.message.contains("200ms"),
            "message must mention deadline: {:?}",
            error.message
        );
    }
}
