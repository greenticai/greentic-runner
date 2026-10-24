//! Event bridge: consume `greentic.telco-x.request.v1` from NATS, invoke a
//! Telco-X runtime via the [`TelcoXDispatchInvoker`] seam, and publish
//! `greentic.telco-x.response.v1` echoing the correlation id.
//!
//! This is the Telco-X-side counterpart of the runner's `telco-x.call` flow
//! node (out-of-process dispatch). The runner publishes a dispatch request; this
//! bridge runs one Telco-X operation and publishes the reply. It mirrors
//! `aw-event-bridge` (the `agentic.call` side) exactly, so the wire contract is
//! SHARED from [`greentic_types::runtime_dispatch`] rather than mirrored by hand.
//!
//! **Phase 2 transport scaffold.** The transport is complete; the Telco-X
//! *business operations* are not. A real deployment provides a
//! [`TelcoXDispatchInvoker`] that maps `(target, operation, input)` onto the
//! Telco-X solution layer. [`EchoInvoker`] is a credit-free placeholder used by
//! the `telco-x-serve` binary and the round-trip test until that impl exists.
//! See `docs/superpowers/specs/2026-06-21-telco-x-runtime-dispatch-design.md`.

use std::sync::Arc;

use anyhow::Result;
use async_nats::HeaderMap;
use async_trait::async_trait;
use greentic_types::{DispatchError, RuntimeDispatchRequest, RuntimeDispatchResponse};
use serde_json::{Value, json};

// Re-export the shared subject helpers so callers can name the telco-x subjects
// without an extra greentic-types dependency.
pub use greentic_types::{request_topic, response_topic};

/// Runtime name for Telco-X; selects the request/response subjects
/// `greentic.telco-x.request.v1` / `greentic.telco-x.response.v1`.
pub const RUNTIME_NAME: &str = "telco-x";

/// Result of invoking the local Telco-X runtime for one dispatch.
pub struct InvokeOutcome {
    /// Whether the operation completed successfully.
    pub ok: bool,
    /// Operation output payload (operation-defined).
    pub output: Value,
    /// Optional runtime-emitted events (empty today).
    pub events: Vec<Value>,
}

/// Seam over the actual Telco-X invocation. A production impl wraps the Telco-X
/// solution layer; the round-trip test and the `telco-x-serve` placeholder use
/// [`EchoInvoker`].
///
/// `target` is the Telco-X resource/target the `telco-x.call.<target>` node
/// addresses; `operation` selects the behaviour (e.g. `provision`, `lookup`,
/// `playbook.step` — the catalogue is Telco-X-owned and out of scope here);
/// `input` is the opaque node input.
#[async_trait]
pub trait TelcoXDispatchInvoker: Send + Sync {
    /// Run one Telco-X operation.
    ///
    /// * `tenant` / `env` — multi-tenant context echoed from the request headers.
    /// * `target` — the Telco-X target/resource id.
    /// * `operation` — the operation name (may be empty for a default).
    /// * `input` — opaque node input.
    /// * `idempotency_key` — correlation/idempotency hint; doubles as the session
    ///   id when the input carries no explicit one.
    async fn invoke(
        &self,
        tenant: &str,
        env: &str,
        target: &str,
        operation: &str,
        input: Value,
        idempotency_key: Option<&str>,
    ) -> Result<InvokeOutcome>;
}

/// Credit-free placeholder invoker: echoes the request back as a successful
/// outcome. Lets the bridge be exercised end-to-end (round-trip test,
/// `telco-x-serve`) before a real Telco-X runtime is wired through the seam.
pub struct EchoInvoker;

#[async_trait]
impl TelcoXDispatchInvoker for EchoInvoker {
    async fn invoke(
        &self,
        _tenant: &str,
        _env: &str,
        target: &str,
        operation: &str,
        input: Value,
        _idempotency_key: Option<&str>,
    ) -> Result<InvokeOutcome> {
        Ok(InvokeOutcome {
            ok: true,
            output: json!({
                "echo": true,
                "target": target,
                "operation": operation,
                "input": input,
            }),
            events: vec![],
        })
    }
}

/// Longest tenant / env id the bridge accepts from a request header.
const MAX_SCOPE_ID_LEN: usize = 128;

/// Substring of `AgentError::LockTimeout`'s `Display` ("session lock could not
/// be acquired within wait window"); see [`is_lock_contention`].
const LOCK_CONTENTION_MARKER: &str = "session lock could not be acquired";

/// Error an invoker can return when the target session is busy (another turn
/// holds its lock). A busy session is never dropped silently: the bridge
/// publishes a `session_busy` error response so the parked caller is not left
/// waiting (this bridge is core NATS, so there is no redelivery to lean on).
#[derive(Debug)]
pub struct LockContention(pub String);

impl std::fmt::Display for LockContention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{LOCK_CONTENTION_MARKER}: {}", self.0)
    }
}

impl std::error::Error for LockContention {}

/// Whether an invoker error means "session lock busy" (typed [`LockContention`],
/// or an error chain carrying the runtime's lock-timeout message).
#[must_use]
pub fn is_lock_contention(error: &anyhow::Error) -> bool {
    error.downcast_ref::<LockContention>().is_some()
        || error
            .chain()
            .any(|cause| cause.to_string().contains(LOCK_CONTENTION_MARKER))
}

/// What became of one request message; drives the JetStream ack decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// A response was published (success or error); ack.
    Handled,
    /// The request can never succeed (malformed / invalid ids); terminate it
    /// (an error response was published when a correlation id existed).
    Rejected,
}

/// Why a request was refused before reaching the invoker.
#[derive(Debug, PartialEq, Eq)]
pub struct Rejection {
    pub code: &'static str,
    pub message: String,
}

/// A request that passed header and payload validation.
#[derive(Debug)]
pub struct ParsedRequest {
    pub tenant: String,
    pub env: String,
    pub correlation: Option<String>,
    pub idempotency: Option<String>,
    pub request: RuntimeDispatchRequest,
}

/// Reject tenant / env ids that are empty, oversized, control-bearing, or could
/// alter a key or URL path built from them (`:`, `/`, `\`, `..`).
pub fn validate_scope_id(kind: &str, value: &str) -> Result<(), Rejection> {
    let bad = value.is_empty()
        || value.len() > MAX_SCOPE_ID_LEN
        || value.contains("..")
        || value
            .chars()
            .any(|c| c == ':' || c == '/' || c == '\\' || c.is_control() || c.is_whitespace());
    if bad {
        return Err(Rejection {
            code: "invalid_request",
            message: format!("invalid {kind} id"),
        });
    }
    Ok(())
}

/// Validate headers and decode the payload (pure; no I/O).
pub fn parse_request(
    get_header: impl Fn(&str) -> Option<String>,
    payload: &[u8],
) -> Result<ParsedRequest, Rejection> {
    let correlation = get_header("Greentic-Correlation-Id");
    // The runner sets the idempotency key equal to the correlation id; prefer the
    // explicit header but fall back to the correlation id so the invoker always
    // has a stable session hint.
    let idempotency = get_header("Greentic-Idempotency-Key").or_else(|| correlation.clone());
    let tenant = get_header("Greentic-Tenant").unwrap_or_default();
    let env = get_header("Greentic-Env").unwrap_or_else(|| "default".to_string());
    validate_scope_id("tenant", &tenant)?;
    validate_scope_id("env", &env)?;
    let request: RuntimeDispatchRequest =
        serde_json::from_slice(payload).map_err(|error| Rejection {
            code: "invalid_request",
            message: format!("malformed dispatch request: {error}"),
        })?;
    Ok(ParsedRequest {
        tenant,
        env,
        correlation,
        idempotency,
        request,
    })
}

/// Error code published when the target session stayed locked.
pub const SESSION_BUSY_CODE: &str = "session_busy";

fn error_response(code: &str, message: String) -> RuntimeDispatchResponse {
    RuntimeDispatchResponse {
        ok: false,
        output: Value::Null,
        events: vec![],
        error: Some(DispatchError {
            code: code.into(),
            message,
        }),
    }
}

/// Map an invoker failure to its error response: a busy session is
/// `session_busy` (the caller may retry), anything else `invoke_failed`.
fn failure_response(error: &anyhow::Error) -> RuntimeDispatchResponse {
    let code = if is_lock_contention(error) {
        SESSION_BUSY_CODE
    } else {
        "invoke_failed"
    };
    error_response(code, error.to_string())
}

/// Invoke; `Err` carries the invoker error so the caller can classify it.
async fn dispatch(
    invoker: Arc<dyn TelcoXDispatchInvoker>,
    tenant: &str,
    env: &str,
    idempotency_key: Option<&str>,
    req: RuntimeDispatchRequest,
) -> Result<RuntimeDispatchResponse, anyhow::Error> {
    let outcome = invoker
        .invoke(
            tenant,
            env,
            &req.target,
            &req.operation,
            req.input,
            idempotency_key,
        )
        .await?;
    Ok(RuntimeDispatchResponse {
        ok: outcome.ok,
        output: outcome.output,
        events: outcome.events,
        error: None,
    })
}

/// Invoke and build the response (no NATS I/O). Errors map to an error response.
pub async fn build_response(
    invoker: Arc<dyn TelcoXDispatchInvoker>,
    tenant: &str,
    env: &str,
    idempotency_key: Option<&str>,
    req: RuntimeDispatchRequest,
) -> RuntimeDispatchResponse {
    match dispatch(invoker, tenant, env, idempotency_key, req).await {
        Ok(resp) => resp,
        Err(error) => failure_response(&error),
    }
}

async fn publish_response(
    client: &async_nats::Client,
    correlation: Option<&str>,
    scope: Option<(&str, &str)>,
    resp: &RuntimeDispatchResponse,
) -> Result<()> {
    let mut out_headers = HeaderMap::new();
    if let Some(correlation_value) = correlation {
        out_headers.insert("Greentic-Correlation-Id", correlation_value);
    }
    if let Some((tenant, env)) = scope {
        out_headers.insert("Greentic-Tenant", tenant);
        out_headers.insert("Greentic-Env", env);
    }
    let response_bytes = serde_json::to_vec(resp)?;
    client
        .publish_with_headers(
            response_topic(RUNTIME_NAME),
            out_headers,
            response_bytes.into(),
        )
        .await?;
    Ok(())
}

/// Handle one request message end-to-end: decode, invoke, publish response,
/// and report the [`Disposition`] so a durable consumer can ack / terminate.
///
/// The correlation id is echoed VERBATIM (the runner's call node encodes
/// `::pack=…::flow=…::thread=…::reply=…` resume markers there and parses
/// them back on response, so the bridge must not alter it). A request refused
/// at validation gets an error response whenever it carried a correlation id,
/// so the awaiting flow fails fast instead of waiting out its deadline.
pub async fn handle_message_outcome(
    client: &async_nats::Client,
    invoker: Arc<dyn TelcoXDispatchInvoker>,
    msg: async_nats::Message,
) -> Result<Disposition> {
    let headers = msg.headers.as_ref();
    let get_header = |name: &str| -> Option<String> {
        headers
            .and_then(|header_map| header_map.get(name))
            .map(|value| value.as_str().to_string())
    };

    let parsed = match parse_request(get_header, &msg.payload) {
        Ok(parsed) => parsed,
        Err(rejection) => {
            tracing::warn!(code = rejection.code, message = %rejection.message, "rejecting dispatch request");
            if let Some(correlation) = get_header("Greentic-Correlation-Id") {
                let resp = error_response(rejection.code, rejection.message);
                publish_response(client, Some(&correlation), None, &resp).await?;
            }
            return Ok(Disposition::Rejected);
        }
    };

    let resp = match dispatch(
        invoker,
        &parsed.tenant,
        &parsed.env,
        parsed.idempotency.as_deref(),
        parsed.request,
    )
    .await
    {
        Ok(resp) => resp,
        Err(error) => {
            if is_lock_contention(&error) {
                tracing::warn!(%error, "session busy; answering with session_busy");
            }
            failure_response(&error)
        }
    };
    publish_response(
        client,
        parsed.correlation.as_deref(),
        Some((&parsed.tenant, &parsed.env)),
        &resp,
    )
    .await?;
    Ok(Disposition::Handled)
}

/// Handle one request message end-to-end (see [`handle_message_outcome`]).
pub async fn handle_message(
    client: &async_nats::Client,
    invoker: Arc<dyn TelcoXDispatchInvoker>,
    msg: async_nats::Message,
) -> Result<()> {
    handle_message_outcome(client, invoker, msg)
        .await
        .map(|_| ())
}

/// Queue group shared by all bridge replicas, so a request is delivered to ONE
/// worker rather than fanned out to every replica.
pub const QUEUE_GROUP: &str = "telco-x-workers";

/// Subscribe to the request subject as part of [`QUEUE_GROUP`] and serve
/// forever (one spawned task per message).
pub async fn run_bridge(
    client: async_nats::Client,
    invoker: Arc<dyn TelcoXDispatchInvoker>,
) -> Result<()> {
    use futures_util::StreamExt;
    let mut subscriber = client
        .queue_subscribe(request_topic(RUNTIME_NAME), QUEUE_GROUP.to_string())
        .await?;
    while let Some(msg) = subscriber.next().await {
        let client = client.clone();
        let invoker = invoker.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_message(&client, invoker, msg).await {
                tracing::error!(%error, "event bridge failed to handle request");
            }
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_types::DispatchMode;
    use std::sync::Mutex;

    type SeenCall = (String, String, Value, Option<String>);

    struct StubInvoker {
        seen: Mutex<Vec<SeenCall>>,
    }

    #[async_trait]
    impl TelcoXDispatchInvoker for StubInvoker {
        async fn invoke(
            &self,
            _tenant: &str,
            _env: &str,
            target: &str,
            operation: &str,
            input: Value,
            idempotency_key: Option<&str>,
        ) -> Result<InvokeOutcome> {
            self.seen.lock().unwrap().push((
                target.to_string(),
                operation.to_string(),
                input.clone(),
                idempotency_key.map(str::to_string),
            ));
            Ok(InvokeOutcome {
                ok: true,
                output: json!({"done": true}),
                events: vec![],
            })
        }
    }

    fn sample_request() -> RuntimeDispatchRequest {
        RuntimeDispatchRequest {
            target: "line-1".into(),
            operation: "provision".into(),
            mode: DispatchMode::Await,
            input: json!({"msisdn": "+100"}),
            deadline_ms: Some(30_000),
        }
    }

    #[test]
    fn subjects_use_telco_x_runtime_name() {
        assert_eq!(request_topic(RUNTIME_NAME), "greentic.telco-x.request.v1");
        assert_eq!(response_topic(RUNTIME_NAME), "greentic.telco-x.response.v1");
    }

    #[tokio::test]
    async fn handle_invokes_and_forwards_target_operation_and_idempotency() {
        let invoker = Arc::new(StubInvoker {
            seen: Mutex::new(vec![]),
        });
        let resp = build_response(
            invoker.clone(),
            "acme",
            "prod",
            Some("sess-1::pack=p::flow=f"),
            sample_request(),
        )
        .await;

        assert!(resp.ok);
        assert_eq!(resp.output["done"], json!(true));
        assert!(resp.error.is_none());

        let seen = invoker.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let (target, operation, input, idempotency) = &seen[0];
        assert_eq!(target, "line-1");
        assert_eq!(operation, "provision");
        assert_eq!(input["msisdn"], json!("+100"));
        assert_eq!(idempotency.as_deref(), Some("sess-1::pack=p::flow=f"));
    }

    #[tokio::test]
    async fn echo_invoker_returns_request_shape() {
        let resp = build_response(
            Arc::new(EchoInvoker),
            "acme",
            "prod",
            Some("c"),
            sample_request(),
        )
        .await;
        assert!(resp.ok);
        assert_eq!(resp.output["echo"], json!(true));
        assert_eq!(resp.output["target"], json!("line-1"));
        assert_eq!(resp.output["operation"], json!("provision"));
    }

    #[tokio::test]
    async fn invoke_error_maps_to_error_response() {
        struct FailInvoker;

        #[async_trait]
        impl TelcoXDispatchInvoker for FailInvoker {
            async fn invoke(
                &self,
                _tenant: &str,
                _env: &str,
                _target: &str,
                _operation: &str,
                _input: Value,
                _idempotency_key: Option<&str>,
            ) -> Result<InvokeOutcome> {
                Err(anyhow::anyhow!("boom"))
            }
        }

        let resp = build_response(
            Arc::new(FailInvoker),
            "acme",
            "prod",
            Some("c"),
            sample_request(),
        )
        .await;

        assert!(!resp.ok);
        assert_eq!(resp.output, Value::Null);
        let error = resp.error.expect("error response must carry details");
        assert_eq!(error.code, "invoke_failed");
        assert_eq!(error.message, "boom");
    }

    fn header_fn<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| (*v).to_string())
        }
    }

    fn payload() -> Vec<u8> {
        serde_json::to_vec(&sample_request()).unwrap()
    }

    #[test]
    fn valid_headers_parse_and_default_env() {
        let headers = [
            ("Greentic-Tenant", "acme"),
            ("Greentic-Correlation-Id", "c1"),
        ];
        let parsed = parse_request(header_fn(&headers), &payload()).unwrap();
        assert_eq!(parsed.tenant, "acme");
        assert_eq!(parsed.env, "default");
        assert_eq!(parsed.idempotency.as_deref(), Some("c1"));
    }

    #[test]
    fn scope_ids_with_separators_or_empty_are_rejected() {
        for bad in ["", "a:b", "a/b", "..", "a..b", "a b", "a\\b", "x\n"] {
            assert!(validate_scope_id("tenant", bad).is_err(), "{bad:?}");
        }
        assert!(validate_scope_id("tenant", &"a".repeat(MAX_SCOPE_ID_LEN + 1)).is_err());
        assert!(validate_scope_id("tenant", "acme-1_prod.eu").is_ok());
        let headers = [("Greentic-Tenant", "a:b")];
        assert!(parse_request(header_fn(&headers), &payload()).is_err());
        let missing_tenant: [(&str, &str); 0] = [];
        assert!(parse_request(header_fn(&missing_tenant), &payload()).is_err());
    }

    #[test]
    fn malformed_payload_is_rejected_with_invalid_request() {
        let headers = [("Greentic-Tenant", "acme")];
        let rejection = parse_request(header_fn(&headers), b"not json").unwrap_err();
        assert_eq!(rejection.code, "invalid_request");
    }

    #[test]
    fn lock_contention_is_detected_typed_and_by_message() {
        assert!(is_lock_contention(&anyhow::Error::new(LockContention(
            "s".into()
        ))));
        let wrapped = anyhow::anyhow!("session lock could not be acquired within wait window")
            .context("agent step failed");
        assert!(is_lock_contention(&wrapped));
        assert!(!is_lock_contention(&anyhow::anyhow!("boom")));
    }

    #[test]
    fn lock_contention_answers_session_busy_instead_of_dropping() {
        let busy = failure_response(&anyhow::Error::new(LockContention("s".into())));
        assert!(!busy.ok);
        assert_eq!(busy.error.map(|e| e.code).as_deref(), Some("session_busy"));
        let other = failure_response(&anyhow::anyhow!("boom"));
        assert_eq!(
            other.error.map(|e| e.code).as_deref(),
            Some("invoke_failed")
        );
    }

    #[test]
    fn queue_group_is_stable() {
        assert_eq!(QUEUE_GROUP, "telco-x-workers");
    }
}
