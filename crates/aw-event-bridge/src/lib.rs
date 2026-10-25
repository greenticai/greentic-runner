//! Event bridge: consume `greentic.agentic.request.v1` from NATS, invoke the
//! local agentic-worker runtime via the [`AgentDispatchInvoker`] seam, and
//! publish `greentic.agentic.response.v1` echoing the correlation id.
//!
//! This is the agentic-side counterpart of the runner's `agentic.call` flow
//! node (out-of-process path, "Option C"): the runner publishes a dispatch
//! request; this bridge runs one agentic step and publishes the reply.
//!
//! Unlike `sorx-event-bridge`, this crate lives inside the runner workspace and
//! pins the same `greentic-types` lineage as the runner, so the wire contract is
//! SHARED from [`greentic_types::runtime_dispatch`] rather than mirrored by hand.

pub mod jetstream;

// Re-export so callers can use `aw_event_bridge::run_bridge_jetstream` without
// naming the submodule explicitly.
pub use jetstream::run_bridge_jetstream;

use std::sync::Arc;

use anyhow::Result;
use async_nats::HeaderMap;
use async_trait::async_trait;
use greentic_types::{RuntimeDispatchRequest, RuntimeDispatchResponse};
use serde_json::Value;

// Re-export the shared subject helpers so serve-mode callers can name the
// agentic subjects without an extra greentic-types dependency.
pub use greentic_types::{DispatchError, request_topic, response_topic};

/// Runtime name for the agentic worker; selects the request/response subjects
/// `greentic.agentic.request.v1` / `greentic.agentic.response.v1`.
pub const RUNTIME_NAME: &str = "agentic";

/// Result of invoking the local agentic-worker runtime for one dispatch.
pub struct InvokeOutcome {
    /// Whether the step completed successfully.
    pub ok: bool,
    /// Step output payload (e.g. `{reply, trail, terminated_by}`).
    pub output: Value,
    /// Optional runtime-emitted events (empty for the agentic worker today).
    pub events: Vec<Value>,
    /// Why `ok` is false, when the step ended without a usable reply
    /// (timeout, iteration cap, token budget). `output` still carries the
    /// partial `{reply, trail, terminated_by}` so a consumer can use it.
    pub error: Option<DispatchError>,
}

/// Header carrying the per-dispatch id: random, minted ONCE per publish by the
/// runner, so JetStream redelivery of the same message carries the same value
/// while a later turn of the same conversation carries a different one.
///
/// It is deliberately NOT folded into the correlation id / idempotency key: the
/// bridges reuse those as the downstream session id, so a nonce there would give
/// every turn a fresh agent memory.
pub const DISPATCH_ID_HEADER: &str = "Greentic-Dispatch-Id";

/// Longest dispatch id accepted.
const MAX_DISPATCH_ID_LEN: usize = 64;

/// A dispatch id is used as a key segment: `[A-Za-z0-9_-]{1,64}` or it is
/// treated as absent.
#[must_use]
pub fn valid_dispatch_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_DISPATCH_ID_LEN
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// The identifiers of one dispatch handed to an invoker.
#[derive(Clone, Copy, Debug, Default)]
pub struct DispatchIdentity<'a> {
    /// Correlation/idempotency hint (session hint; same for every turn).
    pub idempotency_key: Option<&'a str>,
    /// Unique per publish, stable across redelivery of that message.
    pub dispatch_id: Option<&'a str>,
}

/// Seam over the actual agentic-worker invocation. The production impl wraps
/// `greentic_aw_runtime::AgentRuntime`; tests use a stub.
///
/// `target` is the agent id (the runner's `agentic.call.<agent_id>` node maps
/// the node target to this field); `operation` is reserved for future
/// multi-operation agents and may be empty. `input` is the opaque node input —
/// the production invoker extracts `user_text` from it exactly as the in-process
/// `agent_node` path does.
#[async_trait]
pub trait AgentDispatchInvoker: Send + Sync {
    /// Run one agentic step.
    ///
    /// * `tenant` / `env` — multi-tenant context echoed from the request headers.
    /// * `target` — the agent id.
    /// * `operation` — reserved; may be empty.
    /// * `input` — opaque node input (expects at least `{"user_text": "..."}`).
    /// * `idempotency_key` — correlation/idempotency hint; doubles as the
    ///   session id when the input carries no explicit `session_id`.
    async fn invoke(
        &self,
        tenant: &str,
        env: &str,
        target: &str,
        operation: &str,
        input: Value,
        idempotency_key: Option<&str>,
    ) -> Result<InvokeOutcome>;

    /// [`Self::invoke`] with the per-dispatch identity the publisher stamped.
    ///
    /// The default ignores the dispatch id and calls [`Self::invoke`], so an
    /// invoker that does not deduplicate redeliveries needs no change.
    async fn invoke_identified(
        &self,
        tenant: &str,
        env: &str,
        target: &str,
        operation: &str,
        input: Value,
        ident: DispatchIdentity<'_>,
    ) -> Result<InvokeOutcome> {
        self.invoke(tenant, env, target, operation, input, ident.idempotency_key)
            .await
    }

    /// The wall-clock limit one step on `target` is configured with, when the
    /// invoker knows it. A durable consumer sizes its per-message time bound
    /// from this so an agent with a long `limits.timeout` is not cut off by a
    /// fixed constant. `None` (the default) means "use the consumer's default".
    async fn step_timeout(
        &self,
        _tenant: &str,
        _env: &str,
        _target: &str,
    ) -> Option<std::time::Duration> {
        None
    }
}

/// Longest tenant / env id the bridge accepts from a request header.
const MAX_SCOPE_ID_LEN: usize = 128;

/// Substring of `AgentError::LockTimeout`'s `Display` ("session lock could not
/// be acquired within wait window"); see [`is_lock_contention`].
const LOCK_CONTENTION_MARKER: &str = "session lock could not be acquired";

/// Error an invoker can return when the target session is busy (another turn
/// holds its lock). A busy session is never acked silently: depending on the
/// [`BusyPolicy`] the message is redelivered later or answered with a
/// `session_busy` error response, so the parked caller is never left waiting.
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

/// What to do when the target session stays locked for the whole wait window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusyPolicy {
    /// Publish nothing and return [`Disposition::Busy`] so a durable consumer
    /// can Nak with a delay and have the request redelivered. Publishing a
    /// failure here would resume the caller's flow while the first run is
    /// still genuinely in progress.
    Redeliver,
    /// Publish a `session_busy` error response (core NATS has no redelivery,
    /// and the last JetStream delivery has none left either).
    Respond,
}

/// Error code published when the target session stayed locked.
pub const SESSION_BUSY_CODE: &str = "session_busy";

/// What became of one request message; drives the JetStream ack decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// A response was published (success or error); ack.
    Handled,
    /// Session lock busy under [`BusyPolicy::Redeliver`]: no response was
    /// published; the durable consumer must Nak with a delay so the request is
    /// redelivered (bounded by `max_deliver`). Never ack this.
    Busy,
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
    /// `Greentic-Dispatch-Id`, when present and well formed.
    pub dispatch_id: Option<String>,
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

/// The `(tenant, env)` pair to stamp on a response, from the request's own
/// scope headers. `env` defaults to `default` exactly as [`parse_request`]
/// does. `None` when either id fails [`validate_scope_id`] (or tenant is
/// absent): an unvalidated id is never echoed into a header.
///
/// The host's listener drops a response with no tenant/env headers, so every
/// error response whose scope is valid must carry them.
#[must_use]
pub fn response_scope(tenant: Option<&str>, env: Option<&str>) -> Option<(String, String)> {
    let tenant = tenant?;
    let env = env.unwrap_or("default");
    if validate_scope_id("tenant", tenant).is_ok() && validate_scope_id("env", env).is_ok() {
        Some((tenant.to_string(), env.to_string()))
    } else {
        None
    }
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
    let dispatch_id = get_header(DISPATCH_ID_HEADER).filter(|id| valid_dispatch_id(id));
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
        dispatch_id,
        request,
    })
}

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
    invoker: Arc<dyn AgentDispatchInvoker>,
    tenant: &str,
    env: &str,
    ident: DispatchIdentity<'_>,
    req: RuntimeDispatchRequest,
) -> Result<RuntimeDispatchResponse, anyhow::Error> {
    let outcome = invoker
        .invoke_identified(tenant, env, &req.target, &req.operation, req.input, ident)
        .await?;
    Ok(RuntimeDispatchResponse {
        ok: outcome.ok,
        output: outcome.output,
        events: outcome.events,
        error: outcome.error,
    })
}

/// Invoke and build the response (no NATS I/O). Errors map to an error response.
pub async fn build_response(
    invoker: Arc<dyn AgentDispatchInvoker>,
    tenant: &str,
    env: &str,
    idempotency_key: Option<&str>,
    req: RuntimeDispatchRequest,
) -> RuntimeDispatchResponse {
    let ident = DispatchIdentity {
        idempotency_key,
        dispatch_id: None,
    };
    match dispatch(invoker, tenant, env, ident, req).await {
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
    invoker: Arc<dyn AgentDispatchInvoker>,
    msg: async_nats::Message,
    busy: BusyPolicy,
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
                let tenant = get_header("Greentic-Tenant");
                let env = get_header("Greentic-Env");
                let scope = response_scope(tenant.as_deref(), env.as_deref());
                publish_response(
                    client,
                    Some(&correlation),
                    scope.as_ref().map(|(t, e)| (t.as_str(), e.as_str())),
                    &resp,
                )
                .await?;
            }
            return Ok(Disposition::Rejected);
        }
    };

    let resp = match dispatch(
        invoker,
        &parsed.tenant,
        &parsed.env,
        DispatchIdentity {
            idempotency_key: parsed.idempotency.as_deref(),
            dispatch_id: parsed.dispatch_id.as_deref(),
        },
        parsed.request,
    )
    .await
    {
        Ok(resp) => resp,
        Err(error) if is_lock_contention(&error) && busy == BusyPolicy::Redeliver => {
            tracing::warn!(%error, "session busy; requesting redelivery");
            return Ok(Disposition::Busy);
        }
        Err(error) => failure_response(&error),
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
    invoker: Arc<dyn AgentDispatchInvoker>,
    msg: async_nats::Message,
) -> Result<()> {
    // Core NATS has no redelivery: answer a busy session instead of dropping it.
    handle_message_outcome(client, invoker, msg, BusyPolicy::Respond)
        .await
        .map(|_| ())
}

/// Publish an error response for an already-received request message, echoing
/// its correlation id and scope headers. Returns `Ok(false)` without
/// publishing when the message carries no correlation id, since nobody is
/// waiting for it then.
pub async fn publish_error_for_message(
    client: &async_nats::Client,
    msg: &async_nats::Message,
    code: &str,
    message: String,
) -> Result<bool> {
    let header = |name: &str| -> Option<String> {
        msg.headers
            .as_ref()
            .and_then(|header_map| header_map.get(name))
            .map(|value| value.as_str().to_string())
    };
    let Some(correlation) = header("Greentic-Correlation-Id") else {
        return Ok(false);
    };
    let tenant = header("Greentic-Tenant");
    let env = header("Greentic-Env");
    let scope = response_scope(tenant.as_deref(), env.as_deref());
    publish_response(
        client,
        Some(&correlation),
        scope.as_ref().map(|(t, e)| (t.as_str(), e.as_str())),
        &error_response(code, message),
    )
    .await?;
    Ok(true)
}

/// Queue group shared by all bridge replicas, so a request is delivered to ONE
/// worker rather than fanned out to every replica.
pub const QUEUE_GROUP: &str = "agentic-workers";

/// Subscribe to the request subject as part of [`QUEUE_GROUP`] and serve
/// forever (one spawned task per message).
pub async fn run_bridge(
    client: async_nats::Client,
    invoker: Arc<dyn AgentDispatchInvoker>,
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
    use serde_json::json;
    use std::sync::Mutex;

    /// (tenant/env elided, target, operation, payload, idempotency_key)
    type SeenCall = (String, String, Value, Option<String>);

    struct StubInvoker {
        seen: Mutex<Vec<SeenCall>>,
    }

    #[async_trait]
    impl AgentDispatchInvoker for StubInvoker {
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
                output: json!({"reply": "pong", "trail": [], "terminated_by": "reply"}),
                events: vec![],
                error: None,
            })
        }
    }

    fn sample_request() -> RuntimeDispatchRequest {
        RuntimeDispatchRequest {
            target: "greeter".into(),
            operation: String::new(),
            mode: DispatchMode::Await,
            input: json!({"user_text": "ping"}),
            deadline_ms: Some(30_000),
        }
    }

    #[test]
    fn subjects_use_agentic_runtime_name() {
        assert_eq!(request_topic(RUNTIME_NAME), "greentic.agentic.request.v1");
        assert_eq!(response_topic(RUNTIME_NAME), "greentic.agentic.response.v1");
    }

    #[tokio::test]
    async fn handle_invokes_and_maps_agent_output_to_response() {
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
        assert_eq!(resp.output["reply"], json!("pong"));
        assert_eq!(resp.output["terminated_by"], json!("reply"));
        assert!(resp.error.is_none());

        let seen = invoker.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let (target, _operation, input, idempotency) = &seen[0];
        assert_eq!(target, "greeter", "dispatch target maps to agent id");
        assert_eq!(input["user_text"], json!("ping"));
        assert_eq!(
            idempotency.as_deref(),
            Some("sess-1::pack=p::flow=f"),
            "correlation/idempotency hint is forwarded to the invoker"
        );
    }

    #[tokio::test]
    async fn invoke_error_maps_to_error_response() {
        struct FailInvoker;

        #[async_trait]
        impl AgentDispatchInvoker for FailInvoker {
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

    #[test]
    fn dispatch_id_header_is_parsed_and_validated() {
        let payload = serde_json::to_vec(&sample_request()).unwrap();
        let with = |id: &'static str| {
            let headers = [("Greentic-Tenant", "acme"), (DISPATCH_ID_HEADER, id)];
            parse_request(header_fn(&headers), &payload)
                .unwrap()
                .dispatch_id
        };
        assert_eq!(with("abc123").as_deref(), Some("abc123"));
        assert_eq!(with("bad:id"), None, "unsafe ids are treated as absent");
        assert_eq!(with(""), None);
        let headers = [("Greentic-Tenant", "acme")];
        assert_eq!(
            parse_request(header_fn(&headers), &payload)
                .unwrap()
                .dispatch_id,
            None
        );
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

    #[tokio::test]
    async fn lock_contention_answers_session_busy_instead_of_dropping() {
        struct BusyInvoker;

        #[async_trait]
        impl AgentDispatchInvoker for BusyInvoker {
            async fn invoke(
                &self,
                _tenant: &str,
                _env: &str,
                _target: &str,
                _operation: &str,
                _input: Value,
                _idempotency_key: Option<&str>,
            ) -> Result<InvokeOutcome> {
                Err(anyhow::Error::new(LockContention("s".into())))
            }
        }

        let resp = build_response(
            Arc::new(BusyInvoker),
            "acme",
            "prod",
            Some("c"),
            sample_request(),
        )
        .await;
        assert!(!resp.ok);
        assert_eq!(
            resp.error.map(|e| e.code).as_deref(),
            Some(SESSION_BUSY_CODE)
        );
    }

    #[tokio::test]
    async fn invoke_failure_outcome_error_is_forwarded() {
        struct FailedStep;

        #[async_trait]
        impl AgentDispatchInvoker for FailedStep {
            async fn invoke(
                &self,
                _tenant: &str,
                _env: &str,
                _target: &str,
                _operation: &str,
                _input: Value,
                _idempotency_key: Option<&str>,
            ) -> Result<InvokeOutcome> {
                Ok(InvokeOutcome {
                    ok: false,
                    output: json!({"reply": "partial", "trail": [], "terminated_by": "timeout"}),
                    events: vec![],
                    error: Some(DispatchError {
                        code: "timeout".into(),
                        message: "agent step ended: timeout".into(),
                    }),
                })
            }
        }

        let resp =
            build_response(Arc::new(FailedStep), "acme", "prod", None, sample_request()).await;
        assert!(!resp.ok);
        assert_eq!(resp.error.map(|e| e.code).as_deref(), Some("timeout"));
        assert_eq!(resp.output["reply"], json!("partial"));
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
    fn response_scope_is_kept_for_valid_ids_and_dropped_for_invalid_ones() {
        assert_eq!(
            response_scope(Some("acme"), Some("prod")),
            Some(("acme".into(), "prod".into()))
        );
        // Missing env defaults like `parse_request`.
        assert_eq!(
            response_scope(Some("acme"), None),
            Some(("acme".into(), "default".into()))
        );
        assert_eq!(response_scope(None, Some("prod")), None);
        assert_eq!(response_scope(Some("a:b"), Some("prod")), None);
        assert_eq!(response_scope(Some("acme"), Some("../x")), None);
    }

    #[test]
    fn queue_group_is_stable() {
        assert_eq!(QUEUE_GROUP, "agentic-workers");
    }
}
