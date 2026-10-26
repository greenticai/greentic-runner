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

/// Deadline watchdogs in flight, keyed by correlation id. Each entry carries the
/// generation it was registered under, so a task removes only its OWN entry and
/// never a newer watchdog registered under the same (deterministic) id.
///
/// Process-global because the task that publishes a request
/// ([`NatsDispatcher`]) and the task that receives its response
/// (`dispatch_listener`) are separate objects. An ADMITTED response cancels the
/// matching watchdog so a call that already completed never publishes a late
/// `timeout`.
type WatchdogMap = std::collections::HashMap<String, (u64, tokio::task::JoinHandle<()>)>;

static WATCHDOGS: std::sync::LazyLock<parking_lot::Mutex<WatchdogMap>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

static WATCHDOG_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn next_watchdog_generation() -> u64 {
    WATCHDOG_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Register `handle` as the watchdog for `correlation_id` under `generation`,
/// aborting any previous one registered under the same id.
fn register_watchdog(correlation_id: String, generation: u64, handle: tokio::task::JoinHandle<()>) {
    if let Some((_, previous)) = WATCHDOGS
        .lock()
        .insert(correlation_id, (generation, handle))
    {
        previous.abort();
    }
}

/// Remove the entry for `correlation_id` only if it is still `generation`'s.
fn deregister_watchdog(correlation_id: &str, generation: u64) {
    let mut watchdogs = WATCHDOGS.lock();
    if watchdogs
        .get(correlation_id)
        .is_some_and(|(current, _)| *current == generation)
    {
        watchdogs.remove(correlation_id);
    }
}

/// Spawn the watchdog task and register it under `correlation_id` at once.
/// After `deadline_ms` it deregisters itself (so its own echo through the
/// listener does not abort the running task) and then runs `on_timeout`.
/// Returns the generation it was registered under.
fn arm_watchdog<F>(correlation_id: String, deadline_ms: u64, on_timeout: F) -> u64
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let generation = next_watchdog_generation();
    let key = correlation_id.clone();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(deadline_ms)).await;
        deregister_watchdog(&key, generation);
        on_timeout.await;
    });
    register_watchdog(correlation_id, generation, handle);
    generation
}

/// Abort and remove the watchdog `generation` registered, if still current.
fn disarm_watchdog(correlation_id: &str, generation: u64) {
    let mut watchdogs = WATCHDOGS.lock();
    if watchdogs
        .get(correlation_id)
        .is_some_and(|(current, _)| *current == generation)
        && let Some((_, handle)) = watchdogs.remove(correlation_id)
    {
        handle.abort();
    }
}

#[cfg(test)]
pub(crate) fn register_watchdog_for_test(
    correlation_id: &str,
    handle: tokio::task::JoinHandle<()>,
) -> u64 {
    let generation = next_watchdog_generation();
    register_watchdog(correlation_id.to_string(), generation, handle);
    generation
}

#[cfg(test)]
pub(crate) fn deregister_watchdog_for_test(correlation_id: &str, generation: u64) {
    deregister_watchdog(correlation_id, generation);
}

#[cfg(test)]
pub(crate) fn watchdog_pending(correlation_id: &str) -> bool {
    WATCHDOGS.lock().contains_key(correlation_id)
}

/// Cancel the deadline watchdog for `correlation_id`, if one is pending.
/// Returns whether one was cancelled. Called when a response arrives.
pub fn cancel_watchdog(correlation_id: &str) -> bool {
    match WATCHDOGS.lock().remove(correlation_id) {
        Some((_, handle)) => {
            handle.abort();
            true
        }
        None => false,
    }
}

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

/// A fresh random per-publish dispatch id (32 lowercase hex characters).
fn new_dispatch_id() -> String {
    use rand::RngExt as _;
    let mut bytes = [0u8; 16];
    rand::rng().fill(&mut bytes);
    hex::encode(bytes)
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
    // Unique per publish (stable across broker redelivery of this message), so a
    // consumer can dedup a redelivery without replaying an earlier turn of the
    // same conversation. Kept OUT of the correlation id, which bridges reuse as
    // the downstream session id.
    headers.insert("Greentic-Dispatch-Id", new_dispatch_id().as_str());

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

/// Mark a failure that happened before anything reached the wire, so a
/// transient one still lets the flow retry the turn.
fn pre_effect_failure(error: impl Into<anyhow::Error>) -> anyhow::Error {
    crate::runner::engine::pre_effect(error)
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

        // Nothing has been sent when the request cannot even be built or the
        // publish is refused, so both are provably pre-effect: a transient one
        // still lets the flow retry the turn (see `engine::pre_effect`).
        let built = build_request(&request).map_err(pre_effect_failure)?;

        // Arm the deadline watchdog BEFORE the request is on the wire: a fast
        // response can be admitted before this task resumes, and its cancel
        // must find the watchdog rather than let it arm afterwards and fire a
        // spurious timeout.
        let armed = maybe_timeout.map(
            |(deadline_ms, runtime, correlation_id, tenant, env, decision_token)| {
                let timeout_client = self.client.clone();
                let watchdog_key = correlation_id.clone();
                let generation = arm_watchdog(watchdog_key.clone(), deadline_ms, async move {
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
                            correlation_id = %crate::log_safe::log_safe(&correlation_id),
                            deadline_ms,
                            "failed to publish timeout response for awaited dispatch"
                        );
                    }
                });
                (watchdog_key, generation)
            },
        );

        if let Err(error) = self
            .client
            .publish_with_headers(built.subject, built.headers, built.body.into())
            .await
        {
            // Nothing was sent, so there is nothing to time out.
            if let Some((key, generation)) = armed {
                disarm_watchdog(&key, generation);
            }
            return Err(pre_effect_failure(error));
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
    fn pre_effect_failure_marks_without_changing_the_message() {
        let err = pre_effect_failure(anyhow::anyhow!("publish refused"));
        assert!(crate::runner::engine::is_pre_effect_error(&err));
        assert_eq!(err.to_string(), "publish refused");
    }

    /// Minimal NATS stand-in that completes the handshake (answers `PING`).
    async fn fake_nats_server() -> String {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (read, mut write) = stream.into_split();
            let info = r#"INFO {"server_id":"fake","server_name":"fake","version":"2.10.0","proto":1,"max_payload":1024,"headers":true,"host":"127.0.0.1","port":0}"#;
            if write
                .write_all(format!("{info}\r\n").as_bytes())
                .await
                .is_err()
            {
                return;
            }
            let mut lines = BufReader::new(read).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.starts_with("PING") && write.write_all(b"PONG\r\n").await.is_err() {
                    return;
                }
            }
        });
        format!("nats://{addr}")
    }

    /// A publish the client refuses (here: an invalid subject)
    /// fails `dispatch` as a pre-effect error and leaves no watchdog armed.
    #[tokio::test]
    async fn a_refused_publish_is_pre_effect_and_disarms_the_watchdog() {
        let url = fake_nats_server().await;
        let client = async_nats::connect(url).await.expect("fake server connect");
        let dispatcher = NatsDispatcher::new(client);
        let mut request = sample(DispatchMode::Await);
        request.correlation_id = "corr-refused-publish".into();
        // A subject with whitespace fails the client's subject validation,
        // after the watchdog was armed and before anything is sent.
        request.runtime = "bad runtime".into();
        let err = dispatcher
            .dispatch(request)
            .await
            .expect_err("a publish to an invalid subject is refused");
        assert!(crate::runner::engine::is_pre_effect_error(&err), "{err:#}");
        assert!(
            !watchdog_pending("corr-refused-publish"),
            "the watchdog armed before the publish is disarmed"
        );
    }

    #[tokio::test]
    async fn an_expiring_watchdog_never_removes_a_newer_one() {
        let old =
            register_watchdog_for_test("corr-gen", tokio::spawn(std::future::pending::<()>()));
        let newer =
            register_watchdog_for_test("corr-gen", tokio::spawn(std::future::pending::<()>()));
        assert_ne!(old, newer);
        deregister_watchdog_for_test("corr-gen", old);
        assert!(watchdog_pending("corr-gen"), "stale generation is a no-op");
        deregister_watchdog_for_test("corr-gen", newer);
        assert!(!watchdog_pending("corr-gen"));
    }

    #[tokio::test]
    async fn a_response_admitted_right_after_arming_prevents_the_timeout() {
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&fired);
        // Armed first (before publish); the response then cancels it.
        arm_watchdog("corr-early".to_string(), 30, async move {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        assert!(
            cancel_watchdog("corr-early"),
            "cancel must find the armed watchdog"
        );
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(!fired.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_failed_publish_disarms_only_its_own_watchdog() {
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&fired);
        let old = arm_watchdog("corr-disarm".to_string(), 30, async move {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let newer = arm_watchdog("corr-disarm".to_string(), 10_000, async {});
        disarm_watchdog("corr-disarm", old);
        assert!(
            watchdog_pending("corr-disarm"),
            "stale generation is a no-op"
        );
        disarm_watchdog("corr-disarm", newer);
        assert!(!watchdog_pending("corr-disarm"));
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(!fired.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_response_cancels_the_pending_watchdog() {
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&fired);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        register_watchdog_for_test("corr-cancel", handle);
        assert!(cancel_watchdog("corr-cancel"));
        assert!(!cancel_watchdog("corr-cancel"), "second cancel is a no-op");
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        assert!(
            !fired.load(std::sync::atomic::Ordering::SeqCst),
            "cancelled watchdog must never fire"
        );
    }

    #[test]
    fn each_publish_gets_its_own_dispatch_id_but_the_same_correlation() {
        let first = build_request(&sample(DispatchMode::Await)).unwrap();
        let second = build_request(&sample(DispatchMode::Await)).unwrap();
        let id = |built: &BuiltRequest| {
            built
                .headers
                .get("Greentic-Dispatch-Id")
                .map(|v| v.as_str().to_string())
        };
        let (a, b) = (id(&first), id(&second));
        assert!(a.is_some() && b.is_some());
        assert_ne!(a, b, "two turns must not share a dispatch id");
        let key = |built: &BuiltRequest| {
            built
                .headers
                .get("Greentic-Idempotency-Key")
                .map(|v| v.as_str().to_string())
        };
        assert_eq!(key(&first), key(&second), "the session hint is unchanged");
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
