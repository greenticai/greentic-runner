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

    /// [`Self::resume`], reporting whether the response was ADMITTED (handed to
    /// the runtime) or dropped by an admission check (wrong gate, bad token).
    /// An implementation cancels the deadline watchdog itself, at the moment
    /// of admission and BEFORE the resumed flow runs, so a forged or stale
    /// response cannot retire the real watchdog and the resumed flow's next
    /// dispatch (same deterministic id) keeps its own. The default treats
    /// every response as admitted.
    async fn resume_checked(
        &self,
        tenant: TenantCtx,
        correlation_id: &str,
        output: Value,
    ) -> Result<ResumeOutcome> {
        // No admission check here, so every response is admitted: retire the
        // watchdog BEFORE resuming (the resume runs the flow until it next
        // parks, and that park may register a new watchdog under this same id).
        crate::runner::remote_dispatch::cancel_watchdog(correlation_id);
        self.resume(tenant, correlation_id, output)
            .await
            .map(|()| ResumeOutcome::Admitted)
    }
}

/// What a resumer did with a response it did not fail on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// Handed to the runtime.
    Admitted,
    /// Refused by an admission check; nothing was resumed.
    Dropped,
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
    dispatch_to_resumer_outcome(resumer, input).await;
}

/// [`dispatch_to_resumer`], reporting what the resumer did (`None` on failure).
async fn dispatch_to_resumer_outcome(
    resumer: Arc<dyn SessionResumer>,
    input: ResumeInput,
) -> Option<ResumeOutcome> {
    match resumer
        .resume_checked(input.tenant, &input.correlation_id, input.output)
        .await
    {
        // The watchdog was already retired inside `resume_checked`, at
        // admission and before the resumed flow ran; cancelling here would kill
        // a watchdog that flow registered for its next dispatch.
        Ok(outcome) => Some(outcome),
        Err(error) => {
            tracing::error!(
                %error,
                correlation_id = %crate::log_safe::log_safe(&input.correlation_id),
                "failed to resume session on dispatch response; the response is spent and its deadline watchdog was retired at admission, so a wait that is still parked now has no timeout (known limitation, see CLAUDE.md)"
            );
            None
        }
    }
}

/// Subject on which a replica that ADMITTED a response tells every other
/// replica to retire the deadline watchdog it may have armed for the same
/// correlation id.
///
/// The watchdog map is process-local, but the response listeners share a queue
/// group per tenant, so the replica that receives (and admits) a response need
/// not be the one that armed the watchdog. Without this, the arming replica's
/// watchdog outlives the call it guarded and later publishes a bogus `timeout`
/// into whatever wait the conversation is parked on by then (the correlation id
/// is deterministic per conversation).
pub const WATCHDOG_CANCEL_SUBJECT: &str = "greentic.dispatch.watchdog-cancel.v1";

/// Wire body of a watchdog-cancel notice.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WatchdogCancel {
    pub tenant: String,
    pub correlation_id: String,
    /// Random id of the publishing replica; it ignores its own notice, since it
    /// already retired the watchdog at admission and may by now have armed the
    /// next one under the same id.
    pub origin: String,
}

/// Pure: the correlation id to cancel for `payload`, or `None` when the notice
/// is malformed, came from `own_origin`, or names another tenant.
pub fn watchdog_cancel_target(
    scope_tenant: &str,
    own_origin: &str,
    payload: &[u8],
) -> Option<String> {
    let notice: WatchdogCancel = serde_json::from_slice(payload).ok()?;
    if notice.origin == own_origin
        || notice.tenant != scope_tenant
        || notice.correlation_id.is_empty()
    {
        return None;
    }
    Some(notice.correlation_id)
}

/// This process's replica id for the watchdog-cancel broadcast. One value per
/// process, because the watchdog map it filters is per process: a pack reload
/// builds a new `TenantRuntime` whose listeners must still recognise (and
/// ignore) the notices the previous one published.
pub fn process_cancel_origin() -> &'static str {
    static ORIGIN: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ORIGIN.get_or_init(|| {
        use rand::RngExt as _;
        let mut bytes = [0u8; 16];
        rand::rng().fill(&mut bytes);
        hex::encode(bytes)
    })
}

/// Callback invoked with a correlation id the moment a response is admitted,
/// to tell the other replicas to retire their watchdog for it.
pub type WatchdogNotifier = Arc<dyn Fn(&str) + Send + Sync>;

/// Pure: encode a watchdog-cancel notice.
pub fn encode_watchdog_cancel(
    tenant: &str,
    correlation_id: &str,
    origin: &str,
) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&WatchdogCancel {
        tenant: tenant.to_string(),
        correlation_id: correlation_id.to_string(),
        origin: origin.to_string(),
    })
}

/// A [`WatchdogNotifier`] that publishes on [`WATCHDOG_CANCEL_SUBJECT`] (fire
/// and forget on the current tokio runtime) under [`process_cancel_origin`].
pub fn nats_watchdog_notifier(client: async_nats::Client, tenant: String) -> WatchdogNotifier {
    Arc::new(move |correlation_id: &str| {
        let body = match encode_watchdog_cancel(&tenant, correlation_id, process_cancel_origin()) {
            Ok(body) => body,
            Err(error) => {
                tracing::warn!(%error, "could not encode watchdog cancel");
                return;
            }
        };
        let client = client.clone();
        tokio::spawn(async move {
            if let Err(error) = client.publish(WATCHDOG_CANCEL_SUBJECT, body.into()).await {
                tracing::warn!(%error, "could not announce watchdog cancel");
            }
        });
    })
}

/// Retire the local deadline watchdog for an ADMITTED response and tell the
/// other replicas, at admission and before the resumed flow runs (so the
/// notice goes out even if that run later fails).
pub fn retire_watchdog_on_admission(notifier: Option<&WatchdogNotifier>, correlation_id: &str) {
    crate::runner::remote_dispatch::cancel_watchdog(correlation_id);
    if let Some(notify) = notifier {
        notify(correlation_id);
    }
}

/// Retire watchdogs on behalf of other replicas' admitted responses. Plain
/// (non-queue) subscription: EVERY replica must see every notice.
pub async fn run_watchdog_cancel_listener(
    client: async_nats::Client,
    tenant: String,
    origin: String,
) -> Result<()> {
    let mut subscription = client.subscribe(WATCHDOG_CANCEL_SUBJECT).await?;
    while let Some(msg) = subscription.next().await {
        if let Some(correlation_id) = watchdog_cancel_target(&tenant, &origin, &msg.payload) {
            crate::runner::remote_dispatch::cancel_watchdog(&correlation_id);
        }
    }
    Ok(())
}

type Job = futures::future::BoxFuture<'static, ()>;

/// Runs jobs submitted under the same key strictly in submission order, while
/// jobs under different keys run concurrently. A key's queue exists only while
/// it has work, so the map does not grow with finished conversations.
#[derive(Clone, Default)]
pub struct KeyedSerializer {
    queues:
        Arc<parking_lot::Mutex<std::collections::HashMap<String, std::collections::VecDeque<Job>>>>,
}

impl KeyedSerializer {
    /// Queue `job` behind any pending work for `key`, or start a worker for it.
    pub fn submit(&self, key: String, job: Job) {
        {
            let mut queues = self.queues.lock();
            if let Some(queue) = queues.get_mut(&key) {
                queue.push_back(job);
                return;
            }
            queues.insert(key.clone(), std::collections::VecDeque::new());
        }
        let queues = Arc::clone(&self.queues);
        tokio::spawn(async move {
            let mut current = job;
            loop {
                // Isolate panics (the resumer runs WASM): a panicking job must
                // not kill the worker before pop-or-remove, or the key and the
                // queued jobs (with their semaphore permits) would leak.
                if let Err(error) = tokio::spawn(current).await {
                    tracing::error!(%error, key = %key, "keyed job panicked or was cancelled; continuing");
                }
                // Pop-or-remove under one lock so a concurrent `submit` either
                // sees the queue (and is picked up here) or sees no key (and
                // starts its own worker).
                let next = {
                    let mut guard = queues.lock();
                    match guard.get_mut(&key).and_then(|queue| queue.pop_front()) {
                        Some(next) => next,
                        None => {
                            guard.remove(&key);
                            return;
                        }
                    }
                };
                current = next;
            }
        });
    }

    /// Number of keys with pending or running work (test hook).
    #[cfg(test)]
    fn active_keys(&self) -> usize {
        self.queues.lock().len()
    }
}

/// Upper bound on response messages handled concurrently by one listener.
pub const MAX_CONCURRENT_RESUMES: usize = 32;

/// How a listener joins the broker: an optional queue group (so replicas and
/// reloaded runtimes share one delivery of each message instead of each
/// receiving a copy) and an optional tenant filter (messages for other tenants
/// are not this listener's to resume).
#[derive(Clone, Debug, Default)]
pub struct ListenerScope {
    pub queue_group: Option<String>,
    pub tenant: Option<String>,
}

/// Pure: should a listener scoped to `scope_tenant` handle a message whose
/// `Greentic-Tenant` header is `header_tenant`? An unscoped listener handles
/// everything (the decoder applies its own default tenant). A tenant-scoped one
/// handles only a message that names its tenant: with per-tenant queue groups a
/// headerless message would otherwise reach every tenant's listener.
pub fn tenant_matches(scope_tenant: Option<&str>, header_tenant: Option<&str>) -> bool {
    match scope_tenant {
        None => true,
        Some(scope) => {
            matches!(header_tenant, Some(header) if !header.is_empty() && header == scope)
        }
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
    run_response_listener_scoped(client, runtime, resumer, ListenerScope::default()).await
}

/// [`run_response_listener`] with an explicit [`ListenerScope`]. Each message is
/// handled off the receive loop (bounded by [`MAX_CONCURRENT_RESUMES`]) so one
/// slow resume cannot stall every other conversation's response; responses for
/// the SAME correlation id are serialised in arrival order.
pub async fn run_response_listener_scoped(
    client: async_nats::Client,
    runtime: String,
    resumer: Arc<dyn SessionResumer>,
    scope: ListenerScope,
) -> Result<()> {
    let subject = response_topic(&runtime);
    let mut subscription = match scope.queue_group.clone() {
        Some(group) => client.queue_subscribe(subject, group).await?,
        None => client.subscribe(subject).await?,
    };
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_RESUMES));
    let serializer = KeyedSerializer::default();
    while let Some(msg) = subscription.next().await {
        let headers = msg.headers.as_ref();
        let get_header = |name: &str| headers.and_then(|h| h.get(name)).map(|v| v.as_str());
        if !tenant_matches(scope.tenant.as_deref(), get_header("Greentic-Tenant")) {
            if scope.tenant.is_some()
                && get_header("Greentic-Tenant").is_none_or(|header| header.is_empty())
            {
                // The awaiting flow is NOT resumed by a dropped response: it
                // stays parked until its watchdog fires (indefinitely when the
                // dispatch carried no deadline). Name the correlation id so an
                // operator can find the wedged session.
                tracing::warn!(
                    correlation_id = %crate::log_safe::log_safe(
                        get_header("Greentic-Correlation-Id").unwrap_or("<none>")
                    ),
                    "dropping dispatch response without a Greentic-Tenant header on a tenant-scoped listener; \
                     the awaiting flow stays parked until its watchdog fires"
                );
            }
            continue;
        }
        match decode_response(
            get_header("Greentic-Correlation-Id"),
            get_header("Greentic-Tenant"),
            get_header("Greentic-Env"),
            &msg.payload,
        ) {
            Ok(input) => {
                let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
                    break;
                };
                let resumer = Arc::clone(&resumer);
                // Per-conversation order: responses sharing a correlation id
                // are handled one at a time, in arrival order.
                let key = input.correlation_id.clone();
                serializer.submit(
                    key,
                    Box::pin(async move {
                        dispatch_to_resumer_outcome(resumer, input).await;
                        drop(permit);
                    }),
                );
            }
            Err(error) => tracing::warn!(%error, "bad dispatch response; dropping"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_notifies_even_when_nothing_is_armed() {
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let sink = Arc::clone(&seen);
        let notifier: WatchdogNotifier = Arc::new(move |c: &str| {
            sink.lock().unwrap().push(c.to_string());
        });
        retire_watchdog_on_admission(Some(&notifier), "corr-admit");
        retire_watchdog_on_admission(None, "corr-none");
        assert_eq!(*seen.lock().unwrap(), vec!["corr-admit".to_string()]);
    }

    #[test]
    fn process_origin_is_stable_and_filters_own_notice() {
        assert_eq!(process_cancel_origin(), process_cancel_origin());
        let body = encode_watchdog_cancel("acme", "c", process_cancel_origin()).unwrap();
        assert_eq!(
            watchdog_cancel_target("acme", process_cancel_origin(), &body),
            None
        );
        assert_eq!(
            watchdog_cancel_target("acme", "other", &body),
            Some("c".to_string())
        );
    }

    use greentic_types::{DispatchError, RuntimeDispatchResponse};
    use serde_json::json;
    use std::sync::Mutex;

    #[test]
    fn watchdog_cancel_targets_only_other_replicas_same_tenant() {
        let body = |tenant: &str, origin: &str| {
            serde_json::to_vec(&WatchdogCancel {
                tenant: tenant.into(),
                correlation_id: "corr-1".into(),
                origin: origin.into(),
            })
            .unwrap()
        };
        assert_eq!(
            watchdog_cancel_target("acme", "me", &body("acme", "other")),
            Some("corr-1".to_string())
        );
        assert_eq!(
            watchdog_cancel_target("acme", "me", &body("acme", "me")),
            None
        );
        assert_eq!(
            watchdog_cancel_target("acme", "me", &body("globex", "other")),
            None
        );
        assert_eq!(watchdog_cancel_target("acme", "me", b"not json"), None);
    }

    #[tokio::test]
    async fn a_remote_admission_notice_retires_the_local_watchdog() {
        let handle = tokio::spawn(std::future::pending::<()>());
        crate::runner::remote_dispatch::register_watchdog_for_test("corr-remote", handle);
        let body = serde_json::to_vec(&WatchdogCancel {
            tenant: "acme".into(),
            correlation_id: "corr-remote".into(),
            origin: "replica-b".into(),
        })
        .unwrap();
        // Replica A (origin "replica-a") armed the watchdog; B admitted.
        let target = watchdog_cancel_target("acme", "replica-a", &body).unwrap();
        assert!(crate::runner::remote_dispatch::cancel_watchdog(&target));
        assert!(!crate::runner::remote_dispatch::watchdog_pending(
            "corr-remote"
        ));
    }

    #[test]
    fn tenant_filter_only_rejects_a_different_tenant() {
        assert!(tenant_matches(None, Some("acme")));
        assert!(tenant_matches(Some("acme"), Some("acme")));
        assert!(!tenant_matches(Some("acme"), Some("other")));
        assert!(!tenant_matches(Some("acme"), None));
        assert!(!tenant_matches(Some("acme"), Some("")));
        assert!(tenant_matches(None, None));
    }

    #[tokio::test]
    async fn keyed_serializer_orders_per_key_and_cleans_up() {
        let serializer = KeyedSerializer::default();
        let log = Arc::new(Mutex::new(Vec::<(String, u32)>::new()));
        let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        for (key, n, delay) in [("a", 1u32, 40u64), ("a", 2, 0), ("b", 1, 0), ("a", 3, 0)] {
            let log = Arc::clone(&log);
            let done_tx = done_tx.clone();
            serializer.submit(
                key.to_string(),
                Box::pin(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    if let Ok(mut log) = log.lock() {
                        log.push((key.to_string(), n));
                    }
                    let _ = done_tx.send(());
                }),
            );
        }
        for _ in 0..4 {
            done_rx.recv().await;
        }
        let log = log.lock().unwrap().clone();
        let a: Vec<u32> = log
            .iter()
            .filter(|(k, _)| k == "a")
            .map(|(_, n)| *n)
            .collect();
        assert_eq!(a, vec![1, 2, 3], "same key keeps submission order");
        assert_eq!(log[0], ("b".to_string(), 1), "other keys are not blocked");
        for _ in 0..50 {
            if serializer.active_keys() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(serializer.active_keys(), 0);
    }

    struct DroppingResumer;

    #[async_trait]
    impl SessionResumer for DroppingResumer {
        async fn resume(&self, _: TenantCtx, _: &str, _: Value) -> Result<()> {
            Ok(())
        }
        async fn resume_checked(&self, _: TenantCtx, _: &str, _: Value) -> Result<ResumeOutcome> {
            Ok(ResumeOutcome::Dropped)
        }
    }

    #[tokio::test]
    async fn only_an_admitted_response_cancels_the_watchdog() {
        let body = serde_json::to_vec(&RuntimeDispatchResponse {
            ok: true,
            output: json!(null),
            events: vec![],
            error: None,
        })
        .unwrap();
        let handle = tokio::spawn(std::future::pending::<()>());
        crate::runner::remote_dispatch::register_watchdog_for_test("corr-admit", handle);
        let input =
            decode_response(Some("corr-admit"), Some("t1"), Some("default"), &body).unwrap();
        dispatch_to_resumer(Arc::new(DroppingResumer), input).await;
        assert!(
            crate::runner::remote_dispatch::watchdog_pending("corr-admit"),
            "a dropped response leaves the watchdog armed"
        );
        let input =
            decode_response(Some("corr-admit"), Some("t1"), Some("default"), &body).unwrap();
        let resumer = Arc::new(RecordingResumer {
            seen: Mutex::new(vec![]),
        });
        dispatch_to_resumer(resumer, input).await;
        assert!(!crate::runner::remote_dispatch::watchdog_pending(
            "corr-admit"
        ));
    }

    /// Models a resumed flow that parks on a second awaited dispatch with the
    /// same deterministic id: it registers a new watchdog while resuming.
    struct ReparkingResumer;

    #[async_trait]
    impl SessionResumer for ReparkingResumer {
        async fn resume(&self, _: TenantCtx, id: &str, _: Value) -> Result<()> {
            crate::runner::remote_dispatch::register_watchdog_for_test(
                id,
                tokio::spawn(std::future::pending::<()>()),
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn consecutive_awaited_dispatches_keep_the_second_watchdog() {
        let body = serde_json::to_vec(&RuntimeDispatchResponse {
            ok: true,
            output: json!(null),
            events: vec![],
            error: None,
        })
        .unwrap();
        crate::runner::remote_dispatch::register_watchdog_for_test(
            "corr-repark",
            tokio::spawn(std::future::pending::<()>()),
        );
        let input =
            decode_response(Some("corr-repark"), Some("t1"), Some("default"), &body).unwrap();
        dispatch_to_resumer(Arc::new(ReparkingResumer), input).await;
        assert!(
            crate::runner::remote_dispatch::watchdog_pending("corr-repark"),
            "the watchdog the resumed flow armed must survive the listener"
        );
        crate::runner::remote_dispatch::cancel_watchdog("corr-repark");
    }

    #[tokio::test]
    async fn keyed_serializer_survives_a_panicking_job() {
        let serializer = KeyedSerializer::default();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        serializer.submit(
            "k".to_string(),
            Box::pin(async {
                panic!("job panic (expected by test)");
            }),
        );
        serializer.submit(
            "k".to_string(),
            Box::pin(async move {
                let _ = tx.send(());
            }),
        );
        let ran = tokio::time::timeout(std::time::Duration::from_secs(2), rx).await;
        assert!(matches!(ran, Ok(Ok(()))), "next job on the key must run");
        for _ in 0..50 {
            if serializer.active_keys() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(serializer.active_keys(), 0, "key must be cleaned up");
    }

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
