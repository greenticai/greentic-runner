use super::api::{FlowSchema, FlowSummary};
use super::error::{GResult, RunnerError};
use super::host::{
    HostBundle, OutboxKey, SessionKey, SessionOutboxEntry, SessionSnapshot, SpanContext, WaitState,
};
use super::policy::retry_with_jitter;
use super::policy::{Policy, policy_violation};
use super::registry::{AdapterCall, AdapterRegistry};
use greentic_types::TenantCtx;
use parking_lot::RwLock;
use rand::{RngExt, rng};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

const PROVIDER_ID: &str = "greentic-runner";
pub const PAYLOAD_FROM_LAST_INPUT: &str = "$ingress";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FlowKey {
    pack_id: String,
    flow_id: String,
}

impl FlowKey {
    fn new(pack_id: &str, flow_id: &str) -> Self {
        Self {
            pack_id: pack_id.to_string(),
            flow_id: flow_id.to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FlowDefinition {
    pub summary: FlowSummary,
    pub schema: Value,
    pub steps: Vec<FlowStep>,
}

impl FlowDefinition {
    pub fn new(summary: FlowSummary, schema: Value, steps: Vec<FlowStep>) -> Self {
        Self {
            summary,
            schema,
            steps,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FlowStep {
    Adapter(AdapterCall),
    AwaitInput { reason: String },
    Complete { outcome: Value },
}

pub struct StateMachine {
    host: Arc<HostBundle>,
    adapters: AdapterRegistry,
    policy: Policy,
    flows: Arc<RwLock<HashMap<FlowKey, FlowDefinition>>>,
}

impl StateMachine {
    pub fn new(host: Arc<HostBundle>, adapters: AdapterRegistry, policy: Policy) -> Self {
        Self {
            host,
            adapters,
            policy,
            flows: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_flow(&self, definition: FlowDefinition) {
        let mut guard = self.flows.write();
        guard.insert(
            FlowKey::new(&definition.summary.pack_id, &definition.summary.id),
            definition,
        );
    }

    pub fn list_flows(&self) -> Vec<FlowSummary> {
        let guard = self.flows.read();
        guard.values().map(|flow| flow.summary.clone()).collect()
    }

    pub fn get_flow_schema(&self, pack_id: &str, flow_id: &str) -> GResult<FlowSchema> {
        let guard = self.flows.read();
        guard
            .get(&FlowKey::new(pack_id, flow_id))
            .map(|flow| FlowSchema {
                pack_id: pack_id.to_string(),
                id: flow_id.to_string(),
                schema_json: flow.schema.clone(),
            })
            .ok_or_else(|| RunnerError::FlowNotFound {
                flow_id: flow_id.to_string(),
            })
    }

    pub async fn step(
        &self,
        tenant: &TenantCtx,
        pack_id: &str,
        flow_id: &str,
        session_hint: Option<String>,
        input: Value,
    ) -> GResult<(Value, serde_json::Map<String, Value>)> {
        let mut telemetry_ctx = tenant
            .clone()
            .with_provider(PROVIDER_ID.to_string())
            .with_flow(flow_id.to_string());
        if let Some(hint) = session_hint.as_ref() {
            telemetry_ctx = telemetry_ctx.with_session(hint.clone());
        }
        greentic_types::telemetry::set_current_tenant_ctx(&telemetry_ctx);

        let flow = {
            let guard = self.flows.read();
            guard
                .get(&FlowKey::new(pack_id, flow_id))
                .cloned()
                .ok_or_else(|| RunnerError::FlowNotFound {
                    flow_id: flow_id.to_string(),
                })?
        };

        self.ensure_policy_budget(&flow)?;

        let key = SessionKey::new(tenant, pack_id, flow_id, session_hint.clone());
        let session_host = &self.host.session;
        let mut session = match session_host.get(&key).await? {
            Some(snapshot) => snapshot,
            None => {
                let session_id = key
                    .stable_session_id()
                    .unwrap_or_else(Self::generate_session_id);
                SessionSnapshot::new(key.clone(), session_id)
            }
        };
        let is_new = session.revision == 0 && session.outbox.is_empty();
        let expected_revision = session.revision;

        Self::update_state_input(&mut session, input.clone());

        if session.waiting.is_some() {
            if session.cursor.position < flow.steps.len()
                && matches!(
                    flow.steps.get(session.cursor.position),
                    Some(FlowStep::AwaitInput { .. })
                )
            {
                session.cursor.position = session.cursor.position.saturating_add(1);
            }
            session.waiting = None;
        }

        // Per-node outputs for this turn, keyed by the real flow node id (e.g.
        // `insights_agent`), merged from each adapter step's FlowEngine map.
        // Observability only — never feeds back into `outcome` or session state;
        // callers that don't need it (`step` via `handle`) drop the map.
        let mut node_outputs = serde_json::Map::new();

        let outcome = loop {
            if session.cursor.position >= flow.steps.len() {
                let outcome = session
                    .last_outcome
                    .clone()
                    .unwrap_or_else(|| json!({"status": "done"}));
                break outcome;
            }

            let step = flow
                .steps
                .get(session.cursor.position)
                .cloned()
                .ok_or_else(|| RunnerError::FlowNotFound {
                    flow_id: flow.summary.id.clone(),
                })?;

            match step {
                FlowStep::Adapter(call) => {
                    // Merge the FlowEngine's per-node output map (keyed by the
                    // real node id, e.g. `insights_agent`) so mid-flow node
                    // outputs — which the outer step outcome does not expose —
                    // survive to the caller. Observability only.
                    let (outcome, flow_nodes) = self
                        .execute_adapter_step(&flow, &mut session, call, tenant)
                        .await?;
                    session.last_outcome = Some(outcome.clone());
                    node_outputs.extend(flow_nodes);
                    continue;
                }
                FlowStep::AwaitInput { reason } => {
                    let last_response = session
                        .last_outcome
                        .as_ref()
                        .and_then(|value| value.get("response"))
                        .cloned();
                    session.waiting = Some(WaitState {
                        reason: reason.clone(),
                        recorded_at: SystemTime::now(),
                    });
                    let mut pending = json!({
                        "status": "pending",
                        "reason": reason,
                    });
                    if let Some(response) = last_response
                        && let Some(obj) = pending.as_object_mut()
                    {
                        obj.insert("response".into(), response);
                    }
                    session.last_outcome = Some(pending.clone());
                    break pending;
                }
                FlowStep::Complete { outcome } => {
                    session.cursor.position = flow.steps.len();
                    session.last_outcome = Some(json!({
                        "status": "done",
                        "result": outcome,
                    }));
                    break session
                        .last_outcome
                        .clone()
                        .expect("complete step should set outcome");
                }
            }
        };

        self.host
            .state
            .set_json(&session.key, session.state.clone())
            .await?;

        if is_new {
            session_host.put(session).await?;
        } else if !session_host.update_cas(session, expected_revision).await? {
            return Err(RunnerError::Session {
                reason: "compare-and-swap failure".into(),
            });
        }

        Ok((outcome, node_outputs))
    }

    fn ensure_policy_budget(&self, flow: &FlowDefinition) -> GResult<()> {
        if flow.steps.len() > self.policy.max_egress_adapters {
            return Err(policy_violation(format!(
                "flow has {} steps exceeding budget {}",
                flow.steps.len(),
                self.policy.max_egress_adapters
            )));
        }
        Ok(())
    }

    async fn execute_adapter_step(
        &self,
        flow: &FlowDefinition,
        session: &mut SessionSnapshot,
        call: AdapterCall,
        tenant: &TenantCtx,
    ) -> GResult<(Value, serde_json::Map<String, Value>)> {
        let adapter =
            self.adapters
                .get(&call.adapter)
                .ok_or_else(|| RunnerError::AdapterMissing {
                    adapter: call.adapter.clone(),
                })?;

        let resolved_payload = resolve_adapter_payload(&call.payload, session);
        let payload_bytes =
            serde_json::to_vec(&resolved_payload).map_err(|err| RunnerError::Serialization {
                reason: err.to_string(),
            })?;

        if payload_bytes.len() > self.policy.max_payload_bytes {
            return Err(policy_violation(format!(
                "payload exceeds max size {} bytes",
                self.policy.max_payload_bytes
            )));
        }

        let seq = session.cursor.outbox_seq;
        let payload_hash = Self::stable_hash(seq, &payload_bytes);
        let key = OutboxKey::new(seq, payload_hash.clone());

        let adapter_id = call.adapter.clone();
        let operation_id = call.operation.clone();
        let span = SpanContext {
            trace_id: tenant.trace_id.clone(),
            span_id: Some(format!("{}:{}", flow.summary.id, seq)),
        };

        if let Some(entry) = session.outbox.get(&key) {
            self.host
                .telemetry
                .emit(
                    &span,
                    &[
                        ("adapter", adapter_id.as_str()),
                        ("operation", operation_id.as_str()),
                        ("dedup", "hit"),
                    ],
                )
                .await?;
            session.cursor.position += 1;
            session.cursor.outbox_seq = session.cursor.outbox_seq.saturating_add(1);
            session.waiting = None;
            let outcome = json!({
                "status": "done",
                "response": entry.response.clone(),
            });
            session.last_outcome = Some(outcome.clone());
            // Dedup hit: no adapter ran, so no per-node flow map this turn.
            return Ok((outcome, serde_json::Map::new()));
        }

        self.host
            .telemetry
            .emit(
                &span,
                &[
                    ("adapter", adapter_id.as_str()),
                    ("operation", operation_id.as_str()),
                    ("phase", "start"),
                ],
            )
            .await?;

        let adapter_clone = adapter.clone();
        let mut call_clone = call.clone();
        call_clone.payload = resolved_payload.clone();
        // `call_traced` also yields the flow's per-node output map (keyed by the
        // real node id, e.g. `insights_agent`) when the adapter drives a flow.
        //
        // The per-session turn lock is held across the WHOLE retry loop, not
        // just each attempt: otherwise it is released between attempts, another
        // message for the session can consume the parked snapshot in that gap,
        // and the retry then runs out of order. The inner `call_traced` lock
        // sees the key already held by this task and skips.
        let retry_attempts = retry_with_jitter(&self.policy.retry, || {
            let adapter = adapter_clone.clone();
            let call = call_clone.clone();
            async move { adapter.call_traced(&call).await }
        });
        let (response, flow_nodes) = match super::runtime::turn_key_for_payload(&call_clone.payload)
        {
            Some(turn_key) => {
                super::turn_lock::global()
                    .run(turn_key, retry_attempts)
                    .await?
            }
            None => retry_attempts.await?,
        };

        session.cursor.position += 1;
        session.cursor.outbox_seq = session.cursor.outbox_seq.saturating_add(1);
        session.waiting = None;
        session.outbox.insert(
            key,
            SessionOutboxEntry {
                seq,
                hash: payload_hash,
                response: response.clone(),
            },
        );
        Self::update_state_adapter(session, &call, &response);
        let outcome = json!({
            "status": "done",
            "response": response.clone(),
        });
        session.last_outcome = Some(outcome.clone());

        self.host
            .telemetry
            .emit(
                &span,
                &[
                    ("adapter", adapter_id.as_str()),
                    ("operation", operation_id.as_str()),
                    ("phase", "finish"),
                ],
            )
            .await?;

        Ok((outcome, flow_nodes))
    }

    fn update_state_input(session: &mut SessionSnapshot, input: Value) {
        if !matches!(session.state, Value::Object(_)) {
            session.state = Value::Object(Map::new());
        }
        if let Value::Object(map) = &mut session.state {
            map.insert("last_input".to_string(), input);
        }
    }

    fn update_state_adapter(session: &mut SessionSnapshot, call: &AdapterCall, response: &Value) {
        if !matches!(session.state, Value::Object(_)) {
            session.state = Value::Object(Map::new());
        }
        if let Value::Object(map) = &mut session.state {
            map.insert(
                "last_adapter".to_string(),
                Value::String(call.adapter.clone()),
            );
            map.insert(
                "last_operation".to_string(),
                Value::String(call.operation.clone()),
            );
            map.insert("last_response".to_string(), response.clone());
        }
    }

    fn stable_hash(seq: u64, payload: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(seq.to_be_bytes());
        hasher.update(payload);
        hex::encode(hasher.finalize())
    }

    fn generate_session_id() -> String {
        let mut rng = rng();
        let value: u128 = rng.random();
        format!("sess-{value:032x}")
    }
}

fn resolve_adapter_payload(call_payload: &Value, session: &SessionSnapshot) -> Value {
    match call_payload {
        Value::String(token) if token == PAYLOAD_FROM_LAST_INPUT => session
            .state
            .as_object()
            .and_then(|map| map.get("last_input"))
            .cloned()
            .unwrap_or(Value::Null),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::glue::{FnSecretsHost, FnTelemetryHost};
    use crate::engine::host::{SessionHost, StateHost};
    use crate::engine::registry::Adapter;
    use crate::engine::shims::{InMemorySessionHost, InMemoryStateHost};
    use async_trait::async_trait;
    use greentic_types::{EnvId, TenantId};
    use parking_lot::Mutex;
    use std::str::FromStr;

    #[tokio::test]
    async fn pauses_and_resumes_after_wait_step() {
        let secrets = Arc::new(FnSecretsHost::new(|_| Ok(String::new())));
        let telemetry = Arc::new(FnTelemetryHost::new(|_, _| Ok(())));
        let session_store: Arc<InMemorySessionHost> = Arc::new(InMemorySessionHost::new());
        let state_store: Arc<InMemoryStateHost> = Arc::new(InMemoryStateHost::new());
        let host = Arc::new(HostBundle::new(
            secrets,
            telemetry,
            Arc::clone(&session_store) as Arc<dyn SessionHost>,
            Arc::clone(&state_store) as Arc<dyn StateHost>,
        ));

        let adapter = MockChatAdapter::default();
        let mut adapters = AdapterRegistry::default();
        adapters.register("mock", Box::new(adapter.clone()));
        let policy = Policy::default();

        let sm = StateMachine::new(host, adapters, policy);
        sm.register_flow(test_flow());

        let env = EnvId::from_str("local").unwrap();
        let tenant_id = TenantId::from_str("demo").unwrap();
        let tenant_ctx = TenantCtx::new(env, tenant_id);
        let session_hint = Some("demo:telegram:chat:user".to_string());

        // A MockChatAdapter is not FlowEngine-backed, so it contributes no
        // per-node map — node_outputs capture is asserted at the FlowEngine
        // level in `runner::engine` tests. Here we only exercise step outcomes.
        let (first, _first_nodes) = sm
            .step(
                &tenant_ctx,
                "test-pack",
                "support.flow",
                session_hint.clone(),
                json!({ "text": "hi" }),
            )
            .await
            .expect("first step");
        assert_eq!(first["status"], json!("pending"));
        assert_eq!(
            first["response"]["messages"][0]["text"],
            json!("Welcome to support!")
        );

        let key = SessionKey::new(
            &tenant_ctx,
            "test-pack",
            "support.flow",
            session_hint.clone(),
        );
        let snapshot = session_store.get(&key).await.unwrap().unwrap();
        assert!(snapshot.waiting.is_some());
        assert_eq!(snapshot.cursor.position, 1);

        let (second, _second_nodes) = sm
            .step(
                &tenant_ctx,
                "test-pack",
                "support.flow",
                session_hint.clone(),
                json!({ "text": "need help" }),
            )
            .await
            .expect("second step");
        assert_eq!(second["status"], json!("done"));
        assert_eq!(
            second["response"]["messages"][0]["text"],
            json!("echo: need help")
        );

        let snapshot = session_store.get(&key).await.unwrap().unwrap();
        assert!(snapshot.waiting.is_none());
        assert_eq!(snapshot.cursor.position, 3);

        let history = adapter.history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[1]["text"], json!("need help"));
    }

    /// An ingress envelope payload that maps to ONE turn-lock key ("shared")
    /// whatever `msg` is.
    fn turn_payload(msg: &str) -> Value {
        json!({
            "tenant": "demo",
            "flow_id": "retry.flow",
            "session_hint": "shared",
            "reply_scope": { "conversation": "c" },
            "payload": { "msg": msg },
        })
    }

    /// Fails the first attempt of message `m1` (after a pause), recording
    /// start/end events so the test can assert ordering across sessions.
    struct FlakyOrderAdapter {
        events: Arc<Mutex<Vec<String>>>,
        failed_once: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait]
    impl Adapter for FlakyOrderAdapter {
        async fn call(&self, call: &AdapterCall) -> GResult<Value> {
            let msg = call.payload["payload"]["msg"]
                .as_str()
                .unwrap_or("?")
                .to_string();
            self.events.lock().push(format!("start {msg}"));
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            if msg == "m1" && !self.failed_once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                self.events.lock().push(format!("fail {msg}"));
                return Err(RunnerError::AdapterCall {
                    reason: "transient".into(),
                });
            }
            self.events.lock().push(format!("end {msg}"));
            Ok(json!({ "ok": msg }))
        }
    }

    #[tokio::test]
    async fn turn_lock_is_held_across_whole_turn_retries() {
        let host = Arc::new(HostBundle::new(
            Arc::new(FnSecretsHost::new(|_| Ok(String::new()))),
            Arc::new(FnTelemetryHost::new(|_, _| Ok(()))),
            Arc::new(InMemorySessionHost::new()) as Arc<dyn SessionHost>,
            Arc::new(InMemoryStateHost::new()) as Arc<dyn StateHost>,
        ));
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut adapters = AdapterRegistry::default();
        adapters.register(
            "flaky",
            Box::new(FlakyOrderAdapter {
                events: Arc::clone(&events),
                failed_once: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            }),
        );
        let mut policy = Policy::default();
        // A backoff gap wide enough that, without the fix, m2 starts inside it.
        policy.retry.initial_backoff = std::time::Duration::from_millis(100);
        policy.retry.max_backoff = std::time::Duration::from_millis(100);
        let sm = Arc::new(StateMachine::new(host, adapters, policy));
        sm.register_flow(FlowDefinition::new(
            FlowSummary {
                pack_id: "test-pack".into(),
                id: "retry.flow".into(),
                name: "Retry".into(),
                version: "1.0.0".into(),
                description: None,
            },
            json!({ "type": "object" }),
            vec![FlowStep::Adapter(AdapterCall {
                adapter: "flaky".into(),
                operation: "run".into(),
                payload: Value::String(PAYLOAD_FROM_LAST_INPUT.into()),
            })],
        ));
        let tenant = TenantCtx::new(
            EnvId::from_str("local").unwrap(),
            TenantId::from_str("demo").unwrap(),
        );

        let first = {
            let (sm, tenant) = (Arc::clone(&sm), tenant.clone());
            tokio::spawn(async move {
                sm.step(
                    &tenant,
                    "test-pack",
                    "retry.flow",
                    Some("step-1".into()),
                    turn_payload("m1"),
                )
                .await
            })
        };
        // Let m1's first attempt start and fail before m2 arrives.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let second = {
            let (sm, tenant) = (Arc::clone(&sm), tenant.clone());
            tokio::spawn(async move {
                sm.step(
                    &tenant,
                    "test-pack",
                    "retry.flow",
                    Some("step-2".into()),
                    turn_payload("m2"),
                )
                .await
            })
        };
        assert!(first.await.expect("join m1").is_ok());
        assert!(second.await.expect("join m2").is_ok());
        assert_eq!(
            events.lock().clone(),
            vec![
                "start m1", "fail m1", "start m1", "end m1", "start m2", "end m2"
            ],
            "m2 must wait for m1's retries to finish"
        );
    }

    fn test_flow() -> FlowDefinition {
        FlowDefinition::new(
            FlowSummary {
                pack_id: "test-pack".into(),
                id: "support.flow".into(),
                name: "Support".into(),
                version: "1.0.0".into(),
                description: None,
            },
            json!({ "type": "object" }),
            vec![
                FlowStep::Adapter(AdapterCall {
                    adapter: "mock".into(),
                    operation: "send".into(),
                    payload: json!({ "text": "Welcome to support!" }),
                }),
                FlowStep::AwaitInput {
                    reason: "await-user".into(),
                },
                FlowStep::Adapter(AdapterCall {
                    adapter: "mock".into(),
                    operation: "echo".into(),
                    payload: Value::String(PAYLOAD_FROM_LAST_INPUT.into()),
                }),
            ],
        )
    }

    #[derive(Clone, Default)]
    struct MockChatAdapter {
        calls: Arc<Mutex<Vec<Value>>>,
    }

    impl MockChatAdapter {
        fn history(&self) -> Vec<Value> {
            self.calls.lock().clone()
        }
    }

    #[async_trait]
    impl Adapter for MockChatAdapter {
        async fn call(&self, call: &AdapterCall) -> GResult<Value> {
            self.calls.lock().push(call.payload.clone());
            match call.operation.as_str() {
                "send" => Ok(json!({
                    "messages": [{ "text": call.payload.get("text").and_then(Value::as_str).unwrap_or("hello").to_string() }]
                })),
                "echo" => {
                    let text = call
                        .payload
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    Ok(json!({
                        "messages": [{ "text": format!("echo: {text}") }]
                    }))
                }
                _ => Ok(json!({})),
            }
        }
    }
}
