use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use greentic_runner_host::engine::api::{FlowSummary, RunFlowRequest, RunnerApi};
use greentic_runner_host::engine::builder::RunnerBuilder;
use greentic_runner_host::engine::error::GResult;
use greentic_runner_host::engine::glue::{FnSecretsHost, FnTelemetryHost};
use greentic_runner_host::engine::host::HostBundle;
use greentic_runner_host::engine::policy::Policy;
use greentic_runner_host::engine::registry::{Adapter, AdapterCall, AdapterRegistry};
use greentic_runner_host::engine::runtime::IngressEnvelope;
use greentic_runner_host::engine::shims::{InMemorySessionHost, InMemoryStateHost};
use greentic_runner_host::engine::state_machine::{
    FlowDefinition, FlowStep, PAYLOAD_FROM_LAST_INPUT,
};
use greentic_types::{EnvId, TenantCtx, TenantId};
use parking_lot::Mutex;
use serde_json::{Value, json};

#[tokio::test]
async fn multi_turn_flow_pauses_and_resumes() -> Result<()> {
    let secrets = Arc::new(FnSecretsHost::new(|_| Ok(String::new())));
    let telemetry = Arc::new(FnTelemetryHost::new(|_, _| Ok(())));
    let session = Arc::new(InMemorySessionHost::new());
    let state = Arc::new(InMemoryStateHost::new());
    let host = HostBundle::new(secrets, telemetry, session, state);

    let adapter = MockChatAdapter::default();
    let mut adapters = AdapterRegistry::default();
    adapters.register("mock", Box::new(adapter.clone()));

    let runner = RunnerBuilder::new()
        .with_host(host)
        .with_adapters(adapters)
        .with_policy(Policy::default())
        .with_flow(test_flow())
        .build()?;

    let env = EnvId::from_str("local").unwrap();
    let tenant_id = TenantId::from_str("acme").unwrap();
    let tenant_ctx = TenantCtx::new(env, tenant_id);
    let session_hint = Some("acme:teams:chat:user".to_string());

    let mut envelope = ingress_with_text("hi", session_hint.clone());
    let first = runner
        .run_flow(RunFlowRequest {
            tenant: tenant_ctx.clone(),
            pack_id: "test-pack".into(),
            flow_id: "support.flow".into(),
            input: serde_json::to_value(&envelope)?,
            session_hint: envelope.session_hint.clone(),
        })
        .await?
        .outcome;
    assert_eq!(first["status"], json!("pending"));
    assert_eq!(
        first["response"]["messages"][0]["text"],
        json!("Welcome to support!")
    );

    envelope.payload = json!({ "text": "need help" });
    let second = runner
        .run_flow(RunFlowRequest {
            tenant: tenant_ctx,
            pack_id: "test-pack".into(),
            flow_id: "support.flow".into(),
            input: serde_json::to_value(&envelope)?,
            session_hint: envelope.session_hint.clone(),
        })
        .await?
        .outcome;
    assert_eq!(second["status"], json!("done"));
    assert_eq!(
        second["response"]["messages"][0]["text"],
        json!("echo: need help")
    );

    let history = adapter.history();
    assert_eq!(history.len(), 2);
    assert_eq!(history[1]["payload"]["text"], json!("need help"));

    Ok(())
}

fn ingress_with_text(text: &str, session_hint: Option<String>) -> IngressEnvelope {
    IngressEnvelope {
        tenant: "acme".into(),
        env: Some("local".into()),
        pack_id: Some("test-pack".into()),
        flow_id: "support.flow".into(),
        flow_type: Some("messaging".into()),
        action: Some("messaging".into()),
        session_hint,
        provider: Some("teams".into()),
        messaging_endpoint_id: None,
        channel: Some("chat".into()),
        conversation: Some("chat".into()),
        user: Some("user".into()),
        entry_node: None,
        activity_id: Some("activity-1".into()),
        timestamp: None,
        payload: json!({ "text": text }),
        metadata: None,
        reply_scope: None,
    }
    .canonicalize()
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
                "messages": [{ "text": call.payload.get("text").and_then(Value::as_str).unwrap_or("hello") }]
            })),
            "echo" => {
                let text = call
                    .payload
                    .get("payload")
                    .and_then(|payload| payload.get("text"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                Ok(json!({
                    "messages": [{ "text": format!("echo: {text}") }]
                }))
            }
            _ => Ok(Value::Null),
        }
    }
}
