//! The successful-dispatch trace site: a `flow:` tool that completes records a
//! tool event, never its arguments, and the next LLM request shows it.

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::LlmError;
use greentic_aw_runtime::llm::{LlmBackend, LlmRequest, LlmResponse};
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::state::ToolCallRecord;
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, FlowInvokeOutcome, FlowInvoker,
    FlowOperation, FlowToolSource, LlmProviderRef, RunContext, RunTrace, ToolRef,
};
use serde_json::{Value, json};

struct DoneFlow;

impl FlowInvoker for DoneFlow {
    fn list_flows(&self) -> Vec<FlowOperation> {
        vec![FlowOperation {
            flow_ref: "x".into(),
            description: "x flow".into(),
            parameters: json!({ "type": "object" }),
        }]
    }

    fn invoke<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async { Ok(json!({"ok": 1})) })
    }

    fn invoke_interactive<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async { Ok(FlowInvokeOutcome::Completed(json!({"ok": 1}))) })
    }

    fn resume<'a>(
        &'a self,
        _flow_ref: &'a str,
        _snapshot: Value,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async { Err("no resume".to_string()) })
    }
}

struct PromptLlm {
    responses: Mutex<Vec<LlmResponse>>,
    prompts: Mutex<Vec<String>>,
}

impl LlmBackend for PromptLlm {
    fn complete<'a>(
        &'a self,
        req: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        self.prompts.lock().unwrap().push(req.system_prompt.clone());
        let next = {
            let mut q = self.responses.lock().unwrap();
            if q.is_empty() {
                Err(LlmError::Transport("llm queue exhausted".into()))
            } else {
                Ok(q.remove(0))
            }
        };
        Box::pin(async move { next })
    }
}

#[tokio::test]
async fn a_completed_tool_is_traced_without_its_args_and_shown_to_the_next_request() {
    let llm = Arc::new(PromptLlm {
        responses: Mutex::new(vec![
            LlmResponse {
                content: None,
                tool_calls: vec![ToolCallRecord {
                    call_id: "c1".into(),
                    extension_id: "flow:x".into(),
                    tool_name: "x".into(),
                    args: json!({ "secret": "TOPSECRET-ARG" }),
                }],
                tokens_in: 1,
                tokens_out: 1,
            },
            LlmResponse {
                content: Some("done".into()),
                tool_calls: vec![],
                tokens_in: 1,
                tokens_out: 1,
            },
        ]),
        prompts: Mutex::new(Vec::new()),
    });
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(
        &tc,
        "a",
        AgentConfig {
            agent_id: "a".into(),
            system_prompt: "sys".into(),
            tools: vec![ToolRef {
                extension_id: "flow:x".into(),
                tool_name: "x".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            }],
            llm: LlmProviderRef {
                provider: "mock".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: AgentLimits {
                max_iter: 4,
                timeout: Duration::from_secs(60),
                ..AgentLimits::default()
            },
            memory: None,
            knowledge: None,
            guardrails: vec![],
            conversational: false,
            opening_message: None,
            on_text_while_parked: Default::default(),
        },
    );
    let rt = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm.clone(),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_flow_source(Some(Arc::new(FlowToolSource::new(Arc::new(DoneFlow)))));

    let trace = Arc::new(RunTrace::new());
    RunContext::scope(
        RunContext::new("acme", trace.clone()),
        rt.step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "go".into(),
                conversational: false,
                resume_payload: None,
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();

    let events = trace.events();
    assert_eq!(events[0].kind, "tool");
    // The trace sanitises `>` to a space, so `x -> {..}` is stored as `x - {..}`.
    assert_eq!(events[0].summary, r#"x - {"ok":1}"#);
    assert!(events[0].summary.contains(r#"{"ok":1}"#));
    assert!(!events.iter().any(|e| e.summary.contains("TOPSECRET-ARG")));
    let prompts = llm.prompts.lock().unwrap();
    assert!(prompts[1].contains("<run_context>"), "{}", prompts[1]);
    // The owner sees only the narrow form; the raw result stays on the event.
    assert!(prompts[1].contains("x ok"), "{}", prompts[1]);
    assert!(!prompts[1].contains(r#"{"ok":1}"#), "{}", prompts[1]);
}
