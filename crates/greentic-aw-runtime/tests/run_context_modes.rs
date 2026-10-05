//! Shared context, Phase A2: what a nested agent sees of its caller's run,
//! per binding mode, and what crosses an agent boundary.
//!
//! A lost context shows up as a MISSING block, never as an error, so every
//! test asserts both presence (mode allows) and absence (mode forbids).

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::LlmError;
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::state::ToolCallRecord;
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, BindingModes, FlowInvokeOutcome,
    FlowInvoker, FlowOperation, FlowToolSource, LlmProviderRef, RunContext, RunTrace, ShareMode,
    SharePolicy, ToolRef,
};
use serde_json::{Value, json};

const TENANT: &str = "acme";
const ENV: &str = "prod";
#[allow(dead_code)] // used from Task 5 on
const SEED: &str = "SEED-EVENT";

fn tc() -> TenantContext {
    TenantContext::new(TENANT, ENV)
}

fn input() -> AgentInput {
    AgentInput {
        text: "go".into(),
        ..Default::default()
    }
}

fn tool(ext: &str, name: &str) -> ToolRef {
    ToolRef {
        extension_id: ext.into(),
        tool_name: name.into(),
        description: None,
        input_schema: None,
        usage_note: None,
    }
}

fn agent(id: &str, tools: Vec<ToolRef>) -> AgentConfig {
    AgentConfig {
        agent_id: id.into(),
        system_prompt: format!("sys-{id}"),
        tools,
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
    }
}

fn reply(text: &str) -> Result<LlmResponse, LlmError> {
    Ok(LlmResponse {
        content: Some(text.into()),
        tool_calls: vec![],
        tokens_in: 1,
        tokens_out: 1,
    })
}

fn call(ext: &str, name: &str) -> Result<LlmResponse, LlmError> {
    Ok(LlmResponse {
        content: None,
        tool_calls: vec![ToolCallRecord {
            call_id: format!("c-{ext}"),
            extension_id: ext.into(),
            tool_name: name.into(),
            args: json!({}),
        }],
        tokens_in: 1,
        tokens_out: 1,
    })
}

fn runtime(agents: Vec<AgentConfig>, llm: Arc<MockLlmBackend>) -> AgentRuntime {
    let cp = MockConfigProvider::new();
    for a in agents {
        let id = a.agent_id.clone();
        cp.insert(&tc(), &id, a);
    }
    AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm,
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
}

#[allow(dead_code)] // used from Task 5 on
fn seeded() -> Arc<RunTrace> {
    let t = Arc::new(RunTrace::new());
    t.append("host", "reply", SEED);
    t
}

#[allow(dead_code)] // used from Task 5 on
fn policy(agent: &str, binding: &str, mode: ShareMode) -> Option<Arc<SharePolicy>> {
    let mut modes = BindingModes::new();
    modes.insert(binding.to_string(), mode);
    let mut agents = HashMap::new();
    agents.insert(agent.to_string(), modes);
    Some(Arc::new(SharePolicy::new(agents)))
}

fn prompts(llm: &MockLlmBackend) -> Vec<String> {
    llm.seen_system_prompts.lock().unwrap().clone()
}

/// A flow tool `x` that completes with a fixed value.
struct FixedFlow(Value);

impl FlowInvoker for FixedFlow {
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
        let v = self.0.clone();
        Box::pin(async move { Ok(v) })
    }
    fn invoke_interactive<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        let v = self.0.clone();
        Box::pin(async move { Ok(FlowInvokeOutcome::Completed(v)) })
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

/// Two agents in one run: `inner` calls a tool whose result is secret, then
/// `outer` takes a turn. `outer` must see the tool's name and outcome and
/// `inner`'s reply, never the result; `inner` itself sees its own result.
#[tokio::test]
async fn another_agent_sees_a_tool_outcome_but_not_its_result() {
    let llm = Arc::new(MockLlmBackend::new(vec![
        call("flow:x", "x"),
        reply("inner done"),
        reply("outer done"),
    ]));
    let rt = runtime(
        vec![
            agent("inner", vec![tool("flow:x", "x")]),
            agent("outer", vec![]),
        ],
        llm.clone(),
    )
    .with_flow_source(Some(Arc::new(FlowToolSource::new(Arc::new(FixedFlow(
        json!({ "secret": "SECRET-42" }),
    ))))));
    let trace = Arc::new(RunTrace::new());
    RunContext::scope(RunContext::new(TENANT, trace.clone()), async {
        rt.step(tc(), "s-inner", "inner", input()).await.unwrap();
        rt.step(tc(), "s-outer", "outer", input()).await.unwrap();
    })
    .await;
    let p = prompts(&llm);
    assert!(
        p[1].contains("SECRET-42"),
        "the caller sees its own result: {}",
        p[1]
    );
    assert!(
        !p[2].contains("SECRET-42"),
        "another agent must not: {}",
        p[2]
    );
    assert!(p[2].contains("x ok"), "{}", p[2]);
    assert!(p[2].contains("inner done"), "{}", p[2]);
}
