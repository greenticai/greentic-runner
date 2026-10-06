//! Agent-graph turns inherit an OPEN run context (no production path opens a
//! context around a graph in Phase A2: the gate lives in
//! `RuntimeAgentNodeHandler` only): the executor awaits each agent turn inline, so the second
//! agent sees the first one's reply. With no context, nothing changes.

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::graph::{
    AgentTurnFn, AgentTurnRequest, AgentTurnResult, ApprovalFn, ApprovalOutcome, ApprovalRequest,
    GraphConfig, GraphExecError, GraphExecutor, InMemoryCheckpointStore, SupervisorFn,
    SupervisorRequest, ToolCallRequest, ToolFn,
};
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, LlmProviderRef, RunContext, RunTrace,
};

fn tc() -> TenantContext {
    TenantContext::new("acme", "prod")
}

fn agent(id: &str) -> AgentConfig {
    AgentConfig {
        on_text_while_parked: Default::default(),
        agent_id: id.into(),
        system_prompt: format!("sys-{id}"),
        tools: vec![],
        llm: LlmProviderRef {
            provider: "mock".into(),
            model: "m".into(),
            credential_ref: None,
        },
        limits: AgentLimits {
            max_iter: 2,
            timeout: Duration::from_secs(30),
            ..AgentLimits::default()
        },
        memory: None,
        knowledge: None,
        guardrails: vec![],
        conversational: false,
        opening_message: None,
    }
}

fn graph() -> GraphConfig {
    GraphConfig::from_json(
        &serde_json::json!({
            "schemaVersion": 1,
            "entry": "a1",
            "nodes": [
                {"id": "a1", "kind": "agent", "systemPrompt": "s", "model": "m", "tools": []},
                {"id": "a2", "kind": "agent", "systemPrompt": "s", "model": "m", "tools": []},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "a1", "to": "a2"},
                {"from": "a2", "to": "respond"}
            ]
        })
        .to_string(),
    )
    .expect("two agents in sequence is a valid graph")
}

async fn run(open: bool) -> Vec<String> {
    let llm = Arc::new(MockLlmBackend::new(vec![
        Ok(LlmResponse {
            content: Some("a1-reply".into()),
            tool_calls: vec![],
            tokens_in: 1,
            tokens_out: 1,
        }),
        Ok(LlmResponse {
            content: Some("a2-reply".into()),
            tool_calls: vec![],
            tokens_in: 1,
            tokens_out: 1,
        }),
    ]));
    let cp = MockConfigProvider::new();
    cp.insert(&tc(), "a1", agent("a1"));
    cp.insert(&tc(), "a2", agent("a2"));
    let rt = Arc::new(AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm.clone(),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    ));
    // Mirrors runner-host's `run_one_agent_turn`: one `runtime.step` per visit,
    // awaited inside the executor's future.
    let turn: AgentTurnFn = Arc::new(move |req: AgentTurnRequest| {
        let rt = rt.clone();
        Box::pin(async move {
            let out = rt
                .step(
                    tc(),
                    &format!("g-{}", req.node_id),
                    &req.node_id,
                    AgentInput {
                        text: "go".into(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| GraphExecError::AgentTurn(e.to_string()))?;
            Ok(AgentTurnResult {
                reply: out.reply,
                resolved: true,
            })
        })
    });
    let tool: ToolFn =
        Arc::new(|_req: ToolCallRequest| Box::pin(async { Ok(serde_json::json!({})) }));
    let sup: SupervisorFn = Arc::new(|_req: SupervisorRequest| {
        Box::pin(async { Err(GraphExecError::Supervisor("no supervisor here".into())) })
    });
    let approval: ApprovalFn =
        Arc::new(|_req: ApprovalRequest| Box::pin(async { Ok(ApprovalOutcome::Awaiting) }));
    let exec = GraphExecutor::new(
        Arc::new(InMemoryCheckpointStore::default()),
        turn,
        tool,
        sup,
        approval,
    );
    let cfg = graph();
    let tenant = tc();
    let drive = exec.start(&tenant, "run-1", &cfg, "hello");
    if open {
        RunContext::scope(RunContext::new("acme", Arc::new(RunTrace::new())), drive)
            .await
            .unwrap();
    } else {
        drive.await.unwrap();
    }
    llm.seen_system_prompts.lock().unwrap().clone()
}

#[tokio::test]
async fn a_graphs_second_agent_sees_the_first_only_inside_an_open_context() {
    let p = run(true).await;
    assert!(p[1].contains("a1-reply"), "{}", p[1]);
    let p = run(false).await;
    assert_eq!(p[1], "sys-a2", "no context, no block");
}
