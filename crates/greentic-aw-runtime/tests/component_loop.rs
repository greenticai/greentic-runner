//! End-to-end component-tool path through the agent loop.
//!
//! Drives a full `AgentRuntime::step` with a [`ComponentToolSource`] backed by a
//! fake [`ComponentInvoker`] (the trait is public, so an external test impls it
//! directly — no `PackRuntime`/wasmtime needed). A recording LLM backend
//! captures the tools it was offered, then scripts a tool call followed by a
//! final reply, so the test asserts the `component:` tool was both offered to
//! the LLM and dispatched, with its output landing in the trail.

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::{LlmError, TerminationReason};
use greentic_aw_runtime::llm::{LlmBackend, LlmRequest, LlmResponse};
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::state::ToolCallRecord;
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, AgentStep, ComponentInvoker,
    ComponentOperation, ComponentToolSource, LlmProviderRef, ToolRef,
};
use serde_json::json;

/// Fake component invoker: returns a fixed operation list and a fixed `invoke`
/// result. Mirrors the in-crate `FakeInvoker` but lives here because
/// `#[cfg(test)]` items are not visible to integration test crates.
struct FakeComponentInvoker {
    ops: Vec<ComponentOperation>,
    result: Result<serde_json::Value, String>,
}

impl ComponentInvoker for FakeComponentInvoker {
    fn list_operations(&self) -> Vec<ComponentOperation> {
        self.ops.clone()
    }

    fn invoke<'a>(
        &'a self,
        _component_ref: &'a str,
        _operation: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
        let result = self.result.clone();
        Box::pin(async move { result })
    }
}

/// LLM backend that records the `(extension_id, tool_name)` tools it was offered
/// on each turn, then returns the next scripted response.
struct RecordingLlmBackend {
    responses: Mutex<Vec<Result<LlmResponse, LlmError>>>,
    offered: Mutex<Vec<Vec<(String, String)>>>,
}

impl RecordingLlmBackend {
    fn new(responses: Vec<Result<LlmResponse, LlmError>>) -> Self {
        Self {
            responses: Mutex::new(responses),
            offered: Mutex::new(Vec::new()),
        }
    }
}

impl LlmBackend for RecordingLlmBackend {
    fn complete<'a>(
        &'a self,
        req: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        self.offered.lock().expect("offered mutex").push(
            req.tools
                .iter()
                .map(|t| (t.extension_id.clone(), t.tool_name.clone()))
                .collect(),
        );
        let next = {
            let mut queue = self.responses.lock().expect("responses mutex");
            if queue.is_empty() {
                Err(LlmError::Transport("recording queue exhausted".into()))
            } else {
                queue.remove(0)
            }
        };
        Box::pin(async move { next })
    }
}

fn cfg(tools: Vec<ToolRef>) -> AgentConfig {
    AgentConfig {
        agent_id: "a".into(),
        system_prompt: "sys".into(),
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

fn call_issue_refund() -> LlmResponse {
    LlmResponse {
        content: None,
        tool_calls: vec![ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "component:greentic.refund".into(),
            tool_name: "issue_refund".into(),
            args: json!({ "order_id": "42" }),
        }],
        tokens_in: 5,
        tokens_out: 5,
    }
}

fn final_reply(text: &str) -> LlmResponse {
    LlmResponse {
        content: Some(text.into()),
        tool_calls: vec![],
        tokens_in: 5,
        tokens_out: 5,
    }
}

fn issue_refund_op() -> ComponentOperation {
    ComponentOperation {
        component_ref: "greentic.refund".into(),
        operation: "issue_refund".into(),
        description: "Issue a refund".into(),
        parameters: json!({
            "type": "object",
            "properties": { "order_id": { "type": "string" } }
        }),
    }
}

fn build_runtime(
    llm: Arc<RecordingLlmBackend>,
    components: Option<Arc<ComponentToolSource>>,
    config: AgentConfig,
) -> (AgentRuntime, TenantContext) {
    let store = Arc::new(MockAgentStateStore::new());
    let telemetry = Arc::new(MockTelemetry::new());
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(&tc, "a", config);
    let cp = Arc::new(cp);
    let token_meter = Arc::new(MockTokenMeter::new(0));
    let ledger = Arc::new(NoopToolLedger);
    let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let rt = AgentRuntime::new(cp, store, ext, llm, telemetry, token_meter, ledger, None)
        .with_component_source(components);
    (rt, tc)
}

/// Happy path: the source enumerates one component operation, the loop offers
/// `component:greentic.refund/issue_refund` to the LLM, the LLM calls it, and
/// the component's `{"refund_id":"r-1"}` output lands in the trail.
#[tokio::test]
async fn component_tool_offered_called_and_result_in_trail() {
    let invoker = Arc::new(FakeComponentInvoker {
        ops: vec![issue_refund_op()],
        result: Ok(json!({ "refund_id": "r-1" })),
    });
    let source = Arc::new(ComponentToolSource::new(invoker));

    let llm = Arc::new(RecordingLlmBackend::new(vec![
        Ok(call_issue_refund()),
        Ok(final_reply("done")),
    ]));
    let allowed = vec![ToolRef {
        extension_id: "component:greentic.refund".into(),
        tool_name: "issue_refund".into(),
        description: None,
        input_schema: None,
        usage_note: None,
    }];
    let (rt, tc) = build_runtime(llm.clone(), Some(source), cfg(allowed));

    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "go".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();

    // (a) The component tool was offered to the LLM on the first turn.
    let offered = llm.offered.lock().unwrap();
    assert!(
        offered[0].contains(&(
            "component:greentic.refund".to_string(),
            "issue_refund".to_string()
        )),
        "first turn must offer the component tool; got: {:?}",
        offered[0]
    );

    // (b) The component output landed in the trail as the tool result.
    let tool_result = out.trail.iter().find_map(|s| match s {
        AgentStep::ToolCall { name, result, .. } if name == "issue_refund" => Some(result.clone()),
        _ => None,
    });
    assert_eq!(
        tool_result,
        Some(json!({ "refund_id": "r-1" })),
        "component tool result must appear in the trail; trail: {:?}",
        out.trail
    );

    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "done");
}

/// Degrade case: the source enumerates NO operations, so the catalog is empty
/// and `list_tools_for_llm` drops the `component:` ref — the LLM is offered no
/// tools. The step still completes without panic.
#[tokio::test]
async fn empty_component_source_offers_no_tool() {
    let invoker = Arc::new(FakeComponentInvoker {
        ops: vec![],
        result: Ok(json!({})),
    });
    let source = Arc::new(ComponentToolSource::new(invoker));

    let llm = Arc::new(RecordingLlmBackend::new(vec![Ok(final_reply("no tools"))]));
    let allowed = vec![ToolRef {
        extension_id: "component:greentic.refund".into(),
        tool_name: "issue_refund".into(),
        description: None,
        input_schema: None,
        usage_note: None,
    }];
    let (rt, tc) = build_runtime(llm.clone(), Some(source), cfg(allowed));

    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "go".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();

    let offered = llm.offered.lock().unwrap();
    assert!(
        offered[0].is_empty(),
        "empty component source → no tools offered; got: {:?}",
        offered[0]
    );
    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "no tools");
}
