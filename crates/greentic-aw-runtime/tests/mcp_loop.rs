//! End-to-end MCP path through the agent loop.
//!
//! Drives a full `AgentRuntime::step` with a real [`McpToolSource`] backed by
//! two wiremock servers (a fake admin returning one `agentic_worker` MCP server,
//! and a fake MCP server speaking the JSON-RPC contract). A recording LLM
//! backend captures the tools it was offered, then scripts a `tools/call`
//! followed by a final reply, so the test asserts the MCP tool was both offered
//! to the LLM and dispatched, with its structured output landing in the trail.
//!
//! The wiremock mount shapes are replicated from `src/mcp_source.rs`'s in-module
//! tests (those helpers are `#[cfg(test)]` and cannot be reused from here).

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
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, AgentStep, LlmProviderRef, McpToolSource,
    ToolRef,
};
use serde_json::json;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// LLM backend that records the `(extension_id, tool_name)` tools it was offered
/// on each turn, then returns the next scripted response. Lets the test assert
/// what the loop actually presented to the model.
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
        guardrails: vec![],
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
        conversational: false,
        opening_message: None,
    }
}

fn call_get_issue() -> LlmResponse {
    LlmResponse {
        content: None,
        tool_calls: vec![ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "mcp:s1".into(),
            tool_name: "get_issue".into(),
            args: json!({ "id": "42" }),
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

/// Mount the 4-call MCP JSON-RPC contract (initialize, notifications/initialized,
/// tools/list with one `get_issue` tool, tools/call returning `structuredContent`).
/// Replicated from `src/mcp_source.rs` tests.
async fn fake_mcp_server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Mcp-Session-Id", "sess-1")
                .set_body_json(json!({
                    "jsonrpc": "2.0", "id": 1,
                    "result": {
                        "protocolVersion": "2025-06-18",
                        "serverInfo": { "name": "fake", "version": "1.0.0" }
                    }
                })),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({ "method": "notifications/initialized" }),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 2,
            "result": {
                "tools": [
                    {
                        "name": "get_issue",
                        "description": "Get an issue",
                        "inputSchema": {
                            "type": "object",
                            "properties": { "id": { "type": "string" } }
                        }
                    }
                ]
            }
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "tools/call" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 3,
            "result": { "structuredContent": { "ok": 1 } }
        })))
        .mount(&server)
        .await;
    server
}

/// Mount the admin `mcp-servers` endpoint returning one `agentic_worker` server
/// pointed at `transport_url`.
async fn mount_admin(server: &MockServer, transport_url: &str) {
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .and(header("authorization", "Bearer gtc_live_test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "servers": [
                {
                    "id": "s1",
                    "name": "Worker",
                    "transport_url": transport_url,
                    "auth_header_name": null,
                    "auth_token": null,
                    "allowed_tools": null,
                    "roles": ["agentic_worker"]
                }
            ]
        })))
        .mount(server)
        .await;
}

fn build_runtime(
    llm: Arc<RecordingLlmBackend>,
    mcp: Option<Arc<McpToolSource>>,
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
    let rt = AgentRuntime::new(cp, store, ext, llm, telemetry, token_meter, ledger, mcp);
    (rt, tc)
}

/// Happy path: the admin lists one agentic_worker MCP server, the loop builds
/// its catalog, offers `mcp:s1/get_issue` to the LLM, the LLM calls it, and the
/// server's `{"ok":1}` structured output lands in the trail.
#[tokio::test]
async fn mcp_tool_offered_called_and_result_in_trail() {
    let mcp_server = fake_mcp_server().await;
    let admin = MockServer::start().await;
    mount_admin(&admin, &mcp_server.uri()).await;

    let llm = Arc::new(RecordingLlmBackend::new(vec![
        Ok(call_get_issue()),
        Ok(final_reply("done")),
    ]));
    let source = Arc::new(McpToolSource::new(admin.uri(), "gtc_live_test"));
    let allowed = vec![ToolRef {
        extension_id: "mcp:s1".into(),
        tool_name: "get_issue".into(),
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

    // (a) The MCP tool was offered to the LLM on the first turn.
    let offered = llm.offered.lock().unwrap();
    assert!(
        offered[0].contains(&("mcp:s1".to_string(), "get_issue".to_string())),
        "first turn must offer the mcp tool; got: {:?}",
        offered[0]
    );

    // (b) The MCP output {"ok":1} landed in the trail as the tool result.
    let tool_result = out.trail.iter().find_map(|s| match s {
        AgentStep::ToolCall { name, result, .. } if name == "get_issue" => Some(result.clone()),
        _ => None,
    });
    assert_eq!(
        tool_result,
        Some(json!({ "ok": 1 })),
        "mcp tool result must appear in the trail; trail: {:?}",
        out.trail
    );

    // The step completes with the final reply.
    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "done");
}

/// Degrade case: the admin still lists the MCP server, but its transport_url
/// points at a dead port. The catalog probe fails → the catalog is EMPTY, so
/// `list_tools_for_llm` DROPS the `mcp:s1/get_issue` ref and the LLM is offered
/// NO tools. The step still completes without panic.
///
/// (This is the "tool simply isn't offered" branch: with an empty catalog the
/// list side drops the ref before the LLM ever sees it. The dispatch-side
/// `{"error":"unknown mcp tool"}` in-band shape is exercised by the unit test
/// `tools::dispatch_routes_mcp_ref`; here the realistic loop path never reaches
/// dispatch because a well-behaved LLM only calls tools it was offered.)
#[tokio::test]
async fn mcp_unreachable_server_degrades_no_tool_offered() {
    let admin = MockServer::start().await;
    // Dead port → MCP probe fails → empty catalog.
    mount_admin(&admin, "http://127.0.0.1:1/").await;

    let llm = Arc::new(RecordingLlmBackend::new(vec![Ok(final_reply("no tools"))]));
    let source = Arc::new(McpToolSource::new(admin.uri(), "gtc_live_test"));
    let allowed = vec![ToolRef {
        extension_id: "mcp:s1".into(),
        tool_name: "get_issue".into(),
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

    // The LLM was offered NO tools (empty catalog dropped the mcp ref).
    let offered = llm.offered.lock().unwrap();
    assert!(
        offered[0].is_empty(),
        "unreachable mcp server → no tools offered; got: {:?}",
        offered[0]
    );

    // The step still completes cleanly.
    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "no tools");
}
