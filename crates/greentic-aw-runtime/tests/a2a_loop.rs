//! End-to-end A2A-tool path through the agent loop.
//!
//! Drives a full `AgentRuntime::step` with an [`A2aToolSource`] backed by a
//! real `wiremock` HTTP server standing in for the external A2A agent. A
//! recording LLM backend captures the tools it was offered, then scripts a
//! tool call followed by a final reply, so the test asserts the `a2a:` tool
//! was both offered to the LLM and dispatched over real HTTP, with its result
//! landing in the trail.

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
    A2aToolSource, AgentConfig, AgentInput, AgentLimits, AgentRuntime, AgentStep, LlmProviderRef,
    ToolRef,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A real-shaped agent card (mirrors `src/a2a_source/tests/mod.rs::CARD`),
/// with its interface pointing at `/a2a` on the mock server.
const CARD: &str = r#"{
  "name": "Recipe Agent",
  "description": "Helps with recipes and cooking.",
  "version": "1.0.0",
  "supportedInterfaces": [
    { "url": "PLACEHOLDER", "protocolBinding": "JSONRPC", "protocolVersion": "1.0" }
  ],
  "capabilities": { "streaming": false, "pushNotifications": false },
  "defaultInputModes": ["text/plain"],
  "defaultOutputModes": ["text/plain"],
  "skills": [
    { "id": "suggest", "name": "Suggest a recipe", "description": "Suggests a dish.", "tags": ["cooking"] }
  ]
}"#;

/// The path the mocked agent's JSON-RPC interface lives at.
const RPC_PATH: &str = "/a2a";

/// Serve the agent card at the well-known path, its interface pointing back
/// at this same mock server.
async fn mount_card(server: &MockServer) {
    let card = CARD.replace("PLACEHOLDER", &format!("{}{RPC_PATH}", server.uri()));
    Mock::given(method("GET"))
        .and(path("/.well-known/agent-card.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(card))
        .mount(server)
        .await;
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

fn call_recipe_agent() -> LlmResponse {
    LlmResponse {
        content: None,
        tool_calls: vec![ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "a2a:recipe".into(),
            tool_name: "ask".into(),
            args: json!({ "message": "what can I make with eggs?" }),
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

fn recipe_tool_ref() -> ToolRef {
    ToolRef {
        extension_id: "a2a:recipe".into(),
        tool_name: "ask".into(),
        // A full author contract on purpose: without a description the ref
        // could never be listed, and `no_a2a_source_offers_no_a2a_tool` would
        // pass whether or not the source gates the listing.
        description: Some("Ask the recipe agent.".into()),
        input_schema: Some(json!({
            "type": "object",
            "properties": { "message": { "type": "string" } }
        })),
        usage_note: None,
    }
}

fn build_runtime(
    llm: Arc<RecordingLlmBackend>,
    a2a: Option<Arc<A2aToolSource>>,
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
        .with_a2a_source(a2a);
    (rt, tc)
}

/// Happy path: the source's single agent card is fetched, the loop offers
/// `a2a:recipe/ask` to the LLM, the LLM calls it, the mocked agent receives a
/// real HTTP POST carrying the message text, and its reply lands in the
/// trail as `{"reply": "an omelette"}`.
#[tokio::test]
async fn a2a_tool_offered_called_and_result_in_trail() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "message": {
                    "messageId": "m-2",
                    "role": "ROLE_AGENT",
                    "parts": [{ "text": "an omelette" }]
                }
            }
        })))
        .mount(&server)
        .await;

    let source = Arc::new(A2aToolSource::new(vec![("recipe".into(), server.uri())]).unwrap());

    let llm = Arc::new(RecordingLlmBackend::new(vec![
        Ok(call_recipe_agent()),
        Ok(final_reply("An omelette sounds great.")),
    ]));
    let (rt, tc) = build_runtime(llm.clone(), Some(source), cfg(vec![recipe_tool_ref()]));

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

    // (a) The a2a tool was offered to the LLM on the first turn.
    {
        let offered = llm.offered.lock().unwrap();
        assert!(
            offered[0].contains(&("a2a:recipe".to_string(), "ask".to_string())),
            "first turn must offer the a2a tool; got: {:?}",
            offered[0]
        );
    }

    // (b) The mocked agent actually received the message text.
    let requests = server.received_requests().await.unwrap();
    let rpc_call = requests
        .iter()
        .find(|r| r.url.path() == RPC_PATH)
        .expect("the agent must have received a POST to its interface");
    let body: serde_json::Value = rpc_call.body_json().unwrap();
    assert_eq!(
        body["params"]["message"]["parts"][0]["text"],
        json!("what can I make with eggs?"),
        "the agent must receive the tool call's message text; body: {body}"
    );

    // (c) The agent's reply landed in the trail as the tool result.
    let tool_result = out.trail.iter().find_map(|s| match s {
        AgentStep::ToolCall { name, result, .. } if name == "ask" => Some(result.clone()),
        _ => None,
    });
    assert_eq!(
        tool_result,
        Some(json!({ "reply": "an omelette" })),
        "a2a tool result must appear in the trail; trail: {:?}",
        out.trail
    );

    // (d) The turn terminated with a final reply.
    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "An omelette sounds great.");
}

/// Degrade case: no `A2aToolSource` is wired at all, so `a2a:` refs resolve to
/// nothing and are dropped from the tool list — mirroring
/// `empty_component_source_offers_no_tool` in `component_loop.rs`.
#[tokio::test]
async fn no_a2a_source_offers_no_a2a_tool() {
    let llm = Arc::new(RecordingLlmBackend::new(vec![Ok(final_reply("no tools"))]));
    let (rt, tc) = build_runtime(llm.clone(), None, cfg(vec![recipe_tool_ref()]));

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
        "no a2a source → no tools offered; got: {:?}",
        offered[0]
    );
    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "no tools");
}
