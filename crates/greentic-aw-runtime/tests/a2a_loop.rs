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
    assert!(rt.has_a2a_source(), "a wired source must be observable");

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
        Some(json!({ "status": "completed", "agent": "recipe", "reply": "an omelette" })),
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
    assert!(!rt.has_a2a_source());

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

// --- Multi-turn continuation through the agent loop ----------------------

/// The `{contextId, taskId}` each `SendMessage` POST carried, in order.
async fn sent_references(server: &MockServer) -> Vec<(Option<String>, Option<String>)> {
    server
        .received_requests()
        .await
        .expect("request log")
        .iter()
        .filter(|r| r.url.path() == RPC_PATH)
        .map(|r| {
            let body: serde_json::Value = r.body_json().expect("a json body");
            let get = |k: &str| body["params"]["message"][k].as_str().map(str::to_string);
            (get("contextId"), get("taskId"))
        })
        .collect()
}

/// A task reply carrying a context id and a status message.
fn task_in_context(state: &str, says: &str) -> serde_json::Value {
    json!({"jsonrpc": "2.0", "id": 1, "result": {"task": {
        "id": "t-1",
        "contextId": "ctx-1",
        "status": {
            "state": state,
            "message": {"messageId": "m-s", "role": "ROLE_AGENT", "parts": [{"text": says}]}
        }
    }}})
}

/// The whole point of the slice, end to end: the remote agent asks for more
/// information on turn one, the worker's model sees that it is waiting, the
/// user answers, and the SECOND turn resumes the SAME remote task — carried
/// across `AgentRuntime::step` boundaries by the conversation state, not by
/// anything the two steps share in memory.
#[tokio::test]
async fn a_second_turn_resumes_the_remote_task_the_first_turn_left_open() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(task_in_context("TASK_STATE_INPUT_REQUIRED", "Which city?")),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {"task": {
                "id": "t-1",
                "contextId": "ctx-1",
                "status": {"state": "TASK_STATE_COMPLETED"},
                "artifacts": [{"parts": [{"text": "Hotel Diagonal, Barcelona"}]}]
            }}
        })))
        .mount(&server)
        .await;

    let source = Arc::new(A2aToolSource::new(vec![("recipe".into(), server.uri())]).unwrap());
    let llm = Arc::new(RecordingLlmBackend::new(vec![
        Ok(call_recipe_agent()),
        Ok(final_reply("Which city should I book in?")),
        Ok(call_recipe_agent()),
        Ok(final_reply("Booked: Hotel Diagonal.")),
    ]));
    let (rt, tc) = build_runtime(llm, Some(source), cfg(vec![recipe_tool_ref()]));

    let turn_one = rt
        .step(
            tc.clone(),
            "s",
            "a",
            AgentInput {
                text: "book a hotel in Spain".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();

    // (a) The model was told the remote agent is waiting, and what it asked.
    let first_result = tool_result_of(&turn_one);
    assert_eq!(first_result["status"], "input_required", "{first_result}");
    assert_eq!(first_result["question"], "Which city?");
    assert!(
        first_result.get("reply").is_none(),
        "an unanswered question must not arrive under `reply`: {first_result}"
    );

    // (b) The user answers, on the SAME session id.
    let turn_two = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "Barcelona".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    let second_result = tool_result_of(&turn_two);
    assert_eq!(second_result["status"], "completed", "{second_result}");
    assert_eq!(second_result["reply"], "Hotel Diagonal, Barcelona");

    // (c) And the second call went to the same remote task, not a new one.
    let sent = sent_references(&server).await;
    assert_eq!(sent.len(), 2, "two calls; got {sent:?}");
    assert_eq!(sent[0], (None, None), "turn one opens the conversation");
    assert_eq!(
        sent[1],
        (Some("ctx-1".into()), Some("t-1".into())),
        "turn two must resume the same remote task; got {sent:?}"
    );
}

/// A different Greentic conversation is a different remote conversation.
/// Sharing one would let one user's exchange continue inside another's.
#[tokio::test]
async fn a_different_session_never_inherits_the_first_ones_remote_context() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(task_in_context("TASK_STATE_INPUT_REQUIRED", "Which city?")),
        )
        .mount(&server)
        .await;

    let source = Arc::new(A2aToolSource::new(vec![("recipe".into(), server.uri())]).unwrap());
    let llm = Arc::new(RecordingLlmBackend::new(vec![
        Ok(call_recipe_agent()),
        Ok(final_reply("asking")),
        Ok(call_recipe_agent()),
        Ok(final_reply("asking")),
    ]));
    let (rt, tc) = build_runtime(llm, Some(source), cfg(vec![recipe_tool_ref()]));

    for session in ["session-one", "session-two"] {
        rt.step(
            tc.clone(),
            session,
            "a",
            AgentInput {
                text: "book a hotel".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    }

    let sent = sent_references(&server).await;
    assert_eq!(
        sent,
        vec![(None, None), (None, None)],
        "each conversation opens its own remote context; got {sent:?}"
    );
}

/// Two tenants using the SAME session id must not meet. The state store keys
/// by tenant, so they get different states; this asserts the observable
/// consequence rather than the key.
#[tokio::test]
async fn another_tenant_on_the_same_session_id_gets_its_own_remote_context() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(task_in_context("TASK_STATE_INPUT_REQUIRED", "Which city?")),
        )
        .mount(&server)
        .await;

    let source = Arc::new(A2aToolSource::new(vec![("recipe".into(), server.uri())]).unwrap());
    let llm = Arc::new(RecordingLlmBackend::new(vec![
        Ok(call_recipe_agent()),
        Ok(final_reply("asking")),
        Ok(call_recipe_agent()),
        Ok(final_reply("asking")),
    ]));
    let store = Arc::new(MockAgentStateStore::new());
    let cp = MockConfigProvider::new();
    let acme = TenantContext::new("acme", "prod");
    let globex = TenantContext::new("globex", "prod");
    cp.insert(&acme, "a", cfg(vec![recipe_tool_ref()]));
    cp.insert(&globex, "a", cfg(vec![recipe_tool_ref()]));
    let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let rt = AgentRuntime::new(
        Arc::new(cp),
        store,
        ext,
        llm,
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_a2a_source(Some(source));

    for tenant in [acme, globex] {
        rt.step(
            tenant,
            "shared-session-id",
            "a",
            AgentInput {
                text: "book a hotel".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    }

    let sent = sent_references(&server).await;
    assert_eq!(
        sent,
        vec![(None, None), (None, None)],
        "a second tenant must not resume the first's remote conversation; got {sent:?}"
    );
}

/// A remote failure must not reach the model under `reply`, at the loop level
/// and not only in the renderer's unit tests.
#[tokio::test]
async fn a_failed_remote_task_reaches_the_model_as_a_failure_not_a_reply() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(task_in_context("TASK_STATE_FAILED", "upstream timeout")),
        )
        .mount(&server)
        .await;

    let source = Arc::new(A2aToolSource::new(vec![("recipe".into(), server.uri())]).unwrap());
    let llm = Arc::new(RecordingLlmBackend::new(vec![
        Ok(call_recipe_agent()),
        Ok(final_reply("The recipe agent could not answer.")),
    ]));
    let (rt, tc) = build_runtime(llm, Some(source), cfg(vec![recipe_tool_ref()]));

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

    let result = tool_result_of(&out);
    assert_eq!(result["status"], "failed", "{result}");
    assert!(
        result.get("reply").is_none(),
        "a failed remote task must not read like an answer: {result}"
    );
    assert!(
        result["error"]
            .as_str()
            .is_some_and(|e| e.contains("upstream timeout")),
        "{result}"
    );
}

/// The a2a tool result recorded in a step's trail.
fn tool_result_of(out: &greentic_aw_runtime::AgentOutput) -> serde_json::Value {
    out.trail
        .iter()
        .find_map(|s| match s {
            AgentStep::ToolCall { name, result, .. } if name == "ask" => Some(result.clone()),
            _ => None,
        })
        .expect("the a2a tool must have been dispatched")
}
