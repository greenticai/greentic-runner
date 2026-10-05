//! Side turns while a `flow:` tool is parked on a card
//! (`AgentConfig::on_text_while_parked = side_turn`).
//!
//! Same harness as `flow_tool_suspend.rs`, except the scripted LLM REJECTS a
//! history that breaks provider tool-call pairing, the way OpenAI does.

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
use greentic_aw_runtime::state::{AgentStateStore, ChatMessage, ToolCallRecord};
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentOutput, AgentRuntime, FlowInvokeOutcome,
    FlowInvoker, FlowOperation, FlowToolSource, LlmProviderRef, ParkedTextPolicy, ToolRef,
};
use serde_json::{Value, json};

const SESSION: &str = "sess";

/// Scripted flow invoker: `invoke_interactive` and `resume` each pop the next
/// scripted outcome; every call is recorded.
#[derive(Default)]
struct ScriptedFlows {
    invoke: Mutex<Vec<FlowInvokeOutcome>>,
    resume: Mutex<Vec<FlowInvokeOutcome>>,
    invoked: Mutex<Vec<String>>,
    resumed: Mutex<Vec<(String, Value, Value)>>,
}

impl ScriptedFlows {
    fn new(invoke: Vec<FlowInvokeOutcome>, resume: Vec<FlowInvokeOutcome>) -> Self {
        Self {
            invoke: Mutex::new(invoke),
            resume: Mutex::new(resume),
            ..Self::default()
        }
    }
}

impl FlowInvoker for ScriptedFlows {
    fn list_flows(&self) -> Vec<FlowOperation> {
        ["form", "lookup"]
            .into_iter()
            .map(|id| FlowOperation {
                flow_ref: id.into(),
                description: format!("{id} flow"),
                parameters: json!({ "type": "object" }),
            })
            .collect()
    }

    fn invoke<'a>(
        &'a self,
        flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Err(format!("'{flow_ref}' must be invoked interactively")) })
    }

    fn invoke_interactive<'a>(
        &'a self,
        flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        self.invoked.lock().unwrap().push(flow_ref.to_string());
        let next = {
            let mut q = self.invoke.lock().unwrap();
            if q.is_empty() {
                Err("no scripted invoke".to_string())
            } else {
                Ok(q.remove(0))
            }
        };
        Box::pin(async move { next })
    }

    fn resume<'a>(
        &'a self,
        flow_ref: &'a str,
        snapshot: Value,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        self.resumed
            .lock()
            .unwrap()
            .push((flow_ref.to_string(), snapshot, input));
        let next = {
            let mut q = self.resume.lock().unwrap();
            if q.is_empty() {
                Err("no scripted resume".to_string())
            } else {
                Ok(q.remove(0))
            }
        };
        Box::pin(async move { next })
    }
}

/// LLM backend returning scripted responses and recording each request's
/// history, so a test can see what the model was shown.
struct RecordingLlm {
    responses: Mutex<Vec<LlmResponse>>,
    histories: Mutex<Vec<Vec<ChatMessage>>>,
}

impl RecordingLlm {
    fn new(responses: Vec<LlmResponse>) -> Self {
        Self {
            responses: Mutex::new(responses),
            histories: Mutex::new(Vec::new()),
        }
    }
    fn calls(&self) -> usize {
        self.histories.lock().unwrap().len()
    }
}

impl LlmBackend for RecordingLlm {
    fn complete<'a>(
        &'a self,
        req: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        self.histories.lock().unwrap().push(req.history.clone());
        let next = if !pairing_is_valid(&req.history) {
            Err(LlmError::Transport(format!(
                "tool-call pairing broken: {:?}",
                req.history
            )))
        } else {
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

fn tool(flow: &str) -> ToolRef {
    ToolRef {
        extension_id: format!("flow:{flow}"),
        tool_name: flow.into(),
        description: None,
        input_schema: None,
        usage_note: None,
    }
}

fn call(call_id: &str, flow: &str) -> ToolCallRecord {
    ToolCallRecord {
        call_id: call_id.into(),
        extension_id: format!("flow:{flow}"),
        tool_name: flow.into(),
        args: json!({ "q": call_id }),
    }
}

fn tool_calls(calls: Vec<ToolCallRecord>) -> LlmResponse {
    LlmResponse {
        content: Some("let me ask you".into()),
        tool_calls: calls,
        tokens_in: 1,
        tokens_out: 1,
    }
}

fn final_reply(text: &str) -> LlmResponse {
    LlmResponse {
        content: Some(text.into()),
        tool_calls: vec![],
        tokens_in: 1,
        tokens_out: 1,
    }
}

fn waiting(tag: &str) -> FlowInvokeOutcome {
    FlowInvokeOutcome::Waiting {
        snapshot: json!({ "snap": tag }),
        presentation: json!({ "card": tag }),
    }
}

struct Harness {
    rt: AgentRuntime,
    store: Arc<MockAgentStateStore>,
    tc: TenantContext,
}

fn harness(llm: Arc<RecordingLlm>, flows: Arc<ScriptedFlows>, policy: ParkedTextPolicy) -> Harness {
    let store = Arc::new(MockAgentStateStore::new());
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(
        &tc,
        "a",
        AgentConfig {
            agent_id: "a".into(),
            system_prompt: "sys".into(),
            tools: vec![tool("form"), tool("lookup")],
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
            on_text_while_parked: policy,
        },
    );
    let rt = AgentRuntime::new(
        Arc::new(cp),
        store.clone(),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm,
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_flow_source(Some(Arc::new(FlowToolSource::new(flows))));
    Harness { rt, store, tc }
}

impl Harness {
    async fn step(&self, text: &str, resume_payload: Option<Value>) -> AgentOutput {
        self.rt
            .step(
                self.tc.clone(),
                SESSION,
                "a",
                AgentInput {
                    text: text.into(),
                    conversational: false,
                    resume_payload,
                },
            )
            .await
            .unwrap()
    }

    async fn state(&self) -> greentic_aw_runtime::state::ConversationState {
        self.store.load(&self.tc, SESSION).await.unwrap()
    }
}

fn tool_result<'a>(messages: &'a [ChatMessage], call_id: &str) -> Option<&'a Value> {
    messages.iter().find_map(|m| match m {
        ChatMessage::Tool {
            call_id: id,
            content,
        } if id == call_id => Some(content),
        _ => None,
    })
}

/// Every assistant `tool_calls` id is answered by exactly one directly
/// following `Tool`, and every `Tool` answers the assistant before it.
fn pairing_is_valid(messages: &[ChatMessage]) -> bool {
    let mut i = 0;
    while i < messages.len() {
        match &messages[i] {
            ChatMessage::Assistant { tool_calls, .. } if !tool_calls.is_empty() => {
                let mut wanted: Vec<&str> = tool_calls.iter().map(|c| c.call_id.as_str()).collect();
                let mut j = i + 1;
                while let Some(ChatMessage::Tool { call_id, .. }) = messages.get(j) {
                    match wanted.iter().position(|w| w == call_id) {
                        Some(pos) => {
                            wanted.remove(pos);
                        }
                        None => return false,
                    }
                    j += 1;
                }
                if !wanted.is_empty() {
                    return false;
                }
                i = j;
            }
            ChatMessage::Tool { .. } => return false,
            _ => i += 1,
        }
    }
    true
}

fn tool_count(messages: &[ChatMessage], call_id: &str) -> usize {
    messages
        .iter()
        .filter(|m| matches!(m, ChatMessage::Tool { call_id: id, .. } if id == call_id))
        .count()
}

/// With the side-turn policy a park leaves an `awaiting_user_input` result
/// right after the assistant turn, so a later message can follow it validly.
#[tokio::test]
async fn side_turn_policy_writes_a_placeholder_at_park() {
    let llm = Arc::new(RecordingLlm::new(vec![tool_calls(vec![call(
        "c1", "form",
    )])]));
    let flows = Arc::new(ScriptedFlows::new(vec![waiting("A")], vec![]));
    let h = harness(llm, flows, ParkedTextPolicy::SideTurn);

    h.step("book me a room", None).await;

    let state = h.state().await;
    assert_eq!(
        tool_result(&state.messages, "c1"),
        Some(&json!({ "status": "awaiting_user_input" }))
    );
    assert!(pairing_is_valid(&state.messages));
    assert!(state.pending_tool.is_some());
}

/// The default policy leaves the transcript exactly as before: no result for
/// the parked call until it is resumed or cancelled.
#[tokio::test]
async fn cancel_policy_writes_no_placeholder() {
    let llm = Arc::new(RecordingLlm::new(vec![tool_calls(vec![call(
        "c1", "form",
    )])]));
    let flows = Arc::new(ScriptedFlows::new(vec![waiting("A")], vec![]));
    let h = harness(llm, flows, ParkedTextPolicy::Cancel);

    h.step("book me a room", None).await;

    assert_eq!(tool_result(&h.state().await.messages, "c1"), None);
}

/// Resuming replaces the placeholder with the flow's output; the call id is
/// answered exactly once.
#[tokio::test]
async fn a_submit_replaces_the_placeholder_instead_of_adding_a_second_result() {
    let llm = Arc::new(RecordingLlm::new(vec![
        tool_calls(vec![call("c1", "form")]),
        final_reply("booked"),
    ]));
    let flows = Arc::new(ScriptedFlows::new(
        vec![waiting("A")],
        vec![FlowInvokeOutcome::Completed(json!({ "room": "101" }))],
    ));
    let h = harness(llm, flows, ParkedTextPolicy::SideTurn);

    h.step("book me a room", None).await;
    let out = h
        .step("", Some(json!({ "metadata": { "action": "submit" } })))
        .await;

    assert_eq!(out.reply, "booked");
    let state = h.state().await;
    assert_eq!(tool_count(&state.messages, "c1"), 1);
    assert_eq!(
        tool_result(&state.messages, "c1"),
        Some(&json!({ "room": "101" }))
    );
    assert!(pairing_is_valid(&state.messages));
}
