//! Interactive `flow:` tools through the agent loop: a flow that parks on the
//! user (a card) SUSPENDS the turn, and the next turn resumes or cancels it.
//!
//! Drives full `AgentRuntime::step`s against a scripted [`FlowInvoker`] (the
//! trait is public) and a persistent in-memory state store, so each test can
//! assert both what a step returned and what it left in the conversation.

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
    FlowInvoker, FlowOperation, FlowToolSource, LlmProviderRef, ToolRef,
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

fn harness(llm: Arc<RecordingLlm>, flows: Arc<ScriptedFlows>) -> Harness {
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

/// A parking flow ends the turn with `AwaitingToolInput`, hands back its card,
/// and leaves the call pending. A second call in the SAME batch is answered
/// with an error and never runs.
#[tokio::test]
async fn a_parking_flow_suspends_the_turn_and_refuses_the_rest_of_the_batch() {
    let llm = Arc::new(RecordingLlm::new(vec![tool_calls(vec![
        call("c1", "form"),
        call("c2", "lookup"),
    ])]));
    let flows = Arc::new(ScriptedFlows::new(vec![waiting("A")], vec![]));
    let h = harness(llm.clone(), flows.clone());

    let out = h.step("book me a room", None).await;

    assert_eq!(out.terminated_by, TerminationReason::AwaitingToolInput);
    assert_eq!(out.pending_presentation, Some(json!({ "card": "A" })));
    assert_eq!(out.reply, "let me ask you");
    assert_eq!(
        llm.calls(),
        1,
        "the turn must stop after the suspending batch"
    );
    assert_eq!(
        *flows.invoked.lock().unwrap(),
        vec!["form".to_string()],
        "the call behind the suspended one must not run"
    );

    let state = h.state().await;
    let pending = state.pending_tool.as_ref().expect("pending tool saved");
    assert_eq!(pending.call_id, "c1");
    assert_eq!(pending.flow_ref, "form");
    assert_eq!(pending.flow_snapshot, json!({ "snap": "A" }));
    assert_eq!(pending.iterations_used, 1);
    assert!(
        tool_result(&state.messages, "c1").is_none(),
        "the suspended call has no result yet"
    );
    assert_eq!(
        tool_result(&state.messages, "c2"),
        Some(&json!({ "error": "another step is waiting for the user" }))
    );
    // Serialised form the host reads.
    let wire = serde_json::to_value(&out).unwrap();
    assert_eq!(wire["terminated_by"], "awaiting_tool_input");
    assert_eq!(wire["pending_presentation"], json!({ "card": "A" }));
}

/// The user's answer resumes the flow; its output becomes the tool result for
/// the pending call id and the loop continues on the iteration budget it had.
#[tokio::test]
async fn a_resume_payload_completes_the_flow_and_continues_the_loop() {
    let llm = Arc::new(RecordingLlm::new(vec![
        tool_calls(vec![call("c1", "form")]),
        final_reply("booked"),
    ]));
    let flows = Arc::new(ScriptedFlows::new(
        vec![waiting("A")],
        vec![FlowInvokeOutcome::Completed(json!({ "room": "101" }))],
    ));
    let h = harness(llm.clone(), flows.clone());
    h.step("book me a room", None).await;

    let submit = json!({ "metadata": { "action": "submit" }, "nights": "2" });
    let out = h.step("", Some(submit.clone())).await;

    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "booked");
    assert_eq!(out.pending_presentation, None);
    assert_eq!(
        out.usage.iterations, 2,
        "the resumed turn continues the suspended turn's iteration budget"
    );
    let resumed = flows.resumed.lock().unwrap().clone();
    assert_eq!(
        resumed,
        vec![("form".to_string(), json!({ "snap": "A" }), submit)]
    );

    // The model saw the tool result for the pending call, and no new user turn.
    let seen = llm.histories.lock().unwrap()[1].clone();
    assert_eq!(tool_result(&seen, "c1"), Some(&json!({ "room": "101" })));
    assert!(matches!(seen.last(), Some(ChatMessage::Tool { .. })));

    let state = h.state().await;
    assert!(state.pending_tool.is_none());
}

/// A flow that parks again re-suspends with the new snapshot and card, without
/// calling the LLM.
#[tokio::test]
async fn a_flow_that_parks_again_re_suspends() {
    let llm = Arc::new(RecordingLlm::new(vec![tool_calls(vec![call(
        "c1", "form",
    )])]));
    let flows = Arc::new(ScriptedFlows::new(vec![waiting("A")], vec![waiting("B")]));
    let h = harness(llm.clone(), flows);
    h.step("book me a room", None).await;

    let out = h
        .step("", Some(json!({ "metadata": { "action": "next" } })))
        .await;

    assert_eq!(out.terminated_by, TerminationReason::AwaitingToolInput);
    assert_eq!(out.pending_presentation, Some(json!({ "card": "B" })));
    assert_eq!(llm.calls(), 1, "a re-suspension spends no LLM call");
    let state = h.state().await;
    let pending = state.pending_tool.expect("still pending");
    assert_eq!(pending.call_id, "c1");
    assert_eq!(pending.flow_snapshot, json!({ "snap": "B" }));
}

/// A turn without an answer cancels the pending tool and is an ordinary
/// message.
#[tokio::test]
async fn a_message_without_a_resume_payload_cancels_the_pending_tool() {
    let llm = Arc::new(RecordingLlm::new(vec![
        tool_calls(vec![call("c1", "form")]),
        final_reply("ok, cancelled"),
    ]));
    let flows = Arc::new(ScriptedFlows::new(vec![waiting("A")], vec![]));
    let h = harness(llm.clone(), flows.clone());
    h.step("book me a room", None).await;

    let out = h.step("never mind", None).await;

    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert!(flows.resumed.lock().unwrap().is_empty());
    let seen = llm.histories.lock().unwrap()[1].clone();
    let cancelled = tool_result(&seen, "c1").expect("cancelled result");
    assert_eq!(cancelled["status"], "cancelled");
    assert!(
        matches!(seen.last(), Some(ChatMessage::User { content }) if content == "never mind"),
        "the user's text follows the cancelled tool result"
    );
    assert!(h.state().await.pending_tool.is_none());
}

/// An expired suspension is cancelled even when an answer arrives.
#[tokio::test]
async fn an_expired_pending_tool_is_cancelled_not_resumed() {
    let llm = Arc::new(RecordingLlm::new(vec![
        tool_calls(vec![call("c1", "form")]),
        final_reply("that expired"),
    ]));
    let flows = Arc::new(ScriptedFlows::new(vec![waiting("A")], vec![]));
    let h = harness(llm.clone(), flows.clone());
    h.step("book me a room", None).await;

    let mut state = h.state().await;
    state.pending_tool.as_mut().unwrap().expires_at =
        chrono::Utc::now() - chrono::Duration::seconds(1);
    h.store.save(&h.tc, SESSION, &state).await.unwrap();

    let out = h
        .step("", Some(json!({ "metadata": { "action": "submit" } })))
        .await;

    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert!(flows.resumed.lock().unwrap().is_empty());
    let histories = llm.histories.lock().unwrap();
    let cancelled = tool_result(&histories[1], "c1").expect("cancelled result");
    assert_eq!(cancelled["status"], "cancelled");
    assert!(
        cancelled["reason"].as_str().unwrap().contains("expired"),
        "got {cancelled}"
    );
}
