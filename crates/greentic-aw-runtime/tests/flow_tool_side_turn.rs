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

use greentic_aw_runtime::config::{GuardrailMode, GuardrailRef};
use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::{LlmError, TerminationReason};
use greentic_aw_runtime::guardrail::{AcceptAllEvaluator, StaticGuardrailPolicy};
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
    prompts: Mutex<Vec<String>>,
}

impl RecordingLlm {
    fn new(responses: Vec<LlmResponse>) -> Self {
        Self {
            responses: Mutex::new(responses),
            histories: Mutex::new(Vec::new()),
            prompts: Mutex::new(Vec::new()),
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
        self.prompts.lock().unwrap().push(req.system_prompt.clone());
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
    harness_with_store(llm, flows, policy, Arc::new(MockAgentStateStore::new()))
}

fn harness_with_store(
    llm: Arc<RecordingLlm>,
    flows: Arc<ScriptedFlows>,
    policy: ParkedTextPolicy,
    store: Arc<MockAgentStateStore>,
) -> Harness {
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
                    ..Default::default()
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

const CARD_A: &str = "A";

/// Park a `form` flow under the side-turn policy and return the harness.
async fn parked(
    llm_script: Vec<LlmResponse>,
    resume: Vec<FlowInvokeOutcome>,
) -> (Harness, Arc<RecordingLlm>, Arc<ScriptedFlows>) {
    let mut script = vec![tool_calls(vec![call("c1", "form")])];
    script.extend(llm_script);
    let llm = Arc::new(RecordingLlm::new(script));
    let flows = Arc::new(ScriptedFlows::new(vec![waiting(CARD_A)], resume));
    let h = harness(llm.clone(), flows.clone(), ParkedTextPolicy::SideTurn);
    h.step("book me a room", None).await;
    (h, llm, flows)
}

fn submit() -> Option<Value> {
    Some(json!({ "metadata": { "action": "submit" } }))
}

#[tokio::test]
async fn a_typed_message_is_answered_and_the_card_stays_parked() {
    let (h, llm, flows) = parked(vec![final_reply("the signature is in Settings")], vec![]).await;

    let out = h.step("how do I get a mail signature?", None).await;

    assert_eq!(out.reply, "the signature is in Settings");
    assert_eq!(out.terminated_by, TerminationReason::AwaitingToolInput);
    assert!(out.side_turn);
    assert_eq!(out.pending_presentation, Some(json!({ "card": CARD_A })));
    assert_eq!(llm.calls(), 2);
    assert!(flows.resumed.lock().unwrap().is_empty());
    let state = h.state().await;
    let pending = state.pending_tool.as_ref().expect("still parked");
    assert_eq!(pending.side_turns, 1);
    assert!(state.messages.iter().all(|m| !matches!(m,
        ChatMessage::Tool { content, .. } if content["status"] == "cancelled")));
    assert!(pairing_is_valid(&state.messages));
}

#[tokio::test]
async fn two_side_turns_then_a_submit_resumes_the_flow() {
    let (h, _llm, flows) = parked(
        vec![
            final_reply("one"),
            final_reply("two"),
            final_reply("booked"),
        ],
        vec![FlowInvokeOutcome::Completed(json!({ "room": "101" }))],
    )
    .await;

    h.step("q1", None).await;
    h.step("q2", None).await;
    let out = h.step("", submit()).await;

    assert_eq!(out.reply, "booked");
    assert!(!out.side_turn);
    {
        let resumed = flows.resumed.lock().unwrap();
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].1, json!({ "snap": CARD_A }), "original snapshot");
    }
    let state = h.state().await;
    assert!(state.pending_tool.is_none());
    assert_eq!(tool_count(&state.messages, "c1"), 1);
    assert_eq!(
        tool_result(&state.messages, "c1"),
        Some(&json!({ "room": "101" }))
    );
    assert!(pairing_is_valid(&state.messages));
}

#[tokio::test]
async fn a_side_turn_cannot_start_another_flow() {
    let (h, _llm, flows) = parked(
        vec![
            tool_calls(vec![call("c2", "form")]),
            final_reply("I cannot open another form right now"),
        ],
        vec![],
    )
    .await;

    let out = h.step("open the form again", None).await;

    assert!(out.side_turn);
    assert_eq!(
        flows.invoked.lock().unwrap().len(),
        1,
        "no second invocation"
    );
    let state = h.state().await;
    assert_eq!(
        tool_result(&state.messages, "c2"),
        Some(&json!({ "error": "another step is waiting for the user" }))
    );
    assert!(state.pending_tool.is_some());
    assert!(pairing_is_valid(&state.messages));
}

#[tokio::test]
async fn the_message_after_the_cap_cancels_the_park() {
    let (h, _llm, _flows) = parked(
        vec![final_reply("answer"), final_reply("after the cap")],
        vec![],
    )
    .await;
    let mut state = h.state().await;
    if let Some(p) = state.pending_tool.as_mut() {
        p.side_turns = greentic_aw_runtime::state::MAX_SIDE_TURNS;
    }
    h.store.save(&h.tc, SESSION, &state).await.unwrap();

    let out = h.step("one more", None).await;

    assert!(!out.side_turn);
    let state = h.state().await;
    assert!(state.pending_tool.is_none());
    assert_eq!(
        tool_result(&state.messages, "c1").map(|v| v["status"].clone()),
        Some(json!("cancelled"))
    );
    assert!(pairing_is_valid(&state.messages));
}

#[tokio::test]
async fn side_turns_never_extend_the_park_past_24_hours() {
    let (h, _llm, _flows) = parked(vec![final_reply("answer")], vec![]).await;
    let now = chrono::Utc::now();
    let mut state = h.state().await;
    if let Some(p) = state.pending_tool.as_mut() {
        p.parked_at = Some(now - chrono::Duration::hours(23) - chrono::Duration::minutes(30));
        p.expires_at = now + chrono::Duration::minutes(10);
    }
    h.store.save(&h.tc, SESSION, &state).await.unwrap();

    h.step("still there?", None).await;

    let state = h.state().await;
    let pending = state.pending_tool.expect("side turn keeps the park");
    let cap = pending.parked_at.unwrap() + chrono::Duration::hours(24);
    assert!(
        pending.expires_at <= cap,
        "expiry must not pass parked_at + 24h"
    );
}

#[tokio::test]
async fn a_park_older_than_24_hours_cancels_instead_of_side_turning() {
    let (h, _llm, _flows) = parked(vec![final_reply("fresh answer")], vec![]).await;
    let now = chrono::Utc::now();
    let mut state = h.state().await;
    if let Some(p) = state.pending_tool.as_mut() {
        p.parked_at = Some(now - chrono::Duration::hours(25));
        p.expires_at = now + chrono::Duration::minutes(30);
    }
    h.store.save(&h.tc, SESSION, &state).await.unwrap();

    let out = h.step("hello", None).await;

    assert!(!out.side_turn);
    assert!(h.state().await.pending_tool.is_none());
}

#[tokio::test]
async fn an_idle_expired_park_cancels_even_under_the_side_turn_policy() {
    let (h, _llm, _flows) = parked(vec![final_reply("fresh answer")], vec![]).await;
    let mut state = h.state().await;
    if let Some(p) = state.pending_tool.as_mut() {
        p.expires_at = chrono::Utc::now() - chrono::Duration::minutes(1);
    }
    h.store.save(&h.tc, SESSION, &state).await.unwrap();

    let out = h.step("hello", None).await;

    assert!(!out.side_turn);
    assert!(h.state().await.pending_tool.is_none());
}

#[tokio::test]
async fn an_llm_error_during_a_side_turn_keeps_the_park_and_a_valid_transcript() {
    // Script: park, then NOTHING for the side turn (queue exhausted -> LLM error),
    // then a normal answer on the next message.
    let (h, _llm, _flows) = parked(vec![], vec![]).await;

    let failed =
        h.rt.step(
            h.tc.clone(),
            SESSION,
            "a",
            AgentInput {
                text: "q".into(),
                conversational: false,
                resume_payload: None,
                ..Default::default()
            },
        )
        .await;
    assert!(failed.is_err());

    let state = h.state().await;
    assert!(
        state.pending_tool.is_some(),
        "the park survives an LLM error"
    );
    assert!(pairing_is_valid(&state.messages));
}

/// A park recorded before placeholders existed (no `Tool` for the call and no
/// stored card) cannot be re-offered, so it is cancelled rather than left
/// dangling.
#[tokio::test]
async fn an_old_park_without_a_stored_card_is_cancelled() {
    let llm = Arc::new(RecordingLlm::new(vec![
        tool_calls(vec![call("c1", "form")]),
        final_reply("ok"),
    ]));
    let flows = Arc::new(ScriptedFlows::new(vec![waiting(CARD_A)], vec![]));
    let h = harness(llm, flows, ParkedTextPolicy::Cancel);
    h.step("book me a room", None).await;
    let mut state = h.state().await;
    if let Some(p) = state.pending_tool.as_mut() {
        p.presentation = None;
        p.parked_at = None;
    }
    h.store.save(&h.tc, SESSION, &state).await.unwrap();

    // Same state, now read by a worker configured for side turns.
    let llm2 = Arc::new(RecordingLlm::new(vec![final_reply("ok")]));
    let flows2 = Arc::new(ScriptedFlows::new(vec![], vec![]));
    let h2 = harness_with_store(llm2, flows2, ParkedTextPolicy::SideTurn, h.store.clone());
    let out = h2.step("hi", None).await;

    assert!(!out.side_turn);
    assert!(h2.state().await.pending_tool.is_none());
    assert!(pairing_is_valid(&h2.state().await.messages));
}

/// A park written under the cancel policy has no placeholder. A worker now
/// configured for side turns inserts one right after the assistant turn and
/// answers.
#[tokio::test]
async fn a_park_without_a_placeholder_takes_a_side_turn_after_a_policy_switch() {
    let llm = Arc::new(RecordingLlm::new(vec![tool_calls(vec![call(
        "c1", "form",
    )])]));
    let flows = Arc::new(ScriptedFlows::new(vec![waiting(CARD_A)], vec![]));
    let h = harness(llm, flows, ParkedTextPolicy::Cancel);
    h.step("book me a room", None).await;
    assert_eq!(tool_result(&h.state().await.messages, "c1"), None);

    let llm2 = Arc::new(RecordingLlm::new(vec![final_reply("an answer")]));
    let flows2 = Arc::new(ScriptedFlows::new(vec![], vec![]));
    let h2 = harness_with_store(llm2, flows2, ParkedTextPolicy::SideTurn, h.store.clone());
    let out = h2.step("a question", None).await;

    assert!(out.side_turn);
    let state = h2.state().await;
    assert!(state.pending_tool.is_some());
    assert_eq!(
        tool_result(&state.messages, "c1"),
        Some(&json!({ "status": "awaiting_user_input" }))
    );
    assert!(pairing_is_valid(&state.messages));
}

/// A guardrail that fail-closes the turn returns an error before anything is
/// saved, so the park (and its counters) are exactly as before the message.
#[tokio::test]
async fn a_denied_side_message_keeps_the_park_untouched() {
    let (h, _llm, _flows) = parked(vec![final_reply("never used")], vec![]).await;
    let before = h.state().await;
    let rt = h.rt.with_guardrails(
        Arc::new(StaticGuardrailPolicy(vec![GuardrailRef {
            cap_id: "greentic:guardrail/required".into(),
            offer_id: None,
            config: Value::Null,
            mode: GuardrailMode::Enforce,
        }])),
        Arc::new(AcceptAllEvaluator),
    );
    let h = Harness {
        rt,
        store: h.store,
        tc: h.tc,
    };

    let denied =
        h.rt.step(
            h.tc.clone(),
            SESSION,
            "a",
            AgentInput {
                text: "q".into(),
                conversational: false,
                resume_payload: None,
                ..Default::default()
            },
        )
        .await;

    assert!(denied.is_err());
    let after = h.state().await;
    assert_eq!(after.pending_tool, before.pending_tool);
    assert_eq!(after.messages.len(), before.messages.len());
}

/// The resume turn after side turns is told the form has finished, so the
/// model answers about the booking rather than the last side question.
#[tokio::test]
async fn a_resume_after_side_turns_tells_the_model_the_form_finished() {
    let (h, llm, _flows) = parked(
        vec![final_reply("one"), final_reply("booked")],
        vec![FlowInvokeOutcome::Completed(json!({ "room": "101" }))],
    )
    .await;
    h.step("q1", None).await;
    h.step("", submit()).await;

    let prompts = llm.prompts.lock().unwrap().clone();
    let last = prompts.last().expect("resume turn prompt");
    assert!(
        last.contains("has now finished"),
        "resume after a side turn must carry the cue: {last}"
    );
}

#[tokio::test]
async fn a_resume_without_side_turns_carries_no_cue() {
    let (h, llm, _flows) = parked(
        vec![final_reply("booked")],
        vec![FlowInvokeOutcome::Completed(json!({ "room": "101" }))],
    )
    .await;
    h.step("", submit()).await;
    let prompts = llm.prompts.lock().unwrap().clone();
    assert!(!prompts.last().unwrap().contains("has now finished"));
}

/// After an error the host can still re-offer the card: the park is reported
/// while live and gone once cancelled or expired.
#[tokio::test]
async fn parked_card_reports_a_live_park_only() {
    let (h, _llm, _flows) = parked(vec![], vec![]).await;
    assert_eq!(
        h.rt.parked_card(&h.tc, SESSION).await,
        Some(json!({ "card": CARD_A }))
    );
    let mut state = h.state().await;
    if let Some(p) = state.pending_tool.as_mut() {
        p.expires_at = chrono::Utc::now() - chrono::Duration::minutes(1);
    }
    h.store.save(&h.tc, SESSION, &state).await.unwrap();
    assert_eq!(h.rt.parked_card(&h.tc, SESSION).await, None);
    assert_eq!(h.rt.parked_card(&h.tc, "no-such-session").await, None);
}
