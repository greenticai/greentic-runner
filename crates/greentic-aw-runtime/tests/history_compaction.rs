//! `AgentLimits::compact_history`: overflowing history is folded into one
//! system message instead of being dropped, and an off switch changes nothing.
//! Uses test-mock backends — no Redis, no network.

#![cfg(feature = "test-mock")]

use std::sync::Arc;
use std::time::Duration;

use greentic_aw_runtime::compaction::SUMMARY_PREFIX;
use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::state::{AgentStateStore, ChatMessage};
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, LlmProviderRef, ToolRef,
};

fn cfg(compact: bool) -> AgentConfig {
    AgentConfig {
        agent_id: "a".into(),
        system_prompt: "sys".into(),
        tools: Vec::<ToolRef>::new(),
        guardrails: vec![],
        llm: LlmProviderRef {
            provider: "mock".into(),
            model: "m".into(),
            credential_ref: None,
        },
        limits: AgentLimits {
            max_iter: 8,
            timeout: Duration::from_millis(60_000),
            max_history_turns: 2,
            compact_history: compact,
            ..AgentLimits::default()
        },
        memory: None,
        knowledge: None,
        conversational: false,
        opening_message: None,
        on_text_while_parked: Default::default(),
    }
}

fn reply(text: &str) -> Result<LlmResponse, greentic_aw_runtime::error::LlmError> {
    Ok(LlmResponse {
        content: Some(text.into()),
        tool_calls: vec![],
        tokens_in: 5,
        tokens_out: 2,
    })
}

async fn run(
    compact: bool,
    scripted: Vec<Result<LlmResponse, greentic_aw_runtime::error::LlmError>>,
    turns: &[&str],
) -> (Arc<MockLlmBackend>, Vec<ChatMessage>) {
    let tenant = TenantContext::new("acme", "prod");
    let cp = MockConfigProvider::new();
    cp.insert(&tenant, "a", cfg(compact));
    let store = Arc::new(MockAgentStateStore::new());
    let llm = Arc::new(MockLlmBackend::new(scripted));
    let runtime = AgentRuntime::new(
        Arc::new(cp),
        store.clone(),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm.clone(),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    );
    for t in turns {
        runtime
            .step(
                tenant.clone(),
                "s",
                "a",
                AgentInput {
                    text: (*t).into(),
                    conversational: false,
                    resume_payload: None,
                    ..Default::default()
                },
            )
            .await
            .expect("step should succeed");
    }
    let state = store.load(&tenant, "s").await.expect("state loads");
    (llm, state.messages)
}

fn summary_of(messages: &[ChatMessage]) -> Option<&str> {
    messages.iter().find_map(|m| match m {
        ChatMessage::System { content } => content.strip_prefix(SUMMARY_PREFIX),
        _ => None,
    })
}

#[tokio::test]
async fn overflow_is_folded_into_a_summary_when_compaction_is_on() {
    // turn 1: reply; turn 2: reply, then the overflow is summarised.
    let (llm, messages) = run(
        true,
        vec![reply("hi Ana"), reply("tea it is"), reply("Ana likes tea.")],
        &["my name is Ana", "I like tea"],
    )
    .await;
    assert_eq!(summary_of(&messages), Some("Ana likes tea."));
    let kept = messages
        .iter()
        .filter(|m| !matches!(m, ChatMessage::System { .. }))
        .count();
    assert_eq!(kept, 2, "the window still holds max_history_turns messages");
    let prompts = llm.seen_system_prompts.lock().unwrap();
    assert_eq!(prompts.len(), 3, "two turns plus one summarising call");
}

#[tokio::test]
async fn a_failing_summary_call_still_trims_and_never_fails_the_turn() {
    let (_llm, messages) = run(
        true,
        vec![
            reply("hi Ana"),
            reply("tea it is"),
            Err(greentic_aw_runtime::error::LlmError::Transport(
                "boom".into(),
            )),
        ],
        &["my name is Ana", "I like tea"],
    )
    .await;
    assert_eq!(summary_of(&messages), None);
    let kept = messages
        .iter()
        .filter(|m| !matches!(m, ChatMessage::System { .. }))
        .count();
    assert_eq!(kept, 2);
}

#[tokio::test]
async fn compaction_off_makes_no_extra_llm_call_and_writes_no_summary() {
    let (llm, messages) = run(
        false,
        vec![reply("hi Ana"), reply("tea it is")],
        &["my name is Ana", "I like tea"],
    )
    .await;
    assert_eq!(summary_of(&messages), None);
    assert_eq!(llm.seen_system_prompts.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn the_summary_is_rewritten_not_stacked_on_the_next_overflow() {
    let (_llm, messages) = run(
        true,
        vec![
            reply("r1"),
            reply("r2"),
            reply("summary one"),
            reply("r3"),
            reply("summary two"),
        ],
        &["u1", "u2", "u3"],
    )
    .await;
    assert_eq!(summary_of(&messages), Some("summary two"));
    let summaries = messages
        .iter()
        .filter(
            |m| matches!(m, ChatMessage::System { content } if content.starts_with(SUMMARY_PREFIX)),
        )
        .count();
    assert_eq!(summaries, 1);
}
