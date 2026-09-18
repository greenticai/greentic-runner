//! Scripted-LLM unit tests for the Plan-Act-Observe loop.
//! Uses test-mock backends — no Redis, no network.

#![cfg(feature = "test-mock")]

use std::sync::Arc;
use std::time::Duration;

use greentic_aw_runtime::config::{MemoryProviderRef, MemorySettings};
use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::{AgentError, TerminationReason};
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::long_term::RecalledFact;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockLongTermMemory, MockTelemetry,
    NoopToolLedger,
};
use greentic_aw_runtime::state::ToolCallRecord;
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, AgentStep, LlmProviderRef, ToolRef,
};

fn cfg(max_iter: u32, timeout_ms: u64, tools: Vec<ToolRef>, cap: Option<u32>) -> AgentConfig {
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
            max_iter,
            timeout: Duration::from_millis(timeout_ms),
            daily_token_cap_per_tenant: cap,
            ..AgentLimits::default()
        },
        memory: None,
        knowledge: None,
        conversational: false,
        opening_message: None,
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

fn tool_call(call_id: &str, ext: &str, tool: &str) -> LlmResponse {
    LlmResponse {
        content: None,
        tool_calls: vec![ToolCallRecord {
            call_id: call_id.into(),
            extension_id: ext.into(),
            tool_name: tool.into(),
            args: serde_json::json!({}),
        }],
        tokens_in: 5,
        tokens_out: 5,
    }
}

fn build_runtime(
    llm_script: Vec<Result<LlmResponse, greentic_aw_runtime::error::LlmError>>,
    cfg_inner: AgentConfig,
    token_used: u64,
) -> (AgentRuntime, Arc<MockTelemetry>, TenantContext) {
    let llm = Arc::new(MockLlmBackend::new(llm_script));
    let store = Arc::new(MockAgentStateStore::new());
    let telemetry = Arc::new(MockTelemetry::new());
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(&tc, "a", cfg_inner);
    let cp = Arc::new(cp);
    let token_meter = Arc::new(MockTokenMeter::new(token_used));
    let ledger = Arc::new(NoopToolLedger);
    let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let rt = AgentRuntime::new(
        cp,
        store,
        ext,
        llm,
        telemetry.clone(),
        token_meter,
        ledger,
        None,
    );
    (rt, telemetry, tc)
}

#[tokio::test]
async fn happy_path_one_iteration() {
    let (rt, tel, tc) = build_runtime(vec![Ok(final_reply("hi"))], cfg(8, 60_000, vec![], None), 0);
    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "hello".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.reply, "hi");
    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(tel.recorded.lock().unwrap()[0].iterations, 1);
}

#[tokio::test]
async fn max_iterations_terminates_loop() {
    // LLM keeps emitting tool calls for a NON-allowed tool (empty allow-list)
    // → each blocked → loop exhausts at max_iter=3.
    let script = vec![
        Ok(tool_call("c1", "http", "fetch")),
        Ok(tool_call("c2", "http", "fetch")),
        Ok(tool_call("c3", "http", "fetch")),
    ];
    let (rt, tel, tc) = build_runtime(script, cfg(3, 60_000, vec![], None), 0);
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
    assert_eq!(out.terminated_by, TerminationReason::MaxIterations);
    assert_eq!(tel.recorded.lock().unwrap()[0].iterations, 3);
}

#[tokio::test]
async fn timeout_terminates_loop() {
    // timeout=0ms → the iteration-start timeout check fires immediately.
    let (rt, tel, tc) = build_runtime(vec![Ok(final_reply("never"))], cfg(8, 0, vec![], None), 0);
    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "x".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.terminated_by, TerminationReason::Timeout);
    assert_eq!(tel.recorded.lock().unwrap()[0].iterations, 1);
}

#[tokio::test]
async fn tool_not_allowed_becomes_observation_then_reply() {
    // First response: tool call for a tool NOT in the (empty) allow-list →
    // blocked observation. Second response: final reply.
    let script = vec![
        Ok(tool_call("c1", "http", "fetch")),
        Ok(final_reply("ok done")),
    ];
    let (rt, _tel, tc) = build_runtime(script, cfg(4, 60_000, vec![], None), 0);
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
    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "ok done");
    assert!(
        out.trail
            .iter()
            .any(|s| matches!(s, AgentStep::ToolCallBlocked { .. }))
    );
}

#[tokio::test]
async fn tool_dispatch_error_becomes_observation_then_reply() {
    // Tool IS allowed, but for_test ExtensionRuntime has no extensions →
    // invoke_tool returns NotFound → dispatch error becomes an observation
    // (spec §6), loop continues, second response is the final reply.
    let allowed = vec![ToolRef {
        extension_id: "http".into(),
        tool_name: "fetch".into(),
        description: None,
        input_schema: None,
        usage_note: None,
    }];
    let script = vec![
        Ok(tool_call("c1", "http", "fetch")),
        Ok(final_reply("recovered")),
    ];
    let (rt, _tel, tc) = build_runtime(script, cfg(4, 60_000, allowed, None), 0);
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
    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "recovered");
    // The failed tool call appears in the trail as a ToolCall with an error result.
    assert!(
        out.trail
            .iter()
            .any(|s| matches!(s, AgentStep::ToolCall { .. }))
    );
}

#[tokio::test]
async fn tool_call_trail_carries_args_and_duration() {
    // The trail records the model's tool arguments (input) and a wall-clock
    // duration alongside each tool call, so a caller (e.g. the demo trace) can
    // show WHAT was requested and how long it took — not just the tool name.
    let allowed = vec![ToolRef {
        extension_id: "http".into(),
        tool_name: "fetch".into(),
        description: None,
        input_schema: None,
        usage_note: None,
    }];
    let sent_args = serde_json::json!({ "url": "https://example.com", "method": "GET" });
    let script = vec![
        Ok(tool_call_with_args(
            "c1",
            "http",
            "fetch",
            sent_args.clone(),
        )),
        Ok(final_reply("done")),
    ];
    let (rt, _tel, tc) = build_runtime(script, cfg(4, 60_000, allowed, None), 0);
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
    let (args, _duration_ms) = out
        .trail
        .iter()
        .find_map(|s| match s {
            AgentStep::ToolCall {
                call_id,
                args,
                duration_ms,
                ..
            } if call_id == "c1" => Some((args.clone(), *duration_ms)),
            _ => None,
        })
        .expect("tool call c1 recorded in trail");
    assert_eq!(
        args, sent_args,
        "trail carries the model's tool args verbatim"
    );
}

#[tokio::test]
async fn token_budget_exceeded_returns_error() {
    // Daily cap 10, already-used 100 → gate trips before any LLM call.
    let (rt, _tel, tc) = build_runtime(
        vec![Ok(final_reply("hi"))],
        cfg(8, 60_000, vec![], Some(10)),
        100,
    );
    let err = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "x".into(),
                conversational: false,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, AgentError::TokenBudgetExceeded));
}

#[tokio::test]
async fn mixed_text_and_tool_calls_executes_tool_discards_text() {
    // First response: BOTH content and a tool_call (allowed). tool_calls win:
    // content is discarded, the tool dispatches (fails → observation), loop
    // continues. Second response: the real final reply.
    let allowed = vec![ToolRef {
        extension_id: "http".into(),
        tool_name: "fetch".into(),
        description: None,
        input_schema: None,
        usage_note: None,
    }];
    let mixed = LlmResponse {
        content: Some("internal reasoning".into()),
        tool_calls: vec![ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "http".into(),
            tool_name: "fetch".into(),
            args: serde_json::json!({}),
        }],
        tokens_in: 5,
        tokens_out: 5,
    };
    let script = vec![Ok(mixed), Ok(final_reply("the real answer"))];
    let (rt, _tel, tc) = build_runtime(script, cfg(4, 60_000, allowed, None), 0);
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
    assert_eq!(out.reply, "the real answer");
    assert_ne!(out.reply, "internal reasoning");
}

#[tokio::test]
async fn llm_provider_unavailable_after_retries_returns_error() {
    use greentic_aw_runtime::error::LlmError;
    use greentic_aw_runtime::llm::{LlmBackend, RetryingLlmBackend};

    // Scripted backend returns ServiceUnavailable 3 times; RetryingLlmBackend
    // (attempts=3, tiny backoff) exhausts all retries → loop returns
    // LlmProviderUnavailable.
    let inner = MockLlmBackend::new(vec![
        Err(LlmError::ServiceUnavailable),
        Err(LlmError::ServiceUnavailable),
        Err(LlmError::ServiceUnavailable),
    ]);
    let llm: Arc<dyn LlmBackend> =
        Arc::new(RetryingLlmBackend::new(inner, 3, Duration::from_millis(1)));

    let store = Arc::new(MockAgentStateStore::new());
    let telemetry = Arc::new(MockTelemetry::new());
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(&tc, "a", cfg(8, 60_000, vec![], None));
    let cp = Arc::new(cp);
    let token_meter = Arc::new(MockTokenMeter::new(0));
    let ledger = Arc::new(NoopToolLedger);
    let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let rt = AgentRuntime::new(cp, store, ext, llm, telemetry, token_meter, ledger, None);

    let err = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "x".into(),
                conversational: false,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, AgentError::LlmProviderUnavailable));
}

fn fact(text: &str) -> RecalledFact {
    RecalledFact {
        fact: text.into(),
        relation: "about".into(),
        valid_at: None,
        invalid_at: None,
        source_episode_ids: vec![],
    }
}

fn cfg_with_long_term(max_iter: u32) -> AgentConfig {
    let mut c = cfg(max_iter, 60_000, vec![], None);
    c.memory = Some(MemorySettings {
        short_term: None,
        long_term: Some(MemoryProviderRef {
            provider: "provider.memory.chronicle".into(),
            capability: "cap://memory/long-term".into(),
            params: serde_json::Map::new(),
            credential_ref: None,
        }),
    });
    c
}

/// Build a runtime with a long-term backend attached, returning the captured
/// LLM mock (for prompt assertions) and the tenant.
fn build_lt_runtime(
    llm_script: Vec<Result<LlmResponse, greentic_aw_runtime::error::LlmError>>,
    cfg_inner: AgentConfig,
    mem: Arc<MockLongTermMemory>,
) -> (AgentRuntime, Arc<MockLlmBackend>, TenantContext) {
    let llm = Arc::new(MockLlmBackend::new(llm_script));
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(&tc, "a", cfg_inner);
    let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let rt = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        ext,
        llm.clone(),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_long_term_memory(mem);
    (rt, llm, tc)
}

#[tokio::test]
async fn long_term_facts_are_injected_into_system_prompt() {
    let mem = Arc::new(MockLongTermMemory::new(vec![fact(
        "Alice prefers dark mode",
    )]));
    let (rt, llm, tc) = build_lt_runtime(vec![Ok(final_reply("hi"))], cfg_with_long_term(8), mem);
    rt.step(
        tc,
        "s",
        "a",
        AgentInput {
            text: "what do I like?".into(),
            conversational: false,
        },
    )
    .await
    .unwrap();
    let prompts = llm.seen_system_prompts.lock().unwrap();
    assert!(prompts[0].contains("<long_term_memory>"));
    assert!(prompts[0].contains("Alice prefers dark mode"));
}

#[tokio::test]
async fn no_injection_when_long_term_disabled() {
    // Provider attached but the agent has no long-term binding -> inactive.
    let mem = Arc::new(MockLongTermMemory::new(vec![fact("should not appear")]));
    let (rt, llm, tc) = build_lt_runtime(
        vec![Ok(final_reply("hi"))],
        cfg(8, 60_000, vec![], None),
        mem,
    );
    rt.step(
        tc,
        "s",
        "a",
        AgentInput {
            text: "hi".into(),
            conversational: false,
        },
    )
    .await
    .unwrap();
    let prompts = llm.seen_system_prompts.lock().unwrap();
    assert_eq!(prompts[0], "sys");
    assert!(!prompts[0].contains("<long_term_memory>"));
}

#[tokio::test]
async fn turn_is_ingested_as_episode_in_background() {
    let mem = Arc::new(MockLongTermMemory::new(vec![]));
    let (rt, _llm, tc) = build_lt_runtime(
        vec![Ok(final_reply("you like dark mode"))],
        cfg_with_long_term(8),
        mem.clone(),
    );
    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "what do I like?".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.reply, "you like dark mode");

    // Ingest is fire-and-forget; await it deterministically.
    mem.wait_for_ingests(1).await;
    let episodes = mem.ingested();
    assert_eq!(episodes.len(), 1);
    assert!(episodes[0].body.contains("what do I like?"));
    assert!(episodes[0].body.contains("you like dark mode"));
}

#[tokio::test]
async fn no_ingest_when_long_term_disabled() {
    let mem = Arc::new(MockLongTermMemory::new(vec![]));
    let (rt, _llm, tc) = build_lt_runtime(
        vec![Ok(final_reply("hi"))],
        cfg(8, 60_000, vec![], None),
        mem.clone(),
    );
    rt.step(
        tc,
        "s",
        "a",
        AgentInput {
            text: "hi".into(),
            conversational: false,
        },
    )
    .await
    .unwrap();
    // Give any erroneously-spawned task a chance to run, then assert none did.
    tokio::task::yield_now().await;
    assert!(mem.ingested().is_empty());
}

#[tokio::test]
async fn recall_memory_tool_advertised_when_active() {
    let mem = Arc::new(MockLongTermMemory::new(vec![]));
    let (rt, llm, tc) = build_lt_runtime(vec![Ok(final_reply("hi"))], cfg_with_long_term(8), mem);
    rt.step(
        tc,
        "s",
        "a",
        AgentInput {
            text: "hi".into(),
            conversational: false,
        },
    )
    .await
    .unwrap();
    let tools = llm.seen_tool_names.lock().unwrap();
    assert!(tools[0].iter().any(|n| n == "recall_memory"));
}

#[tokio::test]
async fn recall_memory_tool_absent_when_disabled() {
    let mem = Arc::new(MockLongTermMemory::new(vec![]));
    let (rt, llm, tc) = build_lt_runtime(
        vec![Ok(final_reply("hi"))],
        cfg(8, 60_000, vec![], None),
        mem,
    );
    rt.step(
        tc,
        "s",
        "a",
        AgentInput {
            text: "hi".into(),
            conversational: false,
        },
    )
    .await
    .unwrap();
    let tools = llm.seen_tool_names.lock().unwrap();
    assert!(!tools[0].iter().any(|n| n == "recall_memory"));
}

#[tokio::test]
async fn recall_memory_call_is_handled_host_side() {
    // First response: a recall_memory tool call (host built-in). Second: reply.
    let mem = Arc::new(MockLongTermMemory::new(vec![fact(
        "Alice prefers dark mode",
    )]));
    let (rt, _llm, tc) = build_lt_runtime(
        vec![
            Ok(tool_call("c1", "host", "recall_memory")),
            Ok(final_reply("you prefer dark mode")),
        ],
        cfg_with_long_term(8),
        mem,
    );
    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "what do I like?".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.reply, "you prefer dark mode");
    // The recall was handled host-side (not dispatched to the ext runtime, which
    // would have produced a NotFound error), so the tool result carries the fact.
    let result = out
        .trail
        .iter()
        .find_map(|s| match s {
            AgentStep::ToolCall { name, result, .. } if name == "recall_memory" => {
                Some(result.clone())
            }
            _ => None,
        })
        .expect("recall_memory tool call recorded in trail");
    assert!(result.to_string().contains("Alice prefers dark mode"));
}

// ---------------------------------------------------------------------------
// Short-term memory (working-memory) loop tests
// ---------------------------------------------------------------------------

/// Build a tool-call `LlmResponse` with explicit args (for `remember`/`recall`).
fn tool_call_with_args(
    call_id: &str,
    ext: &str,
    tool: &str,
    args: serde_json::Value,
) -> LlmResponse {
    LlmResponse {
        content: None,
        tool_calls: vec![ToolCallRecord {
            call_id: call_id.into(),
            extension_id: ext.into(),
            tool_name: tool.into(),
            args,
        }],
        tokens_in: 5,
        tokens_out: 5,
    }
}

/// Return an `AgentConfig` with `memory.short_term` set so `short_term_active()` is true.
fn cfg_with_short_term(max_iter: u32) -> AgentConfig {
    let mut c = cfg(max_iter, 60_000, vec![], None);
    c.memory = Some(MemorySettings {
        short_term: Some(MemoryProviderRef {
            provider: "in-memory".into(),
            capability: "cap://memory/short-term".into(),
            params: serde_json::Map::new(),
            credential_ref: None,
        }),
        long_term: None,
    });
    c
}

/// Build a runtime with the in-memory short-term provider attached.
fn build_st_runtime(
    llm_script: Vec<Result<LlmResponse, greentic_aw_runtime::error::LlmError>>,
    cfg_inner: AgentConfig,
) -> (AgentRuntime, Arc<MockLlmBackend>, TenantContext) {
    let llm = Arc::new(MockLlmBackend::new(llm_script));
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(&tc, "a", cfg_inner);
    let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let rt = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        ext,
        llm.clone(),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_short_term_memory(Arc::new(
        greentic_aw_runtime::memory::InMemoryMemoryProvider::new(),
    ));
    (rt, llm, tc)
}

#[tokio::test]
async fn remember_and_recall_tools_advertised_when_active() {
    // Config has memory.short_term set and the provider is attached → the LLM
    // request must include both "remember" and "recall" tool names.
    let (rt, llm, tc) = build_st_runtime(vec![Ok(final_reply("hi"))], cfg_with_short_term(8));
    rt.step(
        tc,
        "s",
        "a",
        AgentInput {
            text: "hi".into(),
            conversational: false,
        },
    )
    .await
    .unwrap();
    let tools = llm.seen_tool_names.lock().unwrap();
    assert!(
        tools[0].iter().any(|n| n == "remember"),
        "expected 'remember' in tool list: {:?}",
        tools[0]
    );
    assert!(
        tools[0].iter().any(|n| n == "recall"),
        "expected 'recall' in tool list: {:?}",
        tools[0]
    );
}

#[tokio::test]
async fn short_term_tools_absent_when_disabled() {
    // Provider attached but config has no memory.short_term binding → inactive;
    // neither "remember" nor "recall" should be advertised.
    let (rt, llm, tc) = build_st_runtime(
        vec![Ok(final_reply("hi"))],
        cfg(8, 60_000, vec![], None), // no memory binding
    );
    rt.step(
        tc,
        "s",
        "a",
        AgentInput {
            text: "hi".into(),
            conversational: false,
        },
    )
    .await
    .unwrap();
    let tools = llm.seen_tool_names.lock().unwrap();
    assert!(
        !tools[0].iter().any(|n| n == "remember"),
        "unexpected 'remember' in tool list: {:?}",
        tools[0]
    );
    assert!(
        !tools[0].iter().any(|n| n == "recall"),
        "unexpected 'recall' in tool list: {:?}",
        tools[0]
    );
}

#[tokio::test]
async fn remember_then_recall_roundtrips_via_tools() {
    // Turn 1: LLM emits `remember {key:"fav", value:"blue"}`.
    // Turn 2: LLM emits `recall {key:"fav"}`.
    // Turn 3: LLM gives a final reply.
    // Assert the recall tool result message content is `{"value":"blue"}`.
    let (rt, _llm, tc) = build_st_runtime(
        vec![
            Ok(tool_call_with_args(
                "c1",
                "host",
                "remember",
                serde_json::json!({ "key": "fav", "value": "blue" }),
            )),
            Ok(tool_call_with_args(
                "c2",
                "host",
                "recall",
                serde_json::json!({ "key": "fav" }),
            )),
            Ok(final_reply("your fav is blue")),
        ],
        cfg_with_short_term(8),
    );
    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "what is my fav color?".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.reply, "your fav is blue");
    // Find the recall result in the trail and confirm the stored value came back.
    let recall_result = out
        .trail
        .iter()
        .find_map(|s| match s {
            AgentStep::ToolCall { name, result, .. } if name == "recall" => Some(result.clone()),
            _ => None,
        })
        .expect("recall tool call recorded in trail");
    assert_eq!(
        recall_result.get("value").and_then(|v| v.as_str()),
        Some("blue"),
        "expected value 'blue' in recall result, got: {recall_result}"
    );
}

#[tokio::test]
async fn recall_missing_key_returns_null() {
    // LLM emits `recall {key:"absent"}` for a key that was never stored.
    // Expect the tool result to be `{"value": null}`.
    let (rt, _llm, tc) = build_st_runtime(
        vec![
            Ok(tool_call_with_args(
                "c1",
                "host",
                "recall",
                serde_json::json!({ "key": "absent" }),
            )),
            Ok(final_reply("nothing stored")),
        ],
        cfg_with_short_term(8),
    );
    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "do you know my name?".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.reply, "nothing stored");
    let recall_result = out
        .trail
        .iter()
        .find_map(|s| match s {
            AgentStep::ToolCall { name, result, .. } if name == "recall" => Some(result.clone()),
            _ => None,
        })
        .expect("recall tool call recorded in trail");
    assert!(
        recall_result.get("value").is_some_and(|v| v.is_null()),
        "expected null value for missing key, got: {recall_result}"
    );
}

#[tokio::test]
async fn conversational_agent_is_offered_end_conversation_tool() {
    // conversational = true → the LLM request must carry the `end_conversation`
    // tool and the system prompt must carry the accompanying note.
    let llm = Arc::new(MockLlmBackend::new(vec![Ok(final_reply("hi"))]));
    let mut c = cfg(4, 5_000, vec![], None);
    c.conversational = true;
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(&tc, "a", c);
    let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let rt = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        ext,
        llm.clone(),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    );
    let out = rt
        .step(
            tc,
            "sess-conv",
            "a",
            AgentInput {
                text: "hello".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.terminated_by, TerminationReason::FinalReply); // no tool called here
    let tools = llm.seen_tool_names.lock().unwrap();
    assert!(
        tools[0].iter().any(|t| t == "end_conversation"),
        "expected 'end_conversation' in tool list: {:?}",
        tools[0]
    );
    let prompts = llm.seen_system_prompts.lock().unwrap();
    assert!(
        prompts[0].contains("end_conversation"),
        "expected system prompt to mention 'end_conversation': {:?}",
        prompts[0]
    );
}

#[tokio::test]
async fn non_conversational_agent_has_no_end_conversation_tool() {
    // conversational defaults to false → neither the tool nor the note appear.
    let llm = Arc::new(MockLlmBackend::new(vec![Ok(final_reply("hi"))]));
    let c = cfg(4, 5_000, vec![], None);
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(&tc, "a", c);
    let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let rt = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        ext,
        llm.clone(),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    );
    let _ = rt
        .step(
            tc,
            "sess-conv",
            "a",
            AgentInput {
                text: "hello".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    let tools = llm.seen_tool_names.lock().unwrap();
    assert!(
        !tools[0].iter().any(|t| t == "end_conversation"),
        "unexpected 'end_conversation' in tool list: {:?}",
        tools[0]
    );
    let prompts = llm.seen_system_prompts.lock().unwrap();
    assert!(
        !prompts[0].contains("end_conversation"),
        "unexpected 'end_conversation' note in system prompt: {:?}",
        prompts[0]
    );
}

// ---------------------------------------------------------------------------
// `end_conversation` short-circuit termination (SP1, Task 5)
// ---------------------------------------------------------------------------

/// Build an `end_conversation` tool-call `LlmResponse`, mirroring `tool_call(...)`.
/// `final_message` becomes the tool's `args.final_message` (omitted when `None`);
/// `content` is the accompanying assistant text (the fallback source when
/// `final_message` is absent/empty).
fn end_conversation_call(
    call_id: &str,
    final_message: Option<&str>,
    content: Option<&str>,
) -> LlmResponse {
    let args = match final_message {
        Some(m) => serde_json::json!({ "final_message": m }),
        None => serde_json::json!({}),
    };
    LlmResponse {
        content: content.map(str::to_string),
        tool_calls: vec![ToolCallRecord {
            call_id: call_id.into(),
            extension_id: "host".into(),
            tool_name: "end_conversation".into(),
            args,
        }],
        tokens_in: 5,
        tokens_out: 5,
    }
}

#[tokio::test]
async fn end_conversation_terminates_with_final_message() {
    let mut c = cfg(4, 5_000, vec![], None);
    c.conversational = true;
    let (rt, _tel, tc) = build_runtime(
        vec![Ok(end_conversation_call("c1", Some("Goodbye!"), None))],
        c,
        0,
    );
    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "bye".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.terminated_by, TerminationReason::ConversationEnded);
    assert_eq!(out.reply, "Goodbye!");
}

#[tokio::test]
async fn end_conversation_falls_back_to_accompanying_content() {
    let mut c = cfg(4, 5_000, vec![], None);
    c.conversational = true;
    let (rt, _tel, tc) = build_runtime(
        vec![Ok(end_conversation_call("c1", None, Some("Take care.")))],
        c,
        0,
    );
    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "bye".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.terminated_by, TerminationReason::ConversationEnded);
    assert_eq!(out.reply, "Take care.");
}

#[tokio::test]
async fn end_conversation_empty_reply_when_neither_present() {
    let mut c = cfg(4, 5_000, vec![], None);
    c.conversational = true;
    let (rt, _tel, tc) = build_runtime(vec![Ok(end_conversation_call("c1", None, None))], c, 0);
    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "bye".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.terminated_by, TerminationReason::ConversationEnded);
    assert_eq!(out.reply, "");
}

#[tokio::test]
async fn end_conversation_ignored_when_not_conversational() {
    // Non-conversational agent: the tool is not intercepted. The model's call
    // is not in the (empty) allow-list, so it is blocked (not terminated); the
    // second scripted response is the real final reply.
    let script = vec![
        Ok(end_conversation_call("c1", Some("nope"), None)),
        Ok(final_reply("real reply")),
    ];
    let (rt, _tel, tc) = build_runtime(script, cfg(4, 5_000, vec![], None), 0); // conversational = false
    let out = rt
        .step(
            tc,
            "s",
            "a",
            AgentInput {
                text: "hi".into(),
                conversational: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "real reply");
}
