//! Shared context Phase C: the user ledger in the agent loop. A missing
//! ledger shows up as a MISSING block, never an error, so each test asserts
//! presence where allowed and absence where not.
//!
//! The kill switch (`GREENTIC_AW_USER_LEDGER`) is pinned in its own test
//! binary, `user_ledger_kill_switch.rs`, because it is process-global.

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::LlmError;
use greentic_aw_runtime::error::TerminationReason;
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::state::{AgentStateStore, ToolCallRecord};
use greentic_aw_runtime::tenant::{TenantContext, VerifiedCaller};
use greentic_aw_runtime::tool_call_frame::within;
use greentic_aw_runtime::user_ledger::{
    LedgerError, LedgerEvent, LedgerFuture, LedgerMode, MAX_IN_FLIGHT_APPENDS, UserLedger,
};
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, FlowInvokeOutcome, FlowInvoker,
    FlowOperation, FlowToolSource, LlmProviderRef, RunContext, RunTrace, ShareMode, ToolCallFrame,
    ToolRef, UserLedgerBinding,
};
use serde_json::{Value, json};

const TENANT: &str = "acme";

#[derive(Default)]
struct StubLedger {
    events: Vec<LedgerEvent>,
    fail: bool,
    hang: bool,
    hang_append: bool,
    /// Real-time delay before a read answers.
    read_delay: Option<Duration>,
    reads: Mutex<Vec<String>>,
    appends: Mutex<Vec<(String, String, String)>>,
}

impl UserLedger for StubLedger {
    fn read<'a>(&'a self, subject: &'a str, _limit: u32) -> LedgerFuture<'a, Vec<LedgerEvent>> {
        self.reads.lock().unwrap().push(subject.to_string());
        Box::pin(async move {
            if let Some(d) = self.read_delay {
                tokio::time::sleep(d).await;
            }
            if self.hang {
                std::future::pending::<()>().await;
            }
            if self.fail {
                return Err(LedgerError::Unavailable("stub".into()));
            }
            Ok(self.events.clone())
        })
    }
    fn append<'a>(
        &'a self,
        subject: &'a str,
        kind: &'a str,
        summary: &'a str,
    ) -> LedgerFuture<'a, ()> {
        self.appends
            .lock()
            .unwrap()
            .push((subject.into(), kind.into(), summary.into()));
        let hang = self.hang_append;
        Box::pin(async move {
            if hang {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
}

fn verified(sub: &str) -> TenantContext {
    TenantContext::new(TENANT, "prod").with_caller(Some(VerifiedCaller {
        user_verified: true,
        sub: Some(sub.into()),
        ..VerifiedCaller::default()
    }))
}

fn input(text: &str) -> AgentInput {
    AgentInput {
        text: text.into(),
        ..Default::default()
    }
}

fn agent(id: &str, tools: Vec<ToolRef>) -> AgentConfig {
    AgentConfig {
        agent_id: id.into(),
        system_prompt: format!("sys-{id}"),
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

fn reply(text: &str) -> Result<LlmResponse, LlmError> {
    Ok(LlmResponse {
        content: Some(text.into()),
        tool_calls: vec![],
        tokens_in: 1,
        tokens_out: 1,
    })
}

fn bare_runtime(a: AgentConfig, llm: Arc<MockLlmBackend>) -> AgentRuntime {
    bare_runtime_with_store(a, llm, Arc::new(MockAgentStateStore::new()))
}

fn bare_runtime_with_store(
    a: AgentConfig,
    llm: Arc<MockLlmBackend>,
    store: Arc<MockAgentStateStore>,
) -> AgentRuntime {
    let cp = MockConfigProvider::new();
    let id = a.agent_id.clone();
    cp.insert(&TenantContext::new(TENANT, "prod"), &id, a);
    AgentRuntime::new(
        Arc::new(cp),
        store,
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm,
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
}

fn runtime(
    a: AgentConfig,
    llm: Arc<MockLlmBackend>,
    ledger: Arc<StubLedger>,
    mode: LedgerMode,
) -> AgentRuntime {
    with_ledger(bare_runtime(a, llm), ledger, mode)
}

fn with_ledger(rt: AgentRuntime, ledger: Arc<StubLedger>, mode: LedgerMode) -> AgentRuntime {
    let mut agents = HashMap::new();
    agents.insert("helper".to_string(), mode);
    let dyn_ledger: Arc<dyn UserLedger> = ledger;
    rt.with_user_ledger(Some(Arc::new(UserLedgerBinding::new(
        TENANT, dyn_ledger, agents,
    ))))
}

fn history() -> Vec<LedgerEvent> {
    vec![LedgerEvent::new(
        "unit-1",
        "reply",
        "PREVIOUS-BOOKING",
        "2026-10-05T10:00:00Z",
    )]
}

async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
}

fn prompts(llm: &MockLlmBackend) -> Vec<String> {
    llm.seen_system_prompts.lock().unwrap().clone()
}

#[tokio::test]
async fn a_verified_callers_history_is_injected_and_read_mode_writes_nothing() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("hello again")]));
    let ledger = Arc::new(StubLedger {
        events: history(),
        ..Default::default()
    });
    let rt = runtime(
        agent("helper", vec![]),
        llm.clone(),
        ledger.clone(),
        LedgerMode::Read,
    );
    rt.step(verified("sub-1"), "s1", "helper", input("hi"))
        .await
        .unwrap();
    settle().await;
    let p = prompts(&llm);
    assert!(p[0].contains("<user_history>"));
    assert!(p[0].contains("PREVIOUS-BOOKING"));
    assert_eq!(*ledger.reads.lock().unwrap(), vec!["sub-1".to_string()]);
    assert!(ledger.appends.lock().unwrap().is_empty());
}

/// The binding the host installs is the one a step is checked against: a
/// verified caller of the binding's own tenant gets a turn (guards a silent
/// tenant mismatch between the runtime's steps and the binding).
#[tokio::test]
async fn a_wired_runtime_hands_out_a_turn_for_its_own_tenant() {
    let llm = Arc::new(MockLlmBackend::new(vec![]));
    let ledger = Arc::new(StubLedger::default());
    let rt = runtime(agent("helper", vec![]), llm, ledger, LedgerMode::ReadWrite);
    let binding = rt.user_ledger().expect("installed");
    assert!(binding.turn_for(&verified("sub-1"), "helper").is_some());
    assert!(
        bare_runtime(
            agent("helper", vec![]),
            Arc::new(MockLlmBackend::new(vec![]))
        )
        .user_ledger()
        .is_none(),
        "off by default"
    );
}

/// A read-write turn appends exactly the reply the user got, under the
/// verified subject, with the `reply` kind.
#[tokio::test]
async fn a_read_write_turn_appends_its_reply() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("table booked for two")]));
    let ledger = Arc::new(StubLedger {
        events: history(),
        ..Default::default()
    });
    let rt = runtime(
        agent("helper", vec![]),
        llm.clone(),
        ledger.clone(),
        LedgerMode::ReadWrite,
    );
    let out = rt
        .step(verified("sub-1"), "s1", "helper", input("book a table"))
        .await
        .unwrap();
    settle().await;
    assert_eq!(out.reply, "table booked for two");
    assert!(prompts(&llm)[0].contains("PREVIOUS-BOOKING"));
    assert_eq!(
        *ledger.appends.lock().unwrap(),
        vec![(
            "sub-1".to_string(),
            "reply".to_string(),
            "table booked for two".to_string()
        )]
    );
}

/// No binding means no block and the exact prompt the runtime built before
/// the ledger existed; an installed ledger with an empty history adds nothing
/// to the prompt either.
#[tokio::test]
async fn no_binding_or_no_history_leaves_the_prompt_unchanged() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("ok")]));
    let rt = bare_runtime(agent("helper", vec![]), llm.clone()).with_user_ledger(None);
    rt.step(verified("sub-1"), "s1", "helper", input("hi"))
        .await
        .unwrap();
    assert_eq!(prompts(&llm), vec!["sys-helper".to_string()]);

    let llm = Arc::new(MockLlmBackend::new(vec![reply("ok")]));
    let ledger = Arc::new(StubLedger::default());
    let rt = runtime(
        agent("helper", vec![]),
        llm.clone(),
        ledger.clone(),
        LedgerMode::Read,
    );
    rt.step(verified("sub-1"), "s1", "helper", input("hi"))
        .await
        .unwrap();
    assert_eq!(prompts(&llm), vec!["sys-helper".to_string()]);
    assert_eq!(ledger.reads.lock().unwrap().len(), 1, "read once per step");
}

/// A flow tool `x` whose result is secret.
struct SecretFlow;

impl FlowInvoker for SecretFlow {
    fn list_flows(&self) -> Vec<FlowOperation> {
        vec![FlowOperation {
            flow_ref: "x".into(),
            description: "x flow".into(),
            parameters: json!({ "type": "object" }),
        }]
    }
    fn invoke<'a>(
        &'a self,
        _f: &'a str,
        _a: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async { Ok(json!({ "secret": "SECRET-42" })) })
    }
    fn invoke_interactive<'a>(
        &'a self,
        _f: &'a str,
        _a: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async {
            Ok(FlowInvokeOutcome::Completed(
                json!({ "secret": "SECRET-42" }),
            ))
        })
    }
    fn resume<'a>(
        &'a self,
        _f: &'a str,
        _s: Value,
        _i: Value,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async { Err("no resume".to_string()) })
    }
}

#[tokio::test]
async fn only_the_guarded_reply_is_appended() {
    let llm = Arc::new(MockLlmBackend::new(vec![
        Ok(LlmResponse {
            content: None,
            tool_calls: vec![ToolCallRecord {
                call_id: "c1".into(),
                extension_id: "flow:x".into(),
                tool_name: "x".into(),
                args: json!({ "card": "4111-ARG" }),
            }],
            tokens_in: 1,
            tokens_out: 1,
        }),
        reply("your refund is on its way"),
    ]));
    let ledger = Arc::new(StubLedger::default());
    let tool = ToolRef {
        extension_id: "flow:x".into(),
        tool_name: "x".into(),
        description: None,
        input_schema: None,
        usage_note: None,
    };
    let rt = runtime(
        agent("helper", vec![tool]),
        llm,
        ledger.clone(),
        LedgerMode::ReadWrite,
    )
    .with_flow_source(Some(Arc::new(FlowToolSource::new(Arc::new(SecretFlow)))));
    rt.step(
        verified("sub-1"),
        "s1",
        "helper",
        input("my card is 4111-USER"),
    )
    .await
    .unwrap();
    settle().await;
    let appends = ledger.appends.lock().unwrap().clone();
    assert_eq!(
        appends,
        vec![(
            "sub-1".to_string(),
            "reply".to_string(),
            "your refund is on its way".to_string()
        )]
    );
    let all = format!("{appends:?}");
    for leaked in ["SECRET-42", "4111-ARG", "4111-USER"] {
        assert!(!all.contains(leaked), "{leaked} must not reach the ledger");
    }
    // Two LLM iterations, one read: the view is fetched once per step, not per
    // iteration.
    assert_eq!(
        ledger.reads.lock().unwrap().len(),
        1,
        "the ledger is read once for the whole step"
    );
}

#[tokio::test]
async fn an_unverified_or_anonymous_caller_never_touches_the_ledger() {
    let callers = vec![
        TenantContext::new(TENANT, "prod"),
        TenantContext::new(TENANT, "prod").with_caller(Some(VerifiedCaller {
            user_verified: false,
            sub: Some("self-declared".into()),
            ..VerifiedCaller::default()
        })),
        TenantContext::new(TENANT, "prod").with_caller(Some(VerifiedCaller {
            user_verified: true,
            sub: None,
            ..VerifiedCaller::default()
        })),
    ];
    for tenant in callers {
        let llm = Arc::new(MockLlmBackend::new(vec![reply("ok")]));
        let ledger = Arc::new(StubLedger {
            events: history(),
            ..Default::default()
        });
        let rt = runtime(
            agent("helper", vec![]),
            llm.clone(),
            ledger.clone(),
            LedgerMode::ReadWrite,
        );
        rt.step(tenant, "s1", "helper", input("hi")).await.unwrap();
        settle().await;
        assert!(!prompts(&llm)[0].contains("<user_history>"));
        assert!(ledger.reads.lock().unwrap().is_empty());
        assert!(ledger.appends.lock().unwrap().is_empty());
    }
}

/// The binding names only `helper`; another agent on the same runtime is off.
/// (Tenant mismatch is pinned at unit level in `user_ledger::tests`, because a
/// foreign tenant's step fails its config lookup before the ledger is asked.)
#[tokio::test]
async fn an_agent_the_pack_did_not_name_is_off() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("ok")]));
    let ledger = Arc::new(StubLedger {
        events: history(),
        ..Default::default()
    });
    let rt = runtime(
        agent("other", vec![]),
        llm.clone(),
        ledger.clone(),
        LedgerMode::ReadWrite,
    );
    rt.step(verified("sub-1"), "s1", "other", input("hi"))
        .await
        .unwrap();
    settle().await;
    assert!(!prompts(&llm)[0].contains("<user_history>"));
    assert!(ledger.reads.lock().unwrap().is_empty());
    assert!(ledger.appends.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_failing_ledger_read_does_not_fail_the_turn() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("still answering")]));
    let ledger = Arc::new(StubLedger {
        fail: true,
        ..Default::default()
    });
    let rt = runtime(
        agent("helper", vec![]),
        llm.clone(),
        ledger,
        LedgerMode::Read,
    );
    let out = rt
        .step(verified("sub-1"), "s1", "helper", input("hi"))
        .await
        .unwrap();
    assert_eq!(out.reply, "still answering");
    assert!(!prompts(&llm)[0].contains("<user_history>"));
}

#[tokio::test(start_paused = true)]
async fn a_hanging_ledger_read_costs_at_most_the_read_budget() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("answered")]));
    let ledger = Arc::new(StubLedger {
        hang: true,
        ..Default::default()
    });
    let rt = runtime(
        agent("helper", vec![]),
        llm.clone(),
        ledger,
        LedgerMode::Read,
    );
    let started = tokio::time::Instant::now();
    let out = rt
        .step(verified("sub-1"), "s1", "helper", input("hi"))
        .await
        .unwrap();
    assert_eq!(out.reply, "answered");
    assert!(
        started.elapsed()
            <= greentic_aw_runtime::user_ledger::READ_TIMEOUT + Duration::from_millis(100)
    );
    assert!(!prompts(&llm)[0].contains("<user_history>"));
}

/// Appends whose door never answers hold their in-flight permit; past the cap
/// a turn's append is dropped instead of queueing work without bound. Every
/// turn still answers.
#[tokio::test]
async fn appends_past_the_in_flight_cap_are_dropped() {
    let turns = MAX_IN_FLIGHT_APPENDS + 4;
    let llm = Arc::new(MockLlmBackend::new(
        (0..turns).map(|i| reply(&format!("reply {i}"))).collect(),
    ));
    let ledger = Arc::new(StubLedger {
        hang_append: true,
        ..Default::default()
    });
    let rt = runtime(
        agent("helper", vec![]),
        llm,
        ledger.clone(),
        LedgerMode::ReadWrite,
    );
    for i in 0..turns {
        let out = rt
            .step(verified("sub-1"), &format!("s{i}"), "helper", input("hi"))
            .await
            .unwrap();
        assert_eq!(out.reply, format!("reply {i}"));
        settle().await;
    }
    assert_eq!(
        ledger.appends.lock().unwrap().len(),
        MAX_IN_FLIGHT_APPENDS,
        "only the first {MAX_IN_FLIGHT_APPENDS} appends were started"
    );
}

/// Blocks go in a fixed order: the agent's own prompt, then the user history,
/// then (per iteration) the run context, which stays last.
#[tokio::test]
async fn the_user_history_sits_between_the_prompt_and_the_run_context() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("ok")]));
    let ledger = Arc::new(StubLedger {
        events: history(),
        ..Default::default()
    });
    let rt = runtime(
        agent("helper", vec![]),
        llm.clone(),
        ledger,
        LedgerMode::Read,
    );
    let trace = Arc::new(RunTrace::new());
    trace.append("host", "reply", "SEED-EVENT");
    RunContext::scope(
        RunContext::new(TENANT, trace).with_mode(ShareMode::ReadWrite),
        rt.step(verified("sub-1"), "s1", "helper", input("hi")),
    )
    .await
    .unwrap();
    let p = &prompts(&llm)[0];
    let base = p.find("sys-helper").expect("base prompt");
    let history = p.find("<user_history>").expect("user history");
    let run = p.find("<run_context>").expect("run context");
    assert!(base < history && history < run, "{p}");
}

/// The ledger view is prompt text for this step only: it never becomes an
/// event of the run trace (#828), so a nested agent cannot see it through the
/// run context either. Only the reply record is added.
#[tokio::test]
async fn the_user_history_never_enters_the_run_trace() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("all set")]));
    let ledger = Arc::new(StubLedger {
        events: history(),
        ..Default::default()
    });
    let rt = runtime(
        agent("helper", vec![]),
        llm.clone(),
        ledger,
        LedgerMode::ReadWrite,
    );
    let trace = Arc::new(RunTrace::new());
    RunContext::scope(
        RunContext::new(TENANT, trace.clone()).with_mode(ShareMode::ReadWrite),
        rt.step(verified("sub-1"), "s1", "helper", input("hi")),
    )
    .await
    .unwrap();
    assert!(prompts(&llm)[0].contains("PREVIOUS-BOOKING"), "control");
    let events = trace.events();
    assert_eq!(events.len(), 1, "only the reply record: {events:?}");
    assert_eq!(events[0].kind, "reply");
    assert_eq!(events[0].summary, "all set");
    for e in &events {
        assert!(!e.summary.contains("PREVIOUS-BOOKING"));
        assert!(!e.summary.contains("user_history"));
    }
}

/// #829 lends the same runtime (and binding) to the nested flow engine of a
/// `flow:` tool, inside a tool call frame, with the outer verified caller
/// pinned. A nested agent named in the sidecar must still not READ the
/// history: that would bypass the binding's share mode (spec 4.4).
#[tokio::test]
async fn a_nested_agent_inside_a_flow_tool_frame_never_reads_the_ledger() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("nested answer")]));
    let ledger = Arc::new(StubLedger {
        events: history(),
        ..Default::default()
    });
    let rt = runtime(
        agent("helper", vec![]),
        llm.clone(),
        ledger.clone(),
        LedgerMode::ReadWrite,
    );
    let frame = ToolCallFrame::new(Some("outer-s1"), "call-1");
    within(
        frame,
        rt.step(verified("sub-1"), "nested-s1", "helper", input("hi")),
    )
    .await
    .unwrap();
    settle().await;
    assert!(!prompts(&llm)[0].contains("<user_history>"));
    assert!(!prompts(&llm)[0].contains("PREVIOUS-BOOKING"));
    assert!(
        ledger.reads.lock().unwrap().is_empty(),
        "no ledger read from a nested agent"
    );
}

/// ... and must not APPEND: its reply is, to the outer agent, a tool result,
/// and only the outer agent's guarded reply may cross (spec 4.1).
#[tokio::test]
async fn a_nested_agent_inside_a_flow_tool_frame_never_appends_to_the_ledger() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply(
        "NESTED-REPLY-AS-TOOL-RESULT",
    )]));
    let ledger = Arc::new(StubLedger::default());
    let rt = runtime(
        agent("helper", vec![]),
        llm,
        ledger.clone(),
        LedgerMode::ReadWrite,
    );
    let frame = ToolCallFrame::new(Some("outer-s1"), "call-1");
    within(
        frame,
        rt.step(verified("sub-1"), "nested-s1", "helper", input("hi")),
    )
    .await
    .unwrap();
    settle().await;
    assert!(
        ledger.appends.lock().unwrap().is_empty(),
        "no ledger append from a nested agent"
    );
}

/// The view is prompt text for this step only: it never lands in the persisted
/// conversation history, so it is not replayed (or re-stored) on later turns.
#[tokio::test]
async fn the_user_history_never_lands_in_the_saved_conversation() {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("noted")]));
    let ledger = Arc::new(StubLedger {
        events: history(),
        ..Default::default()
    });
    let store = Arc::new(MockAgentStateStore::new());
    let rt = with_ledger(
        bare_runtime_with_store(agent("helper", vec![]), llm.clone(), store.clone()),
        ledger,
        LedgerMode::ReadWrite,
    );
    rt.step(verified("sub-1"), "s1", "helper", input("hi"))
        .await
        .unwrap();
    assert!(prompts(&llm)[0].contains("PREVIOUS-BOOKING"), "control");
    let state = store.load(&verified("sub-1"), "s1").await.unwrap();
    assert!(!state.messages.is_empty(), "control: the turn was saved");
    let saved = format!("{:?}", state.messages);
    assert!(!saved.contains("PREVIOUS-BOOKING"), "{saved}");
    assert!(!saved.contains("user_history"), "{saved}");
}

/// A flow tool that parks on a card first, then completes on resume.
struct ParkingFlow;

impl FlowInvoker for ParkingFlow {
    fn list_flows(&self) -> Vec<FlowOperation> {
        vec![FlowOperation {
            flow_ref: "form".into(),
            description: "form flow".into(),
            parameters: json!({ "type": "object" }),
        }]
    }
    fn invoke<'a>(
        &'a self,
        _f: &'a str,
        _a: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async { Err("interactive only".to_string()) })
    }
    fn invoke_interactive<'a>(
        &'a self,
        _f: &'a str,
        _a: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async {
            Ok(FlowInvokeOutcome::Waiting {
                snapshot: json!({ "snap": "A" }),
                presentation: json!({ "card": "A" }),
            })
        })
    }
    fn resume<'a>(
        &'a self,
        _f: &'a str,
        _s: Value,
        _i: Value,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async { Ok(FlowInvokeOutcome::Completed(json!({ "room": "101" }))) })
    }
}

/// A turn that parks on a card records nothing (its text never passed the
/// outbound chain); the resumed turn's final reply is recorded exactly once.
#[tokio::test]
async fn a_parked_then_resumed_turn_appends_once_on_the_final_reply() {
    let llm = Arc::new(MockLlmBackend::new(vec![
        Ok(LlmResponse {
            content: Some("PARK-TEXT let me ask you".into()),
            tool_calls: vec![ToolCallRecord {
                call_id: "c1".into(),
                extension_id: "flow:form".into(),
                tool_name: "form".into(),
                args: json!({}),
            }],
            tokens_in: 1,
            tokens_out: 1,
        }),
        reply("room 101 booked"),
    ]));
    let ledger = Arc::new(StubLedger::default());
    let tool = ToolRef {
        extension_id: "flow:form".into(),
        tool_name: "form".into(),
        description: None,
        input_schema: None,
        usage_note: None,
    };
    let store = Arc::new(MockAgentStateStore::new());
    let rt = with_ledger(
        bare_runtime_with_store(agent("helper", vec![tool]), llm, store),
        ledger.clone(),
        LedgerMode::ReadWrite,
    )
    .with_flow_source(Some(Arc::new(FlowToolSource::new(Arc::new(ParkingFlow)))));

    let parked = rt
        .step(verified("sub-1"), "s1", "helper", input("book a room"))
        .await
        .unwrap();
    settle().await;
    assert_eq!(parked.terminated_by, TerminationReason::AwaitingToolInput);
    assert!(
        ledger.appends.lock().unwrap().is_empty(),
        "a parked turn appends nothing"
    );

    let resumed = rt
        .step(
            verified("sub-1"),
            "s1",
            "helper",
            AgentInput {
                text: String::new(),
                conversational: false,
                resume_payload: Some(json!({ "metadata": { "action": "submit" } })),
            },
        )
        .await
        .unwrap();
    settle().await;
    assert_eq!(resumed.terminated_by, TerminationReason::FinalReply);
    assert_eq!(
        *ledger.appends.lock().unwrap(),
        vec![(
            "sub-1".to_string(),
            "reply".to_string(),
            "room 101 booked".to_string()
        )],
        "exactly one append, the resumed final reply"
    );
    assert_eq!(ledger.reads.lock().unwrap().len(), 2, "one read per step");
}

/// The ledger read runs alongside the tool-catalog resolution, so a slow
/// catalog (1 s, a delayed admin) and a slow read (1.2 s) cost about the
/// slower of the two, not their sum (2.2 s). Real time: every catalog source
/// is either synchronous or real HTTP, and paused time over a real socket
/// auto-advances into the client timeouts.
#[tokio::test]
async fn the_read_overlaps_the_catalog_resolution() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let admin = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "servers": [] }))
                .set_delay(Duration::from_millis(1000)),
        )
        .expect(1)
        .mount(&admin)
        .await;
    let mcp = Arc::new(greentic_aw_runtime::McpToolSource::new(
        admin.uri(),
        "gtc_live_test",
    ));
    let cp = MockConfigProvider::new();
    cp.insert(
        &TenantContext::new(TENANT, "prod"),
        "helper",
        agent("helper", vec![]),
    );
    let llm = Arc::new(MockLlmBackend::new(vec![reply("ok")]));
    let ledger = Arc::new(StubLedger {
        events: history(),
        read_delay: Some(Duration::from_millis(1200)),
        ..Default::default()
    });
    let rt = with_ledger(
        AgentRuntime::new(
            Arc::new(cp),
            Arc::new(MockAgentStateStore::new()),
            Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
            llm.clone(),
            Arc::new(MockTelemetry::new()),
            Arc::new(MockTokenMeter::new(0)),
            Arc::new(NoopToolLedger),
            Some(mcp),
        ),
        ledger,
        LedgerMode::Read,
    );
    let started = std::time::Instant::now();
    rt.step(verified("sub-1"), "s1", "helper", input("hi"))
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        prompts(&llm)[0].contains("PREVIOUS-BOOKING"),
        "control: the slow read still landed"
    );
    assert!(
        elapsed >= Duration::from_millis(1200),
        "control: the read was waited for: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(2000),
        "read and catalog must overlap (sum would be 2.2 s): {elapsed:?}"
    );
}
