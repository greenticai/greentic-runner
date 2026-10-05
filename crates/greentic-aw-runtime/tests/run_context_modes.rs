//! Shared context, Phase A2: what a nested agent sees of its caller's run,
//! per binding mode, and what crosses an agent boundary.
//!
//! A lost context shows up as a MISSING block, never as an error, so every
//! test asserts both presence (mode allows) and absence (mode forbids).

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::LlmError;
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::state::ToolCallRecord;
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, BindingModes, FlowInvokeOutcome,
    FlowInvoker, FlowOperation, FlowToolSource, LlmProviderRef, RunContext, RunTrace, ShareMode,
    SharePolicy, ToolRef,
};
use serde_json::{Value, json};

const TENANT: &str = "acme";
const ENV: &str = "prod";
const SEED: &str = "SEED-EVENT";

fn tc() -> TenantContext {
    TenantContext::new(TENANT, ENV)
}

fn input() -> AgentInput {
    AgentInput {
        text: "go".into(),
        ..Default::default()
    }
}

fn tool(ext: &str, name: &str) -> ToolRef {
    ToolRef {
        extension_id: ext.into(),
        tool_name: name.into(),
        description: None,
        input_schema: None,
        usage_note: None,
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

fn call(ext: &str, name: &str) -> Result<LlmResponse, LlmError> {
    Ok(LlmResponse {
        content: None,
        tool_calls: vec![ToolCallRecord {
            call_id: format!("c-{ext}"),
            extension_id: ext.into(),
            tool_name: name.into(),
            args: json!({}),
        }],
        tokens_in: 1,
        tokens_out: 1,
    })
}

fn runtime(agents: Vec<AgentConfig>, llm: Arc<MockLlmBackend>) -> AgentRuntime {
    let cp = MockConfigProvider::new();
    for a in agents {
        let id = a.agent_id.clone();
        cp.insert(&tc(), &id, a);
    }
    AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm,
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
}

fn seeded() -> Arc<RunTrace> {
    let t = Arc::new(RunTrace::new());
    t.append("host", "reply", SEED);
    t
}

fn policy(agent: &str, binding: &str, mode: ShareMode) -> Option<Arc<SharePolicy>> {
    let mut modes = BindingModes::new();
    modes.insert(binding.to_string(), mode);
    let mut agents = HashMap::new();
    agents.insert(agent.to_string(), modes);
    Some(Arc::new(SharePolicy::new(agents)))
}

fn prompts(llm: &MockLlmBackend) -> Vec<String> {
    llm.seen_system_prompts.lock().unwrap().clone()
}

/// A flow tool `x` that completes with a fixed value.
struct FixedFlow(Value);

impl FlowInvoker for FixedFlow {
    fn list_flows(&self) -> Vec<FlowOperation> {
        vec![FlowOperation {
            flow_ref: "x".into(),
            description: "x flow".into(),
            parameters: json!({ "type": "object" }),
        }]
    }
    fn invoke<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        let v = self.0.clone();
        Box::pin(async move { Ok(v) })
    }
    fn invoke_interactive<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        let v = self.0.clone();
        Box::pin(async move { Ok(FlowInvokeOutcome::Completed(v)) })
    }
    fn resume<'a>(
        &'a self,
        _flow_ref: &'a str,
        _snapshot: Value,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async { Err("no resume".to_string()) })
    }
}

/// Two agents in one run: `inner` calls a tool whose result is secret, then
/// `outer` takes a turn. `outer` must see the tool's name and outcome and
/// `inner`'s reply, never the result; `inner` itself sees its own result.
#[tokio::test]
async fn another_agent_sees_a_tool_outcome_but_not_its_result() {
    let llm = Arc::new(MockLlmBackend::new(vec![
        call("flow:x", "x"),
        reply("inner done"),
        reply("outer done"),
    ]));
    let rt = runtime(
        vec![
            agent("inner", vec![tool("flow:x", "x")]),
            agent("outer", vec![]),
        ],
        llm.clone(),
    )
    .with_flow_source(Some(Arc::new(FlowToolSource::new(Arc::new(FixedFlow(
        json!({ "secret": "SECRET-42" }),
    ))))));
    let trace = Arc::new(RunTrace::new());
    RunContext::scope(RunContext::new(TENANT, trace.clone()), async {
        rt.step(tc(), "s-inner", "inner", input()).await.unwrap();
        rt.step(tc(), "s-outer", "outer", input()).await.unwrap();
    })
    .await;
    let p = prompts(&llm);
    assert!(
        p[1].contains("SECRET-42"),
        "the caller sees its own result: {}",
        p[1]
    );
    assert!(
        !p[2].contains("SECRET-42"),
        "another agent must not: {}",
        p[2]
    );
    assert!(p[2].contains("x ok"), "{}", p[2]);
    assert!(p[2].contains("inner done"), "{}", p[2]);
}

use std::sync::atomic::{AtomicBool, Ordering};

/// Flow `x` whose body is an agent turn (`leaf`), run inline the way the flow
/// engine runs a `dw.agent` node (every hop an `.await`). With `park_first`
/// the first interactive call parks on a card and the resume runs the agent.
struct LeafFlow {
    leaf: Arc<AgentRuntime>,
    park_first: AtomicBool,
}

impl LeafFlow {
    async fn run_leaf(&self) -> Result<Value, String> {
        let out = self
            .leaf
            .step(tc(), "s-leaf", "leaf", input())
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "reply": out.reply }))
    }
}

impl FlowInvoker for LeafFlow {
    fn list_flows(&self) -> Vec<FlowOperation> {
        vec![FlowOperation {
            flow_ref: "x".into(),
            description: "x flow".into(),
            parameters: json!({ "type": "object" }),
        }]
    }
    fn invoke<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(self.run_leaf())
    }
    fn invoke_interactive<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async move {
            if self.park_first.swap(false, Ordering::SeqCst) {
                return Ok(FlowInvokeOutcome::Waiting {
                    snapshot: json!({ "s": 1 }),
                    presentation: json!({ "card": 1 }),
                });
            }
            self.run_leaf().await.map(FlowInvokeOutcome::Completed)
        })
    }
    fn resume<'a>(
        &'a self,
        _flow_ref: &'a str,
        _snapshot: Value,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async move { self.run_leaf().await.map(FlowInvokeOutcome::Completed) })
    }
}

fn leaf_runtime() -> (Arc<AgentRuntime>, Arc<MockLlmBackend>) {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("leaf done")]));
    (
        Arc::new(runtime(vec![agent("leaf", vec![])], llm.clone())),
        llm,
    )
}

/// Like `runtime`, but hands back the state store so a test can edit a parked
/// conversation.
fn runtime_with_store(
    agents: Vec<AgentConfig>,
    llm: Arc<MockLlmBackend>,
) -> (AgentRuntime, Arc<MockAgentStateStore>) {
    let store = Arc::new(MockAgentStateStore::new());
    let cp = MockConfigProvider::new();
    for a in agents {
        let id = a.agent_id.clone();
        cp.insert(&tc(), &id, a);
    }
    let rt = AgentRuntime::new(
        Arc::new(cp),
        store.clone(),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm,
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    );
    (rt, store)
}

fn outer_with_flow(
    mode: Option<ShareMode>,
    leaf: Arc<AgentRuntime>,
    park_first: bool,
    outer_llm: Vec<Result<LlmResponse, LlmError>>,
) -> (AgentRuntime, Arc<MockLlmBackend>, Arc<MockAgentStateStore>) {
    let llm = Arc::new(MockLlmBackend::new(outer_llm));
    let flows = FlowToolSource::new(Arc::new(LeafFlow {
        leaf,
        park_first: AtomicBool::new(park_first),
    }));
    let (rt, store) =
        runtime_with_store(vec![agent("outer", vec![tool("flow:x", "x")])], llm.clone());
    let rt = rt
        .with_flow_source(Some(Arc::new(flows)))
        .with_share_policy(mode.and_then(|m| policy("outer", "flow:x", m)));
    (rt, llm, store)
}

/// Entry point 1: a new `flow:` call from the agent loop.
async fn loop_call(mode: Option<ShareMode>) -> (Vec<String>, Arc<RunTrace>) {
    let (leaf, leaf_llm) = leaf_runtime();
    let (outer, _, _) = outer_with_flow(
        mode,
        leaf,
        false,
        vec![call("flow:x", "x"), reply("outer done")],
    );
    let trace = seeded();
    RunContext::scope(
        RunContext::new(TENANT, trace.clone()),
        outer.step(tc(), "s-outer", "outer", input()),
    )
    .await
    .unwrap();
    (prompts(&leaf_llm), trace)
}

fn has_leaf_event(trace: &RunTrace) -> bool {
    trace.events().iter().any(|e| e.actor == "leaf")
}

#[tokio::test]
async fn a_loop_flow_call_shares_per_its_binding_mode() {
    let (p, t) = loop_call(Some(ShareMode::ReadWrite)).await;
    assert!(p[0].contains(SEED), "read_write: view present: {}", p[0]);
    assert!(has_leaf_event(&t), "read_write: the leaf records its reply");

    let (p, t) = loop_call(Some(ShareMode::Read)).await;
    assert!(p[0].contains(SEED), "read: view present: {}", p[0]);
    assert!(!has_leaf_event(&t), "read: the leaf records nothing");

    for mode in [Some(ShareMode::None), None] {
        let (p, t) = loop_call(mode).await;
        assert_eq!(p[0], "sys-leaf", "{mode:?}: no view at all");
        assert!(!has_leaf_event(&t), "{mode:?}: nothing recorded");
    }
}

/// `run_step` must install THIS agent's policy as the caller policy: two
/// agents in one runtime and one run, with different modes for the same
/// `flow:x`, must each get their own.
#[tokio::test]
async fn each_agent_step_applies_its_own_binding_modes() {
    let (leaf, leaf_llm) = {
        let llm = Arc::new(MockLlmBackend::new(vec![reply("l1"), reply("l2")]));
        (
            Arc::new(runtime(vec![agent("leaf", vec![])], llm.clone())),
            llm,
        )
    };
    let llm = Arc::new(MockLlmBackend::new(vec![
        call("flow:x", "x"),
        reply("a done"),
        call("flow:x", "x"),
        reply("b done"),
    ]));
    let (rt, _store) = runtime_with_store(
        vec![
            agent("a", vec![tool("flow:x", "x")]),
            agent("b", vec![tool("flow:x", "x")]),
        ],
        llm,
    );
    let mut agents = HashMap::new();
    for (id, mode) in [("a", ShareMode::Read), ("b", ShareMode::None)] {
        let mut modes = BindingModes::new();
        modes.insert("flow:x".to_string(), mode);
        agents.insert(id.to_string(), modes);
    }
    let flows = FlowToolSource::new(Arc::new(LeafFlow {
        leaf,
        park_first: AtomicBool::new(false),
    }));
    let rt = rt
        .with_flow_source(Some(Arc::new(flows)))
        .with_share_policy(Some(Arc::new(SharePolicy::new(agents))));
    RunContext::scope(RunContext::new(TENANT, seeded()), async {
        rt.step(tc(), "s-a", "a", input()).await.unwrap();
        rt.step(tc(), "s-b", "b", input()).await.unwrap();
    })
    .await;
    let p = prompts(&leaf_llm);
    assert!(
        p[0].contains(SEED),
        "agent a (read): view present: {}",
        p[0]
    );
    assert_eq!(p[1], "sys-leaf", "agent b (none): no view");
}

/// Entry point 2: the resume of a parked flow tool (a new step resumes it).
async fn resumed_call(mode: Option<ShareMode>, legacy_pending: bool) -> Vec<String> {
    let (leaf, leaf_llm) = leaf_runtime();
    let (outer, _, store) = outer_with_flow(
        mode,
        leaf,
        true,
        vec![call("flow:x", "x"), reply("outer done")],
    );
    let trace = seeded();
    let ctx = RunContext::new(TENANT, trace.clone());
    RunContext::scope(ctx.clone(), outer.step(tc(), "s-park", "outer", input()))
        .await
        .unwrap();
    if legacy_pending {
        // A call parked by a runtime that predates `extension_id`.
        use greentic_aw_runtime::state::AgentStateStore;
        let mut state = store.load(&tc(), "s-park").await.unwrap();
        let pending = state.pending_tool.as_mut().expect("the first step parked");
        pending.extension_id = String::new();
        store.save(&tc(), "s-park", &state).await.unwrap();
    }
    let resume = AgentInput {
        text: String::new(),
        resume_payload: Some(json!({ "answer": 1 })),
        ..Default::default()
    };
    RunContext::scope(ctx, outer.step(tc(), "s-park", "outer", resume))
        .await
        .unwrap();
    prompts(&leaf_llm)
}

#[tokio::test]
async fn a_resumed_flow_call_shares_per_its_binding_mode() {
    let p = resumed_call(Some(ShareMode::Read), false).await;
    assert!(
        p[0].contains(SEED),
        "read: view present on resume: {}",
        p[0]
    );
    let p = resumed_call(None, false).await;
    assert_eq!(p[0], "sys-leaf", "no policy: no view on resume");
}

#[tokio::test]
async fn a_call_parked_without_an_extension_id_falls_back_to_the_flow_ref() {
    let p = resumed_call(Some(ShareMode::Read), true).await;
    assert!(
        p[0].contains(SEED),
        "flow:<flow_ref> is the binding: {}",
        p[0]
    );
}

/// Entry point 3: the `flow:` arm `ToolSession` callers reach.
async fn session_call(policy_mode: Option<ShareMode>) -> Vec<String> {
    let (leaf, leaf_llm) = leaf_runtime();
    let (outer, _, _) = outer_with_flow(None, leaf, false, vec![]);
    let session = outer
        .tool_session(&tc(), &[tool("flow:x", "x")], &agent("outer", vec![]).llm)
        .await;
    let caller = policy_mode.map(|m| {
        let mut modes = BindingModes::new();
        modes.insert("flow:x".to_string(), m);
        Arc::new(modes)
    });
    RunContext::scope(
        RunContext::new(TENANT, seeded()).with_caller_policy(caller),
        session.call(
            &greentic_aw_runtime::wire_tool_name("flow:x", "x"),
            json!({}),
        ),
    )
    .await
    .unwrap();
    prompts(&leaf_llm)
}

#[tokio::test]
async fn a_tool_session_flow_call_shares_per_the_context_policy() {
    let p = session_call(Some(ShareMode::Read)).await;
    assert!(p[0].contains(SEED), "{}", p[0]);
    let p = session_call(None).await;
    assert_eq!(p[0], "sys-leaf", "no caller policy fails closed");
}
