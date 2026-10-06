//! The calling agent holds its session lock (TTL 90 s) across a `flow:` tool
//! call, and the iteration-top refresh does not run while that call is
//! pending. A slow nested flow (a nested agent turn) must not let the lock
//! lapse: the loop refreshes it on an interval while the call and the resume
//! of a parked call are pending.
//!
//! Time is paused, so the 100 s flow below costs nothing and the 30 s
//! keep-alive interval is the production one.

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::state::ToolCallRecord;
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, FlowInvokeOutcome, FlowInvoker,
    FlowOperation, FlowToolSource, LlmProviderRef, ToolRef,
};
use serde_json::{Value, json};

const SLOW: Duration = Duration::from_secs(100);

/// A flow whose call (or, with `park_first`, whose resume) takes `SLOW`, and
/// which records how many lock refreshes happened while it was pending.
struct SlowFlow {
    store: Arc<MockAgentStateStore>,
    park_first: bool,
    refreshes_while_pending: Mutex<Vec<usize>>,
}

impl SlowFlow {
    async fn slow(&self) {
        let before = self.store.lock_refreshes();
        tokio::time::sleep(SLOW).await;
        let after = self.store.lock_refreshes();
        self.refreshes_while_pending
            .lock()
            .unwrap()
            .push(after - before);
    }
}

impl FlowInvoker for SlowFlow {
    fn list_flows(&self) -> Vec<FlowOperation> {
        vec![FlowOperation {
            flow_ref: "slow".into(),
            description: "slow".into(),
            parameters: json!({ "type": "object" }),
        }]
    }

    fn invoke<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Ok(json!({})) })
    }

    fn invoke_interactive<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async move {
            if self.park_first {
                return Ok(FlowInvokeOutcome::Waiting {
                    snapshot: json!({ "snap": 1 }),
                    presentation: json!({ "card": 1 }),
                });
            }
            self.slow().await;
            Ok(FlowInvokeOutcome::Completed(json!({ "done": true })))
        })
    }

    fn resume<'a>(
        &'a self,
        _flow_ref: &'a str,
        _snapshot: Value,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
        Box::pin(async move {
            self.slow().await;
            Ok(FlowInvokeOutcome::Completed(json!({ "done": true })))
        })
    }
}

fn script() -> Vec<LlmResponse> {
    vec![
        LlmResponse {
            content: None,
            tool_calls: vec![ToolCallRecord {
                call_id: "c1".into(),
                extension_id: "flow:slow".into(),
                tool_name: "slow".into(),
                args: json!({}),
            }],
            tokens_in: 1,
            tokens_out: 1,
        },
        LlmResponse {
            content: Some("done".into()),
            tool_calls: vec![],
            tokens_in: 1,
            tokens_out: 1,
        },
    ]
}

fn runtime(park_first: bool) -> (AgentRuntime, TenantContext, Arc<SlowFlow>) {
    let tc = TenantContext::new("acme", "prod");
    let cp = MockConfigProvider::new();
    cp.insert(
        &tc,
        "a",
        AgentConfig {
            on_text_while_parked: Default::default(),
            agent_id: "a".into(),
            system_prompt: "sys".into(),
            tools: vec![ToolRef {
                extension_id: "flow:slow".into(),
                tool_name: "slow".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            }],
            llm: LlmProviderRef {
                provider: "mock".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: AgentLimits {
                max_iter: 4,
                timeout: Duration::from_secs(600),
                ..AgentLimits::default()
            },
            memory: None,
            knowledge: None,
            guardrails: vec![],
            conversational: false,
            opening_message: None,
        },
    );
    let store = Arc::new(MockAgentStateStore::new());
    let flows = Arc::new(SlowFlow {
        store: store.clone(),
        park_first,
        refreshes_while_pending: Mutex::new(Vec::new()),
    });
    let rt = AgentRuntime::new(
        Arc::new(cp),
        store,
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        Arc::new(MockLlmBackend::new(script().into_iter().map(Ok).collect())),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_flow_source(Some(Arc::new(FlowToolSource::new(flows.clone()))));
    (rt, tc, flows)
}

#[tokio::test(start_paused = true)]
async fn the_lock_is_refreshed_while_a_slow_flow_tool_call_is_pending() {
    let (rt, tc, flows) = runtime(false);
    let out = rt
        .step(tc, "sess", "a", AgentInput::default())
        .await
        .unwrap();
    assert_eq!(
        out.reply, "done",
        "the slow call completed and the turn ended"
    );
    // 100 s pending with a 30 s interval: refreshed at 30, 60 and 90 s.
    assert_eq!(*flows.refreshes_while_pending.lock().unwrap(), vec![3]);
}

#[tokio::test(start_paused = true)]
async fn the_lock_is_refreshed_while_a_slow_resume_is_pending() {
    let (rt, tc, flows) = runtime(true);
    rt.step(tc.clone(), "sess", "a", AgentInput::default())
        .await
        .unwrap();
    rt.step(
        tc,
        "sess",
        "a",
        AgentInput {
            text: String::new(),
            conversational: false,
            resume_payload: Some(json!({ "room": "101" })),
        },
    )
    .await
    .unwrap();
    assert_eq!(*flows.refreshes_while_pending.lock().unwrap(), vec![3]);
}
