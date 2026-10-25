//! Loop behaviour under failure: timeouts, a lost lock, malformed tool
//! arguments, unbounded history. Included from `loop.rs`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{AgentConfig, AgentLimits, LlmProviderRef};
use crate::error::{AgentError, LlmError, StateError, TerminationReason};
use crate::llm::{LlmBackend, LlmRequest, LlmResponse};
use crate::mock::{MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry};
use crate::state::{
    AgentStateStore, ChatMessage, ConversationState, SessionLock, SessionLockInner, ToolCallRecord,
};
use crate::tenant::TenantContext;
use crate::{AgentInput, AgentRuntime, AgentStep};

fn cfg(limits: AgentLimits) -> AgentConfig {
    AgentConfig {
        agent_id: "a".into(),
        system_prompt: "sys".into(),
        tools: vec![],
        guardrails: vec![],
        llm: LlmProviderRef {
            provider: "openai".into(),
            model: "m".into(),
            credential_ref: None,
        },
        limits,
        memory: None,
        knowledge: None,
        conversational: false,
        opening_message: None,
        on_text_while_parked: Default::default(),
    }
}

fn runtime(
    llm: Arc<dyn LlmBackend>,
    store: Arc<dyn AgentStateStore>,
    limits: AgentLimits,
) -> (AgentRuntime, TenantContext, Arc<MockTelemetry>) {
    let telemetry = Arc::new(MockTelemetry::new());
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(&tc, "a", cfg(limits));
    let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let rt = AgentRuntime::new(
        Arc::new(cp),
        store,
        ext,
        llm,
        telemetry.clone(),
        Arc::new(crate::cost::MockTokenMeter::new(0)),
        Arc::new(crate::mock::NoopToolLedger),
        None,
    );
    (rt, tc, telemetry)
}

fn hi() -> AgentInput {
    AgentInput {
        text: "hi".into(),
        ..Default::default()
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

fn call(ext: &str, tool: &str, args: serde_json::Value) -> ToolCallRecord {
    ToolCallRecord {
        call_id: "c1".into(),
        extension_id: ext.into(),
        tool_name: tool.into(),
        args,
    }
}

/// An LLM that never answers.
struct Hang;
impl LlmBackend for Hang {
    fn complete<'a>(
        &'a self,
        _r: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Err(LlmError::ServiceUnavailable)
        })
    }
}

/// A store whose lock reports it was lost on every refresh.
struct LostLockStore(MockAgentStateStore);
struct Lost;
impl SessionLockInner for Lost {
    fn refresh<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        Box::pin(async { Err(StateError::LockLost) })
    }
    fn release(&self) {}
}
impl AgentStateStore for LostLockStore {
    fn load<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ConversationState, StateError>> + Send + 'a>> {
        self.0.load(tenant, session_id)
    }
    fn save<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        state: &'a ConversationState,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        self.0.save(tenant, session_id, state)
    }
    fn acquire_lock<'a>(
        &'a self,
        _tenant: &'a TenantContext,
        _session_id: &'a str,
        _wait: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<SessionLock, StateError>> + Send + 'a>> {
        Box::pin(async { Ok(SessionLock::new(Box::new(Lost))) })
    }
}

#[test]
fn host_tools_require_the_host_extension_id() {
    let host = call("host", "recall", serde_json::json!({}));
    let imposter = call("mcp:github", "recall", serde_json::json!({}));
    assert!(super::is_host_tool(&host, "recall"));
    assert!(!super::is_host_tool(&imposter, "recall"));
    assert!(!super::is_host_tool(&host, "remember"));
}

#[tokio::test]
async fn a_hung_llm_call_ends_the_step_as_timeout() {
    let limits = AgentLimits {
        timeout: Duration::from_millis(50),
        ..AgentLimits::default()
    };
    let (rt, tc, telemetry) = runtime(Arc::new(Hang), Arc::new(MockAgentStateStore::new()), limits);
    let out = rt.step(tc, "s", "a", hi()).await.unwrap();
    assert_eq!(out.terminated_by, TerminationReason::Timeout);
    assert!(out.reply.is_empty());
    assert_eq!(telemetry.recorded.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_lost_session_lock_aborts_the_step_without_saving() {
    let store = Arc::new(LostLockStore(MockAgentStateStore::new()));
    let llm = Arc::new(MockLlmBackend::new(vec![reply("never reached")]));
    let (rt, tc, telemetry) = runtime(llm, store.clone(), AgentLimits::default());
    let err = rt.step(tc.clone(), "s", "a", hi()).await.unwrap_err();
    assert!(matches!(err, AgentError::LockLost), "got {err:?}");
    let saved = store.0.load(&tc, "s").await.unwrap();
    assert!(saved.messages.is_empty(), "nothing may be saved");
    // The error exit is visible in step telemetry.
    let recorded = telemetry.recorded.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].terminated_by, TerminationReason::Error);
}

/// A store whose `load` always fails with the given error; counts saves.
struct FailingLoadStore {
    inner: MockAgentStateStore,
    make_error: fn() -> StateError,
    saves: std::sync::atomic::AtomicUsize,
}
impl FailingLoadStore {
    fn new(make_error: fn() -> StateError) -> Self {
        Self {
            inner: MockAgentStateStore::new(),
            make_error,
            saves: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}
impl AgentStateStore for FailingLoadStore {
    fn load<'a>(
        &'a self,
        _tenant: &'a TenantContext,
        _session_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ConversationState, StateError>> + Send + 'a>> {
        let err = (self.make_error)();
        Box::pin(async move { Err(err) })
    }
    fn save<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        state: &'a ConversationState,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        self.saves.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.save(tenant, session_id, state)
    }
    fn acquire_lock<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        wait: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<SessionLock, StateError>> + Send + 'a>> {
        self.inner.acquire_lock(tenant, session_id, wait)
    }
}

#[tokio::test]
async fn a_transient_state_load_failure_aborts_retryably_without_saving() {
    let store = Arc::new(FailingLoadStore::new(|| StateError::Redis("blip".into())));
    let llm = Arc::new(MockLlmBackend::new(vec![reply("never reached")]));
    let (rt, tc, _t) = runtime(llm, store.clone(), AgentLimits::default());
    let err = rt.step(tc, "s", "a", hi()).await.unwrap_err();
    assert!(
        matches!(err, AgentError::StateUnavailable(_)),
        "got {err:?}"
    );
    assert!(
        err.to_string()
            .contains(crate::error::STATE_UNAVAILABLE_MARKER)
    );
    assert_eq!(
        store.saves.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a failed load must never be followed by a save"
    );
}

#[tokio::test]
async fn an_undecodable_or_too_new_state_aborts_without_saving_or_retry() {
    for make in [
        (|| StateError::Decode("bad json".into())) as fn() -> StateError,
        || StateError::SchemaIncompatible {
            found: 9,
            supported: 1,
        },
    ] {
        let store = Arc::new(FailingLoadStore::new(make));
        let llm = Arc::new(MockLlmBackend::new(vec![reply("never reached")]));
        let (rt, tc, _t) = runtime(llm, store.clone(), AgentLimits::default());
        let err = rt.step(tc, "s", "a", hi()).await.unwrap_err();
        assert!(matches!(err, AgentError::StateLoad(_)), "got {err:?}");
        assert!(
            !err.to_string()
                .contains(crate::error::STATE_UNAVAILABLE_MARKER)
        );
        assert_eq!(store.saves.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn a_failed_load_does_not_take_the_opening_message_shortcut() {
    let store = Arc::new(FailingLoadStore::new(|| StateError::Redis("blip".into())));
    let llm = Arc::new(MockLlmBackend::new(vec![]));
    let (rt, tc, _t) = runtime(llm, store.clone(), AgentLimits::default());
    let err = rt
        .step(tc, "s", "a", AgentInput::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err, AgentError::StateUnavailable(_)),
        "got {err:?}"
    );
    assert_eq!(store.saves.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn malformed_tool_arguments_are_returned_to_the_model_not_dispatched() {
    let bad = call(
        "mcp:github",
        "create_issue",
        serde_json::json!({ crate::llm::MALFORMED_ARGS_KEY: "{\"title\": " }),
    );
    let llm = Arc::new(MockLlmBackend::new(vec![
        Ok(LlmResponse {
            content: None,
            tool_calls: vec![bad],
            tokens_in: 1,
            tokens_out: 1,
        }),
        reply("sorry, retrying"),
    ]));
    let store = Arc::new(MockAgentStateStore::new());
    let (rt, tc, _) = runtime(llm, store.clone(), AgentLimits::default());
    let out = rt.step(tc.clone(), "s", "a", hi()).await.unwrap();
    assert_eq!(out.reply, "sorry, retrying");
    let failed = out.trail.iter().any(|s| {
        matches!(s, AgentStep::ToolCall { result, .. }
            if result["error"].as_str().is_some_and(|e| e.contains("not valid JSON")))
    });
    assert!(failed, "{:?}", out.trail);
    let saved = store.load(&tc, "s").await.unwrap();
    assert!(
        saved
            .messages
            .iter()
            .any(|m| matches!(m, ChatMessage::Tool { call_id, .. } if call_id == "c1"))
    );
}

#[tokio::test]
async fn an_oversized_history_is_truncated_before_the_llm_call() {
    let store = Arc::new(MockAgentStateStore::new());
    let tc = TenantContext::new("acme", "prod");
    let mut seeded = ConversationState::empty(&tc, "s");
    for i in 0..10 {
        seeded.messages.push(ChatMessage::User {
            content: format!("q{i}"),
            attachments: Vec::new(),
        });
        seeded.messages.push(ChatMessage::Assistant {
            content: format!("a{i}"),
            tool_calls: vec![],
        });
    }
    store.save(&tc, "s", &seeded).await.unwrap();
    // The call fails: the bounded history must still be what is persisted.
    let llm = Arc::new(MockLlmBackend::new(vec![Err(LlmError::BadRequest(
        "context too long".into(),
    ))]));
    let limits = AgentLimits {
        max_history_turns: 4,
        ..AgentLimits::default()
    };
    let (rt, _, _) = runtime(llm, store.clone(), limits);
    let err = rt.step(tc.clone(), "s", "a", hi()).await.unwrap_err();
    assert!(matches!(err, AgentError::Llm(_)));
    let saved = store.load(&tc, "s").await.unwrap();
    assert!(saved.messages.len() <= 5, "len {}", saved.messages.len());
    assert!(matches!(
        saved.messages.first(),
        Some(ChatMessage::User { .. })
    ));
}

/// An LLM that takes `SLOW_LLM` and records how many lock refreshes happened
/// while the call was pending.
struct SlowLlm {
    store: Arc<MockAgentStateStore>,
    refreshes_while_pending: std::sync::Mutex<Vec<usize>>,
}

const SLOW_LLM: Duration = Duration::from_secs(100);

impl LlmBackend for SlowLlm {
    fn complete<'a>(
        &'a self,
        _r: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        Box::pin(async move {
            let before = self.store.lock_refreshes();
            tokio::time::sleep(SLOW_LLM).await;
            let after = self.store.lock_refreshes();
            self.refreshes_while_pending
                .lock()
                .unwrap()
                .push(after - before);
            reply("slow answer")
        })
    }
}

#[tokio::test(start_paused = true)]
async fn the_lock_is_kept_alive_while_a_slow_llm_call_is_pending() {
    let store = Arc::new(MockAgentStateStore::new());
    let llm = Arc::new(SlowLlm {
        store: store.clone(),
        refreshes_while_pending: std::sync::Mutex::new(Vec::new()),
    });
    let limits = AgentLimits {
        timeout: Duration::from_secs(600),
        ..AgentLimits::default()
    };
    let (rt, tc, _telemetry) = runtime(llm.clone(), store, limits);
    let out = rt.step(tc, "s", "a", hi()).await.unwrap();
    assert_eq!(out.reply, "slow answer");
    // 100 s call, 30 s interval: refreshes at 30, 60 and 90 s.
    assert_eq!(*llm.refreshes_while_pending.lock().unwrap(), vec![3]);
}
