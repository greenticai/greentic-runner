//! Loop-level behaviour of the knowledge tier: what the turn does when
//! retrieval succeeds, when it fails, and what the backend is told.
//!
//! These cover the two properties that the loop's old `.unwrap_or_default()`
//! made untestable: that a retrieval failure is REPORTED rather than silently
//! becoming an empty corpus, and that the agent's own provider binding reaches
//! the backend, which is the only way a backend delegating to a per-worker
//! target can know what to call.

#![cfg(feature = "test-mock")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use greentic_aw_runtime::config::{KnowledgeSettings, MemoryProviderRef};
use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::knowledge::{
    IngestOutcome, Knowledge, KnowledgeChunk, KnowledgeError, KnowledgeQuery, KnowledgeResult,
    RetrievedChunk,
};
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, LlmProviderRef, StepObserver,
};
use greentic_types::TenantCtx;

const PROVIDER: &str = "provider.knowledge.extension";

fn binding() -> MemoryProviderRef {
    MemoryProviderRef {
        provider: PROVIDER.into(),
        capability: "cap://dw.knowledge".into(),
        params: serde_json::json!({
            "provider.knowledge.extension.extension_id": "greentic.some-ext",
            "provider.knowledge.extension.tool_name": "search_knowledge",
        })
        .as_object()
        .cloned()
        .expect("params literal is an object"),
        credential_ref: None,
    }
}

fn cfg() -> AgentConfig {
    AgentConfig {
        agent_id: "a".into(),
        system_prompt: "sys".into(),
        tools: vec![],
        guardrails: vec![],
        llm: LlmProviderRef {
            provider: "mock".into(),
            model: "m".into(),
            credential_ref: None,
        },
        limits: AgentLimits {
            max_iter: 4,
            timeout: Duration::from_millis(60_000),
            ..AgentLimits::default()
        },
        memory: None,
        knowledge: Some(KnowledgeSettings {
            knowledge: Some(binding()),
            embedding: None,
            top_k: 3,
        }),
        conversational: false,
        opening_message: None,
    }
}

/// A knowledge backend that records the binding it was handed and answers with
/// whatever the test scripted.
struct ScriptedKnowledge {
    outcome: Mutex<Option<KnowledgeResult<Vec<RetrievedChunk>>>>,
    seen_bindings: Mutex<Vec<Option<MemoryProviderRef>>>,
}

impl ScriptedKnowledge {
    fn new(outcome: KnowledgeResult<Vec<RetrievedChunk>>) -> Self {
        Self {
            outcome: Mutex::new(Some(outcome)),
            seen_bindings: Mutex::new(Vec::new()),
        }
    }

    fn take(&self) -> KnowledgeResult<Vec<RetrievedChunk>> {
        self.outcome
            .lock()
            .expect("scripted knowledge mutex poisoned")
            .take()
            .unwrap_or_else(|| Ok(Vec::new()))
    }
}

#[async_trait::async_trait]
impl Knowledge for ScriptedKnowledge {
    async fn ingest(
        &self,
        _tenant: &TenantCtx,
        _chunks: Vec<KnowledgeChunk>,
    ) -> KnowledgeResult<IngestOutcome> {
        Ok(IngestOutcome::default())
    }

    async fn search(
        &self,
        _tenant: &TenantCtx,
        _query: KnowledgeQuery,
    ) -> KnowledgeResult<Vec<RetrievedChunk>> {
        self.seen_bindings
            .lock()
            .expect("scripted knowledge mutex poisoned")
            .push(None);
        self.take()
    }

    async fn search_bound(
        &self,
        _tenant: &TenantCtx,
        _query: KnowledgeQuery,
        binding: Option<&MemoryProviderRef>,
    ) -> KnowledgeResult<Vec<RetrievedChunk>> {
        self.seen_bindings
            .lock()
            .expect("scripted knowledge mutex poisoned")
            .push(binding.cloned());
        self.take()
    }
}

#[derive(Default)]
struct RecordingObserver {
    results: Mutex<Vec<(String, serde_json::Value)>>,
}

impl StepObserver for RecordingObserver {
    fn on_tool_result(&self, name: &str, _call_id: &str, result: &serde_json::Value) {
        self.results
            .lock()
            .expect("observer mutex poisoned")
            .push((name.to_string(), result.clone()));
    }
}

fn runtime(kb: Arc<dyn Knowledge>) -> (AgentRuntime, Arc<MockLlmBackend>, TenantContext) {
    let llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
        content: Some("answer".into()),
        tool_calls: vec![],
        tokens_in: 1,
        tokens_out: 1,
    })]));
    let tc = TenantContext::new("acme", "prod");
    let cp = MockConfigProvider::new();
    cp.insert(&tc, "a", cfg());
    let rt = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm.clone(),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_knowledge(kb);
    (rt, llm, tc)
}

async fn run(rt: &AgentRuntime, tc: TenantContext, observer: Arc<RecordingObserver>) {
    rt.step_with_observer(
        tc,
        "s",
        "a",
        AgentInput {
            text: "what is the refund policy".into(),
            conversational: false,
        },
        observer,
    )
    .await
    .expect("a knowledge failure must never fail the turn");
}

/// The property the whole change exists for: a retrieval that ERRORS still lets
/// the turn run, and says so — where before it became an empty vector with no
/// log, no trace, and nothing to tell it apart from an empty corpus.
#[tokio::test]
async fn a_failed_retrieval_degrades_without_going_silent() {
    let kb = Arc::new(ScriptedKnowledge::new(Err(KnowledgeError::Backend(
        "the retrieval service refused".into(),
    ))));
    let (rt, llm, tc) = runtime(kb);
    let observer = Arc::new(RecordingObserver::default());

    run(&rt, tc, observer.clone()).await;

    let prompts = llm
        .seen_system_prompts
        .lock()
        .expect("mock llm prompt lock poisoned");
    assert!(
        !prompts[0].contains("<knowledge>"),
        "a failed retrieval must inject nothing rather than an empty block"
    );

    let results = observer.results.lock().expect("observer mutex poisoned");
    let step = results
        .iter()
        .find(|(name, _)| name == "search_knowledge")
        .expect("a failed retrieval must still surface a trace step");
    assert!(
        step.1["error"]
            .as_str()
            .is_some_and(|e| e.contains("the retrieval service refused")),
        "the trace step must carry the failure: {:?}",
        step.1
    );
}

/// The counterpart, so the assertion above is not vacuous: retrieval that
/// SUCCEEDS injects the block and traces chunks rather than an error.
#[tokio::test]
async fn a_successful_retrieval_injects_and_traces_chunks() {
    let kb = Arc::new(ScriptedKnowledge::new(Ok(vec![RetrievedChunk {
        text: "Refunds are processed within 5 business days.".into(),
        score: 0.9,
        doc_id: Some("kb/faq#1".into()),
        chunk_index: None,
        metadata: serde_json::Map::new(),
    }])));
    let (rt, llm, tc) = runtime(kb);
    let observer = Arc::new(RecordingObserver::default());

    run(&rt, tc, observer.clone()).await;

    let prompts = llm
        .seen_system_prompts
        .lock()
        .expect("mock llm prompt lock poisoned");
    assert!(prompts[0].contains("<knowledge>"));
    assert!(prompts[0].contains("Refunds are processed within 5 business days."));

    let results = observer.results.lock().expect("observer mutex poisoned");
    let step = results
        .iter()
        .find(|(name, _)| name == "search_knowledge")
        .expect("a successful retrieval traces its chunks");
    assert_eq!(step.1["count"], 1);
    assert!(step.1.get("error").is_none());
}

/// The agent's own provider binding must reach the backend. Without it a
/// backend that delegates to a per-worker target — "call extension X's tool Y"
/// — has no way to learn what to call, because `AgentRuntime.knowledge` is set
/// once per runtime while the binding arrives per turn.
#[tokio::test]
async fn the_agents_knowledge_binding_reaches_the_backend() {
    let kb = Arc::new(ScriptedKnowledge::new(Ok(Vec::new())));
    let (rt, _llm, tc) = runtime(kb.clone());

    run(&rt, tc, Arc::new(RecordingObserver::default())).await;

    let seen = kb
        .seen_bindings
        .lock()
        .expect("scripted knowledge mutex poisoned");
    assert_eq!(seen.len(), 1, "one retrieval per turn");
    let got = seen[0]
        .as_ref()
        .expect("the loop must pass the agent's knowledge binding, not None");
    assert_eq!(got.provider, PROVIDER);
    assert_eq!(
        got.params
            .get("provider.knowledge.extension.extension_id")
            .and_then(serde_json::Value::as_str),
        Some("greentic.some-ext"),
        "the params carrying the delegation target must survive the trip"
    );
}
