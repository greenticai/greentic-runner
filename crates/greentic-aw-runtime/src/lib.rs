//! Greentic Agentic Worker Runtime — library crate.
//!
//! See `docs/superpowers/specs/2026-05-22-enterprise-aw-runtime-design.md`
//! for the full design spec. This crate exposes the [`AgentRuntime`] entry
//! point and the trait surface (`AgentStateStore`, `ConfigProvider`,
//! `LlmBackend`, `Telemetry`) that the production runner-host and the
//! designer playground both consume.
//!
//! The [`graph`] module provides durable multi-agent graph execution
//! (`GraphExecutor`, `GraphConfig`, `CheckpointStore`); see
//! `docs/superpowers/specs/2026-06-06-runtime-agent-graph-execution-design.md`.

#![deny(unsafe_code)]
#![warn(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

pub mod billing;
pub mod component_source;
pub mod config;
pub mod config_provider;
pub mod cost;
pub mod dispatch_ledger;
pub mod dw;
pub mod error;
pub mod graph;
pub mod guardrail;
pub mod guardrail_provider;
pub mod http_provider;
pub mod knowledge;
pub mod layered_provider;
pub mod llm;
pub mod llm_credential;
pub mod llm_extension;
#[cfg(feature = "greentic-llm-backend")]
pub mod llm_greentic;
pub mod llm_openai;
pub mod long_term;
pub mod r#loop;
pub mod manifest_provider;
pub mod manifest_tools;
pub mod mcp_local;
pub mod mcp_secrets;
pub mod mcp_source;
pub mod mcp_store_pull;
pub mod memory;
pub mod short_term;
pub mod state;
pub mod state_redis;
pub mod telemetry;
pub mod tenant;
pub mod tools;

#[cfg(feature = "test-mock")]
pub mod mock;

/// Test-only constructors. Available to this crate's own `#[cfg(test)]` code
/// and, via `test-mock`, to integration tests and the `aw-serve` harness.
#[cfg(any(test, feature = "test-mock"))]
pub mod test_support;

#[cfg(feature = "serve")]
pub mod serve;

pub use component_source::{
    ComponentInvoker, ComponentOperation, ComponentToolCatalog, ComponentToolEntry,
    ComponentToolSource,
};
pub use config::{
    AgentConfig, AgentLimits, LlmProviderRef, MemoryProviderRef, MemorySettings, ToolRef,
};
pub use config_provider::{CachingConfigProvider, ConfigProvider, InMemoryConfigProvider};
#[cfg(feature = "test-mock")]
pub use cost::MockTokenMeter;
pub use cost::{RedisTokenMeter, TokenMeter};
pub use dispatch_ledger::{DispatchLedger, NoopDispatchLedger, RedisDispatchLedger};
pub use error::{AgentError, ConfigError, LlmError, MemoryError, StateError, TerminationReason};
pub use graph::http_provider::{CachingGraphProvider, HttpGraphProvider};
pub use http_provider::HttpConfigProvider;
pub use layered_provider::LayeredConfigProvider;
pub use llm::{LlmBackend, LlmRequest, LlmResponse, RetryingLlmBackend};
pub use llm_extension::{
    BridgeCredential, ExtensionLlmBackend, LlmExtensionInvoker, RuntimeInvoker,
};
#[cfg(feature = "greentic-llm-backend")]
pub use llm_greentic::GreenticLlmBackend;
pub use llm_openai::{OpenAiLlmBackend, encode_tool_name, split_tool_name};
pub use long_term::{
    EpisodeIngest, EpisodeSource, IngestOutcome, LongTermMemory, LongTermMemoryError, RecallQuery,
    RecalledFact,
};
pub use manifest_provider::ManifestToolOverlayProvider;
pub use mcp_source::{
    MCP_ROLE_AGENTIC_WORKER, MCP_ROLE_FLOW_EDITOR, McpRoute, McpToolCatalog, McpToolEntry,
    McpToolSource, dispatch_route,
};
pub use memory::{InMemoryMemoryProvider, MemoryProvider, MemoryQuery, MemoryRecord};
pub use state::{AgentStateStore, ChatMessage, ConversationState, SessionLock};
pub use state_redis::RedisAgentStateStore;
pub use telemetry::{OtelTelemetry, StepTelemetryCtx, Telemetry};
pub use tenant::{TenantContext, VerifiedCaller};
pub use tools::{RedisToolLedger, ToolLedger};

use std::sync::Arc;

/// Observer for incremental step progress: token deltas as the LLM streams
/// its reply, and tool-call activity as the loop dispatches tools.
///
/// All methods have no-op default bodies, so callers implement only the
/// hooks they consume and the non-streaming [`AgentRuntime::step`] path
/// (which uses [`NoopStepObserver`]) costs nothing.
///
/// **Extending this trait:** add new capabilities as NEW defaulted methods
/// (e.g. `fn on_iteration_started(&self, _iter: u32) {}`) rather than
/// changing an existing signature. The current `on_token_delta(&self,
/// chunk: &str)` is deliberately minimal; per-iteration context must arrive
/// through an additional hook so existing implementors keep compiling.
pub trait StepObserver: Send + Sync {
    /// Whether this observer wants token-level streaming. Defaults to
    /// `false` so the non-streaming [`AgentRuntime::step`] path calls
    /// [`LlmBackend::complete`] and preserves the exact request wire shape
    /// (no `stream: true`) every existing caller relied on before streaming
    /// existed. A streaming consumer overrides this to `true`, which makes
    /// [`r#loop::run_step`] use [`LlmBackend::complete_streaming`] and drive
    /// [`StepObserver::on_token_delta`].
    fn wants_streaming(&self) -> bool {
        false
    }
    /// Called with each incremental text chunk of the assistant reply.
    /// Only invoked when [`StepObserver::wants_streaming`] returns `true`.
    fn on_token_delta(&self, _chunk: &str) {}
    /// Called just before a tool is dispatched.
    fn on_tool_call(&self, _name: &str, _call_id: &str) {}
    /// Called after a tool dispatch succeeds, with the tool's result.
    fn on_tool_result(&self, _name: &str, _call_id: &str, _result: &serde_json::Value) {}
}

/// No-op observer used by the non-streaming [`AgentRuntime::step`].
pub struct NoopStepObserver;
impl StepObserver for NoopStepObserver {}

/// The main entry point for executing a single agentic step.
///
/// Construct via [`AgentRuntime::new`] with the trait objects (config,
/// state, LLM, telemetry, token_meter, ledger) plus a shared
/// `Arc<ExtensionRuntime>` for tool dispatch. Call [`AgentRuntime::step`]
/// per inbound user message.
pub struct AgentRuntime {
    pub(crate) config_provider: Arc<dyn ConfigProvider>,
    pub(crate) state_store: Arc<dyn AgentStateStore>,
    pub(crate) ext_runtime: Arc<greentic_ext_runtime::ExtensionRuntime>,
    pub(crate) llm: Arc<dyn LlmBackend>,
    pub(crate) telemetry: Arc<dyn Telemetry>,
    pub(crate) token_meter: Arc<dyn TokenMeter>,
    pub(crate) billing_meter: Arc<dyn crate::billing::BillingMeter>,
    pub(crate) ledger: Arc<dyn ToolLedger>,
    /// Per-tenant agentic-worker MCP tool source. `None` disables MCP tools
    /// entirely (`mcp:`-prefixed tool refs then resolve to nothing). The real
    /// per-operator wiring lives in the runner host; tests and non-MCP callers
    /// pass `None`.
    pub(crate) mcp: Option<Arc<crate::mcp_source::McpToolSource>>,
    /// Policy that supplies platform/tenant-wide mandatory guardrail refs.
    /// Defaults to [`crate::guardrail::NoMandatoryGuardrails`] (no platform
    /// enforcement). Replaced by a real policy during runner-host wiring.
    pub(crate) guardrail_policy: Arc<dyn crate::guardrail::GuardrailPolicy>,
    /// Evaluator that invokes guardrail WASM extensions.
    /// Defaults to [`crate::guardrail::AcceptAllEvaluator`] until Task 7
    /// wires in the real extension-backed evaluator.
    pub(crate) guardrail_evaluator: Arc<dyn crate::guardrail::GuardrailEvaluator>,
    /// Per-tenant agentic-worker component tool source. `None` disables
    /// component tools entirely (`component:`-prefixed tool refs then resolve to
    /// nothing). Set via [`AgentRuntime::with_component_source`]; the concrete
    /// invoker (over the runner-host `PackRuntime` component host) is injected
    /// at the runner-host edge, never compiled in.
    pub(crate) components: Option<Arc<crate::component_source::ComponentToolSource>>,
    /// Episodic long-term memory backend (e.g. Chronicle). `None` disables the
    /// long-term tier. Set via [`AgentRuntime::with_long_term_memory`]; the
    /// concrete backend is injected at the runner-host edge, never compiled in.
    pub(crate) long_term_memory: Option<Arc<dyn long_term::LongTermMemory>>,
    /// Knowledge / RAG (document-corpus) backend (e.g. Chronicle doc-RAG).
    /// `None` disables the knowledge tier. Set via [`AgentRuntime::with_knowledge`];
    /// the concrete backend is injected at the runner-host edge, never compiled in.
    pub(crate) knowledge: Option<Arc<dyn knowledge::Knowledge>>,
    /// Short-term ("working") memory backend, scoped per `(tenant, session, key)`.
    /// `None` disables the short-term tier. Set via
    /// [`AgentRuntime::with_short_term_memory`]; the host attaches the always-
    /// available in-memory provider, and the tools are gated by
    /// `config.memory.short_term`.
    pub(crate) short_term_memory: Option<Arc<dyn crate::memory::MemoryProvider>>,
}

impl AgentRuntime {
    // Each argument is a distinct injected dependency (config, state, ext,
    // llm, telemetry, token-meter, ledger, mcp); a builder would add ceremony
    // without removing the coupling, so the wide constructor is intentional.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config_provider: Arc<dyn ConfigProvider>,
        state_store: Arc<dyn AgentStateStore>,
        ext_runtime: Arc<greentic_ext_runtime::ExtensionRuntime>,
        llm: Arc<dyn LlmBackend>,
        telemetry: Arc<dyn Telemetry>,
        token_meter: Arc<dyn TokenMeter>,
        ledger: Arc<dyn ToolLedger>,
        mcp: Option<Arc<crate::mcp_source::McpToolSource>>,
    ) -> Self {
        Self {
            config_provider,
            state_store,
            ext_runtime,
            llm,
            telemetry,
            token_meter,
            billing_meter: Arc::new(crate::billing::NoopBillingMeter),
            ledger,
            mcp,
            guardrail_policy: Arc::new(crate::guardrail::NoMandatoryGuardrails),
            guardrail_evaluator: Arc::new(crate::guardrail::AcceptAllEvaluator),
            components: None,
            long_term_memory: None,
            knowledge: None,
            short_term_memory: None,
        }
    }

    /// Override the guardrail policy and evaluator after construction.
    ///
    /// Available in all builds (including production runner-host) so the
    /// runner can inject the real `ExtRuntimeGuardrailEvaluator` +
    /// `StaticGuardrailPolicy` without requiring `test-mock`.
    pub fn with_guardrails(
        mut self,
        policy: Arc<dyn crate::guardrail::GuardrailPolicy>,
        evaluator: Arc<dyn crate::guardrail::GuardrailEvaluator>,
    ) -> Self {
        self.guardrail_policy = policy;
        self.guardrail_evaluator = evaluator;
        self
    }

    /// Install a billing sink. Defaults to [`crate::billing::NoopBillingMeter`]
    /// (no-op, billing disabled); the host calls this when
    /// `GREENTIC_BILLING_BASE_URL` and `GREENTIC_BILLING_SERVICE_SECRET` are
    /// configured. The setter is intentionally separate from [`AgentRuntime::new`]
    /// so the constructor signature remains stable across all consumers.
    #[must_use]
    pub fn with_billing_meter(mut self, meter: Arc<dyn crate::billing::BillingMeter>) -> Self {
        self.billing_meter = meter;
        self
    }

    /// Wire the component tool source so `component:`-prefixed tool refs resolve
    /// to greentic `.gtpack` components invoked over the host component runtime.
    /// Coexists with the MCP/extension tool surfaces; defaults off when not set.
    #[must_use]
    pub fn with_component_source(
        mut self,
        components: Option<Arc<crate::component_source::ComponentToolSource>>,
    ) -> Self {
        self.components = components;
        self
    }

    /// Wire the episodic long-term memory backend (e.g. Chronicle). Coexists
    /// with the short-term/working memory; defaults off when not set.
    #[must_use]
    pub fn with_long_term_memory(mut self, memory: Arc<dyn long_term::LongTermMemory>) -> Self {
        self.long_term_memory = Some(memory);
        self
    }

    /// Wire the short-term ("working") memory backend. Coexists with the
    /// long-term tier; defaults off when not set. The MVP host attaches the
    /// in-memory provider unconditionally; the `remember`/`recall` tools are
    /// advertised only when `config.memory.short_term` is set.
    #[must_use]
    pub fn with_short_term_memory(
        mut self,
        memory: Arc<dyn crate::memory::MemoryProvider>,
    ) -> Self {
        self.short_term_memory = Some(memory);
        self
    }

    /// Ingest one episode (conversation turn, document, event) into long-term
    /// memory. Returns [`LongTermMemoryError::NotConfigured`] when no long-term
    /// backend is wired.
    pub async fn remember_episode(
        &self,
        tenant: &TenantContext,
        episode: long_term::EpisodeIngest,
    ) -> Result<long_term::IngestOutcome, long_term::LongTermMemoryError> {
        let memory = self.long_term_memory.as_ref().ok_or_else(|| {
            long_term::LongTermMemoryError::NotConfigured("long-term memory not wired".into())
        })?;
        let ctx = long_term::to_types_tenant(tenant)?;
        memory.ingest_episode(&ctx, episode).await
    }

    /// Semantic recall over long-term memory. Returns
    /// [`LongTermMemoryError::NotConfigured`] when no long-term backend is wired.
    pub async fn recall_long_term(
        &self,
        tenant: &TenantContext,
        query: long_term::RecallQuery,
    ) -> Result<Vec<long_term::RecalledFact>, long_term::LongTermMemoryError> {
        let memory = self.long_term_memory.as_ref().ok_or_else(|| {
            long_term::LongTermMemoryError::NotConfigured("long-term memory not wired".into())
        })?;
        let ctx = long_term::to_types_tenant(tenant)?;
        memory.recall(&ctx, query).await
    }

    /// Wire the knowledge / RAG backend (e.g. Chronicle doc-RAG). Coexists with
    /// the memory tiers (distinct `cap://dw.knowledge` capability); defaults off.
    #[must_use]
    pub fn with_knowledge(mut self, knowledge: Arc<dyn knowledge::Knowledge>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    /// Hybrid retrieval over the agent's knowledge corpus. Returns
    /// [`knowledge::KnowledgeError::NotConfigured`] when no backend is wired.
    pub async fn search_knowledge(
        &self,
        tenant: &TenantContext,
        query: knowledge::KnowledgeQuery,
    ) -> knowledge::KnowledgeResult<Vec<knowledge::RetrievedChunk>> {
        let kb = self
            .knowledge
            .as_ref()
            .ok_or(knowledge::KnowledgeError::NotConfigured)?;
        let ctx = knowledge::to_types_tenant(tenant)?;
        kb.search(&ctx, query).await
    }

    /// Execute one agentic step against the given session.
    /// Implementation lives in [`r#loop::run_step`].
    pub async fn step(
        &self,
        tenant: TenantContext,
        session_id: &str,
        agent_id: &str,
        message: AgentInput,
    ) -> Result<AgentOutput, AgentError> {
        self.step_with_observer(
            tenant,
            session_id,
            agent_id,
            message,
            Arc::new(NoopStepObserver),
        )
        .await
    }

    /// Execute one agentic step while reporting incremental progress to
    /// `observer` (streamed token deltas + tool-call activity).
    /// [`AgentRuntime::step`] delegates here with a [`NoopStepObserver`].
    pub async fn step_with_observer(
        &self,
        tenant: TenantContext,
        session_id: &str,
        agent_id: &str,
        message: AgentInput,
        observer: Arc<dyn StepObserver>,
    ) -> Result<AgentOutput, AgentError> {
        r#loop::run_step(self, tenant, session_id, agent_id, message, observer).await
    }
}

/// Inbound user message handed to [`AgentRuntime::step`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AgentInput {
    pub text: String,
}

/// Outbound reply produced by [`AgentRuntime::step`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AgentOutput {
    pub reply: String,
    pub trail: Vec<AgentStep>,
    pub terminated_by: TerminationReason,
}

/// One iteration of the Plan-Act-Observe loop, surfaced in the audit
/// trail (`AgentOutput.trail`). Caller decides whether to persist or
/// display.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentStep {
    ToolCall {
        name: String,
        call_id: String,
        result: serde_json::Value,
    },
    ToolCallReused {
        name: String,
        call_id: String,
    },
    ToolCallBlocked {
        name: String,
        reason: String,
    },
    Reply {
        text: String,
    },
}
