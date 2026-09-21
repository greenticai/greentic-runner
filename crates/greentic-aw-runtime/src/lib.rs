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

pub mod a2a_source;
pub mod billing;
pub mod component_source;
pub mod config;
pub mod config_provider;
pub mod cost;
pub mod dispatch_ledger;
pub mod dw;
pub mod end_conversation;
pub mod error;
pub mod flow_source;
pub mod graph;
pub mod guardrail;
pub mod guardrail_provider;
pub mod http_provider;
pub mod knowledge;
pub mod kv;
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
pub mod mcp_scope;
pub mod mcp_secrets;
pub mod mcp_source;
pub mod mcp_store_pull;
pub mod memory;
pub mod scoped_secrets;
pub mod short_term;
pub mod sorla_source;
pub mod state;
pub mod state_kv;
pub mod state_redis;
pub mod telemetry;
pub mod tenant;
pub mod tool_wire_name;
pub mod tools;

#[cfg(feature = "test-mock")]
pub mod mock;

#[cfg(feature = "serve")]
pub mod serve;

pub use a2a_source::{A2aRoute, A2aToolCatalog, A2aToolEntry, A2aToolSource};
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
pub use cost::{KvTokenMeter, RedisTokenMeter, TokenMeter};
pub use dispatch_ledger::{DispatchLedger, NoopDispatchLedger, RedisDispatchLedger};
pub use error::{AgentError, ConfigError, LlmError, MemoryError, StateError, TerminationReason};
pub use flow_source::{FlowInvoker, FlowOperation, FlowToolCatalog, FlowToolEntry, FlowToolSource};
pub use graph::http_provider::{CachingGraphProvider, HttpGraphProvider};
pub use http_provider::HttpConfigProvider;
#[cfg(feature = "state-disk")]
pub use kv::RedbKv;
pub use kv::{AwKv, MemoryKv};
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
    MCP_ROLE_AGENTIC_WORKER, MCP_ROLE_FLOW_EDITOR, McpCallerIdentity, McpPackRoute, McpRoute,
    McpToolCatalog, McpToolEntry, McpToolSource, dispatch_route,
};
pub use memory::{InMemoryMemoryProvider, MemoryProvider, MemoryQuery, MemoryRecord};
pub use sorla_source::{
    SorlaToolCatalog, SorlaToolEntry, SorlaToolSource, SorxInvoker, SorxOperation,
};
pub use state::{AgentStateStore, ChatMessage, ConversationState, SessionLock};
pub use state_kv::KvAgentStateStore;
pub use state_redis::RedisAgentStateStore;
pub use telemetry::{OtelTelemetry, StepTelemetryCtx, Telemetry};
pub use tenant::{TenantContext, VerifiedCaller};
pub use tool_wire_name::{ToolNameCodec, is_wire_safe, wire_tool_name};
pub use tools::{KvToolLedger, RedisToolLedger, ToolLedger};

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
    /// Called just before a tool is dispatched, with its arguments.
    fn on_tool_call(&self, _name: &str, _call_id: &str, _args: &serde_json::Value) {}
    /// Called after a tool dispatch succeeds, with the tool's result.
    fn on_tool_result(&self, _name: &str, _call_id: &str, _result: &serde_json::Value) {}
    /// Called after a tool dispatch fails or is blocked (e.g. not in the
    /// agent's allow-list), with the error/reason payload. Additive hook —
    /// see the "Extending this trait" note above.
    fn on_tool_failed(&self, _name: &str, _call_id: &str, _error: &serde_json::Value) {}
    /// Called after each LLM iteration completes, with that call's token usage.
    /// Additive hook — lets a streaming consumer surface a per-message LLM/token
    /// trace even for turns that call no tools (the token trail is otherwise
    /// only in the final `AgentOutput.trail`, which the SSE stream omits).
    fn on_llm_call(&self, _tokens_in: u64, _tokens_out: u64) {}
    /// Called for every guardrail denial — blocked (Enforce) or merely recorded
    /// (Monitor). Default no-op: only the audit observer forwards these.
    /// Additive hook — see the "Extending this trait" note above.
    fn on_guardrail(&self, _obs: &crate::guardrail::GuardrailObservation) {}
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
    /// Per-tenant agentic-worker flow tool source. `None` disables flow tools
    /// entirely (`flow:`-prefixed tool refs then resolve to nothing). Set via
    /// [`AgentRuntime::with_flow_source`]; the concrete invoker (over the
    /// runner-host pack flow runtime) is injected at the runner-host edge, never
    /// compiled in.
    pub(crate) flows: Option<Arc<crate::flow_source::FlowToolSource>>,
    /// Per-tenant agentic-worker SoRLa SoR tool source. `None` disables sorla
    /// tools entirely (`sorla:`-prefixed tool refs then resolve to nothing).
    /// Set via [`AgentRuntime::with_sorla_source`]; the concrete invoker (over
    /// the host SoRX interact client) is injected at the runner-host edge,
    /// never compiled in.
    pub(crate) sorla: Option<Arc<crate::sorla_source::SorlaToolSource>>,
    /// Agentic-worker A2A tool source. `None` disables A2A tools entirely
    /// (`a2a:`-prefixed tool refs are then dropped from the tool list and
    /// reported by the preflight check). Set via
    /// [`AgentRuntime::with_a2a_source`]. Unlike the flow and component
    /// sources this one owns its transport, since an A2A agent is plain
    /// HTTP(S) reachable from here.
    pub(crate) a2a: Option<Arc<crate::a2a_source::A2aToolSource>>,
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
            flows: None,
            sorla: None,
            a2a: None,
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

    /// Wire the flow tool source so `flow:`-prefixed tool refs resolve to pack
    /// flows invoked over the host flow runtime. Coexists with the
    /// MCP/extension/component tool surfaces; defaults off when not set.
    #[must_use]
    pub fn with_flow_source(
        mut self,
        flows: Option<Arc<crate::flow_source::FlowToolSource>>,
    ) -> Self {
        self.flows = flows;
        self
    }

    /// Attach a per-tenant SoRLa SoR tool source: `sorla:<pack>` tool refs
    /// resolve to a SoR BusinessAction invoked over the host SoRX interact
    /// client. Coexists with the mcp/component/flow sources.
    #[must_use]
    pub fn with_sorla_source(
        mut self,
        sorla: Option<Arc<crate::sorla_source::SorlaToolSource>>,
    ) -> Self {
        self.sorla = sorla;
        self
    }

    /// Wire the A2A tool source so `a2a:`-prefixed tool refs resolve to
    /// external A2A agents invoked over HTTP(S). Coexists with the
    /// mcp/component/flow/sorla sources; defaults off when not set.
    #[must_use]
    pub fn with_a2a_source(mut self, a2a: Option<Arc<crate::a2a_source::A2aToolSource>>) -> Self {
        self.a2a = a2a;
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

    /// Whether a knowledge / RAG backend is mounted. Mirrors the check
    /// [`knowledge::knowledge_active`] applies against `runtime.knowledge` in the
    /// agentic loop, exposed so hosts and tests can confirm the seam is wired
    /// without issuing a (network-bound) [`Self::search_knowledge`] call.
    ///
    /// This answers "can retrieval be attempted at all", NOT "is a corpus
    /// mounted": a host may mount a backend that delegates elsewhere per turn,
    /// in which case this is true with no corpus behind it. Ask
    /// [`knowledge::Knowledge::wrapped_backend`] for that, via
    /// [`Self::knowledge_backend`].
    #[must_use]
    pub fn has_knowledge(&self) -> bool {
        self.knowledge.is_some()
    }

    /// Whether an A2A tool source is wired, so `a2a:` refs can resolve.
    ///
    /// Exposed for the host's wiring regression test. The runtime is built in
    /// runner-host, where this field is not visible, and a dropped
    /// `with_a2a_source` call otherwise fails only as "the tool vanished" at
    /// run time.
    #[must_use]
    pub fn has_a2a_source(&self) -> bool {
        self.a2a.is_some()
    }

    /// The mounted knowledge backend, if any.
    ///
    /// Exposed so a host that mounts a SECOND backend can wrap the first
    /// instead of replacing it: [`Self::with_knowledge`] overwrites the field,
    /// and two mounts that each assume they are the only one would silently
    /// disable whichever ran first.
    #[must_use]
    pub fn knowledge_backend(&self) -> Option<Arc<dyn knowledge::Knowledge>> {
        self.knowledge.clone()
    }

    /// Hybrid retrieval over the agent's knowledge corpus. Returns
    /// [`knowledge::KnowledgeError::NotConfigured`] when no backend is wired.
    ///
    /// `binding` is the agent's own `config.knowledge.knowledge` provider ref
    /// for this turn. It is threaded through because a backend may delegate to
    /// a target named in the binding's `params` — see
    /// [`knowledge::Knowledge::search_bound`] for why that cannot ride on the
    /// runtime-level field or inside [`knowledge::KnowledgeQuery`].
    pub async fn search_knowledge(
        &self,
        tenant: &TenantContext,
        query: knowledge::KnowledgeQuery,
        binding: Option<&config::MemoryProviderRef>,
    ) -> knowledge::KnowledgeResult<Vec<knowledge::RetrievedChunk>> {
        let kb = self
            .knowledge
            .as_ref()
            .ok_or(knowledge::KnowledgeError::NotConfigured)?;
        let ctx = knowledge::to_types_tenant(tenant)?;
        kb.search_bound(&ctx, query, binding).await
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
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AgentInput {
    pub text: String,
    /// Whether THIS invocation is a conversational segment. Set from the flow
    /// node's `conversational` flag so a node marked conversational makes the
    /// agent's host `end_conversation` tool available (and the closing prompt
    /// note) even when the agent's own [`AgentConfig::conversational`] default
    /// is false — the node, not just the agent config, can opt in. OR-ed with
    /// the agent config in the loop.
    #[serde(default)]
    pub conversational: bool,
}

/// Token + iteration accounting for one [`AgentRuntime::step`]. Surfaced on
/// [`AgentOutput`] so a caller (e.g. the designer's Run Demo trace) can show
/// per-turn LLM usage without a separate telemetry channel.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StepUsage {
    /// Prompt tokens summed across every LLM call in this step.
    pub tokens_in: u64,
    /// Completion tokens summed across every LLM call in this step.
    pub tokens_out: u64,
    /// Plan-Act-Observe iterations the loop ran this step.
    pub iterations: u32,
}

/// Outbound reply produced by [`AgentRuntime::step`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AgentOutput {
    pub reply: String,
    pub trail: Vec<AgentStep>,
    pub terminated_by: TerminationReason,
    /// Per-step token + iteration usage (default zero for callers that don't
    /// track it, e.g. mocks).
    #[serde(default)]
    pub usage: StepUsage,
}

/// One iteration of the Plan-Act-Observe loop, surfaced in the audit
/// trail (`AgentOutput.trail`). Caller decides whether to persist or
/// display.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentStep {
    /// One LLM iteration's assistant output + token cost, recorded before that
    /// iteration's tool calls. Lets a caller show a per-message breakdown (each
    /// model call), not just the aggregate `AgentOutput.usage`. `content` is the
    /// assistant text for the turn (empty when the model only emitted tool calls).
    LlmCall {
        content: String,
        tokens_in: u64,
        tokens_out: u64,
    },
    ToolCall {
        name: String,
        call_id: String,
        /// The arguments the model passed to the tool (its input). Lets a caller
        /// show what was actually requested, not just the tool name. Defaults to
        /// `null` for older trails that predate this field.
        #[serde(default)]
        args: serde_json::Value,
        result: serde_json::Value,
        /// Wall-clock time spent dispatching this tool call, in milliseconds.
        /// `0` for host built-ins that resolve instantly and for older trails.
        #[serde(default)]
        duration_ms: u64,
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
    /// The worker's BUILT-IN knowledge base (`KnowledgeSettings`) was searched
    /// for this turn and returned `chunks`, which were injected into the system
    /// prompt before the first LLM call. Recorded once per step, ahead of every
    /// `LlmCall`, and only when retrieval succeeded with at least one chunk — a
    /// failed or empty retrieval injects nothing and so has nothing to cite.
    ///
    /// This is NOT a tool call: the model did not ask for it, the loop ran it.
    /// Recording it as a synthetic `ToolCall` would inflate tool-call counts and
    /// anything metering them. It exists so a trail consumer (greentic-start's
    /// `agent_provenance`) can cite what a knowledge-grounded answer drew on.
    ///
    /// The chunks are recorded faithfully, text included: the trail is
    /// server-side data. Whether a chunk's text may reach an end user's
    /// browser is the CONSUMER's disclosure decision, not the runtime's.
    ///
    /// Adding a variant to this `#[serde(tag = "kind")]` enum is a contract
    /// change: a strict `Vec<AgentStep>` deserialiser built against an older
    /// runtime rejects `"kind": "knowledge_retrieval"`. Readers must skip kinds
    /// they do not know.
    KnowledgeRetrieval {
        chunks: Vec<knowledge::RetrievedChunk>,
    },
}
