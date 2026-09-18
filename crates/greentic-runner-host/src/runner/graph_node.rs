use anyhow::Result;
use serde_json::Value;

/// Bridges a `DwAgentGraph` flow node into the graph executor.
///
/// The concrete impl (constructed in the runner binary / pack loader, Task 8)
/// wraps [`greentic_aw_runtime::graph::GraphExecutor`]. The engine holds it as
/// a trait object so `engine.rs` stays free of AW-runtime construction details
/// — mirroring [`super::agent_node::AgentNodeHandler`].
#[async_trait::async_trait]
pub trait GraphNodeHandler: Send + Sync {
    /// Execute (or resume) a graph run for this session. `flow_input`
    /// expects `{"user_text": "..."}`; returns
    /// `{"reply", "trail", "terminated_by"}` — the same envelope as DwAgent.
    async fn execute(
        &self,
        tenant_id: &str,
        env_id: &str,
        graph_id: &str,
        session_id: &str,
        flow_input: &Value,
    ) -> Result<Value>;
}

// ---------------------------------------------------------------------------
// agentic-worker feature: full DwAgentGraph / GraphExecutor integration
// ---------------------------------------------------------------------------

#[cfg(feature = "agentic-worker")]
mod aw {
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Result;
    use greentic_aw_runtime::CachingGraphProvider;
    use greentic_aw_runtime::HttpGraphProvider;
    use greentic_aw_runtime::ToolLedger;
    use greentic_aw_runtime::config_provider::InMemoryConfigProvider;
    use greentic_aw_runtime::error::{AgentError, ConfigError};
    use greentic_aw_runtime::graph::{
        AgentTurnFn, AgentTurnRequest, AgentTurnResult, ApprovalFn, ApprovalOutcome,
        ApprovalRequest, BoxFut, CheckpointError, CheckpointStore, GraphConfig, GraphExecError,
        GraphExecutor, GraphRole, GraphRunState, RunStatus, SupervisorFn, SupervisorRequest,
        SupervisorResult, ToolCallRequest, ToolFn,
    };
    use greentic_aw_runtime::state::{AgentStateStore, ChatMessage, ConversationState};
    use greentic_aw_runtime::tools::dispatch_tool_call;
    use greentic_aw_runtime::{
        AgentConfig, AgentInput, AgentLimits, AgentOutput, AgentRuntime, LlmBackend,
        LlmProviderRef, StepObserver, Telemetry, TenantContext, TokenMeter, ToolRef,
    };
    use greentic_ext_runtime::ExtensionRuntime;
    use serde_json::{Value, json};

    use crate::trace::agent_audit::AgentAuditObserver;
    use crate::trace::audit_sink::AuditSink;

    use super::GraphNodeHandler;

    /// Build a [`greentic_types::TenantCtx`] for the agent-graph audit
    /// observer from the flow node's plain `tenant_id`/`env_id` strings.
    ///
    /// Mirrors `agent_node::tenant_ctx_for_audit` exactly (duplicated rather
    /// than shared: that helper is module-private to `agent_node`'s `aw`
    /// module). An id that fails the newtype's validation (should not happen
    /// once a graph node has been routed to a tenant) still yields a
    /// well-formed `TenantCtx` rather than panicking — the audit event is
    /// best-effort, never load-bearing.
    fn tenant_ctx_for_audit(tenant_id: &str, env_id: &str) -> greentic_types::TenantCtx {
        let env = greentic_types::EnvId::from_str(env_id)
            .unwrap_or_else(|_| greentic_types::EnvId::new("local").expect("local env id"));
        let tenant = greentic_types::TenantId::from_str(tenant_id)
            .unwrap_or_else(|_| greentic_types::TenantId::new("local").expect("local tenant id"));
        greentic_types::TenantCtx::new(env, tenant)
    }

    /// Fixed, user-safe reply returned when a graph run fails. The detailed
    /// error is logged but never surfaced to the flow output, so internal
    /// failure modes do not leak to end users. Mirrors
    /// [`super::super::agent_node`]'s `SANITISED_ERROR_REPLY`.
    const SANITISED_ERROR_REPLY: &str = "Something went wrong. Please try again.";

    /// Reply returned when the requested graph is not available (no such graph
    /// in the provider). User-safe; the missing-graph id is logged separately.
    const GRAPH_UNAVAILABLE_REPLY: &str =
        "This worker isn't available right now. Please try again later.";

    /// Resolution sentinel emitted by the agent's reply when it considers the
    /// issue fully resolved. Matched case-insensitively and stripped from the
    /// visible reply. Copied verbatim from the greentic-designer spike
    /// (`src/orchestrate/agent_graph/agent_turn.rs`).
    const RESOLVED_SENTINEL: &str = "[[RESOLVED]]";

    /// Upper bound on the number of fresh-run-id suffixes tried when a session's
    /// run id is already in a terminal state (see [`derive_run_id`]).
    const MAX_RUN_ID_SUFFIX: u32 = 100;

    /// Error returned by [`RuntimeGraphNodeHandler::derive_run_id`].
    ///
    /// Kept local to `runner-host`: `GraphExecError` (from `greentic-aw-runtime`)
    /// has no suffix-exhaustion variant and we should not pollute the upstream
    /// crate with a runner-host-specific concern.
    #[derive(Debug, thiserror::Error)]
    enum RunIdError {
        /// Checkpoint IO failed while scanning for a free slot.
        ///
        /// Note: `checkpoint.load` returns `CheckpointError` directly (not
        /// `GraphExecError`), so we convert both via their respective `From`
        /// impls. `GraphExecError::Checkpoint` wraps `CheckpointError` upstream;
        /// here we keep the original for a tighter error surface.
        #[error(transparent)]
        Checkpoint(#[from] CheckpointError),
        /// All `MAX_RUN_ID_SUFFIX` slots for this session are occupied (terminal).
        /// Distinct from `GraphExecError::IterationCap` (graph visit limit) so
        /// the caller can surface a targeted user reply.
        #[error("all run-id slots exhausted for base run `{base_run_id}`")]
        SuffixExhausted { base_run_id: String },
    }

    /// `true` when `reply` contains the resolution sentinel (case-insensitive).
    fn detect_resolved(reply: &str) -> bool {
        reply
            .to_ascii_lowercase()
            .contains(&RESOLVED_SENTINEL.to_ascii_lowercase())
    }

    /// Strip the sentinel from `reply` (first case-insensitive occurrence) and
    /// trim surrounding whitespace so the user-visible text is clean.
    fn strip_sentinel(reply: &str) -> String {
        let lower = reply.to_ascii_lowercase();
        if let Some(idx) = lower.find(&RESOLVED_SENTINEL.to_ascii_lowercase()) {
            let mut out = reply.to_string();
            out.replace_range(idx..idx + RESOLVED_SENTINEL.len(), "");
            return out.trim().to_string();
        }
        reply.trim().to_string()
    }

    // -----------------------------------------------------------------------
    // GraphConfigSource
    // -----------------------------------------------------------------------

    /// Resolves a [`GraphConfig`] for a `(tenant, graph_id)` pair.
    ///
    /// Object-safe (returns [`BoxFut`]) so it can be held as
    /// `Arc<dyn GraphConfigSource>`. The pack loader (Task 8) fills the
    /// concrete implementation; tests use [`InMemoryGraphProvider`].
    pub trait GraphConfigSource: Send + Sync {
        fn graph_config<'a>(
            &'a self,
            tenant: &'a TenantContext,
            graph_id: &'a str,
        ) -> BoxFut<'a, Result<GraphConfig, ConfigError>>;
    }

    /// [`GraphConfigSource`] backed by an in-memory `graph_id -> GraphConfig`
    /// map. Graphs are operator-global for the MVP: lookup is keyed purely by
    /// `graph_id`; the `tenant` argument is accepted (to satisfy the trait
    /// contract) but not used for keying. Reuses [`ConfigError::AgentNotFound`]
    /// as the crate's canonical "not found" variant.
    pub struct InMemoryGraphProvider {
        graphs: HashMap<String, GraphConfig>,
    }

    impl InMemoryGraphProvider {
        /// Wrap a `graph_id -> GraphConfig` map in a [`GraphConfigSource`].
        pub fn new(graphs: HashMap<String, GraphConfig>) -> Self {
            Self { graphs }
        }
    }

    impl GraphConfigSource for InMemoryGraphProvider {
        fn graph_config<'a>(
            &'a self,
            _tenant: &'a TenantContext,
            graph_id: &'a str,
        ) -> BoxFut<'a, Result<GraphConfig, ConfigError>> {
            let found = self.graphs.get(graph_id).cloned();
            let graph_id_owned = graph_id.to_string();
            Box::pin(async move { found.ok_or(ConfigError::AgentNotFound(graph_id_owned)) })
        }
    }

    // -----------------------------------------------------------------------
    // GraphConfigSource adapter for CachingGraphProvider<HttpGraphProvider>
    // -----------------------------------------------------------------------

    /// Adapts [`CachingGraphProvider<HttpGraphProvider>`] into the
    /// [`GraphConfigSource`] trait so it can be stored as an
    /// `Arc<dyn GraphConfigSource>` alongside [`InMemoryGraphProvider`].
    ///
    /// This is a 5-line adapter that bridges the inherent-method API of
    /// `CachingGraphProvider` (defined in `greentic-aw-runtime`) into the
    /// trait object used by `runner-host`.
    impl GraphConfigSource for CachingGraphProvider<HttpGraphProvider> {
        fn graph_config<'a>(
            &'a self,
            tenant: &'a TenantContext,
            graph_id: &'a str,
        ) -> BoxFut<'a, Result<GraphConfig, ConfigError>> {
            Box::pin(async move { self.graph_config(tenant, graph_id).await })
        }
    }

    // -----------------------------------------------------------------------
    // LayeredGraphProvider
    // -----------------------------------------------------------------------

    /// Tries the primary [`GraphConfigSource`], falling back to the secondary
    /// when the primary reports the graph missing or has an infrastructure
    /// failure. A `Misconfigured` error is propagated, never masked by the
    /// fallback — matching [`greentic_aw_runtime::LayeredConfigProvider`]'s
    /// semantics exactly.
    ///
    /// Fallback triggers:
    /// - `ConfigError::AgentNotFound` — graph absent from the primary (HTTP
    ///   registry doesn't know it yet; local pack may have it).
    /// - `ConfigError::Internal` — transient infrastructure failure (network
    ///   timeout, 5xx) — local pack can serve as a degraded fallback.
    ///
    /// Non-fallback (surfaces immediately):
    /// - `ConfigError::Misconfigured` — corrupt document or bad auth token;
    ///   the operator must fix the document — silently serving stale local
    ///   data would mask the problem.
    pub struct LayeredGraphProvider<P: GraphConfigSource, F: GraphConfigSource> {
        primary: P,
        fallback: F,
    }

    impl<P: GraphConfigSource, F: GraphConfigSource> LayeredGraphProvider<P, F> {
        pub fn new(primary: P, fallback: F) -> Self {
            Self { primary, fallback }
        }
    }

    impl<P: GraphConfigSource, F: GraphConfigSource> GraphConfigSource for LayeredGraphProvider<P, F> {
        fn graph_config<'a>(
            &'a self,
            tenant: &'a TenantContext,
            graph_id: &'a str,
        ) -> BoxFut<'a, Result<GraphConfig, ConfigError>> {
            Box::pin(async move {
                match self.primary.graph_config(tenant, graph_id).await {
                    Ok(cfg) => Ok(cfg),
                    Err(ConfigError::AgentNotFound(_)) | Err(ConfigError::Internal(_)) => {
                        self.fallback.graph_config(tenant, graph_id).await
                    }
                    // Misconfigured + any future ConfigError variant: surface,
                    // never mask. New variants fall through here and propagate
                    // by default; revisit only if a new variant should fall back.
                    Err(other) => Err(other),
                }
            })
        }
    }

    // -----------------------------------------------------------------------
    // Admin graph registry wiring
    // -----------------------------------------------------------------------

    /// Build a cached [`HttpGraphProvider`] from `GREENTIC_AW_ADMIN_ENDPOINT` +
    /// `GREENTIC_AW_ADMIN_TOKEN`. Returns `None` when either is unset/empty,
    /// so the runtime keeps using the local pack provider alone.
    ///
    /// Mirrors [`super::agent_node::registry_from_env`] exactly — same env
    /// vars, same "both must be present" semantics.
    fn graph_registry_from_env() -> Option<CachingGraphProvider<HttpGraphProvider>> {
        let endpoint = std::env::var("GREENTIC_AW_ADMIN_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty())?;
        let token = std::env::var("GREENTIC_AW_ADMIN_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())?;
        Some(CachingGraphProvider::new(HttpGraphProvider::new(
            endpoint, token,
        )))
    }

    // -----------------------------------------------------------------------
    // Pack-sidecar graph loader
    // -----------------------------------------------------------------------

    /// Parse an `agent-graph.json` sidecar (raw bytes from a `.gtpack`) into a
    /// validated [`GraphConfig`].
    ///
    /// Mirrors the lenient posture of
    /// [`super::super::agent_node::agent_configs_from_manifest`]: every failure
    /// mode — invalid UTF-8, malformed JSON, schema-version mismatch, or graph
    /// validation error — is logged via [`tracing::warn!`] (with `pack_id`) and
    /// yields `None`, so a single bad graph never prevents the rest of the pack
    /// from loading. The `pack_id` argument is used only in log messages.
    pub fn graph_config_from_sidecar(pack_id: &str, bytes: &[u8]) -> Option<GraphConfig> {
        let raw = match std::str::from_utf8(bytes) {
            Ok(raw) => raw,
            Err(error) => {
                tracing::warn!(
                    pack_id,
                    error = %error,
                    "skipping agent-graph sidecar: not valid UTF-8"
                );
                return None;
            }
        };
        match GraphConfig::from_json(raw) {
            Ok(config) => Some(config),
            Err(error) => {
                tracing::warn!(
                    pack_id,
                    error = %error,
                    "skipping malformed agent-graph sidecar in pack"
                );
                None
            }
        }
    }

    // -----------------------------------------------------------------------
    // Production graph-handler construction
    // -----------------------------------------------------------------------

    /// Build the production `DwAgentGraph` handler if the environment is
    /// configured. Mirrors
    /// [`super::super::agent_node::build_agent_node_handler`]: it returns `None`
    /// (so `DwAgentGraph` flow dispatch errors clearly) under any of these
    /// graceful-degradation conditions:
    /// - `graphs` is empty (no graphs from any pack sidecar);
    /// - `GREENTIC_AW_REDIS_URL` is unset/empty;
    /// - the AW Redis connection fails;
    /// - the extension runtime fails to initialise.
    ///
    /// The shared runtime Arcs (state store, ext runtime, LLM backend,
    /// telemetry, token meter, ledger) are built exactly as for the single-agent
    /// path; the durable checkpoint store reuses the same multiplexed Redis
    /// connection manager.
    pub async fn build_graph_node_handler(
        graphs: HashMap<String, GraphConfig>,
        audit_sink: Option<AuditSink>,
        packs: Arc<Vec<Arc<crate::pack::PackRuntime>>>,
        merged_agents: HashMap<String, AgentConfig>,
    ) -> Option<Arc<dyn GraphNodeHandler>> {
        use crate::runner::aw_backends::{AwBackends, build_aw_backends};
        use greentic_aw_runtime::OtelTelemetry;

        if graphs.is_empty() {
            return None; // nothing to serve
        }

        let AwBackends {
            state_store,
            token_meter,
            tool_ledger: ledger,
            checkpoint_store: checkpoint,
        } = build_aw_backends().await?;

        // Process-level graph serve path has no per-tenant secrets context;
        // tool secrets resolve from the env only. It likewise has no embedding
        // host, so the ext LLM port falls back to the env-keyed `EnvLlmPort`.
        let ext_runtime = super::super::agent_node::build_ext_runtime(
            std::sync::Arc::new(super::super::agent_node::EnvSecretsBackend),
            None,
            &packs,
        )?;
        let llm = super::super::agent_node::build_llm_backend(&ext_runtime);
        let telemetry = Arc::new(OtelTelemetry);

        let graph_count = graphs.len();
        // Build the graph config source: HTTP registry (cached) primary →
        // InMemoryGraphProvider (local packs) fallback, when the admin env
        // vars are configured; otherwise local packs only.
        //
        // Mirrors the agent-config composition in
        // `agent_node::build_agent_node_handler`:
        //   Some(http) → CachingConfigProvider(Layered(http, overlay))
        //   None       → CachingConfigProvider(overlay)
        //
        // The graph path skips the ManifestToolOverlay (no manifest overlay
        // exists for graphs) and the CachingGraphProvider is already embedded
        // inside the `CachingGraphProvider<HttpGraphProvider>` returned by
        // `graph_registry_from_env`. The local InMemoryGraphProvider needs no
        // separate cache (it is in-memory and O(1)).
        let local = InMemoryGraphProvider::new(graphs);
        let provider: Arc<dyn GraphConfigSource> = match graph_registry_from_env() {
            Some(http) => Arc::new(LayeredGraphProvider::new(http, local)),
            None => Arc::new(local),
        };

        let handler = RuntimeGraphNodeHandler::from_parts(
            provider,
            checkpoint,
            state_store,
            ext_runtime,
            llm,
            telemetry,
            token_meter,
            ledger,
            audit_sink,
            packs,
            Arc::new(merged_agents),
        );

        tracing::info!(graph_count, "AW graph runtime constructed");
        Some(Arc::new(handler))
    }

    // -----------------------------------------------------------------------
    // RuntimeGraphNodeHandler
    // -----------------------------------------------------------------------

    /// Supplies the per-visit [`AgentTurnFn`]/[`SupervisorFn`] closures used by
    /// [`RuntimeGraphNodeHandler::execute`].
    ///
    /// `execute` already receives the graph run's real `tenant_id`/`env_id`
    /// (see its signature), but [`AgentTurnFn`]/[`SupervisorFn`] (from
    /// `greentic-aw-runtime`, not modifiable here) only pass an
    /// [`AgentTurnRequest`]/[`SupervisorRequest`] at invocation time — neither
    /// carries a tenant. So the audit sink + real tenant cannot be baked into
    /// a closure built once at handler-construction time and reused
    /// call-after-call; instead `execute` asks this seam for a *fresh*
    /// closure on every call, passing the sink + the real tenant it just
    /// resolved from its own parameters (EPIC-B B-3b Task 1 plumbing; Task 2
    /// makes `run_one_agent_turn`/`run_one_supervisor_turn` actually build an
    /// `AgentAuditObserver` from them instead of ignoring them).
    ///
    /// [`RuntimeTurnSource`] (production) rebuilds via
    /// [`build_agent_turn`]/[`build_supervisor`] from the shared runtime Arcs
    /// every call. [`FixedTurnSource`] (test-only, via
    /// [`RuntimeGraphNodeHandler::with_effects`]) ignores the sink/tenant and
    /// hands back the same injected closures every time, preserving today's
    /// deterministic-effect test style untouched.
    trait TurnEffectSource: Send + Sync {
        fn agent_turn(
            &self,
            audit_sink: Option<AuditSink>,
            real_tenant: greentic_types::TenantCtx,
        ) -> AgentTurnFn;
        fn supervisor(
            &self,
            audit_sink: Option<AuditSink>,
            real_tenant: greentic_types::TenantCtx,
        ) -> SupervisorFn;
    }

    /// Production [`TurnEffectSource`]: holds the constituent runtime Arcs
    /// (extension runtime, LLM backend, telemetry, token meter, ledger; the
    /// state store lives directly on [`RuntimeGraphNodeHandler`]) so it can
    /// build a lightweight [`AgentRuntime`] *per agent visit* with a fresh
    /// single-entry [`InMemoryConfigProvider`] — mirroring the
    /// greentic-designer spike's per-visit runtime construction.
    struct RuntimeTurnSource {
        state_store: Arc<dyn AgentStateStore>,
        ext_runtime: Arc<ExtensionRuntime>,
        llm: Arc<dyn LlmBackend>,
        telemetry: Arc<dyn Telemetry>,
        token_meter: Arc<dyn TokenMeter>,
        ledger: Arc<dyn ToolLedger>,
        /// MCP tool source, built ONCE (mirrors the other Arcs) so its 5-min
        /// TTL catalog cache + warmed HTTP client are reused across every
        /// graph-agent visit rather than rebuilt (empty-cache) per turn.
        mcp_source: Option<Arc<greentic_aw_runtime::McpToolSource>>,
        /// The tenant's loaded packs, used to build a fresh, tenant-scoped
        /// [`greentic_aw_runtime::ComponentToolSource`] on EVERY turn (unlike
        /// `mcp_source`, this is NOT built once — the component source is
        /// tenant-pinned, and building it per turn from the in-memory pack
        /// list is cheap; see [`run_one_agent_turn`]).
        packs: Arc<Vec<Arc<crate::pack::PackRuntime>>>,
        /// Process-level merged agent configs (pack + operator-overridden),
        /// threaded into [`run_one_agent_turn`] so a graph node's `agent_ref`
        /// resolves to that agent's full [`AgentConfig`] at turn time.
        merged_agents: Arc<HashMap<String, AgentConfig>>,
    }

    impl TurnEffectSource for RuntimeTurnSource {
        fn agent_turn(
            &self,
            audit_sink: Option<AuditSink>,
            real_tenant: greentic_types::TenantCtx,
        ) -> AgentTurnFn {
            build_agent_turn(
                self.state_store.clone(),
                self.ext_runtime.clone(),
                self.llm.clone(),
                self.telemetry.clone(),
                self.token_meter.clone(),
                self.ledger.clone(),
                self.mcp_source.clone(),
                self.packs.clone(),
                self.merged_agents.clone(),
                audit_sink,
                real_tenant,
            )
        }

        fn supervisor(
            &self,
            audit_sink: Option<AuditSink>,
            real_tenant: greentic_types::TenantCtx,
        ) -> SupervisorFn {
            build_supervisor(
                self.state_store.clone(),
                self.ext_runtime.clone(),
                self.llm.clone(),
                self.telemetry.clone(),
                self.token_meter.clone(),
                self.ledger.clone(),
                self.merged_agents.clone(),
                audit_sink,
                real_tenant,
            )
        }
    }

    /// **Test-only** [`TurnEffectSource`] that always returns the same
    /// injected closures, ignoring the audit sink/tenant. Lets
    /// [`RuntimeGraphNodeHandler::with_effects`] keep injecting deterministic
    /// effects without needing real `AgentRuntime` Arcs.
    #[cfg(test)]
    struct FixedTurnSource {
        agent_turn: AgentTurnFn,
        supervisor: SupervisorFn,
    }

    #[cfg(test)]
    impl TurnEffectSource for FixedTurnSource {
        fn agent_turn(
            &self,
            _audit_sink: Option<AuditSink>,
            _real_tenant: greentic_types::TenantCtx,
        ) -> AgentTurnFn {
            self.agent_turn.clone()
        }

        fn supervisor(
            &self,
            _audit_sink: Option<AuditSink>,
            _real_tenant: greentic_types::TenantCtx,
        ) -> SupervisorFn {
            self.supervisor.clone()
        }
    }

    /// Production [`GraphNodeHandler`] wrapping the durable graph executor.
    ///
    /// Tool dispatch goes straight through the shared [`ExtensionRuntime`] via
    /// [`dispatch_tool_call`].
    pub struct RuntimeGraphNodeHandler {
        graphs: Arc<dyn GraphConfigSource>,
        checkpoint: Arc<dyn CheckpointStore>,
        state_store: Arc<dyn AgentStateStore>,
        turn_source: Arc<dyn TurnEffectSource>,
        tool: ToolFn,
        approval: ApprovalFn,
        /// Best-effort agent-step audit sink (EPIC-B B-3b), threaded from
        /// [`build_graph_node_handler`]. `None` when no NATS audit client is
        /// configured (`GREENTIC_EVENTS_NATS_URL` unset/unreachable) — the
        /// `run_agent_step` seam then stays on the plain `.step()` path,
        /// byte-identical regardless of this field's value.
        audit_sink: Option<AuditSink>,
    }

    impl RuntimeGraphNodeHandler {
        /// Build a handler from the runtime's constituent parts.
        ///
        /// **Deviation from the Task-7 `from_runtime(runtime: Arc<AgentRuntime>, …)`
        /// signature:** `AgentRuntime`'s inner Arcs (`ext_runtime`, `llm`,
        /// `telemetry`, `token_meter`, `ledger`) are `pub(crate)` and cannot be
        /// extracted from an `Arc<AgentRuntime>` outside the `greentic-aw-runtime`
        /// crate, so a per-visit runtime cannot be reconstructed from a shared
        /// `AgentRuntime`. The task explicitly allows this: "an acceptable
        /// alternative is a `from_parts(...)` constructor taking those Arcs
        /// directly." The pack loader (Task 8) already holds these Arcs when it
        /// would otherwise build an `AgentRuntime`, so it wires them here instead.
        #[allow(clippy::too_many_arguments)]
        pub fn from_parts(
            graphs: Arc<dyn GraphConfigSource>,
            checkpoint: Arc<dyn CheckpointStore>,
            state_store: Arc<dyn AgentStateStore>,
            ext_runtime: Arc<ExtensionRuntime>,
            llm: Arc<dyn LlmBackend>,
            telemetry: Arc<dyn Telemetry>,
            token_meter: Arc<dyn TokenMeter>,
            ledger: Arc<dyn ToolLedger>,
            audit_sink: Option<AuditSink>,
            packs: Arc<Vec<Arc<crate::pack::PackRuntime>>>,
            merged_agents: Arc<HashMap<String, AgentConfig>>,
        ) -> Self {
            let tool = build_tool(ext_runtime.clone());
            // Built once here (like the other Arcs) so the MCP catalog cache +
            // HTTP client are reused across every graph-agent turn.
            // No per-tenant secrets manager reachable in this constructor
            // (out of scope for this task — see `agent_node::mcp_source_from_env`'s
            // doc comment); matches this path's pre-existing behavior.
            let mcp_source = super::super::agent_node::mcp_source_from_env(None);
            let turn_source: Arc<dyn TurnEffectSource> = Arc::new(RuntimeTurnSource {
                state_store: state_store.clone(),
                ext_runtime,
                llm,
                telemetry,
                token_meter,
                ledger,
                mcp_source,
                packs,
                merged_agents,
            });
            // NOTE: the real `ApprovalFn` is designer-provided (it wires the
            // `greentic.approval.request.v1` / `.response.v1` NATS round trip
            // so a human decision can arrive asynchronously). This in-process
            // runner-host path does not yet have that transport wired up, so
            // it always parks (`Awaiting`) — safe (a graph run simply stays
            // `AwaitingInput`, and `derive_run_id` resumes rather than forks
            // it on every later call for the same session) but not yet
            // resolvable from here. A future designer-side approval bridge
            // (subscriber + node) replaces this with a real consumer.
            let approval = default_approval_awaiting();
            Self {
                graphs,
                checkpoint,
                state_store,
                turn_source,
                tool,
                approval,
                audit_sink,
            }
        }

        /// **Test-only** constructor that injects effect closures directly,
        /// bypassing per-visit [`AgentRuntime`] construction. Not available
        /// outside `#[cfg(test)]`; production code must use [`from_parts`].
        ///
        /// [`from_parts`]: RuntimeGraphNodeHandler::from_parts
        #[cfg(test)]
        #[allow(clippy::too_many_arguments)]
        pub(crate) fn with_effects(
            graphs: Arc<dyn GraphConfigSource>,
            checkpoint: Arc<dyn CheckpointStore>,
            state_store: Arc<dyn AgentStateStore>,
            agent_turn: AgentTurnFn,
            tool: ToolFn,
            supervisor: SupervisorFn,
            approval: ApprovalFn,
        ) -> Self {
            Self {
                graphs,
                checkpoint,
                state_store,
                turn_source: Arc::new(FixedTurnSource {
                    agent_turn,
                    supervisor,
                }),
                tool,
                approval,
                audit_sink: None,
            }
        }

        /// Resolve the run id to drive for this `(session, graph)` and whether
        /// the existing record (if any) is mid-flight.
        ///
        /// Base run id is `"{safe_session}__{graph_id}"` where `safe_session` is
        /// the incoming `session_id` with every `':'` replaced by `'_'`.
        ///
        /// **Why sanitize `':'`?**  Flow session ids frequently embed channel
        /// correlation ids that contain colons (e.g. MS Teams thread ids such as
        /// `msteams:thread:19xyz`). The checkpoint store key contract forbids
        /// `':'`, so using the raw session id causes every such graph run to fail
        /// at save-time. The replacement is stable: the same incoming
        /// `session_id` always maps to the same `safe_session`, so checkpoint
        /// resume works correctly across calls.
        ///
        /// When the base slot is already terminal (`Succeeded`/`Failed`), retry
        /// `"{safe_session}__{graph_id}__{n}"` for `n = 2..` until a slot is
        /// absent (→ start fresh) or non-terminal — `Running` or `AwaitingInput`
        /// (→ resume). Bounded at
        /// [`MAX_RUN_ID_SUFFIX`]. Exhausting all suffixes returns
        /// [`RunIdError::SuffixExhausted`] (distinct from
        /// [`GraphExecError::IterationCap`] so the caller can surface a
        /// targeted reply).
        async fn derive_run_id(
            &self,
            tenant: &TenantContext,
            graph_id: &str,
            session_id: &str,
        ) -> Result<RunSlot, RunIdError> {
            // Sanitize colons: flow session ids (e.g. Teams thread correlation
            // ids) may contain ':' which the checkpoint key contract forbids.
            let safe_session = session_id.replace(':', "_");
            let base = format!("{safe_session}__{graph_id}");
            match self.checkpoint.load(tenant, &base).await? {
                None => return Ok(RunSlot::start(base)),
                // `AwaitingInput` is a parked, non-terminal state (the run is
                // paused at an approval node, not done) — same resume
                // treatment as `Running`. Without this arm, every call after
                // a park would fall through to the terminal branch below and
                // mint a fresh `__n` run id, orphaning the parked run forever
                // (it can never be decided because nothing ever resumes it)
                // and silently forking the conversation.
                Some(rec)
                    if matches!(rec.status, RunStatus::Running | RunStatus::AwaitingInput) =>
                {
                    return Ok(RunSlot::resume(base));
                }
                Some(_) => {}
            }
            for n in 2..=MAX_RUN_ID_SUFFIX {
                let candidate = format!("{safe_session}__{graph_id}__{n}");
                match self.checkpoint.load(tenant, &candidate).await? {
                    None => return Ok(RunSlot::start(candidate)),
                    Some(rec)
                        if matches!(rec.status, RunStatus::Running | RunStatus::AwaitingInput) =>
                    {
                        return Ok(RunSlot::resume(candidate));
                    }
                    Some(_) => continue,
                }
            }
            // All suffix slots are occupied (terminal). This is a distinct
            // error from the graph's own IterationCap: it means the session has
            // exhausted the run-id namespace, not that any single run looped.
            Err(RunIdError::SuffixExhausted { base_run_id: base })
        }
    }

    /// Outcome of [`RuntimeGraphNodeHandler::derive_run_id`]: which run id to
    /// drive and whether to start it fresh or resume an in-flight record.
    struct RunSlot {
        run_id: String,
        resume: bool,
    }

    impl RunSlot {
        fn start(run_id: String) -> Self {
            Self {
                run_id,
                resume: false,
            }
        }
        fn resume(run_id: String) -> Self {
            Self {
                run_id,
                resume: true,
            }
        }
    }

    #[async_trait::async_trait]
    impl GraphNodeHandler for RuntimeGraphNodeHandler {
        async fn execute(
            &self,
            tenant_id: &str,
            env_id: &str,
            graph_id: &str,
            session_id: &str,
            flow_input: &Value,
        ) -> Result<Value> {
            // Contract mirror of agent_node.rs: a missing/empty `user_text`
            // resolves to an empty string and the run proceeds (it never returns
            // Err for this case).
            let user_text = flow_input
                .get("user_text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();

            let tenant = TenantContext::new(tenant_id, env_id);

            // Resolve the graph. A missing graph is a user-facing "unavailable"
            // outcome, never an Err.
            let cfg = match self.graphs.graph_config(&tenant, graph_id).await {
                Ok(cfg) => cfg,
                Err(error) => {
                    tracing::warn!(error = %error, graph_id, "graph config not found");
                    return Ok(error_envelope(GRAPH_UNAVAILABLE_REPLY));
                }
            };

            // Acquire the session lock before driving (mirrors the single-agent
            // loop's default 5s wait). Held across the drive via the guard.
            let _lock = match self
                .state_store
                .acquire_lock(&tenant, session_id, Duration::from_secs(5))
                .await
            {
                Ok(guard) => guard,
                Err(error) => {
                    tracing::warn!(error = %error, graph_id, session_id, "session lock failed");
                    return Ok(error_envelope(SANITISED_ERROR_REPLY));
                }
            };

            let slot = match self.derive_run_id(&tenant, graph_id, session_id).await {
                Ok(slot) => slot,
                Err(RunIdError::SuffixExhausted { .. }) => {
                    tracing::warn!(
                        graph_id,
                        session_id,
                        "all run-id slots exhausted for session"
                    );
                    return Ok(error_envelope(
                        "Too many runs for this session. Please start a new conversation.",
                    ));
                }
                Err(RunIdError::Checkpoint(error)) => {
                    tracing::warn!(error = %error, graph_id, session_id, "run-id derivation failed");
                    return Ok(error_envelope(SANITISED_ERROR_REPLY));
                }
            };

            // The real tenant/env `execute` already received — the same one
            // driving executor/checkpoint/run-id above. (The synthetic
            // `"graph"`/`"run"` tenant is built *inside* the turn functions,
            // for per-visit AgentRuntime *state* only.) Rebuilt fresh on
            // every call — see `TurnEffectSource` — so the sink + real tenant
            // reach `run_one_agent_turn`/`run_one_supervisor_turn`, which feed
            // them into the shared `run_agent_step` seam that builds the
            // `AgentAuditObserver` (EPIC-B B-3b Task 2).
            let real_tenant = tenant_ctx_for_audit(tenant_id, env_id);
            let agent_turn = self
                .turn_source
                .agent_turn(self.audit_sink.clone(), real_tenant.clone());
            let supervisor = self
                .turn_source
                .supervisor(self.audit_sink.clone(), real_tenant);

            let executor = GraphExecutor::new(
                self.checkpoint.clone(),
                agent_turn,
                self.tool.clone(),
                supervisor,
                self.approval.clone(),
            );

            let result = if slot.resume {
                executor.resume(&tenant, &slot.run_id).await
            } else {
                executor
                    .start(&tenant, &slot.run_id, &cfg, &user_text)
                    .await
            };

            match result {
                // `AwaitingInput` means the run parked at an approval node
                // (see `default_approval_awaiting` below): it is neither a
                // completed respond nor a failure, so it must not be
                // reported as `terminated_by: "respond"` (a caller branching
                // on that value would wrongly treat the pause as done) nor
                // surfaced as an error. This runner-host path has no
                // approval-transport integration yet (that lands with the
                // designer NATS bridge), so a parked run has no other
                // observable signal beyond this envelope + the warning
                // below — the run itself remains durably `AwaitingInput` in
                // the checkpoint store and `derive_run_id` resumes it (not
                // forks a new run) on every subsequent call for the same
                // session until a decision arrives.
                Ok(outcome) if outcome.status == RunStatus::AwaitingInput => {
                    tracing::warn!(
                        graph_id,
                        session_id,
                        run_id = %slot.run_id,
                        "graph run parked awaiting human approval; no approval \
                         transport is wired on this in-process runner-host path, \
                         so the run stays AwaitingInput until an external decision \
                         resolves it"
                    );
                    Ok(json!({
                        "reply": outcome.reply,
                        "trail": outcome.trail,
                        "terminated_by": "awaiting_approval",
                    }))
                }
                Ok(outcome) => Ok(json!({
                    "reply": outcome.reply,
                    "trail": outcome.trail,
                    "terminated_by": "respond",
                })),
                Err(error) => {
                    tracing::warn!(error = %error, graph_id, session_id, "graph run failed");
                    // The DwAgent envelope only carries "respond" / "error".
                    // IterationCap (graph's own node-visit limit) gets a distinct,
                    // still user-safe reply; every other runtime failure falls back
                    // to the generic sanitised message.
                    let reply = match error {
                        GraphExecError::IterationCap { .. } => {
                            "This is taking longer than expected. Please try again."
                        }
                        _ => SANITISED_ERROR_REPLY,
                    };
                    Ok(error_envelope(reply))
                }
            }
        }
    }

    /// Build the `{"reply", "trail", "terminated_by": "error"}` envelope. The
    /// trail is empty: a failed/unavailable run surfaces no completed-node trail
    /// to the flow output (the detail is logged instead).
    fn error_envelope(reply: &str) -> Value {
        json!({
            "reply": reply,
            "trail": Vec::<Value>::new(),
            "terminated_by": "error",
        })
    }

    /// Seed a fresh [`ConversationState`] from the graph run's message log so the
    /// per-visit agent has full context (including after a checkpoint resume).
    ///
    /// Tool-role graph messages are folded in as a plain user-context line
    /// rather than a `ChatMessage::Tool` (which needs a `call_id` paired to an
    /// assistant `tool_calls[]` entry the graph state does not track) — matching
    /// the designer spike.
    fn seed_state_from_graph(
        tenant: &TenantContext,
        session_id: &str,
        state: &GraphRunState,
    ) -> ConversationState {
        let mut seeded = ConversationState::empty(tenant, session_id);
        for m in &state.messages {
            match m.role {
                GraphRole::User => seeded.messages.push(ChatMessage::User {
                    content: m.content.clone(),
                }),
                GraphRole::Assistant => seeded.messages.push(ChatMessage::Assistant {
                    content: m.content.clone(),
                    tool_calls: vec![],
                }),
                GraphRole::Tool => seeded.messages.push(ChatMessage::User {
                    content: format!("Tool result: {}", m.content),
                }),
            }
        }
        seeded
    }

    /// Build the [`AgentTurnFn`] effect closure.
    ///
    /// Each invocation constructs a lightweight [`AgentRuntime`] with a fresh
    /// single-entry [`InMemoryConfigProvider`] (agent id `"{graph_id}.{node_id}"`,
    /// the node's system prompt, no tools — the graph owns tool dispatch via the
    /// explicit Tool node) reusing the shared runtime Arcs. Conversation context
    /// is re-seeded from the durable [`GraphRunState`] on every visit; the
    /// per-visit session id is `"{session_id}__{graph_id}__{node_id}"`.
    #[allow(clippy::too_many_arguments)]
    fn build_agent_turn(
        state_store: Arc<dyn AgentStateStore>,
        ext_runtime: Arc<ExtensionRuntime>,
        llm: Arc<dyn LlmBackend>,
        telemetry: Arc<dyn Telemetry>,
        token_meter: Arc<dyn TokenMeter>,
        ledger: Arc<dyn ToolLedger>,
        mcp_source: Option<Arc<greentic_aw_runtime::McpToolSource>>,
        packs: Arc<Vec<Arc<crate::pack::PackRuntime>>>,
        merged_agents: Arc<HashMap<String, AgentConfig>>,
        audit_sink: Option<AuditSink>,
        real_tenant: greentic_types::TenantCtx,
    ) -> AgentTurnFn {
        Arc::new(move |req: AgentTurnRequest| {
            let state_store = state_store.clone();
            let ext_runtime = ext_runtime.clone();
            let llm = llm.clone();
            let telemetry = telemetry.clone();
            let token_meter = token_meter.clone();
            let ledger = ledger.clone();
            let mcp_source = mcp_source.clone();
            let packs = packs.clone();
            let merged_agents = merged_agents.clone();
            let audit_sink = audit_sink.clone();
            let real_tenant = real_tenant.clone();
            Box::pin(async move {
                run_one_agent_turn(
                    req,
                    state_store,
                    ext_runtime,
                    llm,
                    telemetry,
                    token_meter,
                    ledger,
                    mcp_source,
                    packs,
                    merged_agents,
                    audit_sink.as_ref(),
                    &real_tenant,
                )
                .await
            }) as BoxFut<'static, Result<AgentTurnResult, GraphExecError>>
        })
    }

    /// Drive one graph-node agent step: [`AgentRuntime::step`] when no audit
    /// sink is configured, or [`AgentRuntime::step_with_observer`] with an
    /// [`AgentAuditObserver`] when one is (EPIC-B B-3b Task 2). Shared by
    /// [`run_one_agent_turn`] and [`run_one_supervisor_turn`] so the
    /// audit-injection seam is defined exactly once.
    ///
    /// `tenant`/`session_id`/`agent_id` are the per-visit SYNTHETIC state
    /// identifiers (`TenantContext::new("graph", "run")` + the derived
    /// session/agent ids) — unchanged by this wiring, since the
    /// `AgentRuntime`'s state store must keep using them for per-visit
    /// durability. `real_tenant` is used ONLY to build the audit observer's
    /// `TenantCtx`, so audit events publish under the real tenant while turn
    /// state stays keyed under "graph"/"run".
    ///
    /// Off by default: when `audit_sink` is `None` (no NATS audit client
    /// configured), this is exactly `runtime.step(...)` — byte-identical to
    /// the call before this observer existed.
    async fn run_agent_step(
        runtime: &AgentRuntime,
        tenant: TenantContext,
        session_id: &str,
        agent_id: &str,
        input: AgentInput,
        audit_sink: Option<&AuditSink>,
        real_tenant: &greentic_types::TenantCtx,
    ) -> std::result::Result<AgentOutput, AgentError> {
        match audit_sink {
            Some(sink) => {
                let observer: Arc<dyn StepObserver> = Arc::new(AgentAuditObserver::new(
                    sink.clone(),
                    real_tenant.clone(),
                    agent_id.to_string(),
                    session_id.to_string(),
                ));
                runtime
                    .step_with_observer(tenant, session_id, agent_id, input, observer)
                    .await
            }
            None => runtime.step(tenant, session_id, agent_id, input).await,
        }
    }

    /// Parse a graph node's declared tool reference string into a [`ToolRef`].
    ///
    /// Format convention: `"<extension_id>/<tool_name>"`, split on the LAST
    /// `/` (so an `extension_id` that itself contains `/`, e.g.
    /// `"component:owner/repo"`, is preserved intact). Returns `None` — with a
    /// `tracing::warn!` naming the skipped string — when there is no `/`, or
    /// when either side of the split is empty. Never panics: this runs against
    /// node-authored config, not host-controlled input.
    fn parse_tool_ref(s: &str) -> Option<ToolRef> {
        match s.rsplit_once('/') {
            Some((extension_id, tool_name))
                if !extension_id.is_empty() && !tool_name.is_empty() =>
            {
                Some(ToolRef {
                    extension_id: extension_id.to_string(),
                    tool_name: tool_name.to_string(),
                    description: None,
                    input_schema: None,
                    usage_note: None,
                })
            }
            _ => {
                tracing::warn!(tool_ref = %s, "skipping malformed graph-node tool ref (expected \"<extension_id>/<tool_name>\")");
                None
            }
        }
    }

    /// Map a graph node's declared `tools: Vec<String>` (see
    /// [`AgentTurnRequest::tools`]) to the `Vec<ToolRef>` the ephemeral
    /// per-visit [`AgentConfig`] expects. Malformed entries are dropped by
    /// [`parse_tool_ref`] (warned, never fatal) rather than failing the turn.
    fn map_tool_refs(tools: &[String]) -> Vec<ToolRef> {
        tools.iter().filter_map(|t| parse_tool_ref(t)).collect()
    }

    /// The [`AgentConfig`] a graph agent-turn runs.
    ///
    /// `req.agent_ref` present (`Some(id)`) → the full published config
    /// resolved from `merged` (memory/knowledge/guardrails intact, exactly as
    /// stored) — errors when `id` is not in the map. `req.agent_ref` absent
    /// (`None`) → the stripped ephemeral config fabricated from the node's
    /// own inline fields (today's unchanged one-shot behaviour: no
    /// guardrails, no memory, no knowledge).
    fn resolve_turn_config(
        req: &AgentTurnRequest,
        merged: &HashMap<String, AgentConfig>,
        agent_id: &str,
    ) -> Result<AgentConfig, GraphExecError> {
        if let Some(id) = req.agent_ref.as_deref() {
            return merged.get(id).cloned().ok_or_else(|| {
                GraphExecError::AgentTurn(format!("referenced agent '{id}' not found"))
            });
        }
        // `inherit_from` is the specialist case: keep this node's own prompt and
        // model — that is what makes a specialist a specialist — but adopt the
        // referenced worker's capability surface. Using `agent_ref` here instead
        // would overwrite the prompt and collapse every specialist in a
        // generated graph into an identical copy of its parent.
        //
        // Like the supervisor path, an id that does not resolve FAILS rather
        // than running stripped: a specialist believed to be guarded and
        // tool-equipped but running bare is worse than one that never claimed
        // to be.
        if let Some(id) = req.inherit_from.as_deref() {
            let parent = merged.get(id).ok_or_else(|| {
                GraphExecError::AgentTurn(format!(
                    "node '{}': inherit_from '{}' not found in merged agents; \
                     refusing to run without its tools and guardrails",
                    req.node_id, id
                ))
            })?;
            return Ok(AgentConfig {
                agent_id: agent_id.to_string(),
                system_prompt: req.system_prompt.clone(),
                llm: LlmProviderRef {
                    provider: req.provider.clone().unwrap_or_else(|| "openai".into()),
                    model: req.model.clone(),
                    credential_ref: None,
                },
                // Inherited capability surface.
                tools: parent.tools.clone(),
                guardrails: parent.guardrails.clone(),
                memory: parent.memory.clone(),
                knowledge: parent.knowledge.clone(),
                limits: parent.limits.clone(),
                conversational: false,
                opening_message: None,
            });
        }
        Ok(AgentConfig {
            agent_id: agent_id.to_string(),
            system_prompt: req.system_prompt.clone(),
            tools: map_tool_refs(&req.tools),
            guardrails: vec![],
            llm: LlmProviderRef {
                provider: req.provider.clone().unwrap_or_else(|| "openai".into()),
                model: req.model.clone(),
                credential_ref: None,
            },
            limits: AgentLimits::default(),
            memory: None,
            knowledge: None,
            conversational: false,
            opening_message: None,
        })
    }

    /// Drive one [`AgentRuntime::step`] for an agent-node visit. Derives the
    /// agent/session ids, seeds conversation context, runs the step, and maps the
    /// reply's resolution sentinel.
    ///
    /// `audit_sink`/`real_tenant` are forwarded to [`run_agent_step`], which
    /// injects an [`AgentAuditObserver`] built from `real_tenant` when a sink
    /// is configured (EPIC-B B-3b Task 2).
    ///
    /// When `req.agent_ref` is `Some`, [`resolve_turn_config`] resolves the
    /// full published `AgentConfig` from `merged_agents` and the runtime is
    /// built with the same fidelity as `agent_node::build_agent_runtime`
    /// (guardrails + short-term memory + feature-gated long-term/knowledge).
    /// `agent_ref: None` stays byte-unchanged: the stripped ephemeral config,
    /// no guardrails/memory/knowledge attached.
    #[allow(clippy::too_many_arguments)]
    async fn run_one_agent_turn(
        req: AgentTurnRequest,
        state_store: Arc<dyn AgentStateStore>,
        ext_runtime: Arc<ExtensionRuntime>,
        llm: Arc<dyn LlmBackend>,
        telemetry: Arc<dyn Telemetry>,
        token_meter: Arc<dyn TokenMeter>,
        ledger: Arc<dyn ToolLedger>,
        mcp_source: Option<Arc<greentic_aw_runtime::McpToolSource>>,
        packs: Arc<Vec<Arc<crate::pack::PackRuntime>>>,
        merged_agents: Arc<HashMap<String, AgentConfig>>,
        audit_sink: Option<&AuditSink>,
        real_tenant: &greentic_types::TenantCtx,
    ) -> Result<AgentTurnResult, GraphExecError> {
        // The request carries no tenant/graph/session — they live on the
        // GraphRunState's seeded session. The executor seeds the run state with
        // the tenant-scoped messages, so we reconstruct a turn-scoped tenant +
        // session purely for the per-visit runtime. Conversation durability is
        // owned by the graph checkpoint, not this ephemeral store.
        let tenant = TenantContext::new("graph", "run");
        let session_id = format!("graph__{}", req.node_id);
        let agent_id = format!("graph.{}", req.node_id);

        let seeded = seed_state_from_graph(&tenant, &session_id, &req.state);
        if let Err(error) = state_store.save(&tenant, &session_id, &seeded).await {
            return Err(GraphExecError::AgentTurn(format!(
                "seeding conversation state: {error}"
            )));
        }

        let cfg = resolve_turn_config(&req, &merged_agents, &agent_id)?;
        // Key the config provider — and the `.step()` call — by the id the
        // runtime will actually resolve config under: the referenced agent's
        // own id when `agent_ref` is set (so guardrail/memory/knowledge
        // bindings on that published agent are addressed correctly), else the
        // synthetic per-node id used today.
        let step_agent_id = req.agent_ref.clone().unwrap_or_else(|| agent_id.clone());
        let mut provider = InMemoryConfigProvider::new();
        provider.insert(&tenant, &step_agent_id, cfg.clone());

        // TODO(guardrail): inline (agent_ref: None) graph-node AgentRuntime bypasses
        // guardrails (v1 scope is dw.agent flow nodes only). A referenced agent
        // (agent_ref: Some) gets full guardrail/memory/knowledge fidelity below.
        // MCP tool source: mirror the `dw.agent` path so a graph agent declaring an
        // `mcp:<server>/<tool>` ref resolves it against the tenant's registered MCP
        // servers (same double-authorization: tenant registers the server + the node's
        // tool allowlist must reference it). `None` unless the admin creds are set.
        //
        // Component tool source: unlike `mcp_source` (built once, reused across
        // every graph-agent visit), the component source is tenant-pinned, so it
        // is rebuilt PER TURN from the in-memory `packs` list — cheap, and keeps
        // a `component:<ref>` tool ref scoped to the real tenant rather than the
        // synthetic "graph"/"run" state tenant used above.
        let component_source = super::super::agent_node::component_source_from_packs(
            &packs,
            real_tenant.tenant_id.as_str(),
        );
        let mut runtime = AgentRuntime::new(
            Arc::new(provider),
            state_store,
            ext_runtime.clone(),
            llm,
            telemetry,
            token_meter,
            ledger,
            mcp_source,
        )
        .with_component_source(component_source);

        // Full fidelity ONLY for a referenced agent — mirrors
        // `agent_node::build_agent_runtime`'s guardrail/short-term-memory/
        // long-term/knowledge attachment sequence exactly. The `agent_ref:
        // None` inline path stays byte-unchanged (no guardrails/memory/
        // knowledge), preserving today's one-shot graph-node behaviour.
        if req.agent_ref.is_some() {
            runtime = runtime
                .with_guardrails(
                    Arc::new(greentic_aw_runtime::guardrail::StaticGuardrailPolicy(
                        cfg.guardrails.clone(),
                    )),
                    Arc::new(
                        greentic_aw_runtime::guardrail::ExtRuntimeGuardrailEvaluator {
                            ext_runtime: ext_runtime.clone(),
                        },
                    ),
                )
                .with_short_term_memory(Arc::new(
                    greentic_aw_runtime::memory::InMemoryMemoryProvider::new(),
                ));
            #[cfg(feature = "long-term-chronicle")]
            {
                runtime = crate::runner::long_term_memory::attach(runtime).await;
            }
            #[cfg(feature = "knowledge-chronicle")]
            {
                runtime = crate::runner::knowledge_mount::attach(runtime).await;
            }
            // Knowledge delegated to a design-extension tool. Not feature-gated
            // (see `knowledge_ext`); it wraps whatever the Chronicle mount above
            // left in place. Inside the `agent_ref.is_some()` arm with the rest
            // of the full-fidelity attachment sequence, so the `agent_ref: None`
            // inline path stays byte-unchanged.
            runtime = crate::runner::knowledge_ext::attach(runtime, ext_runtime.clone());
        }

        // The latest user turn is the most recent user message in the seeded log
        // (the executor pushes the initial user message and re-seeds it here);
        // pass an empty input so we do not duplicate it — the step appends the
        // input as a fresh user turn, so an empty string keeps the history intact.
        let input = AgentInput {
            text: String::new(),
            ..Default::default()
        };

        let out = run_agent_step(
            &runtime,
            tenant.clone(),
            &session_id,
            &step_agent_id,
            input,
            audit_sink,
            real_tenant,
        )
        .await
        .map_err(|e| GraphExecError::AgentTurn(format!("agent step failed: {e}")))?;

        Ok(AgentTurnResult {
            resolved: detect_resolved(&out.reply),
            reply: strip_sentinel(&out.reply),
        })
    }

    /// Routing sentinel that the supervisor LLM must include in its reply to
    /// select a branch. Parsed case-insensitively.
    const ROUTE_SENTINEL_PREFIX: &str = "[[ROUTE:";
    const ROUTE_SENTINEL_SUFFIX: &str = "]]";

    /// Build the [`SupervisorFn`] effect closure.
    ///
    /// Each invocation constructs a lightweight [`AgentRuntime`] (same pattern
    /// as [`build_agent_turn`]) with a system prompt that appends a route menu
    /// to the node's own `systemPrompt`. The reply is scanned for
    /// `[[ROUTE:<branch>]]`; if the sentinel is absent or the branch is unknown
    /// the executor falls back to the FIRST route with a `tracing::warn!`.
    #[allow(clippy::too_many_arguments)]
    fn build_supervisor(
        state_store: Arc<dyn AgentStateStore>,
        ext_runtime: Arc<ExtensionRuntime>,
        llm: Arc<dyn LlmBackend>,
        telemetry: Arc<dyn Telemetry>,
        token_meter: Arc<dyn TokenMeter>,
        ledger: Arc<dyn ToolLedger>,
        merged_agents: Arc<HashMap<String, AgentConfig>>,
        audit_sink: Option<AuditSink>,
        real_tenant: greentic_types::TenantCtx,
    ) -> SupervisorFn {
        Arc::new(move |req: SupervisorRequest| {
            let merged_agents = merged_agents.clone();
            let state_store = state_store.clone();
            let ext_runtime = ext_runtime.clone();
            let llm = llm.clone();
            let telemetry = telemetry.clone();
            let token_meter = token_meter.clone();
            let ledger = ledger.clone();
            let audit_sink = audit_sink.clone();
            let real_tenant = real_tenant.clone();
            Box::pin(async move {
                run_one_supervisor_turn(
                    req,
                    state_store,
                    ext_runtime,
                    llm,
                    telemetry,
                    token_meter,
                    ledger,
                    merged_agents,
                    audit_sink.as_ref(),
                    &real_tenant,
                )
                .await
            }) as BoxFut<'static, Result<SupervisorResult, GraphExecError>>
        })
    }

    /// Resolve the guardrails a supervisor node runs under.
    ///
    /// A supervisor inherits its worker's GUARDRAILS and nothing else — it
    /// routes, it does not work, so tools, memory and knowledge stay unset.
    /// Guardrails are the exception because this node sees the operator's raw
    /// input first: it is where inbound content-safety has to run.
    ///
    /// An `agent_ref` that does not resolve FAILS the turn rather than routing
    /// unguarded. A supervisor believed to be protected but running bare is the
    /// precise failure this inheritance exists to prevent, and a silent
    /// downgrade would be indistinguishable from success.
    fn resolve_supervisor_guardrails(
        agent_ref: Option<&str>,
        merged: &HashMap<String, AgentConfig>,
        node_id: &str,
    ) -> Result<Vec<greentic_aw_runtime::config::GuardrailRef>, GraphExecError> {
        let Some(id) = agent_ref else {
            return Ok(vec![]);
        };
        merged
            .get(id)
            .map(|parent| parent.guardrails.clone())
            .ok_or_else(|| {
                GraphExecError::Supervisor(format!(
                    "node '{node_id}': agent_ref '{id}' not found in merged agents; \
                     refusing to route without its guardrails"
                ))
            })
    }

    /// Drive one supervisor routing turn via [`AgentRuntime::step`].
    ///
    /// The routing prompt appends a route menu to the node's `systemPrompt`:
    ///
    /// ```text
    /// <node system_prompt>
    ///
    /// Available routes:
    /// - billing: Billing and payment questions
    /// - tech: Technical support issues
    ///
    /// Reply with [[ROUTE:<branch>]] to select a route.
    /// ```
    ///
    /// The agent reply is scanned for `[[ROUTE:<branch>]]` (case-insensitive).
    /// If the sentinel is absent or the branch does not match a declared route,
    /// the first route is used as fallback with a `tracing::warn!`.
    ///
    /// `audit_sink`/`real_tenant` are forwarded to [`run_agent_step`], which
    /// injects an [`AgentAuditObserver`] built from `real_tenant` when a sink
    /// is configured (EPIC-B B-3b Task 2).
    #[allow(clippy::too_many_arguments)]
    async fn run_one_supervisor_turn(
        req: SupervisorRequest,
        state_store: Arc<dyn AgentStateStore>,
        ext_runtime: Arc<ExtensionRuntime>,
        llm: Arc<dyn LlmBackend>,
        telemetry: Arc<dyn Telemetry>,
        token_meter: Arc<dyn TokenMeter>,
        ledger: Arc<dyn ToolLedger>,
        merged_agents: Arc<HashMap<String, AgentConfig>>,
        audit_sink: Option<&AuditSink>,
        real_tenant: &greentic_types::TenantCtx,
    ) -> Result<SupervisorResult, GraphExecError> {
        let tenant = TenantContext::new("graph", "run");
        let session_id = format!("graph__{}_sup", req.node_id);
        let agent_id = format!("graph.{}.supervisor", req.node_id);

        // Build the route menu suffix.
        let mut route_menu = String::from("\n\nAvailable routes:\n");
        for r in &req.routes {
            route_menu.push_str(&format!("- {}: {}\n", r.branch, r.description));
        }
        route_menu.push_str("\nReply with [[ROUTE:<branch>]] to select a route.");

        let routing_system_prompt = format!("{}{}", req.system_prompt, route_menu);

        let seeded = seed_state_from_graph(&tenant, &session_id, &req.state);
        if let Err(error) = state_store.save(&tenant, &session_id, &seeded).await {
            return Err(GraphExecError::Supervisor(format!(
                "seeding conversation state: {error}"
            )));
        }

        let guardrails =
            resolve_supervisor_guardrails(req.agent_ref.as_deref(), &merged_agents, &req.node_id)?;

        let cfg = AgentConfig {
            agent_id: agent_id.clone(),
            system_prompt: routing_system_prompt,
            tools: vec![],
            guardrails,
            llm: LlmProviderRef {
                provider: req.provider.clone().unwrap_or_else(|| "openai".into()),
                model: req.model.clone(),
                credential_ref: None,
            },
            limits: AgentLimits::default(),
            memory: None,
            knowledge: None,
            conversational: false,
            opening_message: None,
        };
        let mut provider = InMemoryConfigProvider::new();
        provider.insert(&tenant, &agent_id, cfg);

        let runtime = AgentRuntime::new(
            Arc::new(provider),
            state_store,
            ext_runtime,
            llm,
            telemetry,
            token_meter,
            ledger,
            // Supervisor routing runs with an empty tool list; MCP tools are
            // never offered on this path.
            None,
        );

        let input = AgentInput {
            text: String::new(),
            ..Default::default()
        };

        let out = run_agent_step(
            &runtime,
            tenant.clone(),
            &session_id,
            &agent_id,
            input,
            audit_sink,
            real_tenant,
        )
        .await
        .map_err(|e| GraphExecError::Supervisor(format!("supervisor step failed: {e}")))?;

        // Parse [[ROUTE:<branch>]] from the reply (case-insensitive).
        // Do this BEFORE stripping so we read from the unmodified reply.
        let branch = parse_route_branch(&out.reply, &req.routes).unwrap_or_else(|| {
            let fallback = req
                .routes
                .first()
                .map(|r| r.branch.clone())
                .unwrap_or_default();
            tracing::warn!(
                node_id = req.node_id,
                reply = %out.reply,
                fallback = %fallback,
                "supervisor reply missing or unknown [[ROUTE:]] sentinel; falling back to first route"
            );
            fallback
        });

        // Strip the [[ROUTE:<branch>]] sentinel from the stored assistant message
        // so the message log does not expose routing instructions to downstream nodes.
        let raw_reply = strip_route_sentinel(&out.reply);

        Ok(SupervisorResult { branch, raw_reply })
    }

    /// Parse `[[ROUTE:<branch>]]` from the reply and return the branch label if
    /// it matches one of the declared routes. Returns `None` if the sentinel is
    /// absent or the extracted branch is not in the route list.
    fn parse_route_branch(
        reply: &str,
        routes: &[greentic_aw_runtime::graph::SupervisorRoute],
    ) -> Option<String> {
        let lower = reply.to_ascii_lowercase();
        let prefix_lower = ROUTE_SENTINEL_PREFIX.to_ascii_lowercase();
        let suffix_lower = ROUTE_SENTINEL_SUFFIX.to_ascii_lowercase();
        let start = lower.find(&prefix_lower)?;
        let after_prefix = start + prefix_lower.len();
        let end = lower[after_prefix..].find(&suffix_lower)?;
        let branch = reply[after_prefix..after_prefix + end].trim().to_string();
        // Validate against declared routes (case-sensitive match, per spec).
        if routes.iter().any(|r| r.branch == branch) {
            Some(branch)
        } else {
            None
        }
    }

    /// Strip the `[[ROUTE:<branch>]]` sentinel (case-insensitive, first occurrence)
    /// from a supervisor reply, trimming surrounding whitespace.
    ///
    /// If no sentinel is present the reply is returned trimmed. Used to clean
    /// up the text before it is pushed into the message log as an assistant
    /// message.
    fn strip_route_sentinel(reply: &str) -> String {
        let lower = reply.to_ascii_lowercase();
        let prefix_lower = ROUTE_SENTINEL_PREFIX.to_ascii_lowercase();
        let suffix_lower = ROUTE_SENTINEL_SUFFIX.to_ascii_lowercase();
        if let Some(start) = lower.find(&prefix_lower) {
            let after_prefix = start + prefix_lower.len();
            if let Some(end) = lower[after_prefix..].find(&suffix_lower) {
                let sentinel_end = after_prefix + end + suffix_lower.len();
                let mut out = reply.to_string();
                out.replace_range(start..sentinel_end, "");
                return out.trim().to_string();
            }
        }
        reply.trim().to_string()
    }

    /// Build the [`ToolFn`] effect closure.
    ///
    /// `tool_name` is parsed as `"extension_id/tool_name"` (split on the FIRST
    /// `'/'`). Anything without a `'/'`, or an empty side, is a
    /// [`GraphExecError::Tool`]. Dispatch goes through the shared
    /// [`ExtensionRuntime`] via [`dispatch_tool_call`], which wraps the blocking
    /// `invoke_tool` in `spawn_blocking`.
    fn build_tool(ext_runtime: Arc<ExtensionRuntime>) -> ToolFn {
        Arc::new(move |req: ToolCallRequest| {
            let ext_runtime = ext_runtime.clone();
            Box::pin(async move { run_one_tool(req, ext_runtime).await })
                as BoxFut<'static, Result<Value, GraphExecError>>
        })
    }

    /// Default [`ApprovalFn`] for the in-process runner-host path: always
    /// reports `Awaiting`.
    ///
    /// The real `ApprovalFn` is designer-provided and wires the
    /// `greentic.approval.request.v1` / `.response.v1` round trip so a human
    /// decision can arrive asynchronously and unpark the run. This in-process
    /// default has no such transport, so it parks the run safely
    /// (`RunStatus::AwaitingInput`) but never resolves it. `derive_run_id`
    /// resumes (not forks) the parked run on every subsequent call for the
    /// same session, so it stays resolvable once a real `ApprovalFn` is
    /// wired in. See the `from_parts` doc comment; the designer-side
    /// approval bridge (subscriber + node, cross-repo Phase 2) supplies the
    /// real consumer.
    fn default_approval_awaiting() -> ApprovalFn {
        Arc::new(|_req: ApprovalRequest| Box::pin(async move { Ok(ApprovalOutcome::Awaiting) }))
    }

    /// Parse + dispatch a single Tool-node call. See [`build_tool`].
    async fn run_one_tool(
        req: ToolCallRequest,
        ext_runtime: Arc<ExtensionRuntime>,
    ) -> Result<Value, GraphExecError> {
        let (extension_id, tool_name) = req.tool_name.split_once('/').ok_or_else(|| {
            GraphExecError::Tool(format!(
                "tool name '{}' must be 'extension_id/tool_name'",
                req.tool_name
            ))
        })?;
        if extension_id.is_empty() || tool_name.is_empty() {
            return Err(GraphExecError::Tool(format!(
                "tool name '{}' must be 'extension_id/tool_name' (non-empty parts)",
                req.tool_name
            )));
        }

        // The graph Tool node carries no arguments today: dispatch with an empty
        // object. A future schema can thread structured args from node config.
        let call = greentic_aw_runtime::state::ToolCallRecord {
            call_id: format!("{}__{}", req.node_id, tool_name),
            extension_id: extension_id.to_string(),
            tool_name: tool_name.to_string(),
            args: json!({}),
        };

        // Graph Tool nodes never carry mcp:, component:, or sorla: ids (they
        // use 'extension_id/tool' syntax over the WASM runtime), so neither
        // the MCP, component, nor sorla catalog is threaded here — the
        // agent-graph path does not wire a sorla source in SP1.
        // No per-request TenantContext is available at the graph-node layer;
        // use a no-op placeholder so the extension's host-LLM port receives an
        // empty context (equivalent to the previous `invoke_tool` default).
        let tenant = TenantContext::new("", "");
        dispatch_tool_call(ext_runtime, None, None, None, None, call, &tenant)
            .await
            .map_err(|e| GraphExecError::Tool(format!("dispatch '{}': {e}", req.tool_name)))
    }

    #[cfg(all(test, feature = "agentic-worker"))]
    mod tests {
        use std::collections::HashMap;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        use greentic_aw_runtime::graph::model::SupervisorRoute;
        use greentic_aw_runtime::graph::{
            AgentTurnFn, AgentTurnRequest, AgentTurnResult, ApprovalFn, ApprovalOutcome,
            ApprovalRequest, GraphConfig, InMemoryCheckpointStore, SupervisorFn, SupervisorRequest,
            SupervisorResult, ToolCallRequest, ToolFn,
        };
        use greentic_aw_runtime::mock::MockAgentStateStore;
        use serde_json::json;

        use super::*;

        // -------------------------------------------------------------------
        // Fixtures
        // -------------------------------------------------------------------

        /// Minimal triage-style graph: agent → lookup tool → router → respond,
        /// router maxIterations=3. Mirrors the aw-runtime test fixture (which is
        /// `pub(crate)` to that crate and thus not importable here).
        fn triage_graph_json(max_iterations: u32) -> String {
            json!({
                "schemaVersion": 1,
                "entry": "agent",
                "nodes": [
                    {"id": "agent", "kind": "agent", "systemPrompt": "You triage.", "model": "gpt-4o-mini", "tools": []},
                    {"id": "lookup", "kind": "tool", "toolName": "kb/search"},
                    {"id": "router", "kind": "router", "maxIterations": max_iterations},
                    {"id": "respond", "kind": "respond"}
                ],
                "edges": [
                    {"from": "agent", "to": "lookup"},
                    {"from": "lookup", "to": "router"},
                    {"from": "router", "to": "agent", "branch": "loop"},
                    {"from": "router", "to": "respond", "branch": "resolved"}
                ]
            })
            .to_string()
        }

        fn triage_cfg(max_iterations: u32) -> GraphConfig {
            GraphConfig::from_json(&triage_graph_json(max_iterations)).expect("fixture valid")
        }

        /// Graph with a human-in-the-loop approval gate: `start` (agent) ->
        /// `approval` -> `respond` (branch `"approved"`). Mirrors the
        /// aw-runtime executor test fixture (`approval_json` in
        /// `graph/executor.rs`), which is `pub(crate)` there and thus not
        /// importable here.
        fn approval_graph_json() -> String {
            json!({
                "schemaVersion": 2,
                "entry": "start",
                "nodes": [
                    {"id": "start", "kind": "agent", "systemPrompt": "greet the user", "model": "gpt-4o-mini", "tools": []},
                    {"id": "approval", "kind": "approval", "title": "Approve refund?", "mode": "always"},
                    {"id": "respond", "kind": "respond"}
                ],
                "edges": [
                    {"from": "start", "to": "approval"},
                    {"from": "approval", "to": "respond", "branch": "approved"}
                ]
            })
            .to_string()
        }

        fn approval_cfg() -> GraphConfig {
            GraphConfig::from_json(&approval_graph_json()).expect("approval fixture valid")
        }

        /// An [`ApprovalFn`] whose decision flips based on a shared flag:
        /// `Awaiting` while `false`, `Decided { branch: "approved" }` once
        /// `true`. Lets a single handler drive both the initial park and a
        /// later resume-with-decision within one test.
        fn approval_fn_toggle(decide: Arc<std::sync::atomic::AtomicBool>) -> ApprovalFn {
            Arc::new(move |_req: ApprovalRequest| {
                let decide = decide.clone();
                Box::pin(async move {
                    if decide.load(Ordering::SeqCst) {
                        Ok(ApprovalOutcome::Decided {
                            branch: "approved".to_string(),
                        })
                    } else {
                        Ok(ApprovalOutcome::Awaiting)
                    }
                })
            })
        }

        fn provider_with(graph_id: &str, cfg: GraphConfig) -> Arc<InMemoryGraphProvider> {
            let mut graphs = HashMap::new();
            graphs.insert(graph_id.to_string(), cfg);
            Arc::new(InMemoryGraphProvider::new(graphs))
        }

        /// Agent closure that resolves on the n-th (non-replayed) call.
        fn agent_fn_resolves_on(counter: Arc<AtomicU32>, resolve_on_call: u32) -> AgentTurnFn {
            Arc::new(move |req: AgentTurnRequest| {
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                let resolved = n >= resolve_on_call;
                let reply = format!("reply-{n} from {}", req.node_id);
                Box::pin(async move { Ok(AgentTurnResult { reply, resolved }) })
            })
        }

        fn tool_fn_ok() -> ToolFn {
            Arc::new(move |_req: ToolCallRequest| {
                Box::pin(async move { Ok(json!({"found": true})) })
            })
        }

        /// A no-op supervisor fn for tests that do not exercise supervisor nodes.
        fn supervisor_fn_noop() -> SupervisorFn {
            Arc::new(|_req: SupervisorRequest| {
                Box::pin(async move {
                    Err(greentic_aw_runtime::graph::GraphExecError::Supervisor(
                        "supervisor fn should not be called in this test".into(),
                    ))
                })
            })
        }

        /// A trivial approval fn that always reports `Awaiting` — none of
        /// these fixtures contain an approval node, so this is never invoked.
        fn approval_fn_awaiting() -> ApprovalFn {
            Arc::new(|_req: ApprovalRequest| Box::pin(async move { Ok(ApprovalOutcome::Awaiting) }))
        }

        fn handler_with(
            graphs: Arc<dyn GraphConfigSource>,
            agent_turn: AgentTurnFn,
            tool: ToolFn,
        ) -> RuntimeGraphNodeHandler {
            RuntimeGraphNodeHandler::with_effects(
                graphs,
                Arc::new(InMemoryCheckpointStore::default()),
                Arc::new(MockAgentStateStore::new()),
                agent_turn,
                tool,
                supervisor_fn_noop(),
                approval_fn_awaiting(),
            )
        }

        // -------------------------------------------------------------------
        // parse_tool_ref / map_tool_refs tests
        // -------------------------------------------------------------------

        #[test]
        fn parse_tool_ref_splits_on_last_slash() {
            assert_eq!(
                super::parse_tool_ref("myext/dothing"),
                Some(ToolRef {
                    extension_id: "myext".into(),
                    tool_name: "dothing".into(),
                    description: None,
                    input_schema: None,
                    usage_note: None,
                })
            );
            assert_eq!(
                super::parse_tool_ref("component:owner/repo/list"),
                Some(ToolRef {
                    extension_id: "component:owner/repo".into(),
                    tool_name: "list".into(),
                    description: None,
                    input_schema: None,
                    usage_note: None,
                })
            );
            assert_eq!(super::parse_tool_ref("noslash"), None);
            assert_eq!(super::parse_tool_ref("trailing/"), None);
        }

        #[test]
        fn parse_tool_ref_rejects_leading_slash() {
            // Empty extension_id half — also malformed, never panics.
            assert_eq!(super::parse_tool_ref("/dothing"), None);
        }

        #[test]
        fn map_tool_refs_drops_malformed_entries_and_keeps_valid_ones() {
            let tools = vec![
                "myext/dothing".to_string(),
                "noslash".to_string(),
                "other/thing".to_string(),
            ];
            let refs = super::map_tool_refs(&tools);
            assert_eq!(
                refs,
                vec![
                    ToolRef {
                        extension_id: "myext".into(),
                        tool_name: "dothing".into(),
                        description: None,
                        input_schema: None,
                        usage_note: None,
                    },
                    ToolRef {
                        extension_id: "other".into(),
                        tool_name: "thing".into(),
                        description: None,
                        input_schema: None,
                        usage_note: None,
                    },
                ]
            );
        }

        // -------------------------------------------------------------------
        // graph_config_from_sidecar tests
        // -------------------------------------------------------------------

        #[test]
        fn sidecar_valid_triage_parses() {
            let bytes = triage_graph_json(3).into_bytes();
            let cfg = super::graph_config_from_sidecar("test-pack", &bytes);
            assert!(cfg.is_some(), "valid triage sidecar must parse");
        }

        #[test]
        fn sidecar_malformed_json_returns_none() {
            let bytes = b"{ this is not valid json";
            // Must not panic; a malformed sidecar is skipped (None).
            let cfg = super::graph_config_from_sidecar("test-pack", bytes);
            assert!(cfg.is_none(), "malformed JSON sidecar must yield None");
        }

        #[test]
        fn sidecar_unsupported_schema_version_returns_none() {
            let bytes = json!({
                "schemaVersion": 99,
                "entry": "agent",
                "nodes": [
                    {"id": "agent", "kind": "agent", "systemPrompt": "x", "model": "gpt-4o-mini", "tools": []},
                    {"id": "respond", "kind": "respond"}
                ],
                "edges": [{"from": "agent", "to": "respond"}]
            })
            .to_string()
            .into_bytes();
            let cfg = super::graph_config_from_sidecar("test-pack", &bytes);
            assert!(
                cfg.is_none(),
                "unsupported schemaVersion must be rejected (None)"
            );
        }

        // -------------------------------------------------------------------
        // Tests
        // -------------------------------------------------------------------

        #[tokio::test]
        async fn executes_graph_and_returns_dw_agent_envelope() {
            let handler = handler_with(
                provider_with("triage", triage_cfg(3)),
                agent_fn_resolves_on(Arc::new(AtomicU32::new(0)), 1),
                tool_fn_ok(),
            );

            let out = handler
                .execute("t", "e", "triage", "sess-1", &json!({"user_text": "help"}))
                .await
                .expect("execute should succeed");

            assert_eq!(out["terminated_by"].as_str(), Some("respond"));
            assert!(
                out["reply"].as_str().unwrap_or("").contains("reply-1"),
                "reply should carry agent output: {:?}",
                out["reply"]
            );
            assert!(
                out["trail"]
                    .as_array()
                    .map(|a| !a.is_empty())
                    .unwrap_or(false),
                "trail should be a non-empty array: {:?}",
                out["trail"]
            );
        }

        #[tokio::test]
        async fn same_session_resumes_completed_run_with_fresh_run() {
            // Shared checkpoint + state stores so the second call sees the first
            // run's terminal record and mints a fresh `__2` run id.
            let graphs = provider_with("triage", triage_cfg(3));
            let checkpoint = Arc::new(InMemoryCheckpointStore::default());
            let state_store = Arc::new(MockAgentStateStore::new());
            let counter = Arc::new(AtomicU32::new(0));

            let handler = RuntimeGraphNodeHandler::with_effects(
                graphs,
                checkpoint.clone(),
                state_store,
                agent_fn_resolves_on(counter, 1),
                tool_fn_ok(),
                supervisor_fn_noop(),
                approval_fn_awaiting(),
            );

            let first = handler
                .execute("t", "e", "triage", "sess-1", &json!({"user_text": "one"}))
                .await
                .expect("first call succeeds");
            assert_eq!(first["terminated_by"].as_str(), Some("respond"));

            let second = handler
                .execute("t", "e", "triage", "sess-1", &json!({"user_text": "two"}))
                .await
                .expect("second call succeeds with fresh run id");
            assert_eq!(second["terminated_by"].as_str(), Some("respond"));

            // The fresh run id (`__2`) must exist in the checkpoint store.
            let tenant = TenantContext::new("t", "e");
            let fresh = checkpoint
                .load(&tenant, "sess-1__triage__2")
                .await
                .expect("store ok");
            assert!(fresh.is_some(), "fresh __2 run id should be recorded");
        }

        /// Task C3: a graph run parked at an approval node (`RunStatus::
        /// AwaitingInput`) must be surfaced as neither a completed respond
        /// nor a failure, and — unlike a terminal (`Succeeded`/`Failed`) run
        /// — the SAME run id must be resumed on every later call for the
        /// same session, not forked into a fresh `__n` slot (which would
        /// orphan the parked run: it could never be decided because nothing
        /// would ever resume it again).
        #[tokio::test]
        async fn awaiting_input_parks_without_orphaning_and_resumes_on_decision() {
            let graphs = provider_with("approval-graph", approval_cfg());
            let checkpoint = Arc::new(InMemoryCheckpointStore::default());
            let state_store = Arc::new(MockAgentStateStore::new());
            let decide = Arc::new(std::sync::atomic::AtomicBool::new(false));

            let handler = RuntimeGraphNodeHandler::with_effects(
                graphs,
                checkpoint.clone(),
                state_store,
                agent_fn_resolves_on(Arc::new(AtomicU32::new(0)), 1),
                tool_fn_ok(),
                supervisor_fn_noop(),
                approval_fn_toggle(decide.clone()),
            );
            let tenant = TenantContext::new("t", "e");

            // Call 1: the run drives to the approval node and parks.
            let first = handler
                .execute(
                    "t",
                    "e",
                    "approval-graph",
                    "sess-approve",
                    &json!({"user_text": "please refund"}),
                )
                .await
                .expect("a parked run must be Ok, not an Err");
            assert_ne!(
                first["terminated_by"].as_str(),
                Some("respond"),
                "a parked run must not be reported as a completed respond: {first:?}"
            );
            assert_ne!(
                first["terminated_by"].as_str(),
                Some("error"),
                "a parked run must not be reported as an error: {first:?}"
            );

            let rec = checkpoint
                .load(&tenant, "sess-approve__approval-graph")
                .await
                .expect("store accessible")
                .expect("base run id must be recorded");
            assert_eq!(
                rec.status,
                RunStatus::AwaitingInput,
                "persisted record must reflect the park"
            );

            // Call 2 (still undecided): must RESUME the base run id in
            // place, not fork "__2" (the bug this test guards against).
            let second = handler
                .execute(
                    "t",
                    "e",
                    "approval-graph",
                    "sess-approve",
                    &json!({"user_text": "still waiting"}),
                )
                .await
                .expect("a re-parked run must be Ok, not an Err");
            assert_ne!(second["terminated_by"].as_str(), Some("respond"));

            let forked = checkpoint
                .load(&tenant, "sess-approve__approval-graph__2")
                .await
                .expect("store accessible");
            assert!(
                forked.is_none(),
                "an AwaitingInput run must resume in place, not fork a fresh run id"
            );

            // Call 3: the decision has arrived — the SAME base run id
            // resolves via `executor.resume()`, not a fresh run.
            decide.store(true, Ordering::SeqCst);
            let third = handler
                .execute(
                    "t",
                    "e",
                    "approval-graph",
                    "sess-approve",
                    &json!({"user_text": "resolve"}),
                )
                .await
                .expect("execute should succeed once decided");
            assert_eq!(third["terminated_by"].as_str(), Some("respond"));

            let rec = checkpoint
                .load(&tenant, "sess-approve__approval-graph")
                .await
                .expect("store accessible")
                .expect("base run id must still be recorded");
            assert_eq!(
                rec.status,
                RunStatus::Succeeded,
                "the base run id must resolve once decided, not a forked one"
            );
        }

        #[tokio::test]
        async fn missing_graph_returns_structured_error_reply() {
            let handler = handler_with(
                provider_with("triage", triage_cfg(3)),
                agent_fn_resolves_on(Arc::new(AtomicU32::new(0)), 1),
                tool_fn_ok(),
            );

            let out = handler
                .execute("t", "e", "unknown", "sess-1", &json!({"user_text": "hi"}))
                .await
                .expect("missing graph must NOT return Err");

            assert_eq!(out["terminated_by"].as_str(), Some("error"));
            assert!(
                out["reply"]
                    .as_str()
                    .unwrap_or("")
                    .to_ascii_lowercase()
                    .contains("available"),
                "reply should mention availability: {:?}",
                out["reply"]
            );
        }

        #[tokio::test]
        async fn missing_user_text_matches_agent_node_contract() {
            // agent_node.rs treats missing `user_text` as an empty string and
            // proceeds (never Err). Assert the same contract here.
            let handler = handler_with(
                provider_with("triage", triage_cfg(3)),
                agent_fn_resolves_on(Arc::new(AtomicU32::new(0)), 1),
                tool_fn_ok(),
            );

            let out = handler
                .execute("t", "e", "triage", "sess-1", &json!({}))
                .await
                .expect("missing user_text must NOT return Err (matches agent_node)");

            assert_eq!(out["terminated_by"].as_str(), Some("respond"));
        }

        #[tokio::test]
        async fn iteration_cap_maps_to_error_envelope() {
            // maxIterations=1000 on the router → the executor's global
            // MAX_NODE_VISITS cap (64) trips first → IterationCap → "error".
            let handler = handler_with(
                provider_with("triage", triage_cfg(1000)),
                // never resolves → loops until the global visit cap
                agent_fn_resolves_on(Arc::new(AtomicU32::new(0)), u32::MAX),
                tool_fn_ok(),
            );

            let out = handler
                .execute("t", "e", "triage", "sess-1", &json!({"user_text": "loop"}))
                .await
                .expect("iteration cap must NOT return Err");

            assert_eq!(out["terminated_by"].as_str(), Some("error"));
            assert_ne!(
                out["reply"].as_str(),
                Some(SANITISED_ERROR_REPLY),
                "iteration cap should use a distinct reply"
            );
        }

        /// Regression test: flow session ids can contain `':'` (e.g. MS Teams
        /// channel correlation ids like `"msteams:thread:19xyz"`). The checkpoint
        /// store rejects keys that contain `':'`, so without sanitization every
        /// such graph run fails at save-time. [`derive_run_id`] must replace every
        /// `':'` with `'_'` before composing the checkpoint key.
        #[tokio::test]
        async fn colon_bearing_session_id_executes_successfully() {
            let handler = handler_with(
                provider_with("triage", triage_cfg(3)),
                agent_fn_resolves_on(Arc::new(AtomicU32::new(0)), 1),
                tool_fn_ok(),
            );

            // Session id with multiple colons — typical of Teams thread ids.
            let out = handler
                .execute(
                    "t",
                    "e",
                    "triage",
                    "msteams:thread:19xyz",
                    &json!({"user_text": "hello"}),
                )
                .await
                .expect("colon-bearing session id must NOT return Err");

            assert_eq!(
                out["terminated_by"].as_str(),
                Some("respond"),
                "colon-bearing session id should complete successfully: {:?}",
                out
            );
        }

        // -------------------------------------------------------------------
        // LayeredGraphProvider tests
        // -------------------------------------------------------------------

        /// Provider that always returns the given error.
        struct ErrGraphProvider(fn() -> ConfigError);

        impl GraphConfigSource for ErrGraphProvider {
            fn graph_config<'a>(
                &'a self,
                _tenant: &'a TenantContext,
                _graph_id: &'a str,
            ) -> BoxFut<'a, Result<GraphConfig, ConfigError>> {
                let e = (self.0)();
                Box::pin(async move { Err(e) })
            }
        }

        /// Fallback that panics if consulted — proves primary short-circuits.
        struct PanicGraphProvider;

        impl GraphConfigSource for PanicGraphProvider {
            fn graph_config<'a>(
                &'a self,
                _tenant: &'a TenantContext,
                _graph_id: &'a str,
            ) -> BoxFut<'a, Result<GraphConfig, ConfigError>> {
                Box::pin(
                    async move { panic!("fallback must not be consulted when primary returns Ok") },
                )
            }
        }

        #[tokio::test]
        async fn layered_primary_ok_short_circuits_fallback() {
            let mut graphs = HashMap::new();
            graphs.insert("g".to_string(), triage_cfg(3));
            let primary = InMemoryGraphProvider::new(graphs);
            let layered = LayeredGraphProvider::new(primary, PanicGraphProvider);
            let tc = TenantContext::new("t", "e");
            // If fallback were consulted, PanicGraphProvider panics and fails this.
            let cfg = layered.graph_config(&tc, "g").await.unwrap();
            assert_eq!(cfg.graph.entry, "agent");
        }

        #[tokio::test]
        async fn layered_falls_back_on_not_found() {
            let mut graphs = HashMap::new();
            graphs.insert("g".to_string(), triage_cfg(3));
            let fb = InMemoryGraphProvider::new(graphs);
            let layered = LayeredGraphProvider::new(
                ErrGraphProvider(|| ConfigError::AgentNotFound("g".into())),
                fb,
            );
            let tc = TenantContext::new("t", "e");
            let cfg = layered.graph_config(&tc, "g").await.unwrap();
            assert_eq!(cfg.graph.entry, "agent");
        }

        #[tokio::test]
        async fn layered_falls_back_on_internal() {
            let mut graphs = HashMap::new();
            graphs.insert("g".to_string(), triage_cfg(3));
            let fb = InMemoryGraphProvider::new(graphs);
            let layered = LayeredGraphProvider::new(
                ErrGraphProvider(|| ConfigError::Internal("down".into())),
                fb,
            );
            let tc = TenantContext::new("t", "e");
            let cfg = layered.graph_config(&tc, "g").await.unwrap();
            assert_eq!(cfg.graph.entry, "agent");
        }

        #[tokio::test]
        async fn layered_propagates_misconfigured_without_fallback() {
            let fb = InMemoryGraphProvider::new(HashMap::new());
            let layered = LayeredGraphProvider::new(
                ErrGraphProvider(|| ConfigError::Misconfigured("bad schema".into())),
                fb,
            );
            let tc = TenantContext::new("t", "e");
            let result = layered.graph_config(&tc, "g").await;
            assert!(
                matches!(result, Err(ConfigError::Misconfigured(_))),
                "Misconfigured must NOT fall back: {result:?}"
            );
        }

        // -------------------------------------------------------------------
        // graph_registry_from_env tests
        // -------------------------------------------------------------------

        #[test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        fn graph_registry_from_env_requires_both_vars() {
            // SAFETY: #[serial] serializes env-mutating tests; vars cleaned up.
            unsafe {
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }
            assert!(super::graph_registry_from_env().is_none());

            unsafe {
                std::env::set_var("GREENTIC_AW_ADMIN_ENDPOINT", "http://localhost:9999");
            }
            assert!(
                super::graph_registry_from_env().is_none(),
                "endpoint alone is not enough"
            );

            unsafe {
                std::env::set_var("GREENTIC_AW_ADMIN_TOKEN", "gtc_live_x");
            }
            assert!(super::graph_registry_from_env().is_some());

            // Token-alone (no endpoint) must also be None.
            unsafe {
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
            }
            assert!(
                super::graph_registry_from_env().is_none(),
                "token alone is not enough"
            );

            unsafe {
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }
        }

        // -------------------------------------------------------------------
        // Supervisor fixture
        // -------------------------------------------------------------------

        /// Supervisor graph (schemaVersion 2): sup → agent_billing / agent_tech →
        /// router → respond. Mirrors the aw-runtime `supervisor_json()` fixture
        /// (which is `pub(crate)` to that crate and cannot be imported here).
        ///
        /// Topology:
        ///   sup (supervisor: routes=[billing, tech])
        ///    ├─[billing]─► agent_billing ─► router_billing ─┬─[loop]──► sup
        ///    │                                               └─[resolved]─► respond
        ///    └─[tech]────► agent_tech ───► router_tech    ─┬─[loop]──► sup
        ///                                                   └─[resolved]─► respond
        fn supervisor_graph_json() -> String {
            json!({
                "schemaVersion": 2,
                "entry": "sup",
                "nodes": [
                    {
                        "id": "sup",
                        "kind": "supervisor",
                        "systemPrompt": "Route the request to the correct specialist.",
                        "model": "gpt-4o-mini",
                        "routes": [
                            {"branch": "billing", "description": "Billing and payment questions"},
                            {"branch": "tech",    "description": "Technical support issues"}
                        ]
                    },
                    {"id": "agent_billing", "kind": "agent", "systemPrompt": "Billing.", "model": "gpt-4o-mini", "tools": []},
                    {"id": "router_billing", "kind": "router", "maxIterations": 2},
                    {"id": "agent_tech",    "kind": "agent", "systemPrompt": "Tech.",    "model": "gpt-4o-mini", "tools": []},
                    {"id": "router_tech",   "kind": "router", "maxIterations": 2},
                    {"id": "respond",       "kind": "respond"}
                ],
                "edges": [
                    {"from": "sup",           "to": "agent_billing",  "branch": "billing"},
                    {"from": "sup",           "to": "agent_tech",     "branch": "tech"},
                    {"from": "agent_billing", "to": "router_billing"},
                    {"from": "router_billing","to": "agent_billing",  "branch": "loop"},
                    {"from": "router_billing","to": "respond",        "branch": "resolved"},
                    {"from": "agent_tech",    "to": "router_tech"},
                    {"from": "router_tech",   "to": "agent_tech",     "branch": "loop"},
                    {"from": "router_tech",   "to": "respond",        "branch": "resolved"}
                ]
            })
            .to_string()
        }

        fn supervisor_cfg() -> GraphConfig {
            GraphConfig::from_json(&supervisor_graph_json()).expect("supervisor fixture valid")
        }

        /// Helpers for building [`SupervisorRoute`] values inline.
        fn route(branch: &str, description: &str) -> SupervisorRoute {
            SupervisorRoute {
                branch: branch.to_string(),
                description: description.to_string(),
            }
        }

        /// Build a supervisor fn that always routes to `fixed_branch`.
        fn supervisor_fn_routes_to(fixed_branch: &str) -> SupervisorFn {
            let branch = fixed_branch.to_string();
            Arc::new(move |_req: SupervisorRequest| {
                let branch = branch.clone();
                Box::pin(async move {
                    Ok(SupervisorResult {
                        branch: branch.clone(),
                        raw_reply: format!("I'll route this. [[ROUTE:{branch}]] Done."),
                    })
                })
            })
        }

        // Build a handler wired with the given supervisor fn (and a no-op agent
        // that always resolves immediately).
        fn supervisor_handler(
            graphs: Arc<dyn GraphConfigSource>,
            supervisor: SupervisorFn,
        ) -> RuntimeGraphNodeHandler {
            let agent: AgentTurnFn = agent_fn_resolves_on(Arc::new(AtomicU32::new(0)), 1);
            RuntimeGraphNodeHandler::with_effects(
                graphs,
                Arc::new(InMemoryCheckpointStore::default()),
                Arc::new(MockAgentStateStore::new()),
                agent,
                tool_fn_ok(),
                supervisor,
                approval_fn_awaiting(),
            )
        }

        // -------------------------------------------------------------------
        // parse_route_branch unit tests
        // -------------------------------------------------------------------

        /// Two routes for the parse tests.
        fn billing_tech_routes() -> Vec<SupervisorRoute> {
            vec![
                route("billing", "Billing and payment questions"),
                route("tech", "Technical support issues"),
            ]
        }

        #[test]
        fn parse_route_extracts_billing_from_reply() {
            // Standard case: sentinel embedded in a longer reply.
            let reply = "I analysed the message. [[ROUTE:billing]] Go ahead.";
            let result = parse_route_branch(reply, &billing_tech_routes());
            assert_eq!(result, Some("billing".to_string()));
        }

        #[test]
        fn parse_route_case_insensitive_prefix() {
            // The prefix [[ROUTE: is matched case-insensitively; the extracted
            // branch label must still match the declared route exactly
            // (case-sensitive per spec).
            let reply = "[[route:tech]] is the answer";
            let result = parse_route_branch(reply, &billing_tech_routes());
            assert_eq!(result, Some("tech".to_string()));
        }

        #[test]
        fn parse_route_missing_sentinel_returns_none() {
            // No sentinel at all → None (caller's unwrap_or_else picks first route).
            let reply = "I think billing would be best here.";
            let result = parse_route_branch(reply, &billing_tech_routes());
            assert_eq!(result, None);
        }

        #[test]
        fn parse_route_unknown_branch_returns_none() {
            // Sentinel present but branch label does not match any declared route.
            let reply = "[[ROUTE:unknown_branch]] routing";
            let result = parse_route_branch(reply, &billing_tech_routes());
            assert_eq!(result, None);
        }

        #[test]
        fn parse_route_multiple_sentinels_uses_first() {
            // When multiple sentinels are present the function uses the FIRST one
            // (find on the lowercased string returns the first occurrence).
            let reply = "[[ROUTE:billing]] or [[ROUTE:tech]]";
            let result = parse_route_branch(reply, &billing_tech_routes());
            assert_eq!(
                result,
                Some("billing".to_string()),
                "multiple sentinels: first occurrence wins"
            );
        }

        #[test]
        fn parse_route_branch_label_whitespace_trimmed() {
            // Whitespace around the branch label inside the sentinel is trimmed.
            let reply = "[[ROUTE:  billing  ]]";
            let result = parse_route_branch(reply, &billing_tech_routes());
            assert_eq!(result, Some("billing".to_string()));
        }

        // -------------------------------------------------------------------
        // strip_route_sentinel unit tests
        // -------------------------------------------------------------------

        #[test]
        fn strip_sentinel_removes_token_leaves_rest() {
            let reply = "I'll route this. [[ROUTE:billing]] Proceeding.";
            let stripped = strip_route_sentinel(reply);
            assert!(
                !stripped.contains("[[ROUTE:billing]]"),
                "sentinel must be removed: {stripped:?}"
            );
            assert!(
                stripped.contains("Proceeding"),
                "text after sentinel must survive: {stripped:?}"
            );
        }

        #[test]
        fn strip_sentinel_no_sentinel_returns_trimmed() {
            let reply = "  No routing needed here.  ";
            let stripped = strip_route_sentinel(reply);
            assert_eq!(stripped, "No routing needed here.");
        }

        #[test]
        fn strip_sentinel_case_insensitive_removal() {
            let reply = "Decision: [[route:tech]] done.";
            let stripped = strip_route_sentinel(reply);
            assert!(
                !stripped.to_ascii_lowercase().contains("[[route:"),
                "sentinel must be stripped case-insensitively: {stripped:?}"
            );
        }

        #[test]
        fn strip_sentinel_only_first_occurrence_removed() {
            // If two sentinels appear, only the first is stripped.
            let reply = "[[ROUTE:billing]] first [[ROUTE:tech]] second";
            let stripped = strip_route_sentinel(reply);
            assert!(
                !stripped.contains("[[ROUTE:billing]]"),
                "first sentinel must be gone: {stripped:?}"
            );
            // The second sentinel remains (we only strip the first occurrence).
            assert!(
                stripped.contains("[[ROUTE:tech]]"),
                "second sentinel must remain: {stripped:?}"
            );
        }

        // -------------------------------------------------------------------
        // Fallback: parse_route_branch None → first-route fallback
        // -------------------------------------------------------------------

        #[test]
        fn parse_route_none_documents_fallback_to_first_route() {
            // This is a pure-fn test of the fallback path used in
            // `run_one_supervisor_turn` via `unwrap_or_else(|| first_route)`.
            // When parse_route_branch returns None, the build_supervisor closure
            // picks req.routes.first().branch as the fallback.
            let routes = billing_tech_routes();
            let garbage = "absolutely no routing sentinel here";
            let parsed = parse_route_branch(garbage, &routes);
            assert_eq!(parsed, None, "no sentinel → None from parser");

            // Simulate the fallback: first route is "billing".
            let fallback = parsed
                .unwrap_or_else(|| routes.first().map(|r| r.branch.clone()).unwrap_or_default());
            assert_eq!(
                fallback, "billing",
                "None → first route 'billing' as fallback"
            );
        }

        // -------------------------------------------------------------------
        // Handler-level supervisor routing tests
        // -------------------------------------------------------------------

        #[tokio::test]
        async fn supervisor_routes_to_billing_returns_respond_envelope() {
            // The mock supervisor always picks "billing". The billing agent
            // resolves immediately. Expect terminated_by="respond" and a
            // non-empty reply.
            let handler = supervisor_handler(
                provider_with("sup_graph", supervisor_cfg()),
                supervisor_fn_routes_to("billing"),
            );

            let out = handler
                .execute(
                    "t",
                    "e",
                    "sup_graph",
                    "sess-sup-1",
                    &json!({"user_text": "I need billing help"}),
                )
                .await
                .expect("supervisor execute must not Err");

            assert_eq!(
                out["terminated_by"].as_str(),
                Some("respond"),
                "billing path must terminate at respond: {out:?}"
            );
            assert!(
                out["reply"].as_str().is_some_and(|r| !r.is_empty()),
                "reply must be non-empty: {out:?}"
            );
            assert!(
                out["trail"].as_array().is_some_and(|t| !t.is_empty()),
                "trail must be non-empty: {out:?}"
            );
        }

        #[tokio::test]
        async fn supervisor_routes_to_tech_returns_respond_envelope() {
            // Same topology, supervisor picks "tech" branch instead.
            let handler = supervisor_handler(
                provider_with("sup_graph", supervisor_cfg()),
                supervisor_fn_routes_to("tech"),
            );

            let out = handler
                .execute(
                    "t",
                    "e",
                    "sup_graph",
                    "sess-sup-2",
                    &json!({"user_text": "my device is broken"}),
                )
                .await
                .expect("supervisor tech-route execute must not Err");

            assert_eq!(
                out["terminated_by"].as_str(),
                Some("respond"),
                "tech path must terminate at respond: {out:?}"
            );
        }

        #[tokio::test]
        async fn supervisor_fallback_on_missing_sentinel_still_completes() {
            // A supervisor fn that returns a raw_reply with NO [[ROUTE:]] sentinel.
            // The build_supervisor closure maps None→first-route ("billing").
            // The handler-level execute() must still complete (not Err).
            //
            // Because with_effects injects the supervisor fn directly (bypassing
            // build_supervisor's parse), we simulate the fallback by having the
            // fn return the first route explicitly — matching what build_supervisor
            // does in production via parse_route_branch(...)
            //   .unwrap_or_else(|| routes.first().branch).
            // The pure-fn fallback path is fully covered by
            // `parse_route_none_documents_fallback_to_first_route`.
            let supervisor: SupervisorFn = Arc::new(|_req: SupervisorRequest| {
                // No sentinel in raw_reply; we return first-route directly as
                // the mock of the fallback behaviour.
                Box::pin(async move {
                    Ok(SupervisorResult {
                        branch: "billing".to_string(),
                        raw_reply: "I think billing is right.".to_string(),
                    })
                })
            });

            let handler =
                supervisor_handler(provider_with("sup_graph", supervisor_cfg()), supervisor);

            let out = handler
                .execute(
                    "t",
                    "e",
                    "sup_graph",
                    "sess-sup-3",
                    &json!({"user_text": "help me"}),
                )
                .await
                .expect("fallback supervisor must not Err");

            assert_eq!(
                out["terminated_by"].as_str(),
                Some("respond"),
                "fallback path must still reach respond: {out:?}"
            );
        }

        // -------------------------------------------------------------------
        // EPIC-B B-3b Task 2: `AgentAuditObserver` injection at the graph
        // turn `.step` seam (`run_agent_step`, shared by `run_one_agent_turn`
        // and `run_one_supervisor_turn`).
        // -------------------------------------------------------------------

        fn real_tenant_ctx(tenant: &str, env: &str) -> greentic_types::TenantCtx {
            greentic_types::TenantCtx::new(
                greentic_types::EnvId::try_from(env).expect("valid env id"),
                greentic_types::TenantId::try_from(tenant).expect("valid tenant id"),
            )
        }

        /// Build an [`AgentRuntime`] scripted to make one `remember` (host
        /// built-in short-term-memory) tool call before replying "done".
        /// Mirrors `agent_node`'s `runtime_with_scripted_remember_call`
        /// fixture — the host built-in path fires
        /// `StepObserver::on_tool_call`/`on_tool_result` without needing a
        /// real WASM extension dispatch, so it is the cheapest way to drive a
        /// genuine tool call through the seam.
        ///
        /// `run_one_agent_turn`/`run_one_supervisor_turn` hardcode
        /// `tools: vec![]` + `memory: None` on their ephemeral per-visit
        /// `AgentConfig` (graph-node agents have no tool/memory access in v1
        /// scope — see the `TODO(guardrail)` comment at each call site), so
        /// no tool call can ever reach the observer through those two
        /// functions today. This fixture instead drives the shared
        /// `run_agent_step` seam directly with its own memory-enabled
        /// config — the exact same `match audit_sink { .. }` code both turn
        /// functions delegate to — so the real-tenant wiring is verified
        /// against genuine tool-call traffic rather than asserted in the
        /// abstract.
        fn agent_runtime_with_scripted_remember_call(
            tenant: &TenantContext,
            agent_id: &str,
        ) -> AgentRuntime {
            use greentic_aw_runtime::cost::MockTokenMeter;
            use greentic_aw_runtime::llm::LlmResponse;
            use greentic_aw_runtime::mock::{MockLlmBackend, MockTelemetry, NoopToolLedger};
            use greentic_aw_runtime::state::ToolCallRecord;
            use greentic_aw_runtime::{InMemoryMemoryProvider, MemoryProviderRef, MemorySettings};

            let llm = Arc::new(MockLlmBackend::new(vec![
                Ok(LlmResponse {
                    content: None,
                    tool_calls: vec![ToolCallRecord {
                        call_id: "c1".into(),
                        extension_id: "host".into(),
                        tool_name: "remember".into(),
                        args: json!({"key": "k", "value": "v"}),
                    }],
                    tokens_in: 1,
                    tokens_out: 1,
                }),
                Ok(LlmResponse {
                    content: Some("done".into()),
                    tool_calls: vec![],
                    tokens_in: 1,
                    tokens_out: 1,
                }),
            ]));

            let cfg = AgentConfig {
                agent_id: agent_id.to_string(),
                system_prompt: "test".into(),
                tools: vec![],
                guardrails: vec![],
                llm: LlmProviderRef {
                    provider: "openai".into(),
                    model: "gpt-4o-mini".into(),
                    credential_ref: None,
                },
                limits: AgentLimits::default(),
                memory: Some(MemorySettings {
                    short_term: Some(MemoryProviderRef {
                        provider: "inmemory".into(),
                        capability: "cap://memory/short-term".into(),
                        params: Default::default(),
                        credential_ref: None,
                    }),
                    long_term: None,
                }),
                knowledge: None,
                conversational: false,
                opening_message: None,
            };
            let mut provider = InMemoryConfigProvider::new();
            provider.insert(tenant, agent_id, cfg);

            AgentRuntime::new(
                Arc::new(provider),
                Arc::new(MockAgentStateStore::new()),
                Arc::new(ExtensionRuntime::for_test().unwrap()),
                llm,
                Arc::new(MockTelemetry::new()),
                Arc::new(MockTokenMeter::new(0)),
                Arc::new(NoopToolLedger),
                None,
            )
            .with_short_term_memory(Arc::new(InMemoryMemoryProvider::new()))
        }

        #[tokio::test]
        async fn run_agent_step_with_audit_sink_enqueues_events_under_real_tenant_not_state_tenant()
        {
            // The SYNTHETIC state tenant every graph-node turn uses today —
            // must NOT leak into the audit subject.
            let state_tenant = TenantContext::new("graph", "run");
            let agent_id = "graph.agent-1";
            let session_id = "graph__agent-1";
            let runtime = agent_runtime_with_scripted_remember_call(&state_tenant, agent_id);

            let (tx, mut rx) = tokio::sync::mpsc::channel(16);
            let sink = AuditSink::from_sender(tx);
            let real_tenant = real_tenant_ctx("acme", "prod");

            let input = AgentInput {
                text: String::new(),
                ..Default::default()
            };
            let out = run_agent_step(
                &runtime,
                state_tenant.clone(),
                session_id,
                agent_id,
                input,
                Some(&sink),
                &real_tenant,
            )
            .await
            .expect("step should succeed");
            assert_eq!(out.reply, "done");

            let (subject, bytes) = rx.try_recv().expect("tool_call audit event enqueued");
            assert_eq!(
                subject, "audit.acme.agent.tool_call",
                "audit subject must carry the REAL tenant (\"acme\"), not the synthetic state tenant (\"graph\")"
            );
            let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
            assert_eq!(value["payload"]["tool"], json!("remember"));
            assert_eq!(value["payload"]["agent_id"], json!(agent_id));

            let (subject, bytes) = rx.try_recv().expect("tool_result audit event enqueued");
            assert_eq!(subject, "audit.acme.agent.tool_result");
            let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
            assert_eq!(value["payload"]["tool"], json!("remember"));

            assert!(
                rx.try_recv().is_err(),
                "exactly two audit events enqueued (one tool_call, one tool_result)"
            );
        }

        #[tokio::test]
        async fn run_agent_step_without_audit_sink_uses_plain_step_path_unchanged() {
            // Same scripted tool call as the audited test above, but no audit
            // sink at all — proves the "off" branch (runtime.step, no
            // observer constructed) still dispatches the tool call and
            // returns the same reply, exactly as it did before
            // AgentAuditObserver existed.
            let state_tenant = TenantContext::new("graph", "run");
            let agent_id = "graph.agent-1";
            let session_id = "graph__agent-1";
            let runtime = agent_runtime_with_scripted_remember_call(&state_tenant, agent_id);
            let real_tenant = real_tenant_ctx("acme", "prod");

            let input = AgentInput {
                text: String::new(),
                ..Default::default()
            };
            let out = run_agent_step(
                &runtime,
                state_tenant.clone(),
                session_id,
                agent_id,
                input,
                None,
                &real_tenant,
            )
            .await
            .expect("step should succeed");

            assert_eq!(out.reply, "done");
        }

        /// Minimal [`MockLlmBackend`] that always replies with `reply` and no
        /// tool calls — enough to drive `run_one_agent_turn`/
        /// `run_one_supervisor_turn` end to end without needing tool access
        /// (which their hardcoded ephemeral `AgentConfig` does not grant).
        fn plain_reply_llm(reply: &str) -> Arc<dyn LlmBackend> {
            use greentic_aw_runtime::llm::LlmResponse;
            use greentic_aw_runtime::mock::MockLlmBackend;

            Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
                content: Some(reply.to_string()),
                tool_calls: vec![],
                tokens_in: 1,
                tokens_out: 1,
            })]))
        }

        fn agent_turn_request(node_id: &str, system_prompt: &str) -> AgentTurnRequest {
            AgentTurnRequest {
                node_id: node_id.to_string(),
                system_prompt: system_prompt.to_string(),
                model: "gpt-4o-mini".to_string(),
                state: GraphRunState::default(),
                provider: None,
                tools: vec![],
                agent_ref: None,
                inherit_from: None,
            }
        }

        /// Shared effect Arcs for `run_one_agent_turn`/`run_one_supervisor_turn`
        /// test calls: state store, ext runtime, telemetry, token meter, ledger.
        type AgentTurnEffects = (
            Arc<dyn AgentStateStore>,
            Arc<ExtensionRuntime>,
            Arc<dyn Telemetry>,
            Arc<dyn TokenMeter>,
            Arc<dyn ToolLedger>,
        );

        fn agent_turn_effects() -> AgentTurnEffects {
            use greentic_aw_runtime::cost::MockTokenMeter;
            use greentic_aw_runtime::mock::{MockTelemetry, NoopToolLedger};

            (
                Arc::new(MockAgentStateStore::new()),
                Arc::new(ExtensionRuntime::for_test().unwrap()),
                Arc::new(MockTelemetry::new()),
                Arc::new(MockTokenMeter::new(0)),
                Arc::new(NoopToolLedger),
            )
        }

        #[tokio::test]
        async fn run_one_agent_turn_without_audit_sink_completes_unchanged() {
            let (state_store, ext_runtime, telemetry, token_meter, ledger) = agent_turn_effects();
            let real_tenant = real_tenant_ctx("acme", "prod");

            let result = run_one_agent_turn(
                agent_turn_request("agent", "You triage."),
                state_store,
                ext_runtime,
                plain_reply_llm("all good [[RESOLVED]]"),
                telemetry,
                token_meter,
                ledger,
                None,
                Arc::new(vec![]),
                Arc::new(HashMap::new()),
                None,
                &real_tenant,
            )
            .await
            .expect("turn should succeed");

            assert!(result.resolved);
            assert_eq!(result.reply, "all good");
        }

        #[tokio::test]
        async fn run_one_agent_turn_with_audit_sink_completes_same_reply_as_without() {
            // `agent_turn_request` declares no tools, and the mock LLM never
            // emits tool calls, so no audit event is expected here either —
            // this test guards that routing through `step_with_observer` does
            // not change the turn's outcome. Genuine tool-call
            // audit-under-real-tenant coverage lives in
            // `run_agent_step_with_audit_sink_enqueues_events_under_real_tenant_not_state_tenant`.
            let (state_store, ext_runtime, telemetry, token_meter, ledger) = agent_turn_effects();
            let (tx, mut rx) = tokio::sync::mpsc::channel(16);
            let sink = AuditSink::from_sender(tx);
            let real_tenant = real_tenant_ctx("acme", "prod");

            let result = run_one_agent_turn(
                agent_turn_request("agent", "You triage."),
                state_store,
                ext_runtime,
                plain_reply_llm("all good [[RESOLVED]]"),
                telemetry,
                token_meter,
                ledger,
                None,
                Arc::new(vec![]),
                Arc::new(HashMap::new()),
                Some(&sink),
                &real_tenant,
            )
            .await
            .expect("turn should succeed");

            assert!(result.resolved);
            assert_eq!(result.reply, "all good");
            assert!(
                rx.try_recv().is_err(),
                "no tool call happens on this path, so no audit event is expected"
            );
        }

        /// Wiring assertion (Task 2): a request whose `tools` field declares a
        /// well-formed ref reaches the ephemeral per-visit `AgentConfig`
        /// unharmed — the turn still completes (the runtime only warns, never
        /// fails, on tools that don't resolve against the test `ExtensionRuntime`
        /// — see `preflight_warn_tools`/`missing_tools` in `greentic-aw-runtime`).
        /// `map_tool_refs`/`parse_tool_ref` above cover the mapping itself;
        /// this proves `run_one_agent_turn` actually calls it instead of the
        /// old hardcoded `tools: vec![]`.
        #[tokio::test]
        async fn run_one_agent_turn_with_declared_tools_still_completes() {
            let (state_store, ext_runtime, telemetry, token_meter, ledger) = agent_turn_effects();
            let real_tenant = real_tenant_ctx("acme", "prod");

            let mut req = agent_turn_request("agent", "You triage.");
            req.tools = vec!["myext/dothing".to_string()];

            let result = run_one_agent_turn(
                req,
                state_store,
                ext_runtime,
                plain_reply_llm("all good [[RESOLVED]]"),
                telemetry,
                token_meter,
                ledger,
                None,
                Arc::new(vec![]),
                Arc::new(HashMap::new()),
                None,
                &real_tenant,
            )
            .await
            .expect("turn should succeed even with a declared-but-unregistered tool");

            assert!(result.resolved);
            assert_eq!(result.reply, "all good");
        }

        // -------------------------------------------------------------------
        // SP1 Task 3: `agent_ref` resolution
        // -------------------------------------------------------------------

        /// A published-agent config with a distinctive `system_prompt` marker
        /// ("FAQ-BOT") plus non-empty `memory`/`knowledge` bindings, standing
        /// in for a real `merged_agents` entry resolved by `agent_ref`.
        fn faq_bot_agent_config() -> AgentConfig {
            use greentic_aw_runtime::config::KnowledgeSettings;
            use greentic_aw_runtime::{MemoryProviderRef, MemorySettings};

            AgentConfig {
                agent_id: "faq".to_string(),
                system_prompt: "You are FAQ-BOT. Answer only from the knowledge base.".to_string(),
                tools: vec![],
                guardrails: vec![],
                llm: LlmProviderRef {
                    provider: "openai".to_string(),
                    model: "gpt-4o-mini".to_string(),
                    credential_ref: None,
                },
                limits: AgentLimits::default(),
                memory: Some(MemorySettings {
                    short_term: Some(MemoryProviderRef {
                        provider: "inmemory".to_string(),
                        capability: "cap://memory/short-term".to_string(),
                        params: Default::default(),
                        credential_ref: None,
                    }),
                    long_term: None,
                }),
                knowledge: Some(KnowledgeSettings {
                    knowledge: Some(MemoryProviderRef {
                        provider: "chronicle".to_string(),
                        capability: "cap://dw.knowledge".to_string(),
                        params: Default::default(),
                        credential_ref: None,
                    }),
                    embedding: None,
                    top_k: 5,
                }),
                conversational: false,
                opening_message: None,
            }
        }

        /// Like [`plain_reply_llm`] but returns the concrete
        /// [`greentic_aw_runtime::mock::MockLlmBackend`] (not erased to
        /// `Arc<dyn LlmBackend>`) so the test can inspect `seen_system_prompts`
        /// after the turn completes.
        fn recording_llm(reply: &str) -> Arc<greentic_aw_runtime::mock::MockLlmBackend> {
            use greentic_aw_runtime::llm::LlmResponse;
            use greentic_aw_runtime::mock::MockLlmBackend;

            Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
                content: Some(reply.to_string()),
                tool_calls: vec![],
                tokens_in: 1,
                tokens_out: 1,
            })]))
        }

        #[test]
        fn resolve_turn_config_agent_ref_present_returns_full_config_from_map() {
            let mut merged = HashMap::new();
            merged.insert("faq".to_string(), faq_bot_agent_config());

            let mut req = agent_turn_request("s", "inline prompt, should be ignored");
            req.agent_ref = Some("faq".to_string());

            let cfg = resolve_turn_config(&req, &merged, "graph.s").expect("resolves");

            assert!(cfg.system_prompt.contains("FAQ-BOT"));
            assert!(cfg.memory.is_some(), "memory carried through unstripped");
            assert!(
                cfg.knowledge.is_some(),
                "knowledge carried through unstripped"
            );
        }

        #[test]
        fn resolve_turn_config_agent_ref_none_returns_stripped_inline_config() {
            let merged = HashMap::new();
            let req = agent_turn_request("s", "You triage.");

            let cfg = resolve_turn_config(&req, &merged, "graph.s").expect("resolves");

            assert_eq!(cfg.system_prompt, "You triage.");
            assert_eq!(cfg.agent_id, "graph.s");
            assert!(cfg.memory.is_none());
            assert!(cfg.knowledge.is_none());
            assert!(cfg.guardrails.is_empty());
        }

        #[test]
        fn resolve_turn_config_agent_ref_missing_returns_err() {
            let merged = HashMap::new();
            let mut req = agent_turn_request("s", "inline prompt");
            req.agent_ref = Some("missing".to_string());

            let err = resolve_turn_config(&req, &merged, "graph.s").expect_err("must error");

            assert!(
                err.to_string()
                    .contains("referenced agent 'missing' not found"),
                "unexpected error message: {err}"
            );
        }

        #[tokio::test]
        async fn run_one_agent_turn_with_agent_ref_uses_referenced_config_system_prompt() {
            let (state_store, ext_runtime, telemetry, token_meter, ledger) = agent_turn_effects();
            let real_tenant = real_tenant_ctx("acme", "prod");

            let mut merged = HashMap::new();
            merged.insert("faq".to_string(), faq_bot_agent_config());

            let mut req = agent_turn_request("s", "inline prompt, should NOT be used");
            req.agent_ref = Some("faq".to_string());

            let llm = recording_llm("all good [[RESOLVED]]");

            let result = run_one_agent_turn(
                req,
                state_store,
                ext_runtime,
                llm.clone(),
                telemetry,
                token_meter,
                ledger,
                None,
                Arc::new(vec![]),
                Arc::new(merged),
                None,
                &real_tenant,
            )
            .await
            .expect("turn should succeed");

            assert!(result.resolved);
            assert_eq!(result.reply, "all good");

            let seen = llm.seen_system_prompts.lock().expect("lock poisoned");
            assert_eq!(seen.len(), 1);
            assert!(
                seen[0].contains("FAQ-BOT"),
                "expected the referenced agent's system prompt, got: {}",
                seen[0]
            );
            assert!(
                !seen[0].contains("should NOT be used"),
                "the node's inline system prompt must not be used when agent_ref is set"
            );
        }

        #[tokio::test]
        async fn run_one_agent_turn_without_agent_ref_uses_inline_system_prompt() {
            // Regression: agent_ref = None stays byte-unchanged — the node's
            // own inline system_prompt is what the LLM sees, not any entry
            // from `merged_agents` (even when one happens to exist).
            let (state_store, ext_runtime, telemetry, token_meter, ledger) = agent_turn_effects();
            let real_tenant = real_tenant_ctx("acme", "prod");

            let mut merged = HashMap::new();
            merged.insert("faq".to_string(), faq_bot_agent_config());

            let req = agent_turn_request("s", "You triage inline.");
            let llm = recording_llm("all good [[RESOLVED]]");

            let result = run_one_agent_turn(
                req,
                state_store,
                ext_runtime,
                llm.clone(),
                telemetry,
                token_meter,
                ledger,
                None,
                Arc::new(vec![]),
                Arc::new(merged),
                None,
                &real_tenant,
            )
            .await
            .expect("turn should succeed");

            assert!(result.resolved);

            let seen = llm.seen_system_prompts.lock().expect("lock poisoned");
            assert_eq!(seen.len(), 1);
            assert!(seen[0].contains("You triage inline."));
            assert!(!seen[0].contains("FAQ-BOT"));
        }

        #[tokio::test]
        async fn run_one_agent_turn_with_missing_agent_ref_errors() {
            let (state_store, ext_runtime, telemetry, token_meter, ledger) = agent_turn_effects();
            let real_tenant = real_tenant_ctx("acme", "prod");

            let mut req = agent_turn_request("s", "inline prompt");
            req.agent_ref = Some("missing".to_string());

            let err = run_one_agent_turn(
                req,
                state_store,
                ext_runtime,
                plain_reply_llm("unused"),
                telemetry,
                token_meter,
                ledger,
                None,
                Arc::new(vec![]),
                Arc::new(HashMap::new()),
                None,
                &real_tenant,
            )
            .await
            .expect_err("must error when agent_ref doesn't resolve");

            assert!(
                err.to_string()
                    .contains("referenced agent 'missing' not found"),
                "unexpected error message: {err}"
            );
        }

        /// The specialist case: `inherit_from` adopts the worker's capability
        /// surface WITHOUT overwriting the node's own prompt. If this ever
        /// regressed to `agent_ref` semantics, every specialist in a generated
        /// graph would collapse into an identical copy of its parent.
        #[test]
        fn inherit_from_keeps_the_node_prompt_and_adopts_the_capability_surface() {
            use greentic_aw_runtime::config::GuardrailRef;

            let mut parent = faq_bot_agent_config();
            parent.guardrails = vec![GuardrailRef {
                cap_id: "greentic:guardrail/pii".to_string(),
                offer_id: None,
                config: serde_json::json!({}),
                mode: Default::default(),
            }];
            let mut merged = HashMap::new();
            merged.insert("faq".to_string(), parent);

            let mut req = agent_turn_request("spec-billing", "You handle billing only.");
            req.inherit_from = Some("faq".to_string());

            let cfg = resolve_turn_config(&req, &merged, "graph.spec-billing")
                .expect("a resolvable inherit_from must succeed");

            assert_eq!(
                cfg.system_prompt, "You handle billing only.",
                "the specialist keeps its OWN prompt — this is the whole point"
            );
            assert_eq!(cfg.guardrails.len(), 1, "guardrails are inherited");
            assert!(cfg.memory.is_some(), "memory is inherited");
            assert!(cfg.knowledge.is_some(), "knowledge is inherited");
        }

        /// An unresolvable `inherit_from` fails rather than running stripped.
        #[test]
        fn inherit_from_unresolvable_fails_rather_than_running_stripped() {
            let merged = HashMap::new();
            let mut req = agent_turn_request("spec", "Specialist prompt.");
            req.inherit_from = Some("ghost".to_string());

            let err = resolve_turn_config(&req, &merged, "graph.spec")
                .expect_err("an unknown inherit_from must not silently succeed");
            let msg = err.to_string();
            assert!(
                msg.contains("ghost") && msg.contains("refusing to run"),
                "error should name the id and say it refused: {msg}"
            );
        }

        /// `agent_ref` still wins when both are set — it already takes
        /// everything, including the prompt.
        #[test]
        fn agent_ref_takes_precedence_over_inherit_from() {
            let mut merged = HashMap::new();
            merged.insert("faq".to_string(), faq_bot_agent_config());

            let mut req = agent_turn_request("spec", "Specialist prompt.");
            req.agent_ref = Some("faq".to_string());
            req.inherit_from = Some("faq".to_string());

            let cfg = resolve_turn_config(&req, &merged, "graph.spec").expect("resolvable");
            assert_eq!(
                cfg.system_prompt, "You are FAQ-BOT. Answer only from the knowledge base.",
                "agent_ref adopts the referenced prompt wholesale"
            );
        }

        /// A supervisor with no `agent_ref` keeps today's behaviour: no
        /// guardrails, and nothing fails.
        #[test]
        fn supervisor_without_agent_ref_resolves_no_guardrails() {
            let merged = HashMap::new();
            let got = resolve_supervisor_guardrails(None, &merged, "router")
                .expect("absent agent_ref is not an error");
            assert!(got.is_empty());
        }

        /// The point of the whole change: a supervisor pointed at its worker
        /// runs under that worker's guardrails.
        #[test]
        fn supervisor_with_agent_ref_inherits_the_parent_guardrails() {
            use greentic_aw_runtime::config::GuardrailRef;

            let mut parent = faq_bot_agent_config();
            parent.guardrails = vec![GuardrailRef {
                cap_id: "greentic:guardrail/pii".to_string(),
                offer_id: None,
                config: serde_json::json!({}),
                mode: Default::default(),
            }];
            let mut merged = HashMap::new();
            merged.insert("faq".to_string(), parent);

            let got = resolve_supervisor_guardrails(Some("faq"), &merged, "router")
                .expect("a resolvable agent_ref must succeed");
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].cap_id, "greentic:guardrail/pii");
        }

        /// An unresolvable `agent_ref` FAILS rather than routing unguarded — a
        /// supervisor believed to be protected but running bare is exactly what
        /// this inheritance exists to prevent.
        #[test]
        fn supervisor_with_unresolvable_agent_ref_fails_rather_than_routing_bare() {
            let merged = HashMap::new();
            let err = resolve_supervisor_guardrails(Some("ghost"), &merged, "router")
                .expect_err("an unknown agent_ref must not silently succeed");
            let msg = err.to_string();
            assert!(
                msg.contains("ghost") && msg.contains("refusing to route"),
                "error should name the id and say it refused: {msg}"
            );
        }

        /// A supervisor inherits guardrails ONLY — the referenced worker's
        /// memory and knowledge are deliberately left behind. It routes, it
        /// does not work.
        #[test]
        fn supervisor_inherits_guardrails_but_not_memory_or_knowledge() {
            use greentic_aw_runtime::config::GuardrailRef;

            let mut parent = faq_bot_agent_config();
            parent.guardrails = vec![GuardrailRef {
                cap_id: "greentic:guardrail/pii".to_string(),
                offer_id: None,
                config: serde_json::json!({}),
                mode: Default::default(),
            }];
            assert!(parent.memory.is_some(), "fixture should carry memory");
            assert!(parent.knowledge.is_some(), "fixture should carry knowledge");

            let mut merged = HashMap::new();
            merged.insert("faq".to_string(), parent);

            let got =
                resolve_supervisor_guardrails(Some("faq"), &merged, "router").expect("resolvable");
            assert_eq!(got.len(), 1, "guardrails come across");
        }

        #[tokio::test]
        async fn run_one_supervisor_turn_without_audit_sink_completes_unchanged() {
            let (state_store, ext_runtime, telemetry, token_meter, ledger) = agent_turn_effects();
            let real_tenant = real_tenant_ctx("acme", "prod");
            let routes = vec![
                SupervisorRoute {
                    branch: "billing".into(),
                    description: "Billing questions".into(),
                },
                SupervisorRoute {
                    branch: "tech".into(),
                    description: "Technical support".into(),
                },
            ];
            let req = SupervisorRequest {
                node_id: "router".to_string(),
                system_prompt: "Route the user.".to_string(),
                model: "gpt-4o-mini".to_string(),
                routes,
                state: GraphRunState::default(),
                provider: None,
                agent_ref: None,
            };

            // No [[ROUTE:..]] sentinel in the reply -> falls back to the
            // first declared route.
            let result = run_one_supervisor_turn(
                req,
                state_store,
                ext_runtime,
                plain_reply_llm("I'm not sure, let me think."),
                telemetry,
                token_meter,
                ledger,
                Arc::new(HashMap::new()),
                None,
                &real_tenant,
            )
            .await
            .expect("supervisor turn should succeed");

            assert_eq!(result.branch, "billing");
        }

        #[tokio::test]
        async fn run_one_supervisor_turn_with_audit_sink_completes_same_branch_as_without() {
            let (state_store, ext_runtime, telemetry, token_meter, ledger) = agent_turn_effects();
            let (tx, mut rx) = tokio::sync::mpsc::channel(16);
            let sink = AuditSink::from_sender(tx);
            let real_tenant = real_tenant_ctx("acme", "prod");
            let routes = vec![
                SupervisorRoute {
                    branch: "billing".into(),
                    description: "Billing questions".into(),
                },
                SupervisorRoute {
                    branch: "tech".into(),
                    description: "Technical support".into(),
                },
            ];
            let req = SupervisorRequest {
                node_id: "router".to_string(),
                system_prompt: "Route the user.".to_string(),
                model: "gpt-4o-mini".to_string(),
                routes,
                state: GraphRunState::default(),
                provider: None,
                agent_ref: None,
            };

            let result = run_one_supervisor_turn(
                req,
                state_store,
                ext_runtime,
                plain_reply_llm("I'm not sure, let me think."),
                telemetry,
                token_meter,
                ledger,
                Arc::new(HashMap::new()),
                Some(&sink),
                &real_tenant,
            )
            .await
            .expect("supervisor turn should succeed");

            assert_eq!(result.branch, "billing");
            assert!(
                rx.try_recv().is_err(),
                "no tool call happens on this path, so no audit event is expected"
            );
        }
    }
}

#[cfg(feature = "agentic-worker")]
pub use aw::{
    GraphConfigSource, InMemoryGraphProvider, LayeredGraphProvider, RuntimeGraphNodeHandler,
    build_graph_node_handler, graph_config_from_sidecar,
};
