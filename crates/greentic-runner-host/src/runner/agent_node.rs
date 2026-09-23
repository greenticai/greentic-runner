use anyhow::Result;
use serde_json::Value;

/// Bridges a `DwAgent` flow node into the agentic-worker runtime.
///
/// The concrete impl (constructed in the runner binary, Task 4.3) wraps
/// `greentic_aw_runtime::AgentRuntime`. The engine holds it as a trait
/// object so `engine.rs` stays free of AW-runtime construction details.
#[async_trait::async_trait]
pub trait AgentNodeHandler: Send + Sync {
    /// Execute one agentic step. `flow_input` is the upstream node's
    /// JSON payload (expects at least `{"user_text": "..."}`); returns
    /// the node output JSON (`{"reply", "trail", "terminated_by"}`).
    ///
    /// `conversational` is the flow node's `conversational` flag (SP3): when
    /// true the agent is offered the host `end_conversation` tool so it can end
    /// the segment the engine park-loop maintains, regardless of the agent's
    /// own config default.
    ///
    /// `caller` is the block the messaging provider verified for this run,
    /// carried on `FlowContext` rather than inside `flow_input` — see
    /// [`crate::caller_identity`] for why identity must not arrive through a
    /// flow's own node mapping. `None` means the turn is anonymous, which is
    /// what every provider predating the contract produces.
    // `caller` is deliberately its own argument rather than a field folded
    // into `flow_input` (authorable) — so this lane, which also carries
    // `conversational`, crosses clippy's seven-argument line.
    #[allow(clippy::too_many_arguments)]
    async fn execute(
        &self,
        tenant_id: &str,
        env_id: &str,
        agent_id: &str,
        session_id: &str,
        flow_input: &Value,
        conversational: bool,
        caller: Option<&Value>,
    ) -> Result<Value>;
}

// ---------------------------------------------------------------------------
// agentic-worker feature: full DwAgent / AgentRuntime integration
// ---------------------------------------------------------------------------

#[cfg(feature = "agentic-worker")]
mod aw {
    use std::collections::HashMap;
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::str::FromStr;
    use std::sync::Arc;

    use anyhow::Result;
    use greentic_aw_runtime::config::AgentConfig;
    use greentic_aw_runtime::config_provider::ConfigProvider;
    use greentic_aw_runtime::error::{AgentError, ConfigError};
    use greentic_aw_runtime::guardrail::GuardrailDirection;
    use greentic_aw_runtime::{AgentInput, AgentRuntime, AgentStep, StepObserver, TenantContext};
    use serde_json::{Value, json};

    use crate::trace::agent_audit::AgentAuditObserver;
    use crate::trace::audit_event::{build_agent_run_metering_event, metering_subject};
    use crate::trace::audit_sink::AuditSink;
    use crate::trace::generate_audit_event_id;

    use super::AgentNodeHandler;

    // -----------------------------------------------------------------------
    // Pack-manifest agent helpers
    // -----------------------------------------------------------------------

    /// Deserialize raw agent blobs from a pack manifest into typed
    /// [`AgentConfig`] structs.
    ///
    /// Malformed blobs are skipped with a [`tracing::warn!`] so a single
    /// bad entry never prevents other agents (or pack-level operators) from
    /// loading. The `pack_id` argument is used only in log messages.
    pub fn agent_configs_from_manifest(
        pack_id: &str,
        blobs: &std::collections::BTreeMap<String, Value>,
    ) -> HashMap<String, AgentConfig> {
        blobs
            .iter()
            .filter_map(|(agent_id, blob)| {
                match serde_json::from_value::<AgentConfig>(blob.clone()) {
                    Ok(config) => Some((agent_id.clone(), config)),
                    Err(deserialize_error) => {
                        tracing::warn!(
                            pack_id,
                            agent_id,
                            error = %deserialize_error,
                            "skipping malformed agent blob in pack manifest"
                        );
                        None
                    }
                }
            })
            .collect()
    }

    /// Merge pack-provided agent configs with operator-declared ones.
    ///
    /// Pack agents form the base layer; operator entries take precedence on
    /// `agent_id` collision (operator always wins). This ensures operators can
    /// override or refine any pack-embedded agent without touching the pack
    /// itself.
    pub fn merge_agent_sources(
        pack_agents: HashMap<String, AgentConfig>,
        operator_agents: HashMap<String, AgentConfig>,
    ) -> HashMap<String, AgentConfig> {
        let mut merged = pack_agents;
        for (agent_id, operator_config) in operator_agents {
            merged.insert(agent_id, operator_config);
        }
        merged
    }

    /// Fill `blobs` from a `dw-agents.json` `sidecar` map, inserting each entry only
    /// when its agent_id is absent — so `manifest.agents` stays authoritative and the
    /// sidecar only bridges packs whose manifest could not carry agents.
    pub fn merge_sidecar_into(
        blobs: &mut std::collections::BTreeMap<String, serde_json::Value>,
        sidecar: std::collections::BTreeMap<String, serde_json::Value>,
    ) {
        for (agent_id, blob) in sidecar {
            blobs.entry(agent_id).or_insert(blob);
        }
    }

    /// Fixed, user-safe reply returned when an agentic step fails. The detailed
    /// [`greentic_aw_runtime::AgentError`] is logged but never surfaced to the
    /// flow output, so internal failure modes do not leak to end users.
    const SANITISED_ERROR_REPLY: &str = "Something went wrong. Please try again.";

    /// Build the structured JSON output emitted by a `DwAgent` node when a
    /// guardrail blocks the step.
    ///
    /// The returned value gives the downstream flow a machine-readable signal
    /// it can branch on (`guardrail.blocked == true`, `terminated_by ==
    /// "guardrail_denied"`) without leaking raw internal error details.
    ///
    /// - `direction` — inbound (user→agent) or outbound (agent→user)
    /// - `code` — guardrail-defined denial code (e.g. `"permission_denied"`)
    /// - `message` — human-readable denial reason surfaced to the user
    /// - `details` — optional JSON string with additional context; parsed into a
    ///   structured value when present, omitted (null) otherwise
    pub(super) fn guardrail_denied_json(
        direction: GuardrailDirection,
        code: &str,
        message: &str,
        details: Option<&str>,
    ) -> Value {
        let details_val =
            details.and_then(|detail_str| serde_json::from_str::<Value>(detail_str).ok());
        json!({
            "guardrail": {
                "blocked": true,
                "direction": direction.as_str(),
                "code": code,
                "message": message,
                "details": details_val,
            },
            "reply": message,
            "trail": Vec::<AgentStep>::new(),
            "terminated_by": "guardrail_denied",
        })
    }

    /// Build an [`HttpGuardrailPolicy`] from `GREENTIC_AW_ADMIN_ENDPOINT` +
    /// `GREENTIC_AW_ADMIN_TOKEN` (the same pair the agent registry uses).
    /// Returns `None` when either is unset/empty, so a non-admin deploy
    /// enforces no mandatory policy (today's behavior).
    fn guardrail_policy_from_env()
    -> Option<greentic_aw_runtime::guardrail_provider::HttpGuardrailPolicy> {
        let endpoint = std::env::var("GREENTIC_AW_ADMIN_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty())?;
        let token = std::env::var("GREENTIC_AW_ADMIN_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())?;
        Some(greentic_aw_runtime::guardrail_provider::HttpGuardrailPolicy::new(endpoint, token))
    }

    /// Production [`AgentNodeHandler`] wrapping the agentic-worker runtime.
    ///
    /// Holds a shared [`AgentRuntime`] and translates a `DwAgent` flow node's
    /// JSON payload into an [`AgentInput`], invoking one Plan-Act-Observe step
    /// per call. Construction (Task 4.3b) lives in the runner binary; the engine
    /// only ever sees this through the [`AgentNodeHandler`] trait object.
    pub struct RuntimeAgentNodeHandler {
        runtime: Arc<AgentRuntime>,
        /// Best-effort agent-step audit sink (EPIC-B B-3). `None` — the
        /// default when no NATS audit client is configured
        /// (`GREENTIC_EVENTS_NATS_URL` unset/unreachable) — keeps `execute`
        /// on the plain [`AgentRuntime::step`] path, byte-identical to the
        /// behaviour before this observer existed.
        audit_sink: Option<AuditSink>,
        /// Session-keyed streaming-observer registry (R2). `None` — the
        /// default when no registry was wired in — keeps `execute` from
        /// looking up a stream observer at all. When `Some`, `execute` looks
        /// up `session_id` in the registry on every call; a miss (no active
        /// SSE stream for that session) behaves exactly like `None`.
        stream_observers: Option<crate::http::agent_stream::StreamObserverRegistry>,
        /// Identity of the deployed unit these agents were loaded from — the
        /// revision's `bundle_id`, stamped onto every step's
        /// [`TenantContext`] so billing can attribute spend to a project.
        ///
        /// `None` on the tenant-only (legacy, non-revision) pack path, where no
        /// bundle is pinned. Billing then OMITS the `project_id` dimension
        /// rather than substituting the agent id, which is not unique across
        /// packs — see [`greentic_aw_runtime::TenantContext::project_id`].
        project_id: Option<String>,
    }

    impl RuntimeAgentNodeHandler {
        /// Wrap a shared [`AgentRuntime`] in a flow-node handler. `audit_sink`
        /// is `Some` only when a NATS audit client was configured; `execute`
        /// then drives the step via [`AgentRuntime::step_with_observer`] with
        /// an [`AgentAuditObserver`] so tool calls/results are published to
        /// `audit.<tenant>.agent.<event>`. When `None`, `execute` uses
        /// [`AgentRuntime::step`] directly (no observer, no behaviour change).
        ///
        /// `stream_observers` (R2) is the session-keyed registry a
        /// `POST /agent/chat/stream` handler inserts into before dispatching a
        /// turn; `execute` looks up `session_id` in it and, when present, fans
        /// the step's token/tool callbacks out to that observer alongside the
        /// audit observer (via [`crate::http::agent_stream::CompositeObserver`]
        /// when both are present).
        ///
        /// `project_id` is the deployed unit's `bundle_id` (`None` on the
        /// legacy tenant-only path); it becomes the `project_id` billing
        /// dimension of every step this handler runs.
        pub fn new(
            runtime: Arc<AgentRuntime>,
            audit_sink: Option<AuditSink>,
            stream_observers: Option<crate::http::agent_stream::StreamObserverRegistry>,
            project_id: Option<String>,
        ) -> Self {
            Self {
                runtime,
                audit_sink,
                stream_observers,
                project_id,
            }
        }
    }

    /// Build a [`greentic_types::TenantCtx`] for the agent-audit observer from
    /// the flow node's plain `tenant_id`/`env_id` strings. Mirrors
    /// `HostConfig::tenant_ctx`'s fallback-to-"local" pattern: an id that fails
    /// the newtype's validation (which should not happen once a flow has been
    /// routed to a tenant) still yields a well-formed `TenantCtx` rather than
    /// panicking — the audit event is best-effort, never load-bearing.
    fn tenant_ctx_for_audit(tenant_id: &str, env_id: &str) -> greentic_types::TenantCtx {
        let env = greentic_types::EnvId::from_str(env_id)
            .unwrap_or_else(|_| greentic_types::EnvId::new("local").expect("local env id"));
        let tenant = greentic_types::TenantId::from_str(tenant_id)
            .unwrap_or_else(|_| greentic_types::TenantId::new("local").expect("local tenant id"));
        greentic_types::TenantCtx::new(env, tenant)
    }

    #[async_trait::async_trait]
    impl AgentNodeHandler for RuntimeAgentNodeHandler {
        async fn execute(
            &self,
            tenant_id: &str,
            env_id: &str,
            agent_id: &str,
            session_id: &str,
            flow_input: &Value,
            conversational: bool,
            caller: Option<&Value>,
        ) -> Result<Value> {
            let user_text = flow_input
                .get("user_text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            // A block the provider stamped but this runtime cannot decode is
            // NOT silently treated as verified: the decode failure warns and
            // the turn proceeds anonymously, which is the restrictive
            // direction and the same state a provider that stamped nothing
            // produces.
            let verified_caller = caller.and_then(|block| {
                match serde_json::from_value::<greentic_aw_runtime::VerifiedCaller>(block.clone()) {
                    Ok(caller) => Some(caller),
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "provider stamped a caller block this runtime cannot decode; \
                             running the turn anonymously"
                        );
                        None
                    }
                }
            });
            let tenant = TenantContext::new(tenant_id, env_id)
                .with_project_id(self.project_id.clone())
                .with_caller(verified_caller);
            let input = AgentInput {
                text: user_text,
                conversational,
            };

            // Off by default: with neither an audit sink nor a registered
            // stream observer, this is exactly the pre-existing
            // `self.runtime.step(...)` call — no observer is constructed and
            // behaviour is byte-identical to before EPIC-B B-3 / R2.
            let mut observers: Vec<Arc<dyn StepObserver>> = Vec::new();
            if let Some(sink) = &self.audit_sink {
                observers.push(Arc::new(AgentAuditObserver::new(
                    sink.clone(),
                    tenant_ctx_for_audit(tenant_id, env_id),
                    agent_id.to_string(),
                    session_id.to_string(),
                )));
            }
            if let Some(reg) = &self.stream_observers
                && let Some(entry) = reg.get(session_id)
            {
                observers.push(entry.value().clone());
            }
            let step_result = match observers.len() {
                0 => self.runtime.step(tenant, session_id, agent_id, input).await,
                1 => {
                    self.runtime
                        .step_with_observer(
                            tenant,
                            session_id,
                            agent_id,
                            input,
                            observers.remove(0),
                        )
                        .await
                }
                _ => {
                    let composite: Arc<dyn StepObserver> =
                        Arc::new(crate::http::agent_stream::CompositeObserver::new(observers));
                    self.runtime
                        .step_with_observer(tenant, session_id, agent_id, input, composite)
                        .await
                }
            };

            match step_result {
                Ok(output) => {
                    // Best-effort per-run metering event (EPIC-D D-1), emitted
                    // alongside (never instead of) the per-step agent-audit
                    // events above. Off by default: with no audit sink
                    // configured, no metering event is built or sent — this
                    // mirrors the audit-sink "off" branch's byte-identical
                    // behaviour above.
                    if let Some(sink) = &self.audit_sink {
                        let tenant_ctx = tenant_ctx_for_audit(tenant_id, env_id);
                        sink.emit(
                            metering_subject(tenant_id),
                            &build_agent_run_metering_event(
                                &tenant_ctx,
                                agent_id,
                                output.trail.len(),
                                chrono::Utc::now(),
                                generate_audit_event_id(),
                            ),
                        );
                    }

                    Ok(json!({
                        "reply": output.reply,
                        "trail": output.trail,
                        "terminated_by": output.terminated_by,
                        "usage": output.usage,
                    }))
                }
                Err(AgentError::GuardrailDenied {
                    direction,
                    code,
                    message,
                    details,
                }) => {
                    // Surface guardrail denials as a structured, machine-readable
                    // node output so flows can branch on `guardrail.blocked`.
                    // The denial code and message are intentionally visible to
                    // the flow (they are governance messages, not internal detail).
                    tracing::info!(
                        agent_id,
                        session_id,
                        %code,
                        "DwAgent blocked by guardrail"
                    );
                    Ok(guardrail_denied_json(
                        direction,
                        &code,
                        &message,
                        details.as_deref(),
                    ))
                }
                Err(error) => {
                    // Never leak the internal AgentError to the flow output. Log
                    // the detail for operators; return a sanitised reply only.
                    tracing::warn!(error = %error, agent_id, session_id, "DwAgent step failed");
                    let mut out = json!({
                        "reply": SANITISED_ERROR_REPLY,
                        "trail": Vec::<AgentStep>::new(),
                        "terminated_by": "error",
                    });
                    // Opt-in diagnostic: surface the internal error detail (never
                    // part of the user-facing reply) when `GREENTIC_AGENT_ERROR_DETAIL`
                    // is truthy, so an operator debugging the offline Test-chat
                    // sidecar sees the real cause instead of only "Something went
                    // wrong". The runner's telemetry `main` exports tracing to OTLP,
                    // not stderr, so the `warn!` above is otherwise invisible.
                    if std::env::var("GREENTIC_AGENT_ERROR_DETAIL")
                        .map(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
                        .unwrap_or(false)
                    {
                        out["error_detail"] = json!(format!("{error} || {error:?}"));
                    }
                    Ok(out)
                }
            }
        }
    }

    /// [`ConfigProvider`] backed by the operator's [`HostConfig::agents`] map.
    ///
    /// Agents are operator-global for the MVP: lookup is keyed purely by
    /// `agent_id`; the `tenant`/`env` arguments are accepted (to satisfy the
    /// trait contract) but not used for keying. This avoids a tenant/env
    /// key-matching footgun against the dispatch path, which derives those
    /// values independently. A future per-tenant config source can replace
    /// this implementation without touching callers.
    pub struct HostConfigProvider {
        agents: HashMap<String, AgentConfig>,
    }

    impl HostConfigProvider {
        /// Wrap the operator-declared agents map in a [`ConfigProvider`].
        pub fn new(agents: HashMap<String, AgentConfig>) -> Self {
            Self { agents }
        }
    }

    impl ConfigProvider for HostConfigProvider {
        fn agent_config<'a>(
            &'a self,
            _tenant: &'a TenantContext,
            agent_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
            let found = self.agents.get(agent_id).cloned();
            let agent_id_owned = agent_id.to_string();
            Box::pin(async move { found.ok_or(ConfigError::AgentNotFound(agent_id_owned)) })
        }
    }

    /// Resolve the extension discovery directory for tool dispatch.
    ///
    /// Honours `GREENTIC_EXTENSIONS_DIR`; otherwise falls back to
    /// `~/.greentic/extensions` (the platform convention), and finally to a
    /// temp-dir path when no home directory can be resolved. A missing or
    /// empty directory is harmless — `list_tools`/`invoke_tool` simply return
    /// empty/NotFound, which is correct for tool-less agents.
    fn extension_discovery_dir() -> PathBuf {
        if let Ok(dir) = std::env::var("GREENTIC_EXTENSIONS_DIR")
            && !dir.is_empty()
        {
            return PathBuf::from(dir);
        }
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(".greentic").join("extensions");
        }
        std::env::temp_dir().join("greentic").join("extensions")
    }

    /// Resolve the directory scanned for `<agent_id>.json` Digital Worker manifests.
    ///
    /// Honours `GREENTIC_AGENT_MANIFESTS_DIR`; otherwise `~/.greentic/agents`, and
    /// finally a temp-dir path when no home is resolvable (keeps the fn total). A
    /// missing dir is harmless — the overlay provider simply finds no manifest and
    /// returns the YAML base unchanged.
    fn manifests_discovery_dir() -> PathBuf {
        if let Ok(dir) = std::env::var("GREENTIC_AGENT_MANIFESTS_DIR")
            && !dir.is_empty()
        {
            return PathBuf::from(dir);
        }
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(".greentic").join("agents");
        }
        std::env::temp_dir().join("greentic").join("agents")
    }

    /// Build an [`HttpConfigProvider`] from `GREENTIC_AW_ADMIN_ENDPOINT` +
    /// `GREENTIC_AW_ADMIN_TOKEN`. Returns `None` when either is unset/empty, so
    /// the runtime keeps using the local overlay alone.
    fn registry_from_env() -> Option<greentic_aw_runtime::HttpConfigProvider> {
        let endpoint = std::env::var("GREENTIC_AW_ADMIN_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty())?;
        let token = std::env::var("GREENTIC_AW_ADMIN_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())?;
        Some(greentic_aw_runtime::HttpConfigProvider::new(
            endpoint, token,
        ))
    }

    /// Build an [`McpToolSource`] from the same admin endpoint/token the agent
    /// registry uses (`GREENTIC_AW_ADMIN_ENDPOINT` + `GREENTIC_AW_ADMIN_TOKEN`).
    ///
    /// MCP tools are ON by default whenever the admin credentials are present:
    /// exposure is already authorized twice upstream (the tenant registers the
    /// server with the `agentic_worker` role in admin, and the agent's
    /// allowlist must explicitly reference `mcp:<server_id>`), so a configured
    /// runner participates without extra ceremony. `GREENTIC_AW_MCP=0` is the
    /// operator opt-out escape hatch for environments where outbound calls to
    /// tenant-registered MCP servers must stay disabled. Returns `None` on
    /// opt-out or when either credential is missing/empty.
    ///
    /// `secrets`, when `Some`, is threaded into [`McpToolSource::with_secrets`]
    /// so `local-wasm` tools dispatched from this source's catalogs can read
    /// their tenant credentials. Callers with no per-tenant secrets context
    /// (the process-level serve path) pass `None`, matching prior behavior.
    ///
    /// [`McpToolSource`]: greentic_aw_runtime::McpToolSource
    /// [`McpToolSource::with_secrets`]: greentic_aw_runtime::McpToolSource::with_secrets
    pub(crate) fn mcp_source_from_env(
        secrets: Option<crate::secrets::DynSecretsManager>,
    ) -> Option<Arc<greentic_aw_runtime::McpToolSource>> {
        if std::env::var("GREENTIC_AW_MCP").ok().as_deref() == Some("0") {
            tracing::info!("GREENTIC_AW_MCP=0; MCP tool source disabled");
            return None;
        }
        let endpoint = std::env::var("GREENTIC_AW_ADMIN_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty())?;
        let token = std::env::var("GREENTIC_AW_ADMIN_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())?;
        tracing::info!(endpoint = %endpoint, "MCP tool source constructed");
        let source = match secrets {
            Some(manager) => {
                greentic_aw_runtime::McpToolSource::with_secrets(endpoint, token, manager)
            }
            None => greentic_aw_runtime::McpToolSource::new(endpoint, token),
        };
        Some(Arc::new(source))
    }

    /// Build the MCP tool source from the route material the operator's loaded
    /// packs carry in `assets/mcp-routes.json`, for a deployed runner that has
    /// no admin credentials.
    ///
    /// Mirrors [`component_source_from_packs`] and [`flow_source_from_packs`]:
    /// discover from the packs rather than a remote admin. It is the FALLBACK
    /// behind [`mcp_source_from_env`], never a replacement — see the precedence
    /// note at the two construction sites.
    ///
    /// Honours the same `GREENTIC_AW_MCP=0` opt-out as [`mcp_source_from_env`]:
    /// an operator who disabled outbound MCP must not have it re-enabled by a
    /// pack. Returns `None` when disabled, when no pack is loaded, or when no
    /// loaded pack declares any route — so `mcp:` tool refs then resolve from
    /// their `ToolRef` contract for the LLM tool list and report an honest
    /// "unknown mcp tool" if actually called.
    ///
    /// Route ids are deduplicated across packs, first pack wins. Server ids are
    /// tenant-scoped in the admin, so a collision between two packs of the same
    /// tenant means the same server; a collision across tenants in one host is
    /// pre-existing and not made worse here (spec §11 O-3).
    ///
    /// The dedup cannot mix UNITS: `packs` is one revision's pack list (the
    /// runtime is built per `TenantRuntime`, i.e. per deployed unit), so every
    /// record here belongs to the unit named by `unit`. `unit` is that
    /// revision's `bundle_id` and scopes the credential read (see
    /// [`greentic_aw_runtime::mcp_secrets::read_mcp_secret_for_unit`]); `None`
    /// on the legacy tenant-only runtime keeps the team / `_` lookup exactly.
    /// The secrets manager an agent's `mcp:` tools resolve their credential
    /// with — the injected host manager, UNLESS `SECRETS_BACKEND` explicitly
    /// names one.
    ///
    /// # Why this is not simply the injected manager
    ///
    /// It was, and that made the pack-backed MCP path unreachable in the lane
    /// it was built for. `greentic-start` injects its own manager
    /// (`HostBuilder::with_secrets_manager`, `revision_boot.rs`) whose kind is
    /// one of `dev-store` / `env` / `vault` — there is no broker variant — and
    /// greentic-designer's `orchestrate::mcp_runtime` projects
    /// `SECRETS_BACKEND=broker` + the two `SECRETS_BROKER_*` variables onto
    /// exactly that child. So the operator asked for a broker, the projection
    /// arrived intact, and the agent loop resolved against a manager that had
    /// never heard of it: every `mcp:` tool failed with "no credential at
    /// secrets://…", having sent no request to the broker at all. Verified end
    /// to end on 2026-08-21 (greentic-designer `docs/superpowers/specs/
    /// 2026-08-20-aw-mcp-in-deployed-bundles-design.md`, the Slice 8 note).
    ///
    /// The flow-node path had the SAME problem and was not fixed alongside
    /// this one: it built its own manager from `SECRETS_BACKEND` and ignored
    /// the injected one, so every `mcp` node in a Cloud Run or Kubernetes
    /// deployment failed on a credential it never looked for in the right
    /// store. This comment previously asserted the opposite, which is why that
    /// went unnoticed for the life of #706. Both paths now share
    /// [`crate::runner::mcp_node::aw::choose_mcp_secrets`].
    ///
    /// # Why it is gated on the variable being SET
    ///
    /// An unset `SECRETS_BACKEND` must keep the injected manager, or every host
    /// that deliberately supplies one — the designer's in-process host, a
    /// greentic-start pod on Vault — would be silently downgraded to the `env`
    /// backend, which `SecretsBackend::from_env` returns for an absent value.
    /// That would break working deployments to fix a broken one. The variable
    /// being present is the operator's explicit instruction, and only then does
    /// it win.
    ///
    /// A failure to build from the variable falls back to the injected manager
    /// rather than to nothing: a malformed broker endpoint should degrade to
    /// the host's own resolution, not strip the agent of every credential.
    pub(crate) fn mcp_secrets_manager(
        injected: &crate::secrets::DynSecretsManager,
    ) -> crate::secrets::DynSecretsManager {
        let requested = std::env::var("SECRETS_BACKEND").ok();
        crate::runner::mcp_node::aw::choose_mcp_secrets(
            requested.as_deref(),
            crate::runner::mcp_node::aw::secrets_from_env(),
            Some(injected),
        )
        // `choose_mcp_secrets` returns `None` only when nothing at all is
        // available, and `injected` is `Some` here by construction.
        .unwrap_or_else(|| injected.clone())
    }

    pub(crate) fn mcp_source_from_packs(
        packs: &[Arc<crate::pack::PackRuntime>],
        tenant: &str,
        secrets: Option<crate::secrets::DynSecretsManager>,
        unit: Option<&str>,
    ) -> Option<Arc<greentic_aw_runtime::McpToolSource>> {
        if std::env::var("GREENTIC_AW_MCP").ok().as_deref() == Some("0") {
            tracing::info!("GREENTIC_AW_MCP=0; pack-backed MCP tool source disabled");
            return None;
        }
        if packs.is_empty() {
            return None;
        }

        let mut seen = std::collections::HashSet::new();
        let mut records = Vec::new();
        for pack in packs {
            let Some(routes) = pack.mcp_routes() else {
                continue;
            };
            for route in routes.iter() {
                if !seen.insert(route.server_id.clone()) {
                    continue;
                }
                records.push(greentic_aw_runtime::McpPackRoute {
                    server_id: route.server_id.clone(),
                    transport: route.transport.clone(),
                    transport_url: route.transport_url.clone(),
                    auth_header_name: route.auth_header_name.clone(),
                    auth_team: route.auth_team.clone(),
                    component_ref: route.component_ref.clone(),
                    component_version: route.component_version.clone(),
                    component_digest: route.component_digest.clone(),
                });
            }
        }

        if records.is_empty() {
            return None;
        }
        tracing::info!(
            tenant = %tenant,
            servers = records.len(),
            "pack-backed MCP tool source constructed"
        );
        Some(Arc::new(
            greentic_aw_runtime::McpToolSource::from_pack_routes(records, secrets)
                .with_unit(unit.map(str::to_string)),
        ))
    }

    /// Build the component tool source from the operator's loaded packs, gated
    /// by `GREENTIC_AW_COMPONENT_TOOLS` (set to "0" to disable). Returns `None`
    /// when disabled or when no packs are loaded, so `component:` tool refs then
    /// resolve to nothing. Mirrors [`mcp_source_from_env`] but discovers tools
    /// from in-pack components rather than a remote admin.
    pub(crate) fn component_source_from_packs(
        packs: &[Arc<crate::pack::PackRuntime>],
        tenant: &str,
    ) -> Option<Arc<greentic_aw_runtime::ComponentToolSource>> {
        if std::env::var("GREENTIC_AW_COMPONENT_TOOLS").ok().as_deref() == Some("0") {
            tracing::info!("GREENTIC_AW_COMPONENT_TOOLS=0; component tool source disabled");
            return None;
        }
        if packs.is_empty() {
            return None;
        }
        let invoker = Arc::new(
            crate::runner::component_invoker::PackRuntimeComponentInvoker::new(
                packs.to_vec(),
                tenant.to_string(),
            ),
        );
        tracing::info!(tenant = %tenant, packs = packs.len(), "component tool source constructed");
        Some(Arc::new(greentic_aw_runtime::ComponentToolSource::new(
            invoker,
        )))
    }

    /// Build the flow tool source from the operator's loaded packs, gated by
    /// `GREENTIC_AW_FLOW_TOOLS` (set to "0" to disable). Returns `None` when
    /// disabled or when no packs are loaded, so `flow:` tool refs then resolve
    /// to nothing. Mirrors [`component_source_from_packs`] but discovers tools
    /// from in-pack flows rather than in-pack components.
    pub(crate) fn flow_source_from_packs(
        packs: &[Arc<crate::pack::PackRuntime>],
        tenant: &str,
    ) -> Option<Arc<greentic_aw_runtime::FlowToolSource>> {
        if std::env::var("GREENTIC_AW_FLOW_TOOLS").ok().as_deref() == Some("0") {
            tracing::info!("GREENTIC_AW_FLOW_TOOLS=0; flow tool source disabled");
            return None;
        }
        if packs.is_empty() {
            return None;
        }
        let invoker = Arc::new(crate::runner::flow_invoker::PackRuntimeFlowInvoker::new(
            packs.to_vec(),
            tenant.to_string(),
        ));
        tracing::info!(tenant = %tenant, packs = packs.len(), "flow tool source constructed");
        Some(Arc::new(greentic_aw_runtime::FlowToolSource::new(invoker)))
    }

    /// Build the sorla (SoR BusinessAction) tool source from a configured
    /// SoRX deployment, gated by `GREENTIC_AW_SORLA_TOOLS` (set to "0" to
    /// disable) and addressed by `GREENTIC_AW_SORX_URL`. Returns `None` when
    /// disabled or when no SoRX URL is configured, so `sorla:` tool refs then
    /// resolve to nothing. Mirrors [`component_source_from_packs`] but
    /// discovers tools from a deployed SoR's capability-admin API rather than
    /// in-pack components.
    ///
    /// The capability fetch (`SorxHttpInvoker::fetch`) is infallible: a
    /// down/unreachable SoR degrades to an empty operation set (logged)
    /// rather than blocking worker startup, so this still returns `Some`
    /// whenever a URL is configured.
    pub(crate) async fn sorla_source_from_env() -> Option<Arc<greentic_aw_runtime::SorlaToolSource>>
    {
        if std::env::var("GREENTIC_AW_SORLA_TOOLS").ok().as_deref() == Some("0") {
            tracing::info!("GREENTIC_AW_SORLA_TOOLS=0; sorla tool source disabled");
            return None;
        }
        let base = std::env::var("GREENTIC_AW_SORX_URL")
            .ok()
            .filter(|s| !s.is_empty())?;
        let invoker = Arc::new(crate::runner::sorx_invoker::SorxHttpInvoker::fetch(base).await);
        tracing::info!("sorla tool source constructed");
        Some(Arc::new(greentic_aw_runtime::SorlaToolSource::new(invoker)))
    }

    /// Build the production [`greentic_ext_runtime::ExtensionRuntime`] used for
    /// tool dispatch, wrapped in an [`Arc`] for sharing with [`AgentRuntime`].
    ///
    /// On construction failure (e.g. wasmtime engine init), logs the error and
    /// returns `None`; the caller then disables `DwAgent` nodes rather than
    /// panicking.
    ///
    /// Unlike the designer (which installs extensions through an explicit flow),
    /// the runner has no install step — so it performs an initial scan of the
    /// `design/` kind directory under the discovery root and registers each
    /// on-disk extension here. Without this the agentic worker would boot with
    /// an empty tool runtime and every extension tool would be silently dropped.
    /// Per-extension failures (bad signature, malformed describe) are logged and
    /// skipped so one broken extension never aborts boot; the watcher still
    /// hot-reloads later changes.
    /// Resolves an agentic-worker tool extension's `secret://…` reference to an
    /// environment variable: strip the `secret://` scheme and upper-case every
    /// run of non-alphanumeric chars to a single `_` — e.g.
    /// `secret://tavily/api_key` → `TAVILY_API_KEY`. Lets local/desktop runs
    /// supply tool secrets via the env (the in-process AW path has no broker).
    pub(crate) struct EnvSecretsBackend;

    impl greentic_ext_runtime::SecretsBackend for EnvSecretsBackend {
        fn get(&self, uri: &str) -> Result<String, greentic_ext_runtime::SecretsError> {
            let name = env_var_name_for_secret(uri);
            std::env::var(&name)
                .map_err(|_| greentic_ext_runtime::SecretsError::NotFound(uri.to_string()))
        }
    }

    /// Tool-secret backend that resolves an extension's `secret://<provider>/<key>`
    /// reference from the per-tenant secrets store first, then falls back to the
    /// process env. This is what makes `gtc start` zero-env: `gtc setup` persists
    /// the value in the dev store, and the injected secrets manager's read-side
    /// candidate fallback bridges the canonical store scope to the pack-namespaced
    /// scope setup actually wrote. The env fallback preserves existing
    /// `TAVILY_API_KEY`-style runs.
    ///
    /// `unit` is the deployed unit's `bundle_id` and scopes the store read. It
    /// has to: one environment can run the same provider credential for two
    /// workers, and without it both resolve
    /// `secrets://{env}/{tenant}/_/{provider}/{key}` — ONE address for the whole
    /// environment, so the second value staged wins and the other worker
    /// silently authenticates as the wrong account. `None` on the legacy
    /// tenant-only path and the process-level serve path, where there is no unit
    /// to scope by.
    struct StoreToolSecretsBackend {
        secrets: crate::secrets::DynSecretsManager,
        tenant: String,
        env: String,
        unit: Option<String>,
    }

    impl StoreToolSecretsBackend {
        fn new(
            secrets: crate::secrets::DynSecretsManager,
            tenant: String,
            unit: Option<String>,
        ) -> Self {
            let env = std::env::var("GREENTIC_ENV")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "dev".to_string());
            Self {
                secrets,
                tenant,
                env,
                unit,
            }
        }

        /// Every store URI a `secret://<provider>/<key>` read will try, in
        /// order: this unit's own address first, then the env-wide one.
        ///
        /// Both are built by
        /// [`crate::secrets::agent_tool_secret_uri`] — the one function the
        /// designer stages through as well, so a writer cannot spell an address
        /// this reader never asks for.
        ///
        /// REVIEW BY 2026-12-18: the env-wide candidate is a COMPATIBILITY
        /// fallback when a unit is known, not a design choice. It exists so an
        /// environment staged before this scope existed keeps working when its
        /// runner moves; the next stage writes the unit-scoped address. Once
        /// every lane stages unit-scoped tool secrets, it lets one unit read a
        /// value another unit's writer left at the shared address — decide then
        /// whether to drop it for the unit case. It remains the ONLY candidate
        /// when no unit is known. This mirrors, deliberately, the same decision
        /// `crate::secrets::pack_secret_path_candidates` records for a pack
        /// secret and `greentic_aw_runtime::mcp_secrets` for an MCP token.
        fn store_uri_candidates(&self, uri: &str) -> Vec<String> {
            let body = uri.strip_prefix("secret://").unwrap_or(uri);
            let Some((provider, key)) = body.split_once('/') else {
                return Vec::new();
            };
            let bare =
                crate::secrets::agent_tool_secret_uri(&self.env, &self.tenant, provider, key, None);
            let scoped = self.unit.as_deref().and_then(|unit| {
                crate::secrets::agent_tool_secret_uri(
                    &self.env,
                    &self.tenant,
                    provider,
                    key,
                    Some(unit),
                )
            });
            // A scoped URI equal to the bare one would make the fallback a
            // duplicate read rather than a fallback; it cannot happen (the unit
            // segment always carries `_unit_<hash>`), but the guard keeps that a
            // fact rather than a hope.
            match (scoped, bare) {
                (Some(scoped), Some(bare)) if scoped != bare => vec![scoped, bare],
                (Some(scoped), None) => vec![scoped],
                (_, Some(bare)) => vec![bare],
                (None, None) => Vec::new(),
            }
        }
    }

    impl greentic_ext_runtime::SecretsBackend for StoreToolSecretsBackend {
        fn get(&self, uri: &str) -> Result<String, greentic_ext_runtime::SecretsError> {
            let candidates = self.store_uri_candidates(uri);
            if !candidates.is_empty() {
                // Read off a dedicated thread with its own current-thread runtime:
                // the extension runtime may invoke this from within the async
                // runner, where a nested `block_on` would panic.
                let secrets = self.secrets.clone();
                let resolved = std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .ok()?;
                    runtime.block_on(async move {
                        for candidate in &candidates {
                            if let Ok(bytes) = secrets.read(candidate).await {
                                return Some(bytes);
                            }
                        }
                        None
                    })
                })
                .join()
                .ok()
                .flatten();
                if let Some(bytes) = resolved
                    && let Ok(value) = String::from_utf8(bytes)
                {
                    return Ok(value);
                }
            }
            // Fallback: env var (preserves pre-store behaviour).
            let name = env_var_name_for_secret(uri);
            std::env::var(&name)
                .map_err(|_| greentic_ext_runtime::SecretsError::NotFound(uri.to_string()))
        }
    }

    /// A process-shared blocking HTTP client for tool extensions.
    ///
    /// `reqwest::blocking::Client` owns an internal tokio runtime; dropping it
    /// from within an async context panics ("cannot drop a runtime …"). The
    /// in-process AW path creates and drops short-lived `ExtensionRuntime`s
    /// inside the async runner, so we keep ONE client alive for the whole
    /// process (built off the async runtime) and hand out cheap clones — a
    /// clone dropped in async context never drops the underlying runtime, which
    /// is released only at process exit (outside any runtime).
    fn shared_blocking_http_client() -> Option<greentic_ext_runtime::reqwest::blocking::Client> {
        use std::sync::OnceLock;
        static CLIENT: OnceLock<Option<greentic_ext_runtime::reqwest::blocking::Client>> =
            OnceLock::new();
        CLIENT
            .get_or_init(|| {
                // Build on a plain OS thread so reqwest's internal runtime is
                // not constructed inside a tokio context.
                std::thread::spawn(|| {
                    greentic_ext_runtime::reqwest::blocking::Client::builder()
                        .timeout(std::time::Duration::from_secs(30))
                        .build()
                        .ok()
                })
                .join()
                .ok()
                .flatten()
            })
            .clone()
    }

    pub(crate) fn env_var_name_for_secret(uri: &str) -> String {
        let body = uri.strip_prefix("secret://").unwrap_or(uri);
        let mut out = String::with_capacity(body.len());
        let mut prev_underscore = false;
        for ch in body.chars() {
            if ch.is_ascii_alphanumeric() {
                out.push(ch.to_ascii_uppercase());
                prev_underscore = false;
            } else if !prev_underscore {
                out.push('_');
                prev_underscore = true;
            }
        }
        out.trim_matches('_').to_string()
    }

    /// Env-keyed [`LlmPort`](greentic_ext_runtime::host_ports::LlmPort) for the
    /// extension runtime.
    ///
    /// Tool extensions that call `host.llm.complete()` internally (e.g. the
    /// adaptive-cards `generate_card` / `data_to_card` tools) route through this
    /// port. Without it wired the ext-runtime returns `"llm not configured for
    /// this runtime"`. It reuses the same env-keyed multi-provider backend the
    /// agent's OWN reasoning LLM uses ([`GreenticLlmBackend`]), so a single
    /// `GREENTIC_LLM_API_KEY` powers both the agent and its tools.
    ///
    /// Provider resolution is deliberately env-global (single key), mirroring the
    /// agent's in-process backend: `ctx`, `role`, and `extension_id` do not select
    /// a provider here (per-role/per-tenant resolution is the designer-admin's job,
    /// not the in-process runner's). The synchronous `complete` drives the async
    /// backend on a dedicated OS thread with its own current-thread runtime — the
    /// same bridge [`StoreToolSecretsBackend::get`] uses — because the ext-runtime
    /// may invoke it from within the async runner, where a nested `block_on`
    /// would panic.
    #[cfg(feature = "greentic-llm-backend")]
    pub(crate) struct EnvLlmPort {
        backend: Arc<dyn greentic_aw_runtime::llm::LlmBackend>,
        provider: String,
        model: String,
    }

    #[cfg(feature = "greentic-llm-backend")]
    impl EnvLlmPort {
        /// Default provider when `GREENTIC_LLM_PROVIDER` is unset. Mirrors the
        /// runner's DeepSeek-first posture for the in-process worker.
        const DEFAULT_PROVIDER: &'static str = "deepseek";
        /// Default model when `GREENTIC_LLM_MODEL` is unset (DeepSeek's chat model,
        /// matching `GreenticLlmBackend`'s live-test default).
        const DEFAULT_MODEL: &'static str = "deepseek-chat";

        /// Build an [`EnvLlmPort`] from the environment, or `None` when no LLM key
        /// is present. Reads `GREENTIC_LLM_API_KEY` (fallback `OPENAI_API_KEY`),
        /// `GREENTIC_LLM_PROVIDER` (default `deepseek`), `GREENTIC_LLM_MODEL`
        /// (default `deepseek-chat`), and `GREENTIC_LLM_BASE_URL` — the SAME env
        /// contract as [`in_process_llm_backend_with_key`], so the agent and its
        /// tools never resolve to different credentials.
        pub(crate) fn from_env() -> Option<Self> {
            let api_key = std::env::var("GREENTIC_LLM_API_KEY")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    std::env::var("OPENAI_API_KEY")
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                })?;
            let provider = std::env::var("GREENTIC_LLM_PROVIDER")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| Self::DEFAULT_PROVIDER.to_string());
            let model = std::env::var("GREENTIC_LLM_MODEL")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| Self::DEFAULT_MODEL.to_string());
            let base_url = std::env::var("GREENTIC_LLM_BASE_URL").ok();
            Some(Self {
                backend: Arc::new(greentic_aw_runtime::GreenticLlmBackend::new(
                    api_key, base_url,
                )),
                provider,
                model,
            })
        }
    }

    /// Map a port request onto an AW request for `provider`/`model`.
    ///
    /// Tool-calling is not exposed to extension-internal completions, so
    /// `tools` is empty; the port's `response_format` has no greentic-llm
    /// counterpart at this layer (JSON coercion, when requested, is the
    /// extension's own concern), so it is not forwarded. `credential_ref` is
    /// `None` because the backend already holds the resolved key.
    ///
    /// Shared by every [`greentic_ext_runtime::host_ports::LlmPort`]
    /// implementation in this crate (`EnvLlmPort` and
    /// `crate::runner::ext_llm_port::AgentLlmPort`) so the mapping cannot
    /// drift between them.
    #[cfg(feature = "greentic-llm-backend")]
    pub(crate) fn port_request_to_llm_request(
        request: greentic_ext_runtime::host_ports::LlmPortRequest,
        provider: &str,
        model: &str,
    ) -> greentic_aw_runtime::llm::LlmRequest {
        use greentic_aw_runtime::state::ChatMessage;

        let history = request
            .messages
            .into_iter()
            .map(|(role, content)| match role.as_str() {
                "assistant" => ChatMessage::Assistant {
                    content,
                    tool_calls: Vec::new(),
                },
                "system" => ChatMessage::System { content },
                // Any non-assistant/non-system role (notably "user") maps to a
                // user turn — the safe default for a chat completion.
                _ => ChatMessage::User { content },
            })
            .collect();

        greentic_aw_runtime::llm::LlmRequest {
            system_prompt: request.system_prompt,
            history,
            tools: Vec::new(),
            provider: greentic_aw_runtime::config::LlmProviderRef {
                provider: provider.to_string(),
                model: model.to_string(),
                credential_ref: None,
            },
        }
    }

    /// Drive an async completion on a dedicated OS thread with its own
    /// current-thread runtime. The ext-runtime may call a port from inside the
    /// async runner, where a nested `block_on` panics. Same bridge as
    /// `StoreToolSecretsBackend::get`.
    ///
    /// Shared by every [`greentic_ext_runtime::host_ports::LlmPort`]
    /// implementation in this crate so the thread bridge exists in exactly one
    /// place.
    #[cfg(feature = "greentic-llm-backend")]
    pub(crate) fn complete_on_thread(
        backend: Arc<dyn greentic_aw_runtime::llm::LlmBackend>,
        request: greentic_aw_runtime::llm::LlmRequest,
    ) -> Result<
        greentic_ext_runtime::host_ports::LlmPortResponse,
        greentic_ext_runtime::host_ports::LlmPortError,
    > {
        use greentic_ext_runtime::host_ports::{LlmPortError, LlmPortResponse};

        let result = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| LlmPortError::Backend(error.to_string()))?;
            runtime
                .block_on(backend.complete(request))
                .map_err(|error| LlmPortError::Backend(error.to_string()))
        })
        .join()
        .map_err(|_| LlmPortError::Backend("llm completion thread panicked".to_string()))??;

        let content = result.content.unwrap_or_default();
        let total_tokens = result
            .tokens_in
            .checked_add(result.tokens_out)
            .filter(|&total| total > 0);
        Ok(LlmPortResponse {
            content,
            total_tokens,
        })
    }

    #[cfg(feature = "greentic-llm-backend")]
    impl greentic_ext_runtime::host_ports::LlmPort for EnvLlmPort {
        fn complete(
            &self,
            _extension_id: &str,
            _ctx: &greentic_ext_runtime::host_ports::HostCallContext,
            _role: &str,
            request: greentic_ext_runtime::host_ports::LlmPortRequest,
        ) -> Result<
            greentic_ext_runtime::host_ports::LlmPortResponse,
            greentic_ext_runtime::host_ports::LlmPortError,
        > {
            let llm_request = port_request_to_llm_request(request, &self.provider, &self.model);
            complete_on_thread(self.backend.clone(), llm_request)
        }
    }

    pub(crate) fn build_ext_runtime(
        secrets_backend: Arc<dyn greentic_ext_runtime::SecretsBackend>,
        host_llm_port: Option<Arc<dyn greentic_ext_runtime::host_ports::LlmPort>>,
        packs: &[Arc<crate::pack::PackRuntime>],
    ) -> Option<Arc<greentic_ext_runtime::ExtensionRuntime>> {
        use greentic_ext_runtime::{
            DiscoveryPaths, ExtensionRuntime, HostOverrides, RuntimeConfig, discovery,
        };

        let root = extension_discovery_dir();
        let paths = DiscoveryPaths::new(root.clone());
        // Wire the provided secrets backend and an HTTP client so tool
        // extensions can read their declared `secret://…` references and reach
        // their upstreams. `HostOverrides::default()` leaves both empty, which
        // silently breaks any AW tool that needs either (e.g. tavily_search).
        // The per-tenant path passes a store-backed backend (zero-env); the
        // process-level serve paths pass the env-only backend.
        //
        // `llm_port` powers tool extensions that call `host.llm.complete()`
        // internally (e.g. adaptive-cards `generate_card`). Resolution order:
        //   1. A `host_llm_port` injected by the embedding host (e.g. the
        //      designer demo) — its own per-tenant, admin-backed path. NOT
        //      feature-gated: a host can inject a port regardless of features.
        //   2. Otherwise, the env-keyed `EnvLlmPort` (standalone runners), only
        //      when the `greentic-llm-backend` feature is on AND an LLM key is
        //      present in env.
        //   3. Otherwise `None`, so the ext-runtime keeps returning "llm not
        //      configured for this runtime" (unchanged no-key behaviour).
        let llm_port: Option<Arc<dyn greentic_ext_runtime::host_ports::LlmPort>> =
            match host_llm_port {
                Some(port) => {
                    tracing::info!("extension runtime LLM port wired (host-provided ext LLM)");
                    Some(port)
                }
                None => {
                    #[cfg(feature = "greentic-llm-backend")]
                    {
                        match EnvLlmPort::from_env() {
                            Some(port) => {
                                tracing::info!(
                                    "extension runtime LLM port wired (env ext LLM, env-keyed greentic-llm)"
                                );
                                Some(Arc::new(port)
                                    as Arc<dyn greentic_ext_runtime::host_ports::LlmPort>)
                            }
                            None => {
                                tracing::info!(
                                    "extension runtime LLM port not configured (no host port, no env key)"
                                );
                                None
                            }
                        }
                    }
                    #[cfg(not(feature = "greentic-llm-backend"))]
                    {
                        tracing::info!(
                            "extension runtime LLM port not configured (no host port; greentic-llm-backend off)"
                        );
                        None
                    }
                }
            };

        let overrides = HostOverrides {
            secrets_backend,
            http_client: shared_blocking_http_client(),
            llm_port,
            ..HostOverrides::default()
        };
        let config = RuntimeConfig::from_paths(paths).with_host_overrides(overrides);
        let mut runtime = match ExtensionRuntime::new(config) {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(error = %error, "extension runtime init failed; DwAgent nodes disabled");
                return None;
            }
        };

        // Initial load of on-disk design extensions (agentic-worker tools live
        // in `<root>/design/<ext>/`).
        let design_dir = root.join("design");
        let mut on_disk = 0usize;
        match discovery::scan_kind_dir(&design_dir) {
            Ok(ext_dirs) => {
                for ext_dir in ext_dirs {
                    match runtime.register_loaded_from_dir(&ext_dir) {
                        Ok(()) => on_disk += 1,
                        Err(error) => tracing::warn!(
                            error = %error, dir = %ext_dir.display(),
                            "skipping extension that failed to load"
                        ),
                    }
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, dir = %design_dir.display(), "scanning design extensions failed")
            }
        }

        // Then the extensions the loaded packs carry at `extensions/*.gtxpack`.
        //
        // This runs SECOND on purpose: `register_loaded_from_dir` inserts by
        // `ExtensionId`, so a pack pass ahead of the disk scan would let a
        // pack-frozen copy overwrite the one an operator installed. See
        // `pack_extensions::is_shadowed` for why disk wins and what that costs.
        //
        // In a k8s or Cloud Run container the directory above is empty — nothing
        // writes `GREENTIC_EXTENSIONS_DIR` there — so this is the only source
        // the worker has, and before it existed every extension tool an operator
        // bound was dropped with a warn after the deploy reported success.
        let from_packs = crate::runner::pack_extensions::register_from_packs(&mut runtime, packs);

        // Log the cap ids, not just a count: an unresolved mandatory guardrail
        // cap blocks every agent turn, and until now the only way to see which
        // caps a runner has was to trigger that failure. Emitted after both
        // sources so the line describes the registry the agent will actually
        // dispatch against.
        let mut cap_ids: Vec<String> = runtime
            .capability_registry()
            .offerings()
            .map(|offering| offering.cap_id.to_string())
            .collect();
        cap_ids.sort();
        tracing::info!(
            loaded = on_disk,
            from_packs = from_packs.loaded,
            shadowed_by_disk = from_packs.shadowed,
            pack_failures = from_packs.failed,
            dir = %design_dir.display(),
            caps = %cap_ids.join(","),
            "loaded design extensions"
        );

        Some(Arc::new(runtime))
    }

    /// Whether `provider` needs an API key to authenticate.
    ///
    /// Authoritative when `greentic-llm-backend` is compiled in: the answer
    /// comes from `greentic_llm::ProviderKind::requires_api_key()`. The local
    /// table is the fallback for builds without that feature (where
    /// `greentic_llm` is not linked at all) and for a provider string that
    /// crate does not recognise; `keyless_table_matches_greentic_llm` pins the
    /// two together.
    ///
    /// Keyless providers talk to a local daemon with no auth (Ollama,
    /// Llamafile) or authenticate out of band (Bedrock, via the AWS credential
    /// chain), so an absent or empty `GREENTIC_LLM_API_KEY` is the NORMAL state
    /// for them and must not disqualify the multi-provider backend.
    ///
    /// `None` — nothing named a provider anywhere — is treated as
    /// key-requiring, because the historical default is OpenAI.
    pub(super) fn provider_requires_api_key(provider: Option<&str>) -> bool {
        let Some(provider) = provider.map(str::trim).filter(|p| !p.is_empty()) else {
            return true;
        };
        #[cfg(feature = "greentic-llm-backend")]
        if let Ok(kind) = provider.parse::<greentic_llm::ProviderKind>() {
            return kind.requires_api_key();
        }
        keyless_table_requires_api_key(provider)
    }

    /// The local mirror of `greentic_llm::ProviderKind::requires_api_key()`,
    /// used for builds without the `greentic-llm-backend` feature and for
    /// provider strings greentic-llm does not recognise. Pinned to the real
    /// thing by `keyless_table_matches_greentic_llm`.
    fn keyless_table_requires_api_key(provider: &str) -> bool {
        !matches!(
            provider.to_ascii_lowercase().as_str(),
            "ollama" | "llamafile" | "bedrock"
        )
    }

    /// The operator's explicitly configured LLM endpoint, if any.
    ///
    /// Unset, blank or whitespace-only all mean "the operator configured
    /// nothing", matching `llm_openai::normalize_base_url`'s reading of the
    /// same variable.
    #[cfg(feature = "greentic-llm-backend")]
    fn custom_llm_base_url() -> Option<String> {
        std::env::var("GREENTIC_LLM_BASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
    }

    /// Whether a resolved (key, provider) pair selects the in-process
    /// multi-provider greentic-llm backend rather than the single-provider
    /// OpenAI fall-through.
    #[cfg(feature = "greentic-llm-backend")]
    pub(super) fn selects_multi_provider_backend(api_key: &str, provider: Option<&str>) -> bool {
        selects_multi_provider_backend_with(api_key, provider, custom_llm_base_url().as_deref())
    }

    /// The pure half of [`selects_multi_provider_backend`], split out so the
    /// rules below are testable without mutating process-global env — which
    /// races every other test in the binary, the same reasoning
    /// `llm_openai::normalize_base_url` was split for.
    ///
    /// Three ways in, and the third is the one that had to be added:
    ///
    /// 1. **A non-empty key.** Always.
    /// 2. **A keyless provider** (Ollama, Llamafile, Bedrock), which
    ///    authenticates through a local daemon or the AWS credential chain.
    /// 3. **An explicitly configured endpoint**, whatever the provider name.
    ///
    /// Rule 3 exists because `provider = "openai"` means "speaks the OpenAI
    /// protocol", not "is api.openai.com". An operator pointing
    /// `GREENTIC_LLM_BASE_URL` at Ollama's `/v1`, LM Studio, vLLM or any other
    /// local gateway has named an endpoint that legitimately wants no key —
    /// and without this they had to invent a dummy one, because an empty key
    /// dropped them onto the fall-through client.
    ///
    /// **That fall-through does not merely lack a key — it cannot reach the
    /// endpoint at all.** `OpenAiLlmBackend` appends `/v1/...` to whatever base
    /// URL it is given (see `normalize_base_url`'s doc comment), so a base URL
    /// that already ends in `/v1` becomes `/v1/v1/chat/completions` and every
    /// request 404s. Recorded here because the symptom — "keyless works with a
    /// dummy key, 404s without one" — reads like an authentication problem and
    /// is a path-construction one. The path behaviour is that client's
    /// documented contract and is deliberately left alone; this stops the
    /// keyless case from landing on it.
    #[cfg(feature = "greentic-llm-backend")]
    fn selects_multi_provider_backend_with(
        api_key: &str,
        provider: Option<&str>,
        base_url: Option<&str>,
    ) -> bool {
        !api_key.trim().is_empty()
            || !provider_requires_api_key(provider)
            || base_url.map(str::trim).is_some_and(|url| !url.is_empty())
    }

    /// The provider name an explicit `GREENTIC_LLM_PROVIDER` names, if any.
    pub(super) fn env_llm_provider() -> Option<String> {
        std::env::var("GREENTIC_LLM_PROVIDER")
            .ok()
            .filter(|provider| !provider.trim().is_empty())
    }

    /// The agent whose LLM configuration represents this runtime, chosen
    /// deterministically: sorted id order, first agent declaring a non-empty
    /// `llm.provider`. `None` when no agent declares one.
    ///
    /// Sorted, not `HashMap` order: this decides both which provider the
    /// in-process agent backend is built for
    /// ([`configured_llm_provider`]) and which provider
    /// `crate::runner::ext_llm_port::AgentLlmPort` resolves an extension's
    /// `host.llm.complete()` call to. An unstable pick means a worker that
    /// answers on a different provider after every restart — and, since both
    /// callers iterate the same map independently, a hash-order pick could
    /// also let the agent's own reasoning LLM and its extensions' LLM calls
    /// silently disagree with each other on the same boot.
    pub(crate) fn first_declared_llm_agent(
        agents: &HashMap<String, AgentConfig>,
    ) -> Option<(&String, &AgentConfig)> {
        let mut ids: Vec<&String> = agents.keys().collect();
        ids.sort();
        ids.into_iter()
            .filter_map(|id| agents.get(id).map(|agent| (id, agent)))
            .find(|(_, agent)| !agent.llm.provider.trim().is_empty())
    }

    /// The provider to judge keylessness by: the env override first (it is the
    /// deployment's explicit statement), then the first agent that declares
    /// one. The in-process backend carries a single key, so a single provider
    /// decides — matching the one-key model in
    /// [`in_process_llm_backend_with_key`].
    pub(super) fn configured_llm_provider(agents: &HashMap<String, AgentConfig>) -> Option<String> {
        env_llm_provider().or_else(|| {
            first_declared_llm_agent(agents).map(|(_, agent)| agent.llm.provider.trim().to_string())
        })
    }

    /// Resolve the [`LlmBackend`] from the environment.
    ///
    /// Prefers the LLM bridge extension when `GREENTIC_AW_LLM_EXTENSION` is set
    /// (LLM-as-extension); otherwise falls back to the env-keyed in-process
    /// OpenAI client. Shared by the single-agent (`build_agent_node_handler`)
    /// and graph (`graph_node::build_graph_node_handler`) construction paths so
    /// both resolve the backend identically.
    pub(crate) fn build_llm_backend(
        ext_runtime: &Arc<greentic_ext_runtime::ExtensionRuntime>,
    ) -> Arc<dyn greentic_aw_runtime::LlmBackend> {
        use std::time::Duration;

        use greentic_aw_runtime::{ExtensionLlmBackend, OpenAiLlmBackend, RetryingLlmBackend};

        match std::env::var("GREENTIC_AW_LLM_EXTENSION")
            .ok()
            .filter(|s| !s.trim().is_empty())
        {
            Some(ext_id) => {
                let api_key = std::env::var("GREENTIC_LLM_API_KEY")
                    .or_else(|_| std::env::var("OPENAI_API_KEY"))
                    .unwrap_or_default();
                match bridge_credential(
                    std::env::var("GREENTIC_LLM_PROVIDER").ok(),
                    std::env::var("GREENTIC_LLM_MODEL").ok(),
                    api_key,
                    std::env::var("GREENTIC_LLM_BASE_URL").ok(),
                ) {
                    Some(cred) => {
                        tracing::info!(
                            extension = %ext_id, provider = %cred.provider, model = %cred.model,
                            "AW LLM via bridge extension"
                        );
                        Arc::new(RetryingLlmBackend::new(
                            ExtensionLlmBackend::new(ext_runtime.clone(), ext_id, cred),
                            3,
                            Duration::from_millis(250),
                        ))
                    }
                    None => {
                        tracing::warn!(
                            provider = env_llm_provider().as_deref().unwrap_or("openai"),
                            "GREENTIC_AW_LLM_EXTENSION set but no LLM API key and the \
                             configured provider requires one; falling back to in-process \
                             OpenAI client"
                        );
                        Arc::new(RetryingLlmBackend::new(
                            OpenAiLlmBackend::new(String::new()),
                            3,
                            Duration::from_millis(250),
                        ))
                    }
                }
            }
            None => in_process_llm_backend(),
        }
    }

    /// In-process LLM backend when no bridge extension is configured.
    ///
    /// With the `greentic-llm-backend` feature and an LLM key present — or a
    /// keyless provider configured — routes the worker's LLM call through
    /// greentic-llm so a `dw.agent` can use any provider its `AgentConfig.llm`
    /// declares (DeepSeek, Anthropic, Gemini, Ollama, …): the provider + model
    /// ride on each request, the key + optional base URL come from the env.
    /// Otherwise falls back to the legacy env-keyed OpenAI client. Shared by
    /// every non-bridge construction path so they never drift.
    ///
    /// This path sees no agent configs, so the provider is whatever
    /// `GREENTIC_LLM_PROVIDER` names.
    pub(crate) fn in_process_llm_backend() -> Arc<dyn greentic_aw_runtime::LlmBackend> {
        in_process_llm_backend_with_key(None, env_llm_provider())
    }

    /// In-process LLM backend, optionally given a store-resolved API key.
    ///
    /// `override_key` (resolved from an agent's `credential_ref` via the
    /// per-tenant secrets store) takes precedence over the env key when present —
    /// this is what makes the in-process desktop LLM path zero-env. When `None`,
    /// the key comes from `GREENTIC_LLM_API_KEY`/`OPENAI_API_KEY` exactly as
    /// before, so env-based and bridge-less runs are unaffected.
    ///
    /// `provider` is the configured LLM provider (see
    /// [`configured_llm_provider`]). It decides one thing: whether an EMPTY key
    /// still selects the multi-provider backend. For a keyless provider it
    /// must, or an Ollama worker silently talks to `api.openai.com` with an
    /// empty bearer — which is what made `GREENTIC_LLM_API_KEY=ollama` a
    /// necessary workaround. For every key-requiring provider the behaviour is
    /// unchanged.
    pub(crate) fn in_process_llm_backend_with_key(
        override_key: Option<String>,
        provider: Option<String>,
    ) -> Arc<dyn greentic_aw_runtime::LlmBackend> {
        use greentic_aw_runtime::{OpenAiLlmBackend, RetryingLlmBackend};
        use std::time::Duration;

        // Only read under `greentic-llm-backend` (below); prefixed so the
        // binding doesn't trip `unused_variables` when that feature is off
        // (it isn't a default feature — see Cargo.toml).
        let _store_resolved = override_key
            .as_ref()
            .map(|key| !key.trim().is_empty())
            .unwrap_or(false);

        #[cfg(feature = "greentic-llm-backend")]
        {
            let api_key = override_key
                .clone()
                .filter(|key| !key.trim().is_empty())
                .or_else(|| std::env::var("GREENTIC_LLM_API_KEY").ok())
                .or_else(|| std::env::var("OPENAI_API_KEY").ok())
                .unwrap_or_default();
            if selects_multi_provider_backend(&api_key, provider.as_deref()) {
                let base_url = std::env::var("GREENTIC_LLM_BASE_URL").ok();
                if api_key.trim().is_empty() {
                    // Once per backend construction, not per token: the returned
                    // `Arc` serves the whole agent handler's lifetime.
                    tracing::warn!(
                        provider = provider.as_deref().unwrap_or_default(),
                        "AW LLM provider is keyless: running with an empty API key. \
                         This is expected for Ollama / Llamafile / Bedrock (which \
                         authenticate through a local daemon or the AWS credential \
                         chain); set GREENTIC_LLM_API_KEY only if your endpoint \
                         actually demands one"
                    );
                }
                tracing::info!(
                    store_resolved = _store_resolved,
                    provider = provider.as_deref().unwrap_or_default(),
                    "AW LLM via in-process greentic-llm (multi-provider)"
                );
                return Arc::new(RetryingLlmBackend::new(
                    greentic_aw_runtime::GreenticLlmBackend::new(api_key, base_url),
                    3,
                    Duration::from_millis(250),
                ));
            }
        }
        let openai_key = override_key
            .filter(|key| !key.trim().is_empty())
            .or_else(|| std::env::var("OPENAI_API_KEY").ok())
            .unwrap_or_default();

        // Fall-through: the single-provider OpenAI-protocol client. Reached
        // either because `greentic-llm-backend` is not compiled in (the block
        // above does not exist at all), or because it is compiled in and no
        // key resolved for a provider that REQUIRES one — a keyless provider
        // (Ollama, Llamafile, Bedrock) takes the branch above with an empty
        // key. Both mean the agent's `AgentConfig.llm.provider` is IGNORED and
        // every request goes to one OpenAI-shaped endpoint — which is a
        // silent, total misroute if the agent declared DeepSeek/Anthropic/… So
        // say so. This runs once per backend construction, not per token: the
        // returned `Arc` is reused for the whole agent handler's lifetime.
        #[cfg(feature = "greentic-llm-backend")]
        let reason = "no LLM API key resolved (tried the agent's llm.credential_ref, \
                      GREENTIC_LLM_API_KEY, then OPENAI_API_KEY)";
        #[cfg(not(feature = "greentic-llm-backend"))]
        let reason = "this binary was built WITHOUT the `greentic-llm-backend` feature, \
                      so the multi-provider backend is not compiled in";
        tracing::warn!(
            reason,
            provider = provider.as_deref().unwrap_or_default(),
            endpoint = %std::env::var("GREENTIC_LLM_BASE_URL")
                .ok()
                .filter(|url| !url.trim().is_empty())
                .unwrap_or_else(|| "https://api.openai.com".to_string()),
            fix = "rebuild with `--features greentic-llm-backend` and supply a key via \
                   GREENTIC_LLM_API_KEY or the agent's llm.credential_ref; or point \
                   GREENTIC_LLM_BASE_URL at an OpenAI-compatible endpoint",
            "AW LLM falling back to the single-provider OpenAI client: the agent's \
             configured llm.provider is IGNORED and every request goes to this endpoint"
        );

        Arc::new(RetryingLlmBackend::new(
            OpenAiLlmBackend::new(openai_key),
            3,
            Duration::from_millis(250),
        ))
    }

    /// Resolve an LLM API key from the per-tenant secrets store via the first
    /// agent that declares `llm.credential_ref`, mirroring the credential URI
    /// `secrets://default/{tenant}/_/llm/{credential_ref}` that
    /// [`greentic_aw_runtime::llm_credential::SecretsBackedCredentialResolver`]
    /// reads. This lets the in-process LLM backend be zero-env (key from store)
    /// when no `GREENTIC_LLM_API_KEY` env is set.
    ///
    /// Returns `None` when no agent declares a credential_ref or the read misses.
    /// The in-process backend carries a single key, so when agents declare
    /// different credential_refs only the first is used — matching the existing
    /// one-key in-process model; the bridge-extension path resolves per-request.
    ///
    /// `pub(crate)` (not private): also reused by `runtime.rs` to resolve the
    /// `greentic_llm::LlmProvider` key for the in-process `operala.call`
    /// (`DeepWorkerInvoker`) wiring, so the two in-process LLM paths (dw.agent,
    /// operala.call) share one key-resolution policy instead of drifting.
    pub(crate) async fn resolve_in_process_llm_key(
        secrets: &crate::secrets::DynSecretsManager,
        tenant: &str,
        merged_agents: &HashMap<String, AgentConfig>,
    ) -> Option<String> {
        let credential_ref = merged_agents
            .values()
            .find_map(|agent| agent.llm.credential_ref.clone())?;
        let uri = format!("secrets://default/{tenant}/_/llm/{credential_ref}");
        let bytes = secrets.read(&uri).await.ok()?;
        let key = String::from_utf8(bytes).ok()?.trim().to_string();
        if key.is_empty() { None } else { Some(key) }
    }

    /// Build a vault-style `BridgeCredential` from resolved parts. `None` when
    /// no API key is present AND the resolved provider needs one — a keyless
    /// provider (Ollama, Llamafile, Bedrock) is credential-complete with an
    /// empty key, and dropping it here is what used to route an Ollama worker
    /// to the OpenAI fall-through. Defaults: provider "openai", model "gpt-4o".
    /// Pure (no env) so it is unit-testable without global state.
    pub(super) fn bridge_credential(
        provider: Option<String>,
        model: Option<String>,
        api_key: String,
        base_url: Option<String>,
    ) -> Option<greentic_aw_runtime::BridgeCredential> {
        let provider = provider
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "openai".into());
        if api_key.trim().is_empty() && provider_requires_api_key(Some(&provider)) {
            return None;
        }
        Some(greentic_aw_runtime::BridgeCredential {
            provider,
            model: model
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "gpt-4o".into()),
            api_key,
            base_url: base_url.filter(|s| !s.trim().is_empty()),
        })
    }

    /// Shared store-agnostic tail: builds the extension runtime, LLM backend,
    /// config providers, and [`AgentRuntime`] given the three already-constructed
    /// store trait objects, then mounts the knowledge (RAG) and long-term-memory
    /// seams so the returned in-process runtime grounds identically to the
    /// out-of-process NATS serve path ([`build_agent_runtime`]).
    ///
    /// Extracted so both the Redis path ([`build_agent_node_handler`]) and the
    /// ephemeral desktop path ([`build_agent_node_handler_ephemeral`]) share
    /// identical post-store construction logic; store differences are the only
    /// divergence between the two callers. Returning the bare [`AgentRuntime`]
    /// (rather than the wrapped handler) also keeps the mounted knowledge seam
    /// observable to the regression test that guards this wiring.
    ///
    /// Returns `None` when the extension runtime fails to initialise (the only
    /// failure mode at this layer — store errors are handled by callers).
    ///
    /// `unit` is the deployed unit's `bundle_id` (`None` on the legacy
    /// tenant-only path); it scopes the pack-carried MCP credential read.
    #[allow(clippy::too_many_arguments)]
    async fn build_runtime_with_stores(
        merged_agents: HashMap<String, AgentConfig>,
        tenant: String,
        secrets: crate::secrets::DynSecretsManager,
        ext_llm_port: Option<Arc<dyn greentic_ext_runtime::host_ports::LlmPort>>,
        packs: Vec<Arc<crate::pack::PackRuntime>>,
        state_store: Arc<dyn greentic_aw_runtime::state::AgentStateStore>,
        token_meter: Arc<dyn greentic_aw_runtime::cost::TokenMeter>,
        ledger: Arc<dyn greentic_aw_runtime::tools::ToolLedger>,
        unit: Option<String>,
    ) -> Option<Arc<AgentRuntime>> {
        use std::time::Duration;

        use greentic_aw_runtime::LayeredConfigProvider;
        use greentic_aw_runtime::ManifestToolOverlayProvider;
        use greentic_aw_runtime::config_provider::CachingConfigProvider;
        use greentic_aw_runtime::{
            ExtensionLlmBackend, LlmBackend, OtelTelemetry, RetryingLlmBackend,
        };

        // Per-tenant path: tool secrets resolve from the store first (zero-env),
        // env as fallback. `secrets` is the injected per-tenant manager whose
        // candidate fallback bridges the gtc-setup dev-store scope.
        // The deployed unit scopes the tool-secret read for the same reason it
        // scopes the MCP one below: this runtime is built per `TenantRuntime`,
        // i.e. per deployed unit, so `unit` already names the only worker whose
        // credentials this backend may resolve.
        let secrets_backend: Arc<dyn greentic_ext_runtime::SecretsBackend> = Arc::new(
            StoreToolSecretsBackend::new(secrets.clone(), tenant.clone(), unit.clone()),
        );
        let ext_runtime = build_ext_runtime(secrets_backend, ext_llm_port, &packs)?;

        // When the LLM bridge extension is configured, resolve credentials
        // per-tenant from the secrets broker rather than from global env vars.
        // The env-keyed OpenAI fallback is preserved for both branches.
        let llm: Arc<dyn LlmBackend> = match std::env::var("GREENTIC_AW_LLM_EXTENSION")
            .ok()
            .filter(|s| !s.trim().is_empty())
        {
            Some(ext_id) => {
                use greentic_aw_runtime::llm_credential::SecretsBackedCredentialResolver;
                let resolver = Arc::new(SecretsBackedCredentialResolver::new(
                    secrets.clone(),
                    tenant.clone(),
                ));
                tracing::info!(
                    extension = %ext_id,
                    tenant = %tenant,
                    "AW LLM via bridge (per-tenant creds)"
                );
                Arc::new(RetryingLlmBackend::new(
                    ExtensionLlmBackend::with_resolver_runtime(
                        ext_runtime.clone(),
                        ext_id,
                        resolver,
                    ),
                    3,
                    Duration::from_millis(250),
                ))
            }
            None => {
                // Zero-env LLM: with no bridge extension and no env key, resolve
                // the agent's `credential_ref` from the per-tenant store (the same
                // manager whose candidate fallback bridges the gtc-setup scope).
                // An env key still wins (legacy single-provider runs unaffected).
                let env_key_present = std::env::var("GREENTIC_LLM_API_KEY")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| {
                        std::env::var("OPENAI_API_KEY")
                            .ok()
                            .filter(|value| !value.trim().is_empty())
                    })
                    .is_some();
                let store_key = if env_key_present {
                    None
                } else {
                    resolve_in_process_llm_key(&secrets, &tenant, &merged_agents).await
                };
                if store_key.is_some() {
                    tracing::info!(
                        tenant = %tenant,
                        "AW LLM key resolved from store via credential_ref (zero-env)"
                    );
                }
                // This path DOES see the agent configs, so a worker that
                // declares a keyless provider (Ollama) reaches the
                // multi-provider backend even with no key anywhere.
                in_process_llm_backend_with_key(store_key, configured_llm_provider(&merged_agents))
            }
        };

        let agent_count = merged_agents.len();
        let overlay = ManifestToolOverlayProvider::new(
            HostConfigProvider::new(merged_agents),
            manifests_discovery_dir(),
        );
        let config_provider: Arc<dyn ConfigProvider> = match registry_from_env() {
            Some(http) => Arc::new(CachingConfigProvider::new(LayeredConfigProvider::new(
                http, overlay,
            ))),
            None => Arc::new(CachingConfigProvider::new(overlay)),
        };
        let telemetry = Arc::new(OtelTelemetry);

        // One manager for every remote tool source that reads a credential at
        // run time (MCP and A2A). Hoisted out of the MCP block so both honour
        // the same explicit `SECRETS_BACKEND` override; see
        // `mcp_secrets_manager` for why that override must win over the
        // injected manager.
        let remote_tool_secrets = mcp_secrets_manager(&secrets);

        let base = AgentRuntime::new(
            config_provider,
            state_store,
            ext_runtime.clone(),
            llm,
            telemetry,
            token_meter,
            ledger,
            // The env-configured ADMIN source wins; the pack-carried routes are
            // the fallback. This is deliberately the OPPOSITE of the flow MCP
            // node's precedence (`mcp_node::aw::invoke_with_secrets`, "A
            // pack-carried route wins"), and the asymmetry is load-bearing:
            //
            // - A flow node's pack route strictly ADDS capability. The admin
            //   catalog supplies the same route and nothing else, so preferring
            //   the pack can only turn a failure into a success.
            // - The agent catalog additionally carries LIVE tool schemas probed
            //   from the server this run, and applies each server's
            //   `allowed_tools`. Preferring the pack there would downgrade every
            //   environment that has a working admin source — the designer's own
            //   in-process host among them.
            //
            // A per-server merge (pack fills only the servers the admin catalog
            // lacks) is a strictly better v2 and is out of scope: it needs a
            // merge rule for a server present in both with different URLs.
            mcp_source_from_env(Some(remote_tool_secrets.clone())).or_else(|| {
                mcp_source_from_packs(
                    &packs,
                    &tenant,
                    Some(remote_tool_secrets.clone()),
                    unit.as_deref(),
                )
            }),
        )
        .with_component_source(component_source_from_packs(&packs, &tenant))
        .with_flow_source(flow_source_from_packs(&packs, &tenant))
        .with_sorla_source(sorla_source_from_env().await)
        // Pack-only: there is no admin A2A source to prefer. This site covers
        // deployed dw.agent units, the desktop runner and the designer's
        // test-chat sidecar; `graph_node::graph_a2a_source` builds the graph
        // turns' source from the same inputs through the same helper.
        // `build_agent_runtime` (process-level serve, no packs) stays without
        // one, as it is for pack MCP.
        .with_a2a_source(crate::runner::a2a_pack_source::a2a_source_from_packs(
            &packs,
            &tenant,
            Some(remote_tool_secrets),
            unit.as_deref(),
        ));

        // Mount the long-term-memory and knowledge (RAG) seams so IN-PROCESS
        // `dw.agent` workers ground on the ingested corpus exactly as the
        // out-of-process NATS serve path does (see [`build_agent_runtime`], which
        // makes the identical call). The backends come from the agent-runtime
        // extensions the binary registered (see `runtime_ext`); with none
        // registered, or the operator env unset, this leaves `base` unchanged.
        // Without this call the in-process handler's `runtime.knowledge` stayed
        // `None`, so `knowledge_active()` was false and `search_knowledge` was
        // never invoked — the model hallucinated instead of retrieving from the
        // corpus that had already been ingested at boot.
        let base = crate::runner::runtime_ext::attach_all(base).await;
        // Knowledge delegated to a design-extension tool. Unconditional and not
        // feature-gated (see `knowledge_ext`): it wraps whatever corpus backend
        // an extension above left in place and acts only on a worker whose knowledge
        // binding names `provider.knowledge.extension`.
        let base = crate::runner::knowledge_ext::attach(base, ext_runtime);

        // Billing metering, identical to the `build_agent_runtime` serve path.
        // Without this the in-process `dw.agent` node ran on the default
        // `NoopBillingMeter`: its LLM spend was never metered AND the credit
        // gate never fired, so an out-of-credit tenant kept running for free
        // through this node while the out-of-process path stopped them.
        //
        // Ship-dark, same as the serve path: a no-op until an operator sets
        // GREENTIC_BILLING_BASE_URL + GREENTIC_BILLING_SERVICE_SECRET.
        let billing_enabled;
        let base = match greentic_aw_runtime::billing::HttpBillingMeter::from_env() {
            Some(http_meter) => {
                billing_enabled = true;
                base.with_billing_meter(Arc::new(http_meter))
            }
            None => {
                billing_enabled = false;
                base
            }
        };
        let runtime = Arc::new(base);

        // `billing_enabled` is logged rather than left implicit: this path
        // silently metered nothing for its whole existence, and a boot line
        // stating it either way is what makes that visible to an operator.
        tracing::info!(
            agent_count,
            tenant = %tenant,
            billing_enabled,
            "AW runtime constructed"
        );
        Some(runtime)
    }

    /// Wrap the shared in-process [`AgentRuntime`] (built by
    /// [`build_runtime_with_stores`], including the mounted knowledge and
    /// long-term-memory seams) in a flow-node handler. Both the Redis path
    /// ([`build_agent_node_handler`]) and the ephemeral desktop path
    /// ([`build_agent_node_handler_ephemeral`]) route through here.
    ///
    /// Returns `None` when the runtime could not be built (extension-runtime init
    /// failure — the only failure mode at this layer).
    ///
    /// `audit_sink` (EPIC-B B-3) is forwarded verbatim to the constructed
    /// [`RuntimeAgentNodeHandler`] — `None` keeps `dw.agent` execution on the
    /// plain [`AgentRuntime::step`] path. `stream_observers` (R2) is likewise
    /// forwarded verbatim — `None` keeps `execute` from consulting the
    /// session-keyed streaming-observer registry at all.
    ///
    /// `project_id` is the deployed unit's `bundle_id`, forwarded verbatim so
    /// billing can attribute this runtime's spend to a project. `None` on the
    /// legacy tenant-only path, where the dimension is omitted entirely.
    #[allow(clippy::too_many_arguments)]
    async fn build_runtime_handler_with_stores(
        merged_agents: HashMap<String, AgentConfig>,
        tenant: String,
        secrets: crate::secrets::DynSecretsManager,
        ext_llm_port: Option<Arc<dyn greentic_ext_runtime::host_ports::LlmPort>>,
        packs: Vec<Arc<crate::pack::PackRuntime>>,
        state_store: Arc<dyn greentic_aw_runtime::state::AgentStateStore>,
        token_meter: Arc<dyn greentic_aw_runtime::cost::TokenMeter>,
        ledger: Arc<dyn greentic_aw_runtime::tools::ToolLedger>,
        audit_sink: Option<AuditSink>,
        stream_observers: Option<crate::http::agent_stream::StreamObserverRegistry>,
        project_id: Option<String>,
    ) -> Option<AgentNodeWiring> {
        // The deployed unit (`bundle_id`) doubles as the MCP credential scope:
        // the same identity billing attributes this runtime's spend to.
        let runtime = build_runtime_with_stores(
            merged_agents,
            tenant,
            secrets,
            ext_llm_port,
            packs,
            state_store,
            token_meter,
            ledger,
            project_id.clone(),
        )
        .await?;
        let handler: Arc<dyn AgentNodeHandler> = Arc::new(RuntimeAgentNodeHandler::new(
            Arc::clone(&runtime),
            audit_sink,
            stream_observers,
            project_id,
        ));
        Some(AgentNodeWiring { handler, runtime })
    }

    /// The `dw.agent` handler together with the [`AgentRuntime`] it drives.
    ///
    /// The runtime is exposed so other in-process node handlers of the SAME
    /// `TenantRuntime` — the `operala.call` deep worker — can reuse its tool
    /// catalogs, secrets scope and ledger instead of building a second,
    /// divergent runtime.
    pub struct AgentNodeWiring {
        pub handler: Arc<dyn AgentNodeHandler>,
        pub runtime: Arc<AgentRuntime>,
    }

    /// Build the production `DwAgent` handler if the environment is configured.
    ///
    /// Returns `None` (so `DwAgent` flow dispatch errors clearly) under any of
    /// these graceful-degradation conditions:
    /// - `merged_agents` is empty (no agents from packs or operator config);
    /// - `GREENTIC_AW_STATE_BACKEND=redis` with `GREENTIC_AW_REDIS_URL`
    ///   unset/empty;
    /// - the AW Redis connection fails;
    /// - the extension runtime fails to initialise.
    ///
    /// An unset `GREENTIC_AW_REDIS_URL` on its own is NOT one of them: the
    /// state backend then auto-selects the process-global in-memory store
    /// (`aw_backends::build_aw_backends`), so this server path runs `dw.agent`
    /// — and hands `operala.call` deep workers their tools — with no Redis.
    ///
    /// `merged_agents` is the result of merging pack-embedded agents (base)
    /// with operator-declared [`HostConfig::agents`] (operator wins on
    /// collision). This merged map replaces the former direct read of
    /// `config.agents` so pack-provided agents are included in the runtime.
    ///
    /// Redis is sourced from the environment because the runner uses an
    /// in-memory flow-state store by default and carries no Redis URL in
    /// [`HostConfig`]; this mirrors the existing env-config convention.
    ///
    /// The `tenant` and `secrets` arguments wire in per-tenant LLM credential
    /// resolution when `GREENTIC_AW_LLM_EXTENSION` is set: requests resolve
    /// credentials from the secrets broker for the calling tenant rather than
    /// reading global env vars. Callers without per-tenant context (e.g.
    /// `serve_agentic`) should use [`build_agent_runtime`] directly, which uses
    /// the env-keyed backend and accepts no secrets context.
    ///
    /// `audit_sink` (EPIC-B B-3) is threaded straight through to the
    /// constructed [`RuntimeAgentNodeHandler`]; `None` (no NATS audit client
    /// configured) keeps `dw.agent` execution on the plain
    /// [`greentic_aw_runtime::AgentRuntime::step`] path — zero behaviour
    /// change from before this parameter existed.
    ///
    /// `project_id` is the deployed unit's `bundle_id` — the value
    /// greentic-designer records as `pack_name` — used as the `project_id`
    /// billing dimension. Pass `None` when no bundle is pinned (the legacy
    /// tenant-only pack path); billing then omits the dimension instead of
    /// attributing spend to a non-unique agent id.
    #[allow(clippy::too_many_arguments)]
    pub async fn build_agent_node_handler(
        merged_agents: HashMap<String, AgentConfig>,
        tenant: String,
        secrets: crate::secrets::DynSecretsManager,
        ext_llm_port: Option<Arc<dyn greentic_ext_runtime::host_ports::LlmPort>>,
        packs: Vec<Arc<crate::pack::PackRuntime>>,
        audit_sink: Option<AuditSink>,
        stream_observers: Option<crate::http::agent_stream::StreamObserverRegistry>,
        project_id: Option<String>,
    ) -> Option<Arc<dyn AgentNodeHandler>> {
        // Boxed so this wrapper adds no depth to the caller's future layout:
        // the runner-desktop `run_pack_async` future already sits close to
        // rustc's query-depth limit.
        Box::pin(build_agent_node_wiring(
            merged_agents,
            tenant,
            secrets,
            ext_llm_port,
            packs,
            audit_sink,
            stream_observers,
            project_id,
        ))
        .await
        .map(|wiring| wiring.handler)
    }

    /// [`build_agent_node_handler`], also returning the [`AgentRuntime`] the
    /// handler drives (see [`AgentNodeWiring`]). Same `None` conditions.
    #[allow(clippy::too_many_arguments)]
    pub async fn build_agent_node_wiring(
        merged_agents: HashMap<String, AgentConfig>,
        tenant: String,
        secrets: crate::secrets::DynSecretsManager,
        ext_llm_port: Option<Arc<dyn greentic_ext_runtime::host_ports::LlmPort>>,
        packs: Vec<Arc<crate::pack::PackRuntime>>,
        audit_sink: Option<AuditSink>,
        stream_observers: Option<crate::http::agent_stream::StreamObserverRegistry>,
        project_id: Option<String>,
    ) -> Option<AgentNodeWiring> {
        use crate::runner::aw_backends::{AwBackends, build_aw_backends};

        if merged_agents.is_empty() {
            return None;
        }

        let AwBackends {
            state_store,
            token_meter,
            tool_ledger: ledger,
            checkpoint_store: _,
        } = build_aw_backends().await?;

        build_runtime_handler_with_stores(
            merged_agents,
            tenant,
            secrets,
            ext_llm_port,
            packs,
            state_store,
            token_meter,
            ledger,
            audit_sink,
            stream_observers,
            project_id,
        )
        .await
    }

    /// Desktop/local builder: in-memory state, token meter, and ledger so
    /// `gtc start` runs agents with NO external infra. State is ephemeral
    /// (lost on process exit) — never used by the server path.
    ///
    /// Returns `None` only when `merged_agents` is empty or the extension
    /// runtime fails to initialise. Unlike [`build_agent_node_handler`] this
    /// function never returns `None` due to a missing Redis URL, making it safe
    /// for desktop environments where no Redis is available.
    ///
    /// `project_id` mirrors [`build_agent_node_handler`]: the deployed unit's
    /// `bundle_id`, or `None` when none is pinned (billing then omits the
    /// dimension).
    #[cfg(feature = "desktop-agent-ephemeral")]
    #[allow(clippy::too_many_arguments)]
    pub async fn build_agent_node_handler_ephemeral(
        merged_agents: HashMap<String, AgentConfig>,
        tenant: String,
        secrets: crate::secrets::DynSecretsManager,
        ext_llm_port: Option<Arc<dyn greentic_ext_runtime::host_ports::LlmPort>>,
        packs: Vec<Arc<crate::pack::PackRuntime>>,
        audit_sink: Option<AuditSink>,
        stream_observers: Option<crate::http::agent_stream::StreamObserverRegistry>,
        project_id: Option<String>,
    ) -> Option<Arc<dyn AgentNodeHandler>> {
        // Boxed so this wrapper adds no depth to the caller's future layout:
        // the runner-desktop `run_pack_async` future already sits close to
        // rustc's query-depth limit.
        Box::pin(build_agent_node_wiring_ephemeral(
            merged_agents,
            tenant,
            secrets,
            ext_llm_port,
            packs,
            audit_sink,
            stream_observers,
            project_id,
        ))
        .await
        .map(|wiring| wiring.handler)
    }

    /// [`build_agent_node_handler_ephemeral`], also returning the
    /// [`AgentRuntime`] the handler drives (see [`AgentNodeWiring`]).
    #[cfg(feature = "desktop-agent-ephemeral")]
    #[allow(clippy::too_many_arguments)]
    pub async fn build_agent_node_wiring_ephemeral(
        merged_agents: HashMap<String, AgentConfig>,
        tenant: String,
        secrets: crate::secrets::DynSecretsManager,
        ext_llm_port: Option<Arc<dyn greentic_ext_runtime::host_ports::LlmPort>>,
        packs: Vec<Arc<crate::pack::PackRuntime>>,
        audit_sink: Option<AuditSink>,
        stream_observers: Option<crate::http::agent_stream::StreamObserverRegistry>,
        project_id: Option<String>,
    ) -> Option<AgentNodeWiring> {
        use greentic_aw_runtime::cost::MockTokenMeter;
        use greentic_aw_runtime::mock::{MockAgentStateStore, NoopToolLedger};
        use std::sync::OnceLock;

        if merged_agents.is_empty() {
            return None;
        }
        // Process-global in-memory state store, shared across every flow
        // invocation in this process. The desktop runner rebuilds the agent
        // handler on each `dw.agent` node call, so a per-call store would erase
        // conversation memory between turns — a multi-turn agent could never
        // act on a user's "yes" to a proposal made on the previous turn. A
        // single shared store keeps memory alive for the lifetime of the
        // process WITHOUT any external infrastructure (Redis remains optional,
        // selected only when GREENTIC_AW_REDIS_URL is set). State is still lost
        // on process exit — that is the documented desktop trade-off.
        static EPHEMERAL_STATE_STORE: OnceLock<Arc<MockAgentStateStore>> = OnceLock::new();
        tracing::warn!(
            tenant = %tenant,
            "AW desktop ephemeral state store (in-memory, process-global; persists across \
             turns for this process, lost on exit)"
        );
        let state_store =
            Arc::clone(EPHEMERAL_STATE_STORE.get_or_init(|| Arc::new(MockAgentStateStore::new())));
        let token_meter = Arc::new(MockTokenMeter::new(0));
        let ledger = Arc::new(NoopToolLedger);
        build_runtime_handler_with_stores(
            merged_agents,
            tenant,
            secrets,
            ext_llm_port,
            packs,
            state_store,
            token_meter,
            ledger,
            audit_sink,
            stream_observers,
            project_id,
        )
        .await
    }

    /// Construct the shared [`AgentRuntime`] from the environment.
    ///
    /// Factored out of [`build_agent_node_handler`] so both the in-process
    /// `dw.agent`/`agentic.call` flow node and the out-of-process NATS serve
    /// mode ([`serve_agentic`]) build an identical runtime (Redis state, env-
    /// resolved LLM backend, design extensions, agent config providers, MCP).
    ///
    /// Returns `None` under the same graceful-degradation conditions as the node
    /// handler: empty agent map, a disabled or unreachable state backend (see
    /// [`build_agent_node_handler`]), or extension-runtime init failure.
    pub async fn build_agent_runtime(
        merged_agents: HashMap<String, AgentConfig>,
    ) -> Option<Arc<AgentRuntime>> {
        use crate::runner::aw_backends::{AwBackends, build_aw_backends};
        use greentic_aw_runtime::LayeredConfigProvider;
        use greentic_aw_runtime::ManifestToolOverlayProvider;
        use greentic_aw_runtime::OtelTelemetry;
        use greentic_aw_runtime::config_provider::CachingConfigProvider;

        if merged_agents.is_empty() {
            return None; // nothing to serve
        }

        let AwBackends {
            state_store,
            token_meter,
            tool_ledger: ledger,
            checkpoint_store: _,
        } = build_aw_backends().await?;

        // Process-level serve path has no per-tenant secrets context, so tool
        // secrets resolve from the env only. It likewise has no embedding host,
        // so the ext LLM port falls back to the env-keyed `EnvLlmPort`.
        // NO pack-carried extensions here, for the same reason this path has no
        // pack-backed MCP fallback (see the `mcp_source_from_env` call below):
        // it is the process-level serve path, its agents come from
        // `GREENTIC_AGENT_MANIFESTS_DIR`, and it holds no `PackRuntime` at all.
        let ext_runtime = build_ext_runtime(Arc::new(EnvSecretsBackend), None, &[])?;

        // Prefer the LLM bridge extension when configured (LLM-as-extension);
        // fall back to the env-keyed in-process OpenAI client otherwise.
        // NOTE: this path has no per-tenant secrets context (it is used by
        // `serve_agentic` and process-level in-proc serve). Per-tenant
        // credential resolution is only available via `build_agent_node_handler`.
        let llm = build_llm_backend(&ext_runtime);

        let agent_count = merged_agents.len();
        // Base config source = the merged agents (pack-embedded ⊕ operator,
        // operator wins). Wrap in the manifest-tool overlay, then layer the
        // admin agent registry on top when configured (registry first, overlay
        // fallback); cache the result either way.
        let overlay = ManifestToolOverlayProvider::new(
            HostConfigProvider::new(merged_agents),
            manifests_discovery_dir(),
        );
        let config_provider: Arc<dyn ConfigProvider> = match registry_from_env() {
            Some(http) => Arc::new(CachingConfigProvider::new(LayeredConfigProvider::new(
                http, overlay,
            ))),
            None => Arc::new(CachingConfigProvider::new(overlay)),
        };
        let telemetry = Arc::new(OtelTelemetry);

        let base = AgentRuntime::new(
            config_provider,
            state_store,
            ext_runtime.clone(),
            llm,
            telemetry,
            token_meter,
            ledger,
            // This process-level serve path has no per-tenant secrets context
            // (see the `ext_runtime`/`llm` construction above), but a `local-wasm`
            // MCP tool still needs to read its tenant secret — the tenant arrives
            // per call and the URI is built from it. So supply the same
            // env-built, tenant-agnostic secrets manager the flow-node path uses
            // (`mcp_node::aw::secrets_from_env`, memoized once per process); the
            // http transport ignores it, the local-wasm transport consumes it.
            //
            // NO pack-backed fallback here, unlike `build_runtime_with_stores`.
            // This is the process-level serve path: its agents come from
            // `GREENTIC_AGENT_MANIFESTS_DIR` and it holds no `PackRuntime` at
            // all, so there is no `assets/mcp-routes.json` to read. A worker
            // reached this way needs `GREENTIC_AW_ADMIN_ENDPOINT` +
            // `GREENTIC_AW_ADMIN_TOKEN` for its `mcp:` tools to dispatch; its
            // tools are still ADVERTISED to the LLM from the agent config's own
            // `ToolRef` schemas.
            mcp_source_from_env(crate::runner::mcp_node::aw::secrets_from_env()),
        )
        .with_guardrails(
            {
                let policy: Arc<dyn greentic_aw_runtime::guardrail::GuardrailPolicy> =
                    match guardrail_policy_from_env() {
                        Some(http) => Arc::new(http),
                        None => Arc::new(greentic_aw_runtime::guardrail::StaticGuardrailPolicy(
                            Vec::new(),
                        )),
                    };
                policy
            },
            Arc::new(
                greentic_aw_runtime::guardrail::ExtRuntimeGuardrailEvaluator {
                    ext_runtime: ext_runtime.clone(),
                },
            ),
        );
        // Short-term ("working") memory: in-memory provider is always available
        // (no external deps); the remember/recall tools stay gated by
        // config.memory.short_term in the loop.
        let base = base.with_short_term_memory(Arc::new(
            greentic_aw_runtime::memory::InMemoryMemoryProvider::new(),
        ));
        // Billing metering: install the HTTP sink when both env vars are set;
        // fall back to the built-in no-op (billing disabled) otherwise.
        // Ship-dark: no-op until GREENTIC_BILLING_BASE_URL +
        // GREENTIC_BILLING_SERVICE_SECRET are configured by the operator.
        let base =
            if let Some(http_meter) = greentic_aw_runtime::billing::HttpBillingMeter::from_env() {
                tracing::info!("billing metering enabled for digital-worker LLM usage");
                base.with_billing_meter(std::sync::Arc::new(http_meter))
            } else {
                base
            };
        // Optionally attach operator-configured long-term memory and knowledge
        // (document RAG) backends from the registered agent-runtime extensions
        // (see `runtime_ext`). With none registered `base` passes unchanged.
        let base = crate::runner::runtime_ext::attach_all(base).await;
        // Knowledge delegated to a design-extension tool — the out-of-process
        // serve path's copy of the mount above. See `knowledge_ext` for why the
        // adapter reads its target per turn rather than per runtime: THIS is the
        // path that serves many agents from one runtime.
        let base = crate::runner::knowledge_ext::attach(base, ext_runtime.clone());
        let runtime = Arc::new(base);

        tracing::info!(agent_count, "AW runtime constructed");
        Some(runtime)
    }

    /// Run the agentic-worker runtime as a NATS-consuming service.
    ///
    /// Builds the production [`AgentRuntime`] via [`build_agent_runtime`] and,
    /// when it could be constructed, serves `greentic.agentic.request.v1`
    /// forever via the shared `aw-event-bridge`. This is the out-of-process
    /// (`agentic.call`) counterpart to the in-process `dw.agent` node.
    ///
    /// When `GREENTIC_AW_REDIS_URL` is set and reachable the serve path wires
    /// a [`greentic_aw_runtime::RedisDispatchLedger`] so JetStream at-least-once
    /// redeliveries are short-circuited without re-running the LLM step. If the
    /// Redis connect fails the ledger falls back to [`greentic_aw_runtime::NoopDispatchLedger`]
    /// (idempotency disabled, warning logged) so serving is never blocked.
    ///
    /// Returns `Ok(())` immediately (a no-op) when the runtime cannot be built
    /// (e.g. no agents, no Redis) so the host can call this unconditionally.
    pub async fn serve_agentic(
        nats_url: &str,
        merged_agents: HashMap<String, AgentConfig>,
    ) -> anyhow::Result<()> {
        use greentic_aw_runtime::dispatch_ledger::RedisDispatchLedger;
        use greentic_aw_runtime::{DispatchLedger, NoopDispatchLedger, RedisAgentStateStore};

        match build_agent_runtime(merged_agents).await {
            Some(runtime) => {
                // Dispatch idempotency ledger: only Redis provides cross-redelivery
                // caching, so it is wired only when the resolved state backend is
                // Redis (URL present AND backend redis/unset). Memory/disk backends
                // use the no-op ledger (at-least-once). Best-effort: a connect
                // failure disables idempotency but never blocks serving.
                let redis_url = std::env::var("GREENTIC_AW_REDIS_URL")
                    .ok()
                    .filter(|url| !url.is_empty());
                // Derive "is Redis" from the SAME selector the backends use, so an
                // unusual value (e.g. `GREENTIC_AW_STATE_BACKEND=cassandra` with a
                // URL) can't disagree between the state store and the ledger gate.
                let backend_env = std::env::var("GREENTIC_AW_STATE_BACKEND").ok();
                let backend_is_redis = matches!(
                    crate::runner::aw_backends::select_state_backend(
                        backend_env.as_deref(),
                        redis_url.as_deref(),
                        None,
                    ),
                    crate::runner::aw_backends::StateBackendChoice::Redis(_)
                );
                let (ledger, ledger_active): (Arc<dyn DispatchLedger>, bool) = match redis_url {
                    Some(url) if backend_is_redis => {
                        match RedisAgentStateStore::connect(&url).await {
                            Ok(store) => {
                                (Arc::new(RedisDispatchLedger::new(store.manager())), true)
                            }
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    "dispatch ledger Redis connect failed; \
                                     idempotency disabled"
                                );
                                (Arc::new(NoopDispatchLedger), false)
                            }
                        }
                    }
                    _ => (Arc::new(NoopDispatchLedger), false),
                };

                tracing::info!(
                    nats_url,
                    dispatch_ledger_active = ledger_active,
                    "agentic serve mode starting"
                );
                greentic_aw_runtime::serve::serve_with_ledger(nats_url, runtime, ledger).await
            }
            None => {
                tracing::info!(
                    "agentic serve mode skipped: no agentic runtime could be constructed"
                );
                Ok(())
            }
        }
    }

    /// Load process-level base agent configs from the manifests directory.
    ///
    /// Reads every `<agent_id>.json` file in [`manifests_discovery_dir`] as a
    /// full [`AgentConfig`] (NOT the tool-only Digital Worker manifest consumed
    /// by [`ManifestToolOverlayProvider`]). This is the ONLY process-level base
    /// agent source: pack-embedded agents and `HostConfig::agents` are both
    /// per-tenant and only materialise inside `TenantRuntime::from_packs`, so an
    /// in-process serve started at process startup cannot see them.
    ///
    /// Returns an empty map when the directory is absent or unreadable. Files
    /// that fail to decode into an [`AgentConfig`], or whose `agent_id` does not
    /// match the file stem, are logged and skipped so one malformed file never
    /// aborts loading. The file stem is the authoritative key (the in-map id is
    /// taken from the stem), mirroring the `<agent_id>.json` convention.
    pub fn load_process_agent_configs() -> HashMap<String, AgentConfig> {
        let dir = manifests_discovery_dir();
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) => {
                tracing::debug!(
                    dir = %dir.display(),
                    error = %error,
                    "agent manifests dir not readable; no process-level agents loaded"
                );
                return HashMap::new();
            }
        };

        let mut agents: HashMap<String, AgentConfig> = HashMap::new();
        for entry in entries.flatten() {
            let path = entry.path();
            let is_json = path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"));
            if !is_json {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "agent config read failed; skipping");
                    continue;
                }
            };
            match serde_json::from_slice::<AgentConfig>(&bytes) {
                Ok(config) => {
                    if config.agent_id != stem {
                        tracing::warn!(
                            file_stem = stem,
                            agent_id = config.agent_id.as_str(),
                            "agent config id does not match filename; keying by filename"
                        );
                    }
                    agents.insert(stem.to_string(), config);
                }
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "agent config decode failed; skipping");
                }
            }
        }
        agents
    }

    #[cfg(test)]
    pub(crate) mod tests {
        use std::collections::HashMap;
        use std::sync::Arc;

        use greentic_aw_runtime::cost::MockTokenMeter;
        use greentic_aw_runtime::llm::LlmResponse;
        use greentic_aw_runtime::mock::{
            MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
        };
        use greentic_aw_runtime::{AgentConfig, AgentLimits, LlmProviderRef};
        use greentic_aw_runtime::{AgentRuntime, TenantContext};
        use serde_json::json;

        use super::*;

        pub(crate) fn sample_agent_config(agent_id: &str) -> AgentConfig {
            AgentConfig {
                agent_id: agent_id.into(),
                system_prompt: "sys".into(),
                tools: vec![],
                guardrails: vec![],
                llm: LlmProviderRef {
                    provider: "openai".into(),
                    model: "gpt-4o-mini".into(),
                    credential_ref: None,
                },
                limits: AgentLimits::default(),
                memory: None,
                knowledge: None,
                conversational: false,
                opening_message: None,
            }
        }

        #[tokio::test]
        async fn execute_returns_reply_json() {
            let llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
                content: Some("pong".into()),
                tool_calls: vec![],
                tokens_in: 1,
                tokens_out: 1,
            })]));
            let store = Arc::new(MockAgentStateStore::new());
            let telemetry = Arc::new(MockTelemetry::new());

            let config_provider = MockConfigProvider::new();
            let tenant = TenantContext::new("t", "e");
            config_provider.insert(
                &tenant,
                "greeter",
                AgentConfig {
                    agent_id: "greeter".into(),
                    system_prompt: "sys".into(),
                    tools: vec![],
                    guardrails: vec![],
                    llm: LlmProviderRef {
                        provider: "mock".into(),
                        model: "m".into(),
                        credential_ref: None,
                    },
                    limits: AgentLimits::default(),
                    memory: None,
                    knowledge: None,
                    conversational: false,
                    opening_message: None,
                },
            );
            let config_provider = Arc::new(config_provider);

            let token_meter = Arc::new(MockTokenMeter::new(0));
            let ledger = Arc::new(NoopToolLedger);
            let ext_runtime = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());

            let runtime = Arc::new(AgentRuntime::new(
                config_provider,
                store,
                ext_runtime,
                llm,
                telemetry,
                token_meter,
                ledger,
                None,
            ));
            let handler = RuntimeAgentNodeHandler::new(runtime, None, None, None);

            let output = handler
                .execute(
                    "t",
                    "e",
                    "greeter",
                    "sess-1",
                    &json!({"user_text": "ping"}),
                    false,
                    None,
                )
                .await
                .expect("execute should succeed");

            assert_eq!(output["reply"].as_str(), Some("pong"));
        }

        // -------------------------------------------------------------------
        // `project_id` billing dimension = the deployed unit's bundle id
        // -------------------------------------------------------------------

        /// Captures the `TenantContext.project_id` of every billing emit the
        /// agent loop makes, so a test can assert what `execute` actually
        /// handed the sink rather than what it looks like it should have.
        #[derive(Default)]
        struct RecordingBillingMeter {
            project_ids: std::sync::Mutex<Vec<Option<String>>>,
        }

        impl greentic_aw_runtime::billing::BillingMeter for RecordingBillingMeter {
            fn emit<'a>(
                &'a self,
                tenant: &'a TenantContext,
                _input_tokens: u64,
                _output_tokens: u64,
                _agent_id: &'a str,
                _model: &'a str,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = Result<(), greentic_aw_runtime::billing::BillingError>,
                        > + Send
                        + 'a,
                >,
            > {
                self.project_ids
                    .lock()
                    .expect("recording meter lock poisoned")
                    .push(tenant.project_id.clone());
                Box::pin(async { Ok(()) })
            }

            fn over_budget<'a>(
                &'a self,
                _tenant: &'a TenantContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>>
            {
                Box::pin(std::future::ready(false))
            }
        }

        /// Run one `execute` through a handler built with `project_id` and
        /// return the project ids the billing sink observed.
        async fn project_ids_seen_by_billing(project_id: Option<String>) -> Vec<Option<String>> {
            let llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
                content: Some("pong".into()),
                tool_calls: vec![],
                tokens_in: 1,
                tokens_out: 1,
            })]));
            let config_provider = MockConfigProvider::new();
            config_provider.insert(
                &TenantContext::new("t", "e"),
                "greeter",
                sample_agent_config("greeter"),
            );
            let meter = Arc::new(RecordingBillingMeter::default());
            let runtime = Arc::new(
                AgentRuntime::new(
                    Arc::new(config_provider),
                    Arc::new(MockAgentStateStore::new()),
                    Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
                    llm,
                    Arc::new(MockTelemetry::new()),
                    Arc::new(MockTokenMeter::new(0)),
                    Arc::new(NoopToolLedger),
                    None,
                )
                .with_billing_meter(meter.clone()),
            );

            RuntimeAgentNodeHandler::new(runtime, None, None, project_id)
                .execute(
                    "t",
                    "e",
                    "greeter",
                    "sess-1",
                    &json!({"user_text": "ping"}),
                    false,
                    None,
                )
                .await
                .expect("execute should succeed");

            let seen = meter
                .project_ids
                .lock()
                .expect("recording meter lock poisoned");
            seen.clone()
        }

        #[tokio::test]
        async fn execute_bills_the_pack_identity_as_project_id() {
            let seen = project_ids_seen_by_billing(Some("customer.support".into())).await;
            assert_eq!(
                seen,
                vec![Some("customer.support".to_string())],
                "the deployed bundle id must reach the billing sink, not the in-pack agent id"
            );
        }

        #[tokio::test]
        async fn execute_bills_no_project_id_when_the_pack_identity_is_unknown() {
            let seen = project_ids_seen_by_billing(None).await;
            assert_eq!(
                seen,
                vec![None],
                "an unknown pack identity must stay absent — never fall back to the agent id"
            );
        }

        // -------------------------------------------------------------------
        // Billing wiring on the in-process `dw.agent` path
        // -------------------------------------------------------------------

        /// The in-process `dw.agent` runtime must talk to the configured
        /// billing service, exactly as the out-of-process serve path does.
        ///
        /// Asserted through observable behaviour rather than by inspecting the
        /// runtime: the agent loop consults `BillingMeter::over_budget` before
        /// it touches the LLM or the state store, and the HTTP sink implements
        /// that as `GET /v1/tenants/{tenant}/wallet`. So a request arriving at
        /// the billing server proves a real meter is installed; with the
        /// default `NoopBillingMeter` nothing is ever sent.
        #[tokio::test]
        #[serial_test::serial]
        // `set_var`/`remove_var` are process-global; `serial` keeps them from
        // racing other env-reading tests.
        #[allow(unsafe_code)]
        async fn in_process_handler_consults_the_configured_billing_service() {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path("/v1/tenants/acme/wallet"))
                // `available: 0` short-circuits the step at the credit gate, so
                // the test never reaches the LLM backend or acquires a lock.
                .respond_with(
                    wiremock::ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"available": "0"})),
                )
                .mount(&server)
                .await;

            // Pin extension discovery at an empty dir: without this the runtime
            // scans the developer's real `~/.greentic/extensions`, which makes
            // the test depend on machine state and cost ~45s locally.
            let empty_extensions = tempfile::tempdir().expect("tempdir");
            unsafe {
                std::env::set_var("GREENTIC_BILLING_BASE_URL", server.uri());
                std::env::set_var("GREENTIC_BILLING_SERVICE_SECRET", "secret");
                std::env::set_var("GREENTIC_EXTENSIONS_DIR", empty_extensions.path());
            }

            let mut agents = HashMap::new();
            agents.insert("greeter".to_string(), sample_agent_config("greeter"));
            let handler = build_runtime_handler_with_stores(
                agents,
                "acme".to_string(),
                crate::secrets::default_manager().expect("env secrets manager"),
                None,
                vec![],
                Arc::new(MockAgentStateStore::new()),
                Arc::new(MockTokenMeter::new(0)),
                Arc::new(NoopToolLedger),
                None,
                None,
                None,
            )
            .await
            .expect("handler should build from mock stores")
            .handler;

            let _ = handler
                .execute(
                    "acme",
                    "prod",
                    "greeter",
                    "s",
                    &json!({"user_text": "hi"}),
                    false,
                    None,
                )
                .await;

            unsafe {
                std::env::remove_var("GREENTIC_BILLING_BASE_URL");
                std::env::remove_var("GREENTIC_BILLING_SERVICE_SECRET");
                std::env::remove_var("GREENTIC_EXTENSIONS_DIR");
            }

            let wallet_calls = server
                .received_requests()
                .await
                .unwrap_or_default()
                .iter()
                .filter(|r| r.url.path() == "/v1/tenants/acme/wallet")
                .count();
            assert!(
                wallet_calls >= 1,
                "in-process dw.agent must consult the billing service; got {wallet_calls} \
                 wallet requests, which means it is still running on NoopBillingMeter \
                 and its LLM spend is never metered"
            );
        }

        // -------------------------------------------------------------------
        // State-store prerequisite of the server (non-ephemeral) path
        // -------------------------------------------------------------------

        /// The SERVER builder — the one greentic-start and the distroless
        /// image use, since they do not enable `desktop-agent-ephemeral` —
        /// must build the agent runtime with NO Redis configured.
        ///
        /// That runtime is also the tool surface `runtime.rs` hands to
        /// `operala.call` deep workers (`OperalaToolContext`), so this pins
        /// both halves of one claim: a Redis-less deployment runs `dw.agent`
        /// AND gives its deep workers their tools. It was documented as the
        /// opposite ("no state store: set GREENTIC_AW_REDIS_URL") after
        /// `build_aw_backends` had already made Redis optional.
        ///
        /// The control proves the assertion is about the backend selector,
        /// not a builder that never returns `None`: naming the redis backend
        /// without a URL is an honest misconfiguration and disables both.
        #[tokio::test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        async fn server_builder_wires_the_agent_runtime_without_redis() {
            let empty_extensions = tempfile::tempdir().expect("tempdir");
            // SAFETY: #[serial] serializes env-mutating tests (crate convention).
            unsafe {
                std::env::remove_var("GREENTIC_AW_REDIS_URL");
                std::env::remove_var("GREENTIC_AW_STATE_BACKEND");
                std::env::remove_var("GREENTIC_AW_STATE_PATH");
                std::env::remove_var("GREENTIC_AW_LLM_EXTENSION");
                std::env::set_var("GREENTIC_EXTENSIONS_DIR", empty_extensions.path());
            }

            async fn build() -> Option<AgentNodeWiring> {
                let mut agents = HashMap::new();
                agents.insert("greeter".to_string(), sample_agent_config("greeter"));
                build_agent_node_wiring(
                    agents,
                    "acme".to_string(),
                    crate::secrets::default_manager().expect("env secrets manager"),
                    None,
                    vec![],
                    None,
                    None,
                    None,
                )
                .await
            }

            let without_redis = build().await;

            unsafe {
                std::env::set_var("GREENTIC_AW_STATE_BACKEND", "redis");
            }
            let explicit_redis_without_url = build().await;

            unsafe {
                std::env::remove_var("GREENTIC_AW_STATE_BACKEND");
                std::env::remove_var("GREENTIC_EXTENSIONS_DIR");
            }

            assert!(
                without_redis.is_some(),
                "with GREENTIC_AW_REDIS_URL unset the server builder must fall back to \
                 the in-memory state store and build the runtime dw.agent and \
                 operala.call deep workers share"
            );
            assert!(
                explicit_redis_without_url.is_none(),
                "GREENTIC_AW_STATE_BACKEND=redis with no URL must disable the runtime"
            );
        }

        // -----------------------------------------------------------------------
        // audit_sink enable/disable branch (EPIC-B B-3)
        // -----------------------------------------------------------------------

        /// Build an [`AgentRuntime`] scripted to make one `remember` (host
        /// built-in short-term-memory) tool call before replying "done". The
        /// host built-in path fires `StepObserver::on_tool_call`/`on_tool_result`
        /// without needing a real WASM extension dispatch, so it is the
        /// cheapest way to drive a genuine tool call through `execute`.
        fn runtime_with_scripted_remember_call(tenant_id: &str, env_id: &str) -> Arc<AgentRuntime> {
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
            let store = Arc::new(MockAgentStateStore::new());
            let telemetry = Arc::new(MockTelemetry::new());

            let config_provider = MockConfigProvider::new();
            let tenant = TenantContext::new(tenant_id, env_id);
            let mut cfg = sample_agent_config("greeter");
            cfg.memory = Some(MemorySettings {
                short_term: Some(MemoryProviderRef {
                    provider: "inmemory".into(),
                    capability: "cap://memory/short-term".into(),
                    params: Default::default(),
                    credential_ref: None,
                }),
                long_term: None,
            });
            config_provider.insert(&tenant, "greeter", cfg);
            let config_provider = Arc::new(config_provider);

            let token_meter = Arc::new(MockTokenMeter::new(0));
            let ledger = Arc::new(NoopToolLedger);
            let ext_runtime = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());

            Arc::new(
                AgentRuntime::new(
                    config_provider,
                    store,
                    ext_runtime,
                    llm,
                    telemetry,
                    token_meter,
                    ledger,
                    None,
                )
                .with_short_term_memory(Arc::new(InMemoryMemoryProvider::new())),
            )
        }

        #[tokio::test]
        async fn execute_with_audit_sink_routes_through_step_with_observer_and_enqueues_events() {
            let runtime = runtime_with_scripted_remember_call("t1", "e1");

            let (tx, mut rx) = tokio::sync::mpsc::channel(16);
            let sink = AuditSink::from_sender(tx);
            let handler = RuntimeAgentNodeHandler::new(runtime, Some(sink), None, None);

            let output = handler
                .execute(
                    "t1",
                    "e1",
                    "greeter",
                    "sess-1",
                    &json!({"user_text": "remember this"}),
                    false,
                    None,
                )
                .await
                .expect("execute should succeed");
            assert_eq!(output["reply"].as_str(), Some("done"));

            let (subject, bytes) = rx.try_recv().expect("tool_call event enqueued");
            assert_eq!(subject, "audit.t1.agent.tool_call");
            let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
            assert_eq!(value["payload"]["tool"], json!("remember"));
            assert_eq!(value["payload"]["agent_id"], json!("greeter"));

            let (subject, bytes) = rx.try_recv().expect("tool_result event enqueued");
            assert_eq!(subject, "audit.t1.agent.tool_result");
            let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
            assert_eq!(value["payload"]["tool"], json!("remember"));

            // EPIC-D D-1: a per-run metering event follows the per-step
            // agent-audit events once the run completes successfully.
            let (subject, bytes) = rx.try_recv().expect("metering event enqueued");
            assert_eq!(subject, "audit.t1.metering.agent_run");
            let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
            assert_eq!(value["type"], json!("greentic.runner.metering.agent_run"));
            assert_eq!(value["payload"]["unit"], json!("agent_run"));
            assert_eq!(value["payload"]["quantity"], json!(1));
            assert_eq!(value["payload"]["agent_id"], json!("greeter"));
            assert_eq!(
                value["payload"]["steps"],
                json!(output["trail"].as_array().expect("trail is an array").len())
            );

            assert!(
                rx.try_recv().is_err(),
                "exactly three audit events enqueued (tool_call, tool_result, metering.agent_run)"
            );
        }

        // -----------------------------------------------------------------------
        // stream_observers registry lookup (R2)
        // -----------------------------------------------------------------------

        #[tokio::test]
        async fn execute_routes_registered_stream_observer_and_receives_tool_events() {
            use crate::http::agent_stream::StreamObserverRegistry;
            use std::sync::Mutex;

            // A recording StepObserver we register under the session id. The
            // scripted-tool-call runtime (shared with the audit test above)
            // drives `MockLlmBackend`, which does not stream token deltas, so
            // this asserts on `on_tool_call`/`on_tool_result` (per the R2
            // design note) rather than `on_token_delta`.
            #[derive(Default)]
            struct Rec {
                hits: Mutex<Vec<String>>,
            }
            impl StepObserver for Rec {
                fn wants_streaming(&self) -> bool {
                    true
                }
                fn on_tool_call(&self, name: &str, _call_id: &str, _args: &Value) {
                    self.hits.lock().unwrap().push(format!("c:{name}"));
                }
                fn on_tool_result(&self, name: &str, _call_id: &str, _result: &Value) {
                    self.hits.lock().unwrap().push(format!("r:{name}"));
                }
                fn on_tool_failed(&self, name: &str, _call_id: &str, _error: &Value) {
                    self.hits.lock().unwrap().push(format!("f:{name}"));
                }
            }

            let runtime = runtime_with_scripted_remember_call("t1", "e1");
            let registry: StreamObserverRegistry = Arc::new(dashmap::DashMap::new());
            let rec = Arc::new(Rec::default());
            registry.insert("sess-1".to_string(), rec.clone() as Arc<dyn StepObserver>);

            let handler = RuntimeAgentNodeHandler::new(runtime, None, Some(registry), None);
            let output = handler
                .execute(
                    "t1",
                    "e1",
                    "greeter",
                    "sess-1",
                    &json!({"user_text": "remember this"}),
                    false,
                    None,
                )
                .await
                .expect("execute should succeed");
            assert_eq!(output["reply"].as_str(), Some("done"));

            let hits = rec.hits.lock().unwrap();
            assert!(
                hits.iter().any(|h| h == "c:remember"),
                "tool call forwarded to the registered stream observer: {hits:?}"
            );
            assert!(
                hits.iter().any(|h| h == "r:remember"),
                "tool result forwarded to the registered stream observer: {hits:?}"
            );
        }

        #[tokio::test]
        async fn execute_ignores_stream_registry_when_session_not_registered() {
            use crate::http::agent_stream::StreamObserverRegistry;

            // A registry that exists but has no entry for this session must
            // behave exactly like `None` — no observer is built at all, so
            // the plain `self.runtime.step(...)` path runs unchanged.
            let runtime = runtime_with_scripted_remember_call("t1", "e1");
            let registry: StreamObserverRegistry = Arc::new(dashmap::DashMap::new());

            let handler = RuntimeAgentNodeHandler::new(runtime, None, Some(registry), None);
            let output = handler
                .execute(
                    "t1",
                    "e1",
                    "greeter",
                    "sess-1",
                    &json!({"user_text": "remember this"}),
                    false,
                    None,
                )
                .await
                .expect("execute should succeed");
            assert_eq!(output["reply"].as_str(), Some("done"));
        }

        #[tokio::test]
        async fn execute_without_audit_sink_uses_plain_step_path_unchanged() {
            // Same scripted tool call as the audited test above, but the
            // handler carries no audit sink at all — proves the "off" branch
            // (self.runtime.step, no observer constructed) still dispatches
            // the tool call and returns the same reply, exactly as it did
            // before AgentAuditObserver existed.
            let runtime = runtime_with_scripted_remember_call("t1", "e1");
            let handler = RuntimeAgentNodeHandler::new(runtime, None, None, None);

            let output = handler
                .execute(
                    "t1",
                    "e1",
                    "greeter",
                    "sess-1",
                    &json!({"user_text": "remember this"}),
                    false,
                    None,
                )
                .await
                .expect("execute should succeed");
            assert_eq!(output["reply"].as_str(), Some("done"));
        }

        #[test]
        fn tenant_ctx_for_audit_uses_real_ids_when_valid() {
            let ctx = super::tenant_ctx_for_audit("acme", "prod");
            assert_eq!(ctx.tenant.as_str(), "acme");
            assert_eq!(ctx.env.as_str(), "prod");
        }

        #[test]
        fn tenant_ctx_for_audit_falls_back_to_local_on_invalid_ids() {
            // Empty strings fail the newtype validation; the helper must not
            // panic and should fall back to "local" for both fields.
            let ctx = super::tenant_ctx_for_audit("", "");
            assert_eq!(ctx.tenant.as_str(), "local");
            assert_eq!(ctx.env.as_str(), "local");
        }

        #[tokio::test]
        async fn host_config_provider_returns_config_for_known_agent() {
            let mut agents = HashMap::new();
            agents.insert("greeter".to_string(), sample_agent_config("greeter"));
            let provider = HostConfigProvider::new(agents);

            let tenant = TenantContext::new("acme", "prod");
            let resolved = provider
                .agent_config(&tenant, "greeter")
                .await
                .expect("known agent resolves");

            assert_eq!(resolved.agent_id, "greeter");
        }

        #[tokio::test]
        async fn overlay_provider_replaces_tools_from_manifest() {
            use greentic_aw_runtime::ManifestToolOverlayProvider;
            use greentic_aw_runtime::config::ToolRef;
            use greentic_aw_runtime::config_provider::ConfigProvider;

            let tmp = tempfile::tempdir().unwrap();
            std::fs::write(
                tmp.path().join("greeter.json"),
                r#"{"id":"greeter","display_name":"G",
                "tenancy":{"tenant":"t","team_policy":"disabled"},
                "locale":{"worker_default_locale":"en-US","policy":"worker_default",
                          "propagation":"current_task_only","output":"worker_default"},
                "extension_tools":[{"extension_id":"greentic.tavily","extension_version":"1.0.0",
                  "tool_name":"web_search","description":"d","input_schema_json":"{\"type\":\"object\"}",
                  "capabilities":["agentic_worker"],"agentic_worker_metadata":{}}]}"#,
            )
            .unwrap();

            let mut agents = HashMap::new();
            agents.insert("greeter".to_string(), sample_agent_config("greeter"));
            let provider = ManifestToolOverlayProvider::new(
                HostConfigProvider::new(agents),
                tmp.path().to_path_buf(),
            );

            let tenant = TenantContext::new("acme", "prod");
            let cfg = provider.agent_config(&tenant, "greeter").await.unwrap();
            assert_eq!(
                cfg.tools,
                vec![ToolRef {
                    extension_id: "greentic.tavily".into(),
                    tool_name: "web_search".into(),
                    description: None,
                    input_schema: None,
                    usage_note: None,
                }]
            );
        }

        #[test]
        fn bridge_credential_defaults_provider_and_model() {
            let c = super::bridge_credential(None, None, "sk-x".into(), None).unwrap();
            assert_eq!(c.provider, "openai");
            assert_eq!(c.model, "gpt-4o");
            assert_eq!(c.api_key, "sk-x");
            assert!(c.base_url.is_none());
        }

        #[test]
        fn bridge_credential_honors_explicit_parts() {
            let c = super::bridge_credential(
                Some("anthropic".into()),
                Some("claude-x".into()),
                "sk-ant".into(),
                Some("https://proxy".into()),
            )
            .unwrap();
            assert_eq!(c.provider, "anthropic");
            assert_eq!(c.model, "claude-x");
            assert_eq!(c.base_url.as_deref(), Some("https://proxy"));
        }

        #[test]
        fn bridge_credential_none_without_key() {
            assert!(
                super::bridge_credential(Some("openai".into()), None, "  ".into(), None).is_none()
            );
        }

        #[test]
        fn bridge_credential_allows_a_keyless_provider_without_a_key() {
            let c = super::bridge_credential(Some("ollama".into()), None, String::new(), None)
                .expect("Ollama needs no API key, so an empty one must still build a credential");
            assert_eq!(c.provider, "ollama");
            assert!(c.api_key.is_empty());
        }

        #[test]
        fn keyless_providers_do_not_require_an_api_key() {
            for provider in ["ollama", "llamafile", "bedrock", "Ollama", " ollama "] {
                assert!(
                    !super::provider_requires_api_key(Some(provider)),
                    "{provider} authenticates without an API key"
                );
            }
            for provider in ["openai", "anthropic", "deepseek", "gemini"] {
                assert!(
                    super::provider_requires_api_key(Some(provider)),
                    "{provider} needs an API key"
                );
            }
            // Nothing named a provider, or a name nothing recognises: assume the
            // historical OpenAI default, which needs a key.
            assert!(super::provider_requires_api_key(None));
            assert!(super::provider_requires_api_key(Some("  ")));
            assert!(super::provider_requires_api_key(Some("not-a-provider")));
        }

        /// The local table and greentic-llm must agree, or a build without the
        /// `greentic-llm-backend` feature judges keylessness differently from
        /// one with it.
        #[cfg(feature = "greentic-llm-backend")]
        #[test]
        fn keyless_table_matches_greentic_llm() {
            for kind in greentic_llm::ProviderKind::all() {
                assert_eq!(
                    super::keyless_table_requires_api_key(kind.as_str()),
                    kind.requires_api_key(),
                    "local keyless table disagrees with greentic-llm about {}",
                    kind.as_str()
                );
            }
        }

        #[cfg(feature = "greentic-llm-backend")]
        #[test]
        fn ollama_without_a_key_selects_the_multi_provider_backend() {
            assert!(
                super::selects_multi_provider_backend_with("", Some("ollama"), None),
                "an Ollama worker with no key must reach greentic-llm, not the \
                 OpenAI fall-through"
            );
            assert!(super::selects_multi_provider_backend_with(
                "  ",
                Some("bedrock"),
                None
            ));
        }

        #[cfg(feature = "greentic-llm-backend")]
        #[test]
        fn openai_without_a_key_or_an_endpoint_keeps_the_openai_fall_through() {
            // Nothing configured: the historical default really is OpenAI, and
            // a keyless request to it is going to fail either way.
            assert!(!super::selects_multi_provider_backend_with(
                "",
                Some("openai"),
                None
            ));
            assert!(!super::selects_multi_provider_backend_with(
                "  ", None, None
            ));
            // A key still selects the multi-provider backend for everyone.
            assert!(super::selects_multi_provider_backend_with(
                "sk-x",
                Some("openai"),
                None
            ));
            assert!(super::selects_multi_provider_backend_with(
                "sk-x", None, None
            ));
        }

        /// `provider = "openai"` means "speaks the OpenAI protocol", not "is
        /// api.openai.com". An operator who pointed the endpoint at a local
        /// gateway had to invent a dummy key, because an empty one dropped them
        /// onto a fall-through client that appends `/v1/...` to a base URL
        /// already ending in `/v1` and 404s every request.
        #[cfg(feature = "greentic-llm-backend")]
        #[test]
        fn a_configured_endpoint_makes_an_empty_key_legitimate() {
            assert!(super::selects_multi_provider_backend_with(
                "",
                Some("openai"),
                Some("http://127.0.0.1:11434/v1")
            ));
            // Also for a provider nobody named at all.
            assert!(super::selects_multi_provider_backend_with(
                "",
                None,
                Some("http://127.0.0.1:1234/v1")
            ));
        }

        /// Blank and whitespace-only mean "configured nothing", matching how
        /// `llm_openai::normalize_base_url` reads the same variable. Without
        /// this, an env var someone exported empty would silently change which
        /// backend every keyless deployment gets.
        #[cfg(feature = "greentic-llm-backend")]
        #[test]
        fn a_blank_endpoint_is_not_a_configured_one() {
            assert!(!super::selects_multi_provider_backend_with(
                "",
                Some("openai"),
                Some("")
            ));
            assert!(!super::selects_multi_provider_backend_with(
                "",
                Some("openai"),
                Some("   ")
            ));
        }

        #[test]
        fn configured_llm_provider_falls_back_to_an_agents_declared_provider() {
            // No env var is read here beyond `GREENTIC_LLM_PROVIDER`, which this
            // test does not set: the agent's own declaration must be found.
            let mut agents = HashMap::new();
            let mut agent = sample_agent_config("greeter");
            agent.llm.provider = "ollama".into();
            agents.insert("greeter".to_string(), agent);
            // Only assert the agent leg when the env is not overriding it, so the
            // test is not at the mercy of the developer's shell.
            if super::env_llm_provider().is_none() {
                assert_eq!(
                    super::configured_llm_provider(&agents).as_deref(),
                    Some("ollama")
                );
                assert_eq!(super::configured_llm_provider(&HashMap::new()), None);
            }
        }

        /// `configured_llm_provider` iterated a HashMap, so a worker declaring
        /// several agents with different providers got a provider chosen by hash
        /// order — a different one across process starts, with nothing
        /// reporting it. The pick must be stable.
        #[test]
        fn configured_llm_provider_is_stable_across_hashmap_orderings() {
            if super::env_llm_provider().is_some() {
                // The env leg wins by design; this test is about the agent leg.
                return;
            }
            for _ in 0..50 {
                let mut agents = HashMap::new();
                for (id, provider) in [("zz", "ollama"), ("aa", "anthropic"), ("mm", "groq")] {
                    let mut agent = sample_agent_config(id);
                    agent.llm.provider = provider.into();
                    agents.insert(id.to_string(), agent);
                }
                assert_eq!(
                    super::configured_llm_provider(&agents).as_deref(),
                    Some("anthropic"),
                    "the sorted-first agent id must decide the provider"
                );
            }
        }

        #[tokio::test]
        async fn host_config_provider_returns_not_found_for_unknown_agent() {
            use greentic_aw_runtime::error::ConfigError;

            let provider = HostConfigProvider::new(HashMap::new());

            let tenant = TenantContext::new("acme", "prod");
            let result = provider.agent_config(&tenant, "missing").await;

            assert!(matches!(result, Err(ConfigError::AgentNotFound(_))));
        }

        // -----------------------------------------------------------------------
        // merge_agent_sources tests
        // -----------------------------------------------------------------------

        #[test]
        fn merge_pack_only_agent_resolves() {
            let mut pack_agents = HashMap::new();
            pack_agents.insert("pack-bot".to_string(), sample_agent_config("pack-bot"));

            let merged = super::merge_agent_sources(pack_agents, HashMap::new());

            assert!(merged.contains_key("pack-bot"));
            assert_eq!(merged["pack-bot"].agent_id, "pack-bot");
        }

        #[test]
        fn merge_operator_only_agent_resolves() {
            let mut operator_agents = HashMap::new();
            operator_agents.insert("op-bot".to_string(), sample_agent_config("op-bot"));

            let merged = super::merge_agent_sources(HashMap::new(), operator_agents);

            assert!(merged.contains_key("op-bot"));
            assert_eq!(merged["op-bot"].agent_id, "op-bot");
        }

        #[test]
        fn merge_operator_wins_on_collision() {
            let mut pack_agents = HashMap::new();
            let mut pack_config = sample_agent_config("shared-bot");
            pack_config.system_prompt = "pack prompt".to_string();
            pack_agents.insert("shared-bot".to_string(), pack_config);

            let mut operator_agents = HashMap::new();
            let mut operator_config = sample_agent_config("shared-bot");
            operator_config.system_prompt = "operator prompt".to_string();
            operator_agents.insert("shared-bot".to_string(), operator_config);

            let merged = super::merge_agent_sources(pack_agents, operator_agents);

            assert_eq!(merged.len(), 1);
            assert_eq!(
                merged["shared-bot"].system_prompt, "operator prompt",
                "operator config must override pack config on agent_id collision"
            );
        }

        // -----------------------------------------------------------------------
        // agent_configs_from_manifest tests
        // -----------------------------------------------------------------------

        #[test]
        fn deserialize_agent_blob_produces_correct_config() {
            let blob = serde_json::json!({
                "agent_id": "demo-agent",
                "system_prompt": "You are helpful.",
                "tools": [],
                "llm": {
                    "provider": "openai",
                    "model": "gpt-4o-mini"
                },
                "limits": {
                    "max_iter": 5,
                    "timeout": 30,
                    "max_history_turns": 10,
                    "llm_retry_attempts": 2,
                    "llm_retry_backoff": 500,
                    "provider_failure_message": null,
                    "daily_token_cap_per_tenant": null
                }
            });

            let config: AgentConfig =
                serde_json::from_value(blob).expect("valid blob must deserialize");

            assert_eq!(config.agent_id, "demo-agent");
            assert_eq!(config.system_prompt, "You are helpful.");
            assert_eq!(config.limits.max_iter, 5);
            assert_eq!(config.limits.timeout, std::time::Duration::from_secs(30));
        }

        #[test]
        fn agent_configs_from_manifest_skips_malformed_blobs() {
            use std::collections::BTreeMap;

            let mut blobs: BTreeMap<String, serde_json::Value> = BTreeMap::new();

            // Valid agent blob
            blobs.insert(
                "good-agent".to_string(),
                serde_json::json!({
                    "agent_id": "good-agent",
                    "system_prompt": "Valid.",
                    "tools": [],
                    "llm": { "provider": "openai", "model": "gpt-4o-mini" },
                    "limits": {
                        "max_iter": 8,
                        "timeout": 60,
                        "max_history_turns": 20,
                        "llm_retry_attempts": 3,
                        "llm_retry_backoff": 250,
                        "provider_failure_message": null,
                        "daily_token_cap_per_tenant": null
                    }
                }),
            );

            // Malformed blob (missing required fields)
            blobs.insert(
                "bad-agent".to_string(),
                serde_json::json!({ "broken": true }),
            );

            let configs = super::agent_configs_from_manifest("test-pack", &blobs);

            assert_eq!(configs.len(), 1, "malformed blob must be skipped");
            assert!(configs.contains_key("good-agent"));
            assert!(!configs.contains_key("bad-agent"));
        }

        #[test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        fn registry_from_env_requires_both_vars() {
            // SAFETY: #[serial] serializes env-mutating tests (crate convention),
            // so no concurrent test observes a torn env; vars cleaned up at the end.
            unsafe {
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }
            assert!(super::registry_from_env().is_none());

            unsafe {
                std::env::set_var("GREENTIC_AW_ADMIN_ENDPOINT", "http://localhost:9999");
            }
            assert!(
                super::registry_from_env().is_none(),
                "endpoint alone is not enough"
            );

            unsafe {
                std::env::set_var("GREENTIC_AW_ADMIN_TOKEN", "gtc_live_x");
            }
            assert!(super::registry_from_env().is_some());

            unsafe {
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }
        }

        #[test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        fn guardrail_policy_from_env_requires_both_vars() {
            // SAFETY: #[serial] serializes env-mutating tests (crate convention),
            // so no concurrent test observes a torn env; vars cleaned up at the end.
            unsafe {
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }
            assert!(super::guardrail_policy_from_env().is_none());

            unsafe {
                std::env::set_var("GREENTIC_AW_ADMIN_ENDPOINT", "http://localhost:9999");
            }
            assert!(
                super::guardrail_policy_from_env().is_none(),
                "endpoint alone is not enough"
            );

            unsafe {
                std::env::set_var("GREENTIC_AW_ADMIN_TOKEN", "gtc_live_x");
            }
            assert!(super::guardrail_policy_from_env().is_some());

            unsafe {
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }
        }

        #[test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        fn mcp_source_from_env_default_on_with_opt_out() {
            // SAFETY: #[serial] serializes env-mutating tests (crate convention),
            // so no concurrent test observes a torn env; vars cleaned up at the end.
            unsafe {
                std::env::remove_var("GREENTIC_AW_MCP");
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }

            // (a) Default-on: endpoint + token present, gate unset → Some.
            unsafe {
                std::env::set_var("GREENTIC_AW_ADMIN_ENDPOINT", "http://localhost:9999");
                std::env::set_var("GREENTIC_AW_ADMIN_TOKEN", "gtc_live_x");
            }
            assert!(
                super::mcp_source_from_env(None).is_some(),
                "MCP is on by default when admin credentials are configured"
            );

            // (b) Explicit opt-out wins even with full credentials.
            unsafe {
                std::env::set_var("GREENTIC_AW_MCP", "0");
            }
            assert!(
                super::mcp_source_from_env(None).is_none(),
                "GREENTIC_AW_MCP=0 disables MCP regardless of credentials"
            );

            // (b') Legacy opt-in value still enables (any non-"0" value does).
            unsafe {
                std::env::set_var("GREENTIC_AW_MCP", "1");
            }
            assert!(super::mcp_source_from_env(None).is_some());

            // (c) Missing credential → None even without an opt-out.
            unsafe {
                std::env::remove_var("GREENTIC_AW_MCP");
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
            }
            assert!(
                super::mcp_source_from_env(None).is_none(),
                "no endpoint → no MCP source"
            );

            unsafe {
                std::env::set_var("GREENTIC_AW_ADMIN_ENDPOINT", "http://localhost:9999");
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }
            assert!(
                super::mcp_source_from_env(None).is_none(),
                "no token → no MCP source"
            );

            unsafe {
                std::env::remove_var("GREENTIC_AW_MCP");
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }
        }

        // -----------------------------------------------------------------------
        // guardrail_denied_json tests
        // -----------------------------------------------------------------------

        #[test]
        fn guardrail_denied_maps_to_structured_output() {
            use greentic_aw_runtime::guardrail::GuardrailDirection;
            let v = super::guardrail_denied_json(
                GuardrailDirection::Inbound,
                "permission_denied",
                "blocked",
                None,
            );
            assert_eq!(v["guardrail"]["blocked"], serde_json::json!(true));
            assert_eq!(v["guardrail"]["direction"], serde_json::json!("inbound"));
            assert_eq!(
                v["guardrail"]["code"],
                serde_json::json!("permission_denied")
            );
            assert_eq!(v["terminated_by"], serde_json::json!("guardrail_denied"));
            assert_eq!(v["reply"], serde_json::json!("blocked"));
        }

        #[cfg(feature = "desktop-agent-ephemeral")]
        #[tokio::test]
        #[allow(unsafe_code)]
        async fn ephemeral_builder_yields_handler_without_redis() {
            // Remove Redis URL so we prove the ephemeral builder does not need it.
            // SAFETY: single-threaded test; no concurrent env mutation.
            unsafe {
                std::env::remove_var("GREENTIC_AW_REDIS_URL");
            }
            let mut agents = HashMap::new();
            agents.insert("greeter".to_string(), sample_agent_config("greeter"));
            // Build a minimal env-backed secrets manager (no broker configured in tests).
            let secrets: crate::secrets::DynSecretsManager =
                Arc::new(greentic_secrets_lib::env::EnvSecretsManager);
            let handler = super::build_agent_node_handler_ephemeral(
                agents,
                "t1".to_string(),
                secrets,
                None,
                Vec::new(),
                None,
                None,
                None,
            )
            .await;
            assert!(
                handler.is_some(),
                "ephemeral builder must not require Redis"
            );
        }

        /// Regression guard for the in-process knowledge (RAG) mount.
        ///
        /// Before the fix `build_runtime_with_stores` built the in-process
        /// [`AgentRuntime`] via `AgentRuntime::new(...)` and returned it WITHOUT
        /// mounting the corpus backend, so `runtime.knowledge` stayed `None`,
        /// `knowledge_active()` was false and `search_knowledge` was never
        /// invoked — the in-process `dw.agent` worker hallucinated instead of
        /// grounding on the ingested corpus. The corpus backend now comes from a
        /// registered [`crate::runner::runtime_ext::AgentRuntimeExtension`] (the
        /// Chronicle one lives in a private crate), so this proves the in-process
        /// path calls every registered extension, and does so BEFORE the
        /// extension-delegated adapter wraps the result.
        ///
        /// Asserted through [`crate::runner::knowledge_ext::corpus_backend`]
        /// rather than `has_knowledge()`, and that is not cosmetic:
        /// [`crate::runner::knowledge_ext::attach`] mounts unconditionally, so
        /// `has_knowledge()` is true on this path whether the extension ran or
        /// not. Asserting it would pass forever over a deleted
        /// `runtime_ext::attach_all` — which is precisely the failure this was
        /// written to catch. And a backend mounted AFTER the adapter would sit on
        /// top of it and replace it, which `corpus_backend` would still find but
        /// the adapter check below would not.
        ///
        /// The extension is process-global once registered, so it acts only
        /// inside this test's task-local scope and leaves every other test's
        /// runtime untouched.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn in_process_runtime_attaches_registered_extensions() {
            use greentic_aw_runtime::cost::MockTokenMeter;
            use greentic_aw_runtime::mock::{MockAgentStateStore, MockKnowledge, NoopToolLedger};

            tokio::task_local! {
                static IN_THIS_TEST: ();
            }

            struct CorpusExtension;

            #[async_trait::async_trait]
            impl crate::runner::runtime_ext::AgentRuntimeExtension for CorpusExtension {
                async fn attach(&self, rt: AgentRuntime) -> AgentRuntime {
                    if IN_THIS_TEST.try_with(|_| ()).is_err() {
                        return rt;
                    }
                    rt.with_knowledge(Arc::new(MockKnowledge::new(Vec::new())))
                }
            }

            static REGISTER: std::sync::Once = std::sync::Once::new();
            REGISTER.call_once(|| {
                crate::runner::runtime_ext::register_agent_runtime_extension(Arc::new(
                    CorpusExtension,
                ));
            });

            // Build the in-process runtime through the SAME shared tail both
            // in-process handlers use (Redis + ephemeral), with cheap mock stores.
            async fn build_runtime() -> Option<Arc<AgentRuntime>> {
                let mut agents = HashMap::new();
                agents.insert("greeter".to_string(), sample_agent_config("greeter"));
                let secrets: crate::secrets::DynSecretsManager =
                    Arc::new(greentic_secrets_lib::env::EnvSecretsManager);
                let state_store: Arc<dyn greentic_aw_runtime::state::AgentStateStore> =
                    Arc::new(MockAgentStateStore::new());
                let token_meter: Arc<dyn greentic_aw_runtime::cost::TokenMeter> =
                    Arc::new(MockTokenMeter::new(0));
                let ledger: Arc<dyn greentic_aw_runtime::tools::ToolLedger> =
                    Arc::new(NoopToolLedger);
                super::build_runtime_with_stores(
                    agents,
                    "t1".to_string(),
                    secrets,
                    None,
                    Vec::new(),
                    state_store,
                    token_meter,
                    ledger,
                    None,
                )
                .await
            }

            // Control: the extension declines outside the scope, so no corpus
            // backend may be mounted — proving the positive case below is the
            // extension doing its job, not a tautology.
            let runtime_optout = build_runtime().await.expect("runtime should build");
            assert!(
                crate::runner::knowledge_ext::corpus_backend(&runtime_optout).is_none(),
                "no corpus backend may be mounted when no extension provides one"
            );

            let runtime = IN_THIS_TEST
                .scope((), build_runtime())
                .await
                .expect("runtime should build");
            assert!(
                crate::runner::knowledge_ext::corpus_backend(&runtime).is_some(),
                "in-process dw.agent runtime must mount a registered extension's \
                 corpus backend, so an ingested corpus is retrievable"
            );
            let top = runtime
                .knowledge_backend()
                .expect("the extension adapter is always mounted");
            assert!(
                top.wrapped_backend().is_some(),
                "the extension-delegated adapter must be the OUTER layer, wrapping \
                 the extension's corpus backend rather than being replaced by it"
            );
        }

        /// In-memory `SecretsManager` for backend tests: returns seeded values,
        /// `NotFound` otherwise.
        struct MapSecrets(std::collections::HashMap<String, Vec<u8>>);

        #[async_trait::async_trait]
        impl greentic_secrets_lib::SecretsManager for MapSecrets {
            async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
                self.0
                    .get(path)
                    .cloned()
                    .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
            }
            async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
                Ok(())
            }
            async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
                Ok(())
            }
        }

        /// Build a backend for one unit over a seeded store.
        fn backend_for(
            unit: Option<&str>,
            seed: &[(&str, &str)],
        ) -> super::StoreToolSecretsBackend {
            let map = seed
                .iter()
                .map(|(uri, value)| (uri.to_string(), value.as_bytes().to_vec()))
                .collect::<std::collections::HashMap<_, _>>();
            super::StoreToolSecretsBackend {
                secrets: Arc::new(MapSecrets(map)),
                tenant: "acme".to_string(),
                env: "dev".to_string(),
                unit: unit.map(str::to_string),
            }
        }

        fn tool_uri(provider: &str, key: &str, unit: Option<&str>) -> String {
            crate::secrets::agent_tool_secret_uri("dev", "acme", provider, key, unit)
                .expect("agent tool uri")
        }

        /// The defect this scope exists for: one environment, two workers, one
        /// `hubspot/access_token`, two different accounts. Before the unit
        /// segment both read one address and the second staged value won, so
        /// one worker silently authenticated as the other's account.
        #[test]
        fn two_units_with_one_provider_and_key_each_read_their_own_value() {
            use greentic_ext_runtime::SecretsBackend as _;
            let seed = [
                (
                    tool_uri("hubspot", "access_token", Some("sales-bot")),
                    "sales-token",
                ),
                (
                    tool_uri("hubspot", "access_token", Some("support-bot")),
                    "support-token",
                ),
            ];
            let seed: Vec<(&str, &str)> = seed
                .iter()
                .map(|(uri, value)| (uri.as_str(), *value))
                .collect();

            assert_eq!(
                backend_for(Some("sales-bot"), &seed)
                    .get("secret://hubspot/access_token")
                    .expect("sales value"),
                "sales-token"
            );
            assert_eq!(
                backend_for(Some("support-bot"), &seed)
                    .get("secret://hubspot/access_token")
                    .expect("support value"),
                "support-token"
            );
        }

        /// A unit's own value beats the env-wide one left by an older stage.
        #[test]
        fn the_unit_scope_wins_over_the_env_wide_address() {
            use greentic_ext_runtime::SecretsBackend as _;
            let scoped = tool_uri("tavily", "api_key", Some("web-chat"));
            let bare = tool_uri("tavily", "api_key", None);
            let backend = backend_for(
                Some("web-chat"),
                &[(scoped.as_str(), "mine"), (bare.as_str(), "shared")],
            );
            assert_eq!(
                backend.get("secret://tavily/api_key").expect("value"),
                "mine"
            );
        }

        /// The compatibility fallback: an environment staged before the unit
        /// scope existed keeps resolving when its runner moves.
        #[test]
        fn a_unit_with_no_own_value_falls_back_to_the_env_wide_address() {
            use greentic_ext_runtime::SecretsBackend as _;
            let bare = tool_uri("tavily", "api_key", None);
            let backend = backend_for(Some("web-chat"), &[(bare.as_str(), "shared")]);
            assert_eq!(
                backend.get("secret://tavily/api_key").expect("value"),
                "shared"
            );
        }

        /// With no unit — the legacy tenant-only path and the process-level
        /// serve path — the env-wide address is the ONLY one tried, and a
        /// unit-scoped value is never picked up by accident.
        #[test]
        fn no_unit_reads_exactly_the_env_wide_address() {
            use greentic_ext_runtime::SecretsBackend as _;
            let backend = backend_for(None, &[]);
            assert_eq!(
                backend.store_uri_candidates("secret://tavily/api_key"),
                vec![tool_uri("tavily", "api_key", None)]
            );

            let scoped = tool_uri("tavily", "api_key", Some("web-chat"));
            let other = backend_for(None, &[(scoped.as_str(), "someone-elses")]);
            // Falls through to the env fallback, which has nothing either.
            assert!(other.get("secret://tavily/api_key").is_err());
        }

        #[test]
        fn the_candidates_are_the_unit_address_then_the_env_wide_one() {
            let backend = backend_for(Some("web-chat"), &[]);
            assert_eq!(
                backend.store_uri_candidates("secret://tavily/api_key"),
                vec![
                    tool_uri("tavily", "api_key", Some("web-chat")),
                    tool_uri("tavily", "api_key", None),
                ]
            );
        }

        /// A reference with no `<provider>/<key>` split has no store address at
        /// all, and must fall through to the env rather than build a malformed
        /// URI.
        #[test]
        fn a_reference_with_no_key_has_no_store_candidates() {
            let backend = backend_for(Some("web-chat"), &[]);
            assert!(backend.store_uri_candidates("secret://tavily").is_empty());
        }

        #[test]
        fn store_tool_secret_backend_maps_secret_uri_to_store_scope() {
            use greentic_ext_runtime::SecretsBackend as _;
            let mut map = std::collections::HashMap::new();
            // The injected manager resolves the canonical store scope (in real
            // runs its candidate fallback bridges to the pack scope; here we seed
            // the canonical path the backend constructs directly).
            map.insert(
                "secrets://dev/acme/_/tavily/api_key".to_string(),
                b"tvly-xyz".to_vec(),
            );
            let secrets: crate::secrets::DynSecretsManager = Arc::new(MapSecrets(map));
            let backend = super::StoreToolSecretsBackend {
                secrets,
                tenant: "acme".to_string(),
                env: "dev".to_string(),
                unit: None,
            };
            let got = backend
                .get("secret://tavily/api_key")
                .expect("resolve tavily key from store");
            assert_eq!(got, "tvly-xyz");
        }

        #[test]
        #[allow(unsafe_code)]
        fn store_tool_secret_backend_falls_back_to_env_on_store_miss() {
            use greentic_ext_runtime::SecretsBackend as _;
            let secrets: crate::secrets::DynSecretsManager =
                Arc::new(MapSecrets(std::collections::HashMap::new()));
            let backend = super::StoreToolSecretsBackend {
                secrets,
                tenant: "acme".to_string(),
                env: "dev".to_string(),
                unit: None,
            };
            // SAFETY: single-threaded test; no concurrent env mutation.
            unsafe {
                std::env::set_var("ZEROENV_MYPROVIDER_MYKEY", "from-env");
            }
            let got = backend
                .get("secret://zeroenv_myprovider/mykey")
                .expect("env fallback resolves");
            assert_eq!(got, "from-env");
            unsafe {
                std::env::remove_var("ZEROENV_MYPROVIDER_MYKEY");
            }
        }

        #[test]
        #[allow(unsafe_code)]
        fn load_process_agent_configs_reads_full_configs_and_skips_bad_files() {
            let dir = tempfile::tempdir().expect("tempdir");

            // Valid full AgentConfig keyed by file stem.
            let good = sample_agent_config("greeter");
            std::fs::write(
                dir.path().join("greeter.json"),
                serde_json::to_vec(&good).expect("serialize"),
            )
            .expect("write good");

            // Malformed JSON — skipped, must not abort the load.
            std::fs::write(dir.path().join("broken.json"), b"{ not json").expect("write broken");

            // Non-JSON file — ignored.
            std::fs::write(dir.path().join("README.md"), b"ignore me").expect("write md");

            let previous = std::env::var("GREENTIC_AGENT_MANIFESTS_DIR").ok();
            unsafe {
                std::env::set_var("GREENTIC_AGENT_MANIFESTS_DIR", dir.path());
            }
            let loaded = super::load_process_agent_configs();
            unsafe {
                match &previous {
                    Some(value) => std::env::set_var("GREENTIC_AGENT_MANIFESTS_DIR", value),
                    None => std::env::remove_var("GREENTIC_AGENT_MANIFESTS_DIR"),
                }
            }

            assert_eq!(loaded.len(), 1, "only the valid config should load");
            assert!(loaded.contains_key("greeter"));
            assert_eq!(loaded["greeter"].agent_id, "greeter");
        }

        #[test]
        fn merge_sidecar_fills_only_missing_keys() {
            use std::collections::BTreeMap;
            let mut blobs: BTreeMap<String, serde_json::Value> =
                BTreeMap::from([("a".to_string(), serde_json::json!({"from": "manifest"}))]);
            let sidecar: BTreeMap<String, serde_json::Value> = BTreeMap::from([
                ("a".to_string(), serde_json::json!({"from": "sidecar"})), // must NOT override
                ("b".to_string(), serde_json::json!({"from": "sidecar"})), // must be added
            ]);
            super::merge_sidecar_into(&mut blobs, sidecar);
            assert_eq!(blobs["a"]["from"], "manifest"); // manifest wins
            assert_eq!(blobs["b"]["from"], "sidecar"); // gap filled
            assert_eq!(blobs.len(), 2);
        }

        #[test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        fn flow_source_disabled_by_env_and_empty_packs() {
            // SAFETY: #[serial] serializes env-mutating tests (crate convention),
            // so no concurrent test observes a torn env; vars cleaned up at the end.
            unsafe {
                std::env::set_var("GREENTIC_AW_FLOW_TOOLS", "0");
            }
            assert!(
                super::flow_source_from_packs(&[], "acme").is_none(),
                "GREENTIC_AW_FLOW_TOOLS=0 must disable the flow tool source"
            );
            unsafe {
                std::env::remove_var("GREENTIC_AW_FLOW_TOOLS");
            }
            assert!(
                super::flow_source_from_packs(&[], "acme").is_none(),
                "empty packs => None even when gate is unset"
            );
        }

        #[test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        fn pack_mcp_source_disabled_by_env_and_empty_packs() {
            // SAFETY: #[serial] serializes env-mutating tests (crate convention),
            // so no concurrent test observes a torn env; vars cleaned up at the end.
            unsafe {
                std::env::set_var("GREENTIC_AW_MCP", "0");
            }
            assert!(
                super::mcp_source_from_packs(&[], "acme", None, None).is_none(),
                "an operator who disabled outbound MCP must not have it \
                 re-enabled by a pack-carried sidecar"
            );
            unsafe {
                std::env::remove_var("GREENTIC_AW_MCP");
            }
            assert!(
                super::mcp_source_from_packs(&[], "acme", None, None).is_none(),
                "empty packs => None even when the gate is unset"
            );
        }

        /// Wiring guard: the in-process runtime built by
        /// `build_runtime_with_stores` carries the pack-backed A2A source.
        /// Without this, a dropped `.with_a2a_source(..)` fails only at run
        /// time, as a bound tool that silently vanished.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        async fn the_in_process_runtime_wires_the_pack_carried_a2a_source() {
            use greentic_aw_runtime::cost::MockTokenMeter;
            use greentic_aw_runtime::mock::{MockAgentStateStore, NoopToolLedger};

            // SAFETY: #[serial] serializes env-mutating tests (crate convention).
            unsafe {
                std::env::remove_var("GREENTIC_AW_A2A");
                std::env::remove_var("GREENTIC_AW_LLM_EXTENSION");
            }

            async fn build(pack_dir: &std::path::Path) -> Arc<AgentRuntime> {
                let mut agents = HashMap::new();
                agents.insert("greeter".to_string(), sample_agent_config("greeter"));
                let secrets: crate::secrets::DynSecretsManager =
                    Arc::new(greentic_secrets_lib::env::EnvSecretsManager);
                let pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(pack_dir));
                super::build_runtime_with_stores(
                    agents,
                    "t1".to_string(),
                    secrets,
                    None,
                    vec![pack],
                    Arc::new(MockAgentStateStore::new()),
                    Arc::new(MockTokenMeter::new(0)),
                    Arc::new(NoopToolLedger),
                    Some("worker-a".to_string()),
                )
                .await
                .expect("runtime should build")
            }

            let with_sidecar = tempfile::tempdir().unwrap();
            std::fs::create_dir_all(with_sidecar.path().join("assets")).unwrap();
            std::fs::write(
                with_sidecar.path().join("assets/a2a-routes.json"),
                br#"[{"agent_id":"recipe","base_url":"https://agent.example.com"}]"#,
            )
            .unwrap();
            assert!(
                build(with_sidecar.path()).await.has_a2a_source(),
                "a pack carrying assets/a2a-routes.json must wire an A2A source"
            );

            let without = tempfile::tempdir().unwrap();
            assert!(
                !build(without.path()).await.has_a2a_source(),
                "control: no sidecar, no source"
            );
        }

        /// The agent path prefers the env-configured ADMIN source and uses the
        /// pack routes only as a fallback — deliberately the opposite of the
        /// flow node, because the admin catalog carries LIVE tool schemas and
        /// `allowed_tools` that a pack cannot. `or_else` is what makes the
        /// fallback lazy, so an admin-backed deployment never even reads its
        /// packs' sidecars; this pins that shape.
        #[test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        fn the_admin_source_wins_and_the_pack_fallback_is_never_consulted() {
            use std::cell::Cell;

            // SAFETY: #[serial] serializes env-mutating tests (crate convention).
            unsafe {
                std::env::set_var("GREENTIC_AW_ADMIN_ENDPOINT", "https://admin.example");
                std::env::set_var("GREENTIC_AW_ADMIN_TOKEN", "gtc_live_x");
                std::env::remove_var("GREENTIC_AW_MCP");
            }

            let consulted = Cell::new(false);
            let chosen = super::mcp_source_from_env(None).or_else(|| {
                consulted.set(true);
                super::mcp_source_from_packs(&[], "acme", None, None)
            });

            unsafe {
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }

            assert!(chosen.is_some(), "the admin source must be built");
            assert!(
                !consulted.get(),
                "the pack fallback must not be consulted when the admin \
                 credentials are present"
            );
        }

        #[tokio::test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        async fn sorla_source_from_env_none_when_gate_opted_out() {
            // SAFETY: #[serial] serializes env-mutating tests (crate convention),
            // so no concurrent test observes a torn env; vars cleaned up at the end.
            unsafe {
                std::env::set_var("GREENTIC_AW_SORLA_TOOLS", "0");
                std::env::set_var("GREENTIC_AW_SORX_URL", "http://localhost:9999");
            }
            assert!(
                super::sorla_source_from_env().await.is_none(),
                "GREENTIC_AW_SORLA_TOOLS=0 must disable the sorla tool source even with a URL set"
            );
            unsafe {
                std::env::remove_var("GREENTIC_AW_SORLA_TOOLS");
                std::env::remove_var("GREENTIC_AW_SORX_URL");
            }
        }

        #[tokio::test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        async fn sorla_source_from_env_none_when_url_unset() {
            // SAFETY: #[serial] serializes env-mutating tests (crate convention).
            unsafe {
                std::env::remove_var("GREENTIC_AW_SORLA_TOOLS");
                std::env::remove_var("GREENTIC_AW_SORX_URL");
            }
            assert!(
                super::sorla_source_from_env().await.is_none(),
                "no GREENTIC_AW_SORX_URL => no sorla source"
            );
        }

        #[tokio::test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        async fn sorla_source_from_env_some_when_url_set_even_if_sor_unreachable() {
            // SAFETY: #[serial] serializes env-mutating tests (crate convention),
            // so no concurrent test observes a torn env; vars cleaned up at the end.
            unsafe {
                std::env::remove_var("GREENTIC_AW_SORLA_TOOLS");
                // Nothing listens here: `SorxHttpInvoker::fetch` must degrade to an
                // empty-ops invoker (logged) rather than failing, so the source is
                // still `Some` — a down/unreachable SoR must never block startup.
                std::env::set_var("GREENTIC_AW_SORX_URL", "http://127.0.0.1:1");
            }
            let source = super::sorla_source_from_env().await;
            unsafe {
                std::env::remove_var("GREENTIC_AW_SORX_URL");
            }
            assert!(
                source.is_some(),
                "a configured URL yields Some(source) even when the SoR is unreachable"
            );
        }

        /// The LLM-key env vars `EnvLlmPort::from_env` reads. Process-global, so
        /// these tests serialize and restore them to avoid cross-test bleed.
        #[cfg(feature = "greentic-llm-backend")]
        const LLM_ENV_VARS: &[&str] = &[
            "GREENTIC_LLM_API_KEY",
            "OPENAI_API_KEY",
            "GREENTIC_LLM_PROVIDER",
            "GREENTIC_LLM_MODEL",
            "GREENTIC_LLM_BASE_URL",
        ];

        /// Run `body` with every LLM env var cleared, restoring prior values
        /// afterwards. Keeps the `EnvLlmPort` tests hermetic and side-effect-free.
        #[cfg(feature = "greentic-llm-backend")]
        #[allow(unsafe_code)]
        fn with_clean_llm_env(body: impl FnOnce()) {
            let saved: Vec<(&str, Option<String>)> = LLM_ENV_VARS
                .iter()
                .map(|name| (*name, std::env::var(name).ok()))
                .collect();
            // SAFETY: #[serial] serializes env-mutating tests (crate convention),
            // so no concurrent test observes a torn env; vars restored at the end.
            unsafe {
                for name in LLM_ENV_VARS {
                    std::env::remove_var(name);
                }
            }
            body();
            unsafe {
                for (name, value) in saved {
                    match value {
                        Some(value) => std::env::set_var(name, value),
                        None => std::env::remove_var(name),
                    }
                }
            }
        }

        #[cfg(feature = "greentic-llm-backend")]
        #[test]
        #[serial_test::serial]
        fn env_llm_port_none_without_key() {
            with_clean_llm_env(|| {
                assert!(
                    super::EnvLlmPort::from_env().is_none(),
                    "no LLM key in env => no port (ext-runtime stays 'llm not configured')"
                );
            });
        }

        #[cfg(feature = "greentic-llm-backend")]
        #[test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        fn env_llm_port_some_with_key_and_defaults() {
            with_clean_llm_env(|| {
                // SAFETY: within `with_clean_llm_env` under #[serial].
                unsafe {
                    std::env::set_var("GREENTIC_LLM_API_KEY", "sk-test-key");
                }
                let port = super::EnvLlmPort::from_env().expect("key present => port");
                // Provider/model fall back to the DeepSeek-first defaults.
                assert_eq!(port.provider, super::EnvLlmPort::DEFAULT_PROVIDER);
                assert_eq!(port.model, super::EnvLlmPort::DEFAULT_MODEL);
            });
        }

        #[cfg(feature = "greentic-llm-backend")]
        #[test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        fn env_llm_port_honours_provider_and_model_overrides() {
            with_clean_llm_env(|| {
                // OPENAI_API_KEY is the documented fallback for the key.
                // SAFETY: within `with_clean_llm_env` under #[serial].
                unsafe {
                    std::env::set_var("OPENAI_API_KEY", "sk-openai");
                    std::env::set_var("GREENTIC_LLM_PROVIDER", "openai");
                    std::env::set_var("GREENTIC_LLM_MODEL", "gpt-4o-mini");
                }
                let port = super::EnvLlmPort::from_env().expect("fallback key present => port");
                assert_eq!(port.provider, "openai");
                assert_eq!(port.model, "gpt-4o-mini");
            });
        }

        #[cfg(feature = "greentic-llm-backend")]
        #[test]
        #[serial_test::serial]
        #[allow(unsafe_code)]
        fn env_llm_port_maps_request_without_network() {
            use greentic_aw_runtime::state::ChatMessage;
            use greentic_ext_runtime::host_ports::{LlmPortRequest, LlmPortResponseFormat};

            with_clean_llm_env(|| {
                // SAFETY: within `with_clean_llm_env` under #[serial].
                unsafe {
                    std::env::set_var("GREENTIC_LLM_API_KEY", "sk-test-key");
                }
                let port = super::EnvLlmPort::from_env().expect("key present => port");
                let request = LlmPortRequest {
                    system_prompt: "be a card generator".into(),
                    messages: vec![
                        ("user".into(), "hello".into()),
                        ("assistant".into(), "hi".into()),
                        ("system".into(), "note".into()),
                        ("weird".into(), "fallback-to-user".into()),
                    ],
                    response_format: LlmPortResponseFormat::Json,
                };
                // Pure mapping — no provider is built, no network call.
                let llm_request =
                    super::port_request_to_llm_request(request, &port.provider, &port.model);
                assert_eq!(llm_request.system_prompt, "be a card generator");
                assert!(llm_request.tools.is_empty(), "tools not exposed to ext LLM");
                assert_eq!(llm_request.provider.provider, "deepseek");
                assert_eq!(llm_request.history.len(), 4);
                assert!(matches!(llm_request.history[0], ChatMessage::User { .. }));
                assert!(matches!(
                    llm_request.history[1],
                    ChatMessage::Assistant { .. }
                ));
                assert!(matches!(llm_request.history[2], ChatMessage::System { .. }));
                // Unknown role falls back to a user turn.
                assert!(matches!(llm_request.history[3], ChatMessage::User { .. }));
            });
        }
    }
}

#[allow(clippy::items_after_test_module)] // helper fn + re-exports follow
#[cfg(test)]
mod gating_tests {
    use super::{DwAgentDispatch, dw_agent_dispatch_mode, should_serve_agentic_inproc};
    use std::collections::HashMap;

    fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    #[test]
    fn skips_when_opt_in_unset() {
        let env = env_from(&[("GREENTIC_EVENTS_NATS_URL", "nats://127.0.0.1:4222")]);
        assert!(!should_serve_agentic_inproc(env));
    }

    #[test]
    fn skips_when_nats_url_unset() {
        let env = env_from(&[("GREENTIC_AGENTIC_SERVE_INPROC", "1")]);
        assert!(!should_serve_agentic_inproc(env));
    }

    #[test]
    fn skips_when_nats_url_blank() {
        let env = env_from(&[
            ("GREENTIC_AGENTIC_SERVE_INPROC", "1"),
            ("GREENTIC_EVENTS_NATS_URL", "   "),
        ]);
        assert!(!should_serve_agentic_inproc(env));
    }

    #[test]
    fn serves_when_both_set() {
        for truthy in ["1", "true", "TRUE", "yes", "on"] {
            let env = env_from(&[
                ("GREENTIC_AGENTIC_SERVE_INPROC", truthy),
                ("GREENTIC_EVENTS_NATS_URL", "nats://127.0.0.1:4222"),
            ]);
            assert!(should_serve_agentic_inproc(env), "{truthy} should enable");
        }
    }

    #[test]
    fn skips_on_falsey_opt_in() {
        for falsey in ["0", "false", "no", "off", "maybe"] {
            let env = env_from(&[
                ("GREENTIC_AGENTIC_SERVE_INPROC", falsey),
                ("GREENTIC_EVENTS_NATS_URL", "nats://127.0.0.1:4222"),
            ]);
            assert!(!should_serve_agentic_inproc(env), "{falsey} should skip");
        }
    }

    #[test]
    fn dw_agent_dispatch_mode_defaults_inproc_and_parses_nats() {
        assert_eq!(dw_agent_dispatch_mode(|_| None), DwAgentDispatch::InProcess);
        assert_eq!(
            dw_agent_dispatch_mode(|k| (k == "GREENTIC_AW_DISPATCH").then(|| "nats".to_string())),
            DwAgentDispatch::Nats
        );
        assert_eq!(
            dw_agent_dispatch_mode(|k| {
                (k == "GREENTIC_AW_DISPATCH").then(|| "inproc".to_string())
            }),
            DwAgentDispatch::InProcess
        );
        assert_eq!(
            dw_agent_dispatch_mode(|k| (k == "GREENTIC_AW_DISPATCH").then(|| "NATS".to_string())),
            DwAgentDispatch::Nats
        );
    }
}

/// Decide whether the runner process should host the agentic-worker NATS
/// service in-process (the opt-in co-host path).
///
/// Returns `true` only when BOTH gates are satisfied:
/// - `GREENTIC_AGENTIC_SERVE_INPROC` is truthy (`1`/`true`/`yes`/`on`,
///   case-insensitive) — opt-in, default OFF; and
/// - `GREENTIC_EVENTS_NATS_URL` is set to a non-empty value (no NATS bus means
///   nothing to serve on).
///
/// Pure over its `get_env` closure so it is unit-testable without touching the
/// real process environment. Feature-independent (no `agentic-worker` gate) so
/// the gating logic stays trivially testable; the actual spawn is gated at the
/// call site.
pub fn should_serve_agentic_inproc(get_env: impl Fn(&str) -> Option<String>) -> bool {
    let opt_in = get_env("GREENTIC_AGENTIC_SERVE_INPROC")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    let nats_set = get_env("GREENTIC_EVENTS_NATS_URL")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    opt_in && nats_set
}

/// How a `dw.agent` flow node executes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // consumed by Task 2.2 (engine.rs)
pub enum DwAgentDispatch {
    /// Run the agentic step in-process (default; today's behaviour).
    InProcess,
    /// Publish to the durable agentic NATS path (scale-to-zero compute).
    Nats,
}

/// Resolve how `dw.agent` nodes execute. `GREENTIC_AW_DISPATCH=nats` routes them
/// over the out-of-process agentic NATS path; anything else (incl. unset) keeps
/// the in-process path — zero regression by default. Pure over `get_env` for
/// testability (mirrors `should_serve_agentic_inproc`).
#[must_use]
#[allow(dead_code)] // consumed by Task 2.2 (engine.rs)
pub fn dw_agent_dispatch_mode(get_env: impl Fn(&str) -> Option<String>) -> DwAgentDispatch {
    match get_env("GREENTIC_AW_DISPATCH") {
        Some(v) if v.trim().eq_ignore_ascii_case("nats") => DwAgentDispatch::Nats,
        _ => DwAgentDispatch::InProcess,
    }
}

#[cfg(feature = "agentic-worker")]
pub use aw::{
    AgentNodeWiring, HostConfigProvider, RuntimeAgentNodeHandler, agent_configs_from_manifest,
    build_agent_node_handler, build_agent_node_wiring, build_agent_runtime,
    load_process_agent_configs, merge_agent_sources, merge_sidecar_into, serve_agentic,
};

#[cfg(feature = "desktop-agent-ephemeral")]
pub use aw::{build_agent_node_handler_ephemeral, build_agent_node_wiring_ephemeral};

#[cfg(feature = "agentic-worker")]
pub(crate) use aw::{
    EnvSecretsBackend, build_ext_runtime, build_llm_backend, component_source_from_packs,
    mcp_secrets_manager, mcp_source_from_env,
};

// `first_declared_llm_agent` itself needs only `agentic-worker`, but its only
// consumer outside this module is `ext_llm_port`, which is gated on
// `greentic-llm-backend` too — so re-exporting it unconditionally under plain
// `agentic-worker` would trip `unused_imports` on a build that has
// `agentic-worker` without `greentic-llm-backend`.
//
// Shared by every `LlmPort` impl in this crate — `EnvLlmPort` here and
// `crate::runner::ext_llm_port::AgentLlmPort` — so the request mapping and the
// dedicated-thread completion bridge exist in exactly one place. Both need
// `greentic-llm-backend` regardless of who calls them: `EnvLlmPort` already
// does, and `ext_llm_port` is gated on it too (it also needs
// `greentic_llm::ProviderKind`, which is only pulled in by that feature).
#[cfg(feature = "greentic-llm-backend")]
pub(crate) use aw::{complete_on_thread, first_declared_llm_agent, port_request_to_llm_request};

// Only consumed by `runtime.rs`'s in-process operala.call wiring, which is
// itself gated behind `operala-in-process` — re-exporting unconditionally
// under plain `agentic-worker` would trip `unused_imports` on builds that have
// `agentic-worker` without `operala-in-process`.
#[cfg(feature = "operala-in-process")]
pub(crate) use aw::resolve_in_process_llm_key;

// flow_source_from_packs is used only inside the aw module (build_runtime_handler_with_stores
// + tests) so it stays pub(crate) there without a top-level re-export.

/// Test-only helpers other `runner` submodules' tests reuse rather than
/// re-implementing (e.g. `ext_llm_port`'s tests need an agent config to vary
/// the provider/model on).
#[cfg(all(test, feature = "agentic-worker"))]
pub(crate) mod test_support {
    pub(crate) use super::aw::tests::sample_agent_config;
}
