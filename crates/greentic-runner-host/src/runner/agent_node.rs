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
    async fn execute(
        &self,
        tenant_id: &str,
        env_id: &str,
        agent_id: &str,
        session_id: &str,
        flow_input: &Value,
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
    use crate::trace::audit_sink::AuditSink;

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
        /// `project_id` is the deployed unit's `bundle_id` (`None` on the
        /// legacy tenant-only path); it becomes the `project_id` billing
        /// dimension of every step this handler runs.
        pub fn new(
            runtime: Arc<AgentRuntime>,
            audit_sink: Option<AuditSink>,
            project_id: Option<String>,
        ) -> Self {
            Self {
                runtime,
                audit_sink,
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
        ) -> Result<Value> {
            let user_text = flow_input
                .get("user_text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let tenant =
                TenantContext::new(tenant_id, env_id).with_project_id(self.project_id.clone());
            let input = AgentInput { text: user_text };

            // Off by default: with no audit sink configured, this is exactly
            // the pre-existing `self.runtime.step(...)` call — no observer is
            // constructed and behaviour is byte-identical to before EPIC-B B-3.
            let step_result = match &self.audit_sink {
                Some(sink) => {
                    let observer: Arc<dyn StepObserver> = Arc::new(AgentAuditObserver::new(
                        sink.clone(),
                        tenant_ctx_for_audit(tenant_id, env_id),
                        agent_id.to_string(),
                        session_id.to_string(),
                    ));
                    self.runtime
                        .step_with_observer(tenant, session_id, agent_id, input, observer)
                        .await
                }
                None => self.runtime.step(tenant, session_id, agent_id, input).await,
            };

            match step_result {
                Ok(output) => Ok(json!({
                    "reply": output.reply,
                    "trail": output.trail,
                    "terminated_by": output.terminated_by,
                })),
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
                    Ok(json!({
                        "reply": SANITISED_ERROR_REPLY,
                        "trail": Vec::<AgentStep>::new(),
                        "terminated_by": "error",
                    }))
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
    /// [`McpToolSource`]: greentic_aw_runtime::McpToolSource
    fn mcp_source_from_env() -> Option<Arc<greentic_aw_runtime::McpToolSource>> {
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
        Some(Arc::new(greentic_aw_runtime::McpToolSource::new(
            endpoint, token,
        )))
    }

    /// Build the component tool source from the operator's loaded packs, gated
    /// by `GREENTIC_AW_COMPONENT_TOOLS` (set to "0" to disable). Returns `None`
    /// when disabled or when no packs are loaded, so `component:` tool refs then
    /// resolve to nothing. Mirrors [`mcp_source_from_env`] but discovers tools
    /// from in-pack components rather than a remote admin.
    fn component_source_from_packs(
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
    /// candidate fallback bridges the canonical `secrets://{env}/{tenant}/_/{provider}/{key}`
    /// scope to the pack-namespaced scope setup actually wrote. The env fallback
    /// preserves existing `TAVILY_API_KEY`-style runs.
    struct StoreToolSecretsBackend {
        secrets: crate::secrets::DynSecretsManager,
        tenant: String,
        env: String,
    }

    impl StoreToolSecretsBackend {
        fn new(secrets: crate::secrets::DynSecretsManager, tenant: String) -> Self {
            let env = std::env::var("GREENTIC_ENV")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "dev".to_string());
            Self {
                secrets,
                tenant,
                env,
            }
        }

        /// Map `secret://<provider>/<key>` to the canonical store URI
        /// `secrets://{env}/{tenant}/_/{provider}/{key}`. The injected manager's
        /// candidate fallback handles the env/team/pack-namespace bridging.
        fn canonical_store_uri(&self, uri: &str) -> Option<String> {
            let body = uri.strip_prefix("secret://").unwrap_or(uri);
            let (provider, key) = body.split_once('/')?;
            if provider.is_empty() || key.is_empty() {
                return None;
            }
            Some(format!(
                "secrets://{}/{}/_/{}/{}",
                self.env, self.tenant, provider, key
            ))
        }
    }

    impl greentic_ext_runtime::SecretsBackend for StoreToolSecretsBackend {
        fn get(&self, uri: &str) -> Result<String, greentic_ext_runtime::SecretsError> {
            if let Some(store_uri) = self.canonical_store_uri(uri) {
                // Read off a dedicated thread with its own current-thread runtime:
                // the extension runtime may invoke this from within the async
                // runner, where a nested `block_on` would panic.
                let secrets = self.secrets.clone();
                let resolved = std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .ok()?;
                    runtime.block_on(async move { secrets.read(&store_uri).await.ok() })
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

    pub(crate) fn build_ext_runtime(
        secrets_backend: Arc<dyn greentic_ext_runtime::SecretsBackend>,
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
        // This lane's HostOverrides has no `Default`, and `with_host_overrides`
        // is a builder on the runtime rather than on the config — so spell
        // every field out and apply the overrides after construction. The
        // non-obvious ones mirror the crate's own `defaults_for_tests`:
        // `runtime_weak` stays unset until the cross-extension dispatch
        // cascade lands, and `call_depth_start` is the recursion guard's floor.
        // `llm_port` and `oauth_config` are both left unset: this host wires
        // neither an in-runtime LLM port nor an OAuth broker for design
        // extensions, so `None` is the same behaviour these two carried before
        // the fields existed.
        let overrides = HostOverrides {
            translator: std::sync::Arc::new(greentic_ext_runtime::host_ports::KeyTranslator),
            secrets_backend,
            http_client: shared_blocking_http_client(),
            llm_port: None,
            url_matcher: greentic_ext_runtime::url_matcher::UrlMatcher::default(),
            runtime_weak: std::sync::Weak::new(),
            call_depth_start: 0,
            oauth_config: None,
        };
        let config = RuntimeConfig::from_paths(paths);
        let mut runtime = match ExtensionRuntime::new(config) {
            Ok(runtime) => runtime.with_host_overrides(overrides),
            Err(error) => {
                tracing::warn!(error = %error, "extension runtime init failed; DwAgent nodes disabled");
                return None;
            }
        };

        // Initial load of on-disk design extensions (agentic-worker tools live
        // in `<root>/design/<ext>/`).
        let design_dir = root.join("design");
        match discovery::scan_kind_dir(&design_dir) {
            Ok(ext_dirs) => {
                let mut loaded = 0usize;
                for ext_dir in ext_dirs {
                    match runtime.register_loaded_from_dir(&ext_dir) {
                        Ok(()) => loaded += 1,
                        Err(error) => tracing::warn!(
                            error = %error, dir = %ext_dir.display(),
                            "skipping extension that failed to load"
                        ),
                    }
                }
                tracing::info!(loaded, dir = %design_dir.display(), "loaded design extensions");
            }
            Err(error) => {
                tracing::warn!(error = %error, dir = %design_dir.display(), "scanning design extensions failed")
            }
        }

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

    /// Whether a resolved (key, provider) pair selects the in-process
    /// multi-provider greentic-llm backend rather than the single-provider
    /// OpenAI fall-through. A non-empty key always does; an empty one does
    /// only for a keyless provider.
    #[cfg(feature = "greentic-llm-backend")]
    pub(super) fn selects_multi_provider_backend(api_key: &str, provider: Option<&str>) -> bool {
        !api_key.trim().is_empty() || !provider_requires_api_key(provider)
    }

    /// The provider name an explicit `GREENTIC_LLM_PROVIDER` names, if any.
    pub(super) fn env_llm_provider() -> Option<String> {
        std::env::var("GREENTIC_LLM_PROVIDER")
            .ok()
            .filter(|provider| !provider.trim().is_empty())
    }

    /// The provider to judge keylessness by: the env override first (it is the
    /// deployment's explicit statement), then the first agent that declares
    /// one. The in-process backend carries a single key, so a single provider
    /// decides — matching the one-key model in
    /// [`in_process_llm_backend_with_key`].
    pub(super) fn configured_llm_provider(agents: &HashMap<String, AgentConfig>) -> Option<String> {
        env_llm_provider().or_else(|| {
            agents
                .values()
                .map(|agent| agent.llm.provider.trim().to_string())
                .find(|provider| !provider.is_empty())
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
    async fn resolve_in_process_llm_key(
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
    /// store trait objects.
    ///
    /// Extracted so both the Redis path ([`build_agent_node_handler`]) and the
    /// ephemeral desktop path ([`build_agent_node_handler_ephemeral`]) share
    /// identical post-store construction logic; store differences are the only
    /// divergence between the two callers.
    ///
    /// Returns `None` when the extension runtime fails to initialise (the only
    /// failure mode at this layer — store errors are handled by callers).
    ///
    /// `audit_sink` (EPIC-B B-3) is forwarded verbatim to the constructed
    /// [`RuntimeAgentNodeHandler`] — `None` keeps `dw.agent` execution on the
    /// plain [`AgentRuntime::step`] path.
    ///
    /// `project_id` is the deployed unit's `bundle_id`, forwarded verbatim so
    /// billing can attribute this runtime's spend to a project. `None` on the
    /// legacy tenant-only path, where the dimension is omitted entirely.
    #[allow(clippy::too_many_arguments)]
    async fn build_runtime_handler_with_stores(
        merged_agents: HashMap<String, AgentConfig>,
        tenant: String,
        secrets: crate::secrets::DynSecretsManager,
        packs: Vec<Arc<crate::pack::PackRuntime>>,
        state_store: Arc<dyn greentic_aw_runtime::state::AgentStateStore>,
        token_meter: Arc<dyn greentic_aw_runtime::cost::TokenMeter>,
        ledger: Arc<dyn greentic_aw_runtime::tools::ToolLedger>,
        audit_sink: Option<AuditSink>,
        project_id: Option<String>,
    ) -> Option<Arc<dyn AgentNodeHandler>> {
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
        let secrets_backend: Arc<dyn greentic_ext_runtime::SecretsBackend> = Arc::new(
            StoreToolSecretsBackend::new(secrets.clone(), tenant.clone()),
        );
        let ext_runtime = build_ext_runtime(secrets_backend)?;

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

        let base = AgentRuntime::new(
            config_provider,
            state_store,
            ext_runtime,
            llm,
            telemetry,
            token_meter,
            ledger,
            mcp_source_from_env(),
        )
        .with_component_source(component_source_from_packs(&packs, &tenant));

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
        Some(Arc::new(RuntimeAgentNodeHandler::new(
            runtime, audit_sink, project_id,
        )))
    }

    /// Build the production `DwAgent` handler if the environment is configured.
    ///
    /// Returns `None` (so `DwAgent` flow dispatch errors clearly) under any of
    /// these graceful-degradation conditions:
    /// - `merged_agents` is empty (no agents from packs or operator config);
    /// - `GREENTIC_AW_REDIS_URL` is unset/empty;
    /// - the AW Redis connection fails;
    /// - the extension runtime fails to initialise.
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
    pub async fn build_agent_node_handler(
        merged_agents: HashMap<String, AgentConfig>,
        tenant: String,
        secrets: crate::secrets::DynSecretsManager,
        packs: Vec<Arc<crate::pack::PackRuntime>>,
        audit_sink: Option<AuditSink>,
        project_id: Option<String>,
    ) -> Option<Arc<dyn AgentNodeHandler>> {
        use greentic_aw_runtime::RedisAgentStateStore;
        use greentic_aw_runtime::cost::RedisTokenMeter;
        use greentic_aw_runtime::tools::RedisToolLedger;

        if merged_agents.is_empty() {
            return None;
        }

        let redis_url = match std::env::var("GREENTIC_AW_REDIS_URL") {
            Ok(url) if !url.is_empty() => url,
            _ => {
                tracing::warn!(
                    "GREENTIC_AW_REDIS_URL unset; DwAgent nodes disabled — every dw.agent \
                     turn will fail with `flow_execution_failed`. Set GREENTIC_AW_REDIS_URL, \
                     or run a runner built with --features desktop-agent-ephemeral for \
                     in-memory state (local single-process use only)"
                );
                return None;
            }
        };

        let state_store = match RedisAgentStateStore::connect(&redis_url).await {
            Ok(store) => Arc::new(store),
            Err(error) => {
                tracing::warn!(error = %error, "AW Redis connect failed; DwAgent nodes disabled");
                return None;
            }
        };

        let manager = state_store.manager();
        let token_meter = Arc::new(RedisTokenMeter::new(manager.clone()));
        let ledger = Arc::new(RedisToolLedger::new(manager));

        build_runtime_handler_with_stores(
            merged_agents,
            tenant,
            secrets,
            packs,
            state_store,
            token_meter,
            ledger,
            audit_sink,
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
    pub async fn build_agent_node_handler_ephemeral(
        merged_agents: HashMap<String, AgentConfig>,
        tenant: String,
        secrets: crate::secrets::DynSecretsManager,
        packs: Vec<Arc<crate::pack::PackRuntime>>,
        audit_sink: Option<AuditSink>,
        project_id: Option<String>,
    ) -> Option<Arc<dyn AgentNodeHandler>> {
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
            packs,
            state_store,
            token_meter,
            ledger,
            audit_sink,
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
    /// handler: empty agent map, missing/unreachable `GREENTIC_AW_REDIS_URL`, or
    /// extension-runtime init failure.
    pub async fn build_agent_runtime(
        merged_agents: HashMap<String, AgentConfig>,
    ) -> Option<Arc<AgentRuntime>> {
        use greentic_aw_runtime::LayeredConfigProvider;
        use greentic_aw_runtime::ManifestToolOverlayProvider;
        use greentic_aw_runtime::config_provider::CachingConfigProvider;
        use greentic_aw_runtime::cost::RedisTokenMeter;
        use greentic_aw_runtime::tools::RedisToolLedger;
        use greentic_aw_runtime::{OtelTelemetry, RedisAgentStateStore};

        if merged_agents.is_empty() {
            return None; // nothing to serve
        }

        let redis_url = match std::env::var("GREENTIC_AW_REDIS_URL") {
            Ok(url) if !url.is_empty() => url,
            _ => {
                tracing::warn!(
                    "GREENTIC_AW_REDIS_URL unset; DwAgent nodes disabled — every dw.agent \
                     turn will fail with `flow_execution_failed`. Set GREENTIC_AW_REDIS_URL, \
                     or run a runner built with --features desktop-agent-ephemeral for \
                     in-memory state (local single-process use only)"
                );
                return None;
            }
        };

        let state_store = match RedisAgentStateStore::connect(&redis_url).await {
            Ok(store) => Arc::new(store),
            Err(error) => {
                tracing::warn!(error = %error, "AW Redis connect failed; DwAgent nodes disabled");
                return None;
            }
        };

        // The connection manager is cheap to clone (multiplexed, ref-counted);
        // share it with the token meter and idempotency ledger.
        let manager = state_store.manager();
        let token_meter = Arc::new(RedisTokenMeter::new(manager.clone()));
        let ledger = Arc::new(RedisToolLedger::new(manager));

        // Process-level serve path has no per-tenant secrets context, so tool
        // secrets resolve from the env only.
        let ext_runtime = build_ext_runtime(Arc::new(EnvSecretsBackend))?;

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
            mcp_source_from_env(),
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
            {
                #[cfg(greentic_guardrail_ext)]
                {
                    Arc::new(
                        greentic_aw_runtime::guardrail::ExtRuntimeGuardrailEvaluator {
                            ext_runtime: ext_runtime.clone(),
                        },
                    )
                }
                // This lane's greentic-ext-runtime has no guardrail interface
                // (extension-design is at 0.2.0). Fail closed rather than
                // silently accepting: an agent that configured a guardrail
                // stops loudly instead of running unprotected.
                #[cfg(not(greentic_guardrail_ext))]
                {
                    Arc::new(greentic_aw_runtime::guardrail::UnavailableGuardrailEvaluator)
                }
            },
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
        // Optionally attach an operator-configured native long-term memory
        // backend. With the `long-term-chronicle` feature off (default) this is
        // a no-op and `base` is wrapped unchanged.
        #[cfg(feature = "long-term-chronicle")]
        let base = crate::runner::long_term_memory::attach(base).await;
        // Optionally attach an operator-configured Chronicle knowledge (document
        // RAG) backend for auto pre-retrieval. No-op with the `knowledge-chronicle`
        // feature off (default).
        #[cfg(feature = "knowledge-chronicle")]
        let base = crate::runner::knowledge_mount::attach(base).await;
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
                // Activate dispatch idempotency when Redis is reachable for the
                // ledger. Best-effort: a connect failure disables idempotency
                // but never blocks serving.
                let (ledger, ledger_active): (Arc<dyn DispatchLedger>, bool) =
                    match std::env::var("GREENTIC_AW_REDIS_URL") {
                        Ok(url) if !url.is_empty() => {
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
    mod tests {
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

        fn sample_agent_config(agent_id: &str) -> AgentConfig {
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
                },
            );
            let config_provider = Arc::new(config_provider);

            let token_meter = Arc::new(MockTokenMeter::new(0));
            let ledger = Arc::new(NoopToolLedger);
            let ext_runtime = Arc::new(crate::runner::agent_node::test_extension_runtime());

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
            let handler = RuntimeAgentNodeHandler::new(runtime, None, None);

            let output = handler
                .execute("t", "e", "greeter", "sess-1", &json!({"user_text": "ping"}))
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
                    Arc::new(crate::runner::agent_node::test_extension_runtime()),
                    llm,
                    Arc::new(MockTelemetry::new()),
                    Arc::new(MockTokenMeter::new(0)),
                    Arc::new(NoopToolLedger),
                    None,
                )
                .with_billing_meter(meter.clone()),
            );

            RuntimeAgentNodeHandler::new(runtime, None, project_id)
                .execute("t", "e", "greeter", "sess-1", &json!({"user_text": "ping"}))
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
                vec![],
                Arc::new(MockAgentStateStore::new()),
                Arc::new(MockTokenMeter::new(0)),
                Arc::new(NoopToolLedger),
                None,
                None,
            )
            .await
            .expect("handler should build from mock stores");

            let _ = handler
                .execute("acme", "prod", "greeter", "s", &json!({"user_text": "hi"}))
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
            let ext_runtime = Arc::new(crate::runner::agent_node::test_extension_runtime());

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
            let handler = RuntimeAgentNodeHandler::new(runtime, Some(sink), None);

            let output = handler
                .execute(
                    "t1",
                    "e1",
                    "greeter",
                    "sess-1",
                    &json!({"user_text": "remember this"}),
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

            assert!(
                rx.try_recv().is_err(),
                "exactly two audit events enqueued (one tool_call, one tool_result)"
            );
        }

        #[tokio::test]
        async fn execute_without_audit_sink_uses_plain_step_path_unchanged() {
            // Same scripted tool call as the audited test above, but the
            // handler carries no audit sink at all — proves the "off" branch
            // (self.runtime.step, no observer constructed) still dispatches
            // the tool call and returns the same reply, exactly as it did
            // before AgentAuditObserver existed.
            let runtime = runtime_with_scripted_remember_call("t1", "e1");
            let handler = RuntimeAgentNodeHandler::new(runtime, None, None);

            let output = handler
                .execute(
                    "t1",
                    "e1",
                    "greeter",
                    "sess-1",
                    &json!({"user_text": "remember this"}),
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

        /// Requires `greentic_dw_manifest_tools`: without it this lane's
        /// `DigitalWorkerManifest` has no `extension_tools` to parse, so
        /// `manifest_to_tool_refs` yields an empty overlay and there is
        /// nothing to replace. `a_tool_declaring_manifest_does_not_reach_the_agent`
        /// below pins what happens here instead.
        #[cfg(greentic_dw_manifest_tools)]
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
                    tool_name: "web_search".into()
                }]
            );
        }

        /// The agent-side mirror of
        /// `manifest_provider::tests::a_tool_declaring_manifest_is_ignored_on_this_lane`:
        /// a Digital Worker manifest may declare agentic-worker tools, but on
        /// this lane none of them reach the agent's config. The overlay is
        /// fail-soft, so this is silent — pin it so a future port has to
        /// delete this test rather than discover the behaviour.
        #[cfg(not(greentic_dw_manifest_tools))]
        #[tokio::test]
        async fn a_tool_declaring_manifest_does_not_reach_the_agent() {
            use greentic_aw_runtime::ManifestToolOverlayProvider;
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
            let base_tools = sample_agent_config("greeter").tools;
            let provider = ManifestToolOverlayProvider::new(
                HostConfigProvider::new(agents),
                tmp.path().to_path_buf(),
            );

            let tenant = TenantContext::new("acme", "prod");
            let cfg = provider.agent_config(&tenant, "greeter").await.unwrap();
            assert_eq!(
                cfg.tools, base_tools,
                "the manifest's greentic.tavily/web_search must not reach the agent on this lane"
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
                super::selects_multi_provider_backend("", Some("ollama")),
                "an Ollama worker with no key must reach greentic-llm, not the \
                 OpenAI fall-through"
            );
            assert!(super::selects_multi_provider_backend("  ", Some("bedrock")));
        }

        #[cfg(feature = "greentic-llm-backend")]
        #[test]
        fn openai_without_a_key_keeps_the_openai_fall_through() {
            assert!(!super::selects_multi_provider_backend("", Some("openai")));
            assert!(!super::selects_multi_provider_backend("  ", None));
            // A key still selects the multi-provider backend for everyone.
            assert!(super::selects_multi_provider_backend(
                "sk-x",
                Some("openai")
            ));
            assert!(super::selects_multi_provider_backend("sk-x", None));
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
                super::mcp_source_from_env().is_some(),
                "MCP is on by default when admin credentials are configured"
            );

            // (b) Explicit opt-out wins even with full credentials.
            unsafe {
                std::env::set_var("GREENTIC_AW_MCP", "0");
            }
            assert!(
                super::mcp_source_from_env().is_none(),
                "GREENTIC_AW_MCP=0 disables MCP regardless of credentials"
            );

            // (b') Legacy opt-in value still enables (any non-"0" value does).
            unsafe {
                std::env::set_var("GREENTIC_AW_MCP", "1");
            }
            assert!(super::mcp_source_from_env().is_some());

            // (c) Missing credential → None even without an opt-out.
            unsafe {
                std::env::remove_var("GREENTIC_AW_MCP");
                std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
            }
            assert!(
                super::mcp_source_from_env().is_none(),
                "no endpoint → no MCP source"
            );

            unsafe {
                std::env::set_var("GREENTIC_AW_ADMIN_ENDPOINT", "http://localhost:9999");
                std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
            }
            assert!(
                super::mcp_source_from_env().is_none(),
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
                Vec::new(),
                None,
                // project_id: no pack identity to attribute billing to, matching
                // the desktop ephemeral call site in greentic-runner-desktop.
                None,
            )
            .await;
            assert!(
                handler.is_some(),
                "ephemeral builder must not require Redis"
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
    HostConfigProvider, RuntimeAgentNodeHandler, agent_configs_from_manifest,
    build_agent_node_handler, build_agent_runtime, load_process_agent_configs, merge_agent_sources,
    merge_sidecar_into, serve_agentic,
};

#[cfg(feature = "desktop-agent-ephemeral")]
pub use aw::build_agent_node_handler_ephemeral;

#[cfg(feature = "agentic-worker")]
pub(crate) use aw::{EnvSecretsBackend, build_ext_runtime, build_llm_backend};

/// Test-only stand-in for `ExtensionRuntime::for_test()`, which this lane's
/// greentic-ext-runtime does not expose. Builds the equivalent: a runtime
/// rooted at a discovery path holding no extensions, plus the crate's own test
/// host overrides. Every lookup therefore returns an empty tool catalog, which
/// is what the agent/graph node tests want — they drive canned LLM replies and
/// never dispatch to a real extension.
#[cfg(all(test, feature = "agentic-worker"))]
pub(crate) fn test_extension_runtime() -> greentic_ext_runtime::ExtensionRuntime {
    greentic_ext_runtime::ExtensionRuntime::new(greentic_ext_runtime::RuntimeConfig::from_paths(
        greentic_ext_runtime::DiscoveryPaths::new(std::path::PathBuf::from(
            "/nonexistent/greentic-runner-host-test-extensions",
        )),
    ))
    .expect("wasmtime engine init for the runner-host test extension runtime")
    .with_host_overrides(greentic_ext_runtime::HostOverrides::defaults_for_tests())
}
