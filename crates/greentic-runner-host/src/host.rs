use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;

use crate::activity::{Activity, WelcomeFlowHint};
use crate::boot;
use crate::component_api::node::{ExecCtx as ComponentExecCtx, TenantCtx as ComponentTenantCtx};
use crate::config::{Fast2FlowRoutingConfig, HostConfig};
use crate::engine::host::{SessionHost, StateHost};
use crate::engine::runtime::{FlowResumeStore, IngressEnvelope};
#[cfg(feature = "greentic-x-provider")]
use crate::greentic_x_provider::RunnerPackFast2FlowRoutingProvider;
use crate::http::health::HealthState;
use crate::pack::{IdentifyOutcome, PackRuntime};
use crate::runner::adapt_timer;
use crate::runner::engine::FlowEngine;
use crate::runtime::{ActivePacks, TenantRuntime};
use crate::secrets::{DynSecretsManager, default_manager};
use crate::storage::{
    DynSessionStore, DynStateStore, new_session_store, new_state_store, session_host_from,
    state_host_from,
};
use crate::wasi::RunnerWasiPolicy;
use greentic_deploy_spec::ids::{BundleId, DeploymentId, RevisionId};
#[cfg(feature = "greentic-x-provider")]
use greentic_x_runtime::{
    Fast2FlowDirective, Fast2FlowMessageEnvelope, Fast2FlowRouteRequest, Fast2FlowRoutingProvider,
};

#[derive(Clone, Debug)]
pub struct TelemetryCfg {
    pub config: greentic_telemetry::TelemetryConfig,
    pub export: greentic_telemetry::export::ExportConfig,
}

/// Builder for composing multi-tenant host instances.
pub struct HostBuilder {
    configs: HashMap<String, HostConfig>,
    telemetry: Option<TelemetryCfg>,
    wasi_policy: RunnerWasiPolicy,
    secrets: Option<DynSecretsManager>,
}

impl HostBuilder {
    pub fn new() -> Self {
        Self {
            configs: HashMap::new(),
            telemetry: None,
            wasi_policy: RunnerWasiPolicy::default(),
            secrets: None,
        }
    }

    pub fn with_config(mut self, config: HostConfig) -> Self {
        self.configs.insert(config.tenant.clone(), config);
        self
    }

    pub fn with_telemetry(mut self, telemetry: TelemetryCfg) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    pub fn with_wasi_policy(mut self, policy: RunnerWasiPolicy) -> Self {
        self.wasi_policy = policy;
        self
    }

    pub fn with_secrets_manager(mut self, manager: DynSecretsManager) -> Self {
        self.secrets = Some(manager);
        self
    }

    pub fn build(self) -> Result<RunnerHost> {
        if self.configs.is_empty() {
            bail!("at least one tenant configuration is required");
        }
        let wasi_policy = Arc::new(self.wasi_policy);
        let configs = self
            .configs
            .into_iter()
            .map(|(tenant, cfg)| (tenant, Arc::new(cfg)))
            .collect();
        let session_store = new_session_store();
        let session_host = session_host_from(Arc::clone(&session_store));
        let state_store = new_state_store();
        let state_host = state_host_from(Arc::clone(&state_store));
        let secrets = match self.secrets {
            Some(manager) => manager,
            None => default_manager().context("failed to initialise default secrets backend")?,
        };
        Ok(RunnerHost {
            configs,
            active: Arc::new(ActivePacks::new()),
            health: Arc::new(HealthState::new()),
            session_store,
            state_store,
            session_host,
            state_host,
            wasi_policy,
            secrets_manager: secrets,
            telemetry: self.telemetry,
        })
    }
}

impl Default for HostBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Runtime host that manages tenant-bound packs and flow execution.
pub struct RunnerHost {
    configs: HashMap<String, Arc<HostConfig>>,
    active: Arc<ActivePacks>,
    health: Arc<HealthState>,
    session_store: DynSessionStore,
    state_store: DynStateStore,
    session_host: Arc<dyn SessionHost>,
    state_host: Arc<dyn StateHost>,
    wasi_policy: Arc<RunnerWasiPolicy>,
    secrets_manager: DynSecretsManager,
    telemetry: Option<TelemetryCfg>,
}

/// Handle exposing tenant internals for embedding hosts (e.g. CLI server).
#[derive(Clone)]
pub struct TenantHandle {
    runtime: Arc<TenantRuntime>,
}

impl RunnerHost {
    pub async fn start(&self) -> Result<()> {
        boot::init(&self.health, self.telemetry.as_ref())?;
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        self.active.replace(HashMap::new());
        Ok(())
    }

    pub async fn load_pack(&self, tenant: &str, pack_path: &Path) -> Result<()> {
        let archive_source = if is_pack_archive(pack_path) {
            Some(pack_path)
        } else {
            None
        };
        let runtime = self
            .prepare_runtime(tenant, pack_path, archive_source)
            .await
            .with_context(|| format!("failed to load tenant {tenant}"))?;
        self.active.insert_pack(tenant, runtime);
        tracing::info!(tenant, pack = %pack_path.display(), "pack loaded");
        Ok(())
    }

    pub async fn handle_activity(&self, tenant: &str, activity: Activity) -> Result<Vec<Activity>> {
        let runtime = self
            .active
            .load_pack(tenant)
            .with_context(|| format!("tenant {tenant} not loaded"))?;
        self.dispatch_activity(&runtime, tenant, activity).await
    }

    /// Execute an activity against a specific deployment/bundle/revision runtime.
    ///
    /// Unlike [`handle_activity`](Self::handle_activity), which resolves the
    /// tenant-only (legacy) runtime, this targets a fully-qualified revision
    /// entry inserted by [`ActivePacks::insert_revision`]. A tenant can host
    /// several concurrent revisions under a traffic split, so the legacy
    /// tenant-only lookup cannot disambiguate them — the ingress revision
    /// dispatcher selects the revision and calls this.
    ///
    /// # Session isolation contract
    ///
    /// This method runs the selected revision's runtime against **whatever
    /// session/state stores that runtime was built with** (at
    /// [`TenantRuntime::load_revision`] time). It does *not* add a revision
    /// dimension to the session key: the session/resume/state backend keys on
    /// `(env, tenant, user)` plus pack/flow, **not** on the revision. If two
    /// live revisions of the same pack for one tenant share a single session
    /// backend, a `wait`/resume snapshot created by revision A can be fetched —
    /// or clobbered — by revision B during a traffic split, retry, or
    /// rebalance, resuming a snapshot against a different flow graph.
    ///
    /// Callers that load more than one revision per tenant onto one host
    /// (i.e. every traffic-split producer) **MUST give each revision an
    /// isolated session and state store** (a per-revision store instance, or a
    /// revision-namespaced backend) when calling `load_revision`. The shared
    /// `RunnerHost` stores (`session_store()`/`state_store()`) are only safe to
    /// reuse across revisions when at most one revision is ever live per
    /// tenant. The greentic-start activation path enforces this.
    pub async fn handle_activity_for_revision(
        &self,
        tenant: &str,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
        activity: Activity,
    ) -> Result<Vec<Activity>> {
        let runtime = self
            .active
            .load_revision(tenant, deployment_id, bundle_id, revision_id)
            .with_context(|| {
                format!(
                    "revision runtime not loaded for tenant {tenant} \
                     (deployment {deployment_id}, revision {revision_id})"
                )
            })?;
        self.dispatch_activity(&runtime, tenant, activity).await
    }

    /// Resolve the per-revision tenant runtime, attaching a uniform "not
    /// loaded" context to the error. The three per-revision identify
    /// fan-out APIs all need this exact lookup; sharing it keeps the
    /// error chain identical across them.
    fn load_revision_runtime(
        &self,
        tenant: &str,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
    ) -> Result<Arc<crate::runtime::TenantRuntime>> {
        self.active
            .load_revision(tenant, deployment_id, bundle_id, revision_id)
            .with_context(|| {
                format!(
                    "revision runtime not loaded for tenant {tenant} \
                     (deployment {deployment_id}, revision {revision_id})"
                )
            })
    }

    /// Per-revision per-`provider_type` `identify-instance` probe (M1 IID.4).
    ///
    /// Given the candidate `provider_types` an env declares messaging
    /// endpoints for, ask each pack loaded under this revision (main +
    /// overlays) which `provider_id` the inbound `payload` claims to address.
    /// The greentic-start resolver pairs the returned `provider_id` with the
    /// `provider_type` and looks the `MessagingEndpointId` up in the env's
    /// admit table; that's how a header-less webhook gets auto-routed to the
    /// right endpoint.
    ///
    /// `payload` is forwarded opaque to every probed component. The M1
    /// IID.4d wrapper convention from `greentic-start` is
    /// `{headers: [{name,value}], body: <parsed-or-null>}`. See the WIT
    /// docstring on `greentic:provider-instance-identity@0.1.0/identify-instance`
    /// for the full contract.
    ///
    /// This is the unscoped legacy API; new callers should use
    /// [`identify_messaging_endpoints_for_revision_scoped`] for per-provider
    /// header allowlist scoping (Phase D). Merge lattice:
    /// `Identified > NoMatch > Unsupported` — first pack to `Identified`
    /// wins and that type drops out of remaining probing.
    ///
    /// The per-pack loop is inlined (rather than factored into a shared
    /// `AsyncFnMut`-based helper) deliberately: routing the loop through an
    /// `AsyncFnMut` closure destabilises HRTB `Send` inference for
    /// downstream consumers spawning the returned future (greentic-start's
    /// hyper `service_fn`). The `Send`-bound test
    /// [`identify_futures_are_send`] guards against silent regression.
    ///
    /// [`identify_messaging_endpoints_for_revision_scoped`]:
    ///     RunnerHost::identify_messaging_endpoints_for_revision_scoped
    pub async fn identify_messaging_endpoints_for_revision(
        &self,
        tenant: &str,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
        provider_types: &[&str],
        payload: &[u8],
    ) -> Result<HashMap<String, IdentifyOutcome>> {
        if provider_types.is_empty() {
            return Ok(HashMap::new());
        }
        let runtime = self.load_revision_runtime(tenant, deployment_id, bundle_id, revision_id)?;
        // Seed every type at Unsupported — the floor of the merge lattice
        // (see `IdentifyOutcome::merge_in`).
        let mut merged: HashMap<String, IdentifyOutcome> = provider_types
            .iter()
            .map(|ty| ((*ty).to_string(), IdentifyOutcome::Unsupported))
            .collect();
        for pack in runtime.all_packs() {
            // Skip types already at the lattice top — no probe could improve them.
            let remaining: Vec<&str> = provider_types
                .iter()
                .copied()
                .filter(|ty| !matches!(merged.get(*ty), Some(IdentifyOutcome::Identified(_))))
                .collect();
            if remaining.is_empty() {
                break;
            }
            let probe = pack
                .identify_endpoints_by_provider_type(&remaining, payload)
                .await?;
            for (ty, outcome) in probe {
                if let Some(existing) = merged.get_mut(&ty) {
                    existing.merge_in(outcome);
                }
            }
        }
        Ok(merged)
    }

    /// Per-provider scoped variant of
    /// [`identify_messaging_endpoints_for_revision`].
    ///
    /// The wrapper is built **per-provider** from the component's cached
    /// `describe-identify-instance` hint (see
    /// [`PackRuntime::resolve_identify_hint`]): hinted components receive
    /// ONLY the headers their hint declares; unhinted components receive
    /// every header the caller passed in (back-compat).
    ///
    /// Loop inlined for the same reason as
    /// [`identify_messaging_endpoints_for_revision`].
    ///
    /// [`identify_messaging_endpoints_for_revision`]:
    ///     RunnerHost::identify_messaging_endpoints_for_revision
    #[allow(clippy::too_many_arguments)]
    pub async fn identify_messaging_endpoints_for_revision_scoped(
        &self,
        tenant: &str,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
        provider_types: &[&str],
        headers: &[(String, String)],
        body: &Value,
    ) -> Result<HashMap<String, IdentifyOutcome>> {
        if provider_types.is_empty() {
            return Ok(HashMap::new());
        }
        let runtime = self.load_revision_runtime(tenant, deployment_id, bundle_id, revision_id)?;
        let mut merged: HashMap<String, IdentifyOutcome> = provider_types
            .iter()
            .map(|ty| ((*ty).to_string(), IdentifyOutcome::Unsupported))
            .collect();
        for pack in runtime.all_packs() {
            let remaining: Vec<&str> = provider_types
                .iter()
                .copied()
                .filter(|ty| !matches!(merged.get(*ty), Some(IdentifyOutcome::Identified(_))))
                .collect();
            if remaining.is_empty() {
                break;
            }
            let probe = pack
                .identify_endpoints_by_provider_type_scoped(&remaining, headers, body)
                .await?;
            for (ty, outcome) in probe {
                if let Some(existing) = merged.get_mut(&ty) {
                    existing.merge_in(outcome);
                }
            }
        }
        Ok(merged)
    }

    /// Per-revision describe-identify-instance hint discovery.
    ///
    /// Fans the cached describe probe out across main pack + overlays;
    /// first non-`None` hint per `provider_type` wins. Lets callers inspect
    /// the per-provider header allowlist without running the expensive
    /// identify-instance probe. `None` value means no pack in this revision
    /// exposes a usable hint for that `provider_type`.
    ///
    /// Loop inlined for the same reason as
    /// [`identify_messaging_endpoints_for_revision`].
    pub async fn describe_identify_instances_for_revision(
        &self,
        tenant: &str,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
        provider_types: &[&str],
    ) -> Result<HashMap<String, Option<crate::identify_hint::IdentifyInstanceHint>>> {
        if provider_types.is_empty() {
            return Ok(HashMap::new());
        }
        let runtime = self.load_revision_runtime(tenant, deployment_id, bundle_id, revision_id)?;
        let mut merged: HashMap<String, Option<crate::identify_hint::IdentifyInstanceHint>> =
            provider_types
                .iter()
                .map(|ty| ((*ty).to_string(), None))
                .collect();
        for pack in runtime.all_packs() {
            // First non-`None` hint per type wins — anything already populated
            // is at the lattice top. Mirror the `matches!` shape the sibling
            // identify fns use so the predicate is consistent across files.
            let remaining: Vec<&str> = provider_types
                .iter()
                .copied()
                .filter(|ty| !matches!(merged.get(*ty), Some(Some(_))))
                .collect();
            if remaining.is_empty() {
                break;
            }
            let probe = pack
                .describe_identify_hints_by_provider_type(&remaining)
                .await?;
            for (ty, hint) in probe {
                if let Some(slot) = merged.get_mut(&ty)
                    && slot.is_none()
                {
                    *slot = hint;
                }
            }
        }
        Ok(merged)
    }

    /// Per-revision provider invocation (Phase D).
    ///
    /// Locates the unique pack in `(deployment_id, bundle_id, revision_id)`
    /// whose `greentic.provider-extension.v1` binds the requested
    /// `provider_type`, verifies `op` is in that declaration's allowlist,
    /// then calls `op` on it with `input_json`.
    ///
    /// Used by greentic-start's Phase D `ProviderRoute` admission arm to
    /// run provider webhooks (e.g. `ingest_http`) without round-tripping
    /// through the flow engine. The provider component returns the parsed
    /// HTTP-out envelope verbatim; greentic-start dispatches the events
    /// it carries back through the flow runtime separately.
    ///
    /// `correlation_id` is threaded into the `ComponentExecCtx` as both
    /// `correlation_id` and `idempotency_key` (mirroring the operator-API
    /// pattern in `build_exec_ctx`). `trace_id` rides through as-is.
    ///
    /// Fails closed when:
    /// - the revision isn't loaded (error chain names deployment + revision)
    /// - no pack in the revision binds `provider_type`
    /// - **multiple packs in the revision bind `provider_type`** — the URL
    ///   → provider routing the caller did at the route table is unable to
    ///   disambiguate at invoke time. Mirrors the within-pack ambiguity
    ///   check in [`ProviderRegistry::resolve`], lifted to the revision
    ///   level so an overlay accidentally redeclaring a main-pack provider
    ///   can't silently shadow the wrong runtime. A future revision of
    ///   this API can accept an explicit `provider_id` to disambiguate
    ///   when D.3 wires identification + invocation together.
    /// - `op` is not in the resolved provider's declared `ops` allowlist
    ///   (defense-in-depth: a caller bug or misconfigured `ProviderRoute`
    ///   cannot smuggle an undeclared op past the schema-core boundary).
    ///
    /// Loop inlined for the same reason as
    /// [`identify_messaging_endpoints_for_revision`].
    ///
    /// [`ProviderRegistry::resolve`]: crate::provider::ProviderRegistry::resolve
    #[allow(clippy::too_many_arguments)]
    pub async fn invoke_provider_for_revision(
        &self,
        tenant: &str,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
        provider_type: &str,
        op: &str,
        input_json: Vec<u8>,
        correlation_id: Option<String>,
        trace_id: Option<String>,
    ) -> Result<Value> {
        let runtime = self.load_revision_runtime(tenant, deployment_id, bundle_id, revision_id)?;
        // Walk ALL packs to detect cross-pack ambiguity. First-match-wins
        // would let main pack silently shadow an overlay that binds the
        // same provider_type — the identify-side merge lattice can return
        // outcomes from any pack, so the invoke side must refuse to guess.
        let mut matched = None;
        for pack in runtime.all_packs() {
            let Some(registry) = pack.provider_registry_optional()? else {
                continue;
            };
            let Some((binding, declared_ops)) =
                registry.try_resolve_with_ops(None, Some(provider_type))?
            else {
                continue;
            };
            if matched.is_some() {
                bail!(
                    "ambiguous provider_type `{provider_type}` in revision \
                     (deployment {deployment_id}, revision {revision_id}): \
                     multiple packs bind the same type; pack manifests must \
                     declare each provider_type at most once across main + overlays"
                );
            }
            matched = Some((Arc::clone(pack), binding, declared_ops));
        }
        let Some((pack, binding, declared_ops)) = matched else {
            bail!(
                "no pack in revision binds provider_type `{provider_type}` \
                 (deployment {deployment_id}, revision {revision_id})"
            );
        };
        if !declared_ops.iter().any(|d| d == op) {
            bail!(
                "op `{op}` is not declared for provider_type `{provider_type}` \
                 in revision (deployment {deployment_id}, revision {revision_id}); \
                 declared ops: {declared_ops:?}"
            );
        }
        let exec_ctx = ComponentExecCtx {
            tenant: ComponentTenantCtx {
                tenant: tenant.to_string(),
                team: None,
                user: None,
                trace_id,
                i18n_id: None,
                correlation_id: correlation_id.clone(),
                deadline_unix_ms: None,
                attempt: 1,
                idempotency_key: correlation_id,
            },
            i18n_id: None,
            flow_id: format!("provider-webhook/{provider_type}"),
            node_id: None,
        };
        pack.invoke_provider(&binding, exec_ctx, op, input_json)
            .await
    }

    /// Shared activity-execution body: resolve the flow, build the canonical
    /// ingress envelope, run the state machine, and normalize replies. Both the
    /// legacy and revision entry points funnel through here so flow resolution
    /// and reply shaping never drift between them.
    async fn dispatch_activity(
        &self,
        runtime: &TenantRuntime,
        tenant: &str,
        activity: Activity,
    ) -> Result<Vec<Activity>> {
        let activity = apply_fast2flow_routing(runtime, tenant, activity)?;

        // Fast2Flow Respond/Deny returns a pre-built response activity
        // (kind = Custom { action: "response" }, no flow_id/pack_id).
        // Short-circuit: return it directly — do NOT resolve a flow or
        // enter the state machine, otherwise a Deny still executes the
        // tenant entry flow with the denial payload.
        if activity.action() == Some("response") && activity.flow_id().is_none() {
            return Ok(vec![activity]);
        }

        let (pack_id, flow_id) = resolve_flow_id(runtime, &activity)?;
        let action = activity.action().map(|value| value.to_string());
        let session = activity.session_id().map(|value| value.to_string());
        let provider = activity.provider_id().map(|value| value.to_string());
        let messaging_endpoint_id = activity
            .messaging_endpoint_id()
            .map(|value| value.to_string());
        let channel = activity.channel().map(|value| value.to_string());
        let conversation = activity.conversation().map(|value| value.to_string());
        let user = activity.user().map(|value| value.to_string());
        let welcome_flow_hint = activity.welcome_flow_hint().cloned();
        let resolved_flow_type =
            activity
                .flow_type()
                .map(|value| value.to_string())
                .or_else(|| {
                    runtime
                        .engine()
                        .flow_by_key(&pack_id, &flow_id)
                        .map(|desc| desc.flow_type.clone())
                });
        let mut payload = activity.into_payload();

        // A card button says where to go next in `nextCardId` and friends.
        // Usually that names another CARD, which the adaptive-card component
        // renders. When it names a FLOW NODE the pack's graph continues there
        // instead — and the target has to be lifted out of the payload, because
        // the component prefers an inbound `nextCardId` over its node's own
        // asset and would fail to resolve a node id as a card
        // (`AC_ASSET_NOT_FOUND`, surfacing as a generic service error).
        //
        // greentic-start does this for the messaging path it owns; this is the
        // same rule for the in-process path, which previously had none — card
        // navigation worked and flow-node navigation did not.
        let entry_node = {
            let node_ids = runtime.engine().flow_node_ids(&pack_id, &flow_id).await;
            let target = crate::runner::card_nav::entry_node_from_card_nav(
                payload.get("metadata").unwrap_or(&serde_json::Value::Null),
                &node_ids,
            );
            if target.is_some()
                && let Some(metadata) = payload.get_mut("metadata")
            {
                crate::runner::card_nav::strip_card_nav_keys(metadata);
            }
            target
        };

        let mut envelope = IngressEnvelope {
            entry_node,
            tenant: tenant.to_string(),
            env: std::env::var("GREENTIC_ENV").ok(),
            pack_id: Some(pack_id.clone()),
            flow_id: flow_id.clone(),
            flow_type: resolved_flow_type,
            action,
            session_hint: session,
            provider,
            messaging_endpoint_id,
            channel,
            conversation,
            user,
            activity_id: None,
            timestamp: None,
            payload,
            metadata: None,
            reply_scope: None,
        }
        .canonicalize();

        let hint_flow_type = welcome_flow_hint.as_ref().and_then(|hint| {
            runtime
                .engine()
                .flow_by_key(&hint.pack_id, &hint.flow_id)
                .map(|desc| desc.flow_type.clone())
        });
        apply_welcome_flow_override(
            runtime.session_store(),
            &mut envelope,
            welcome_flow_hint.as_ref(),
            hint_flow_type,
        )?;

        let result = runtime.state_machine().handle(envelope).await?;
        Ok(normalize_replies(result, tenant))
    }

    pub async fn tenant(&self, tenant: &str) -> Option<TenantHandle> {
        self.active
            .load_pack(tenant)
            .map(|runtime| TenantHandle { runtime })
    }

    pub fn active_packs(&self) -> Arc<ActivePacks> {
        Arc::clone(&self.active)
    }

    pub fn health_state(&self) -> Arc<HealthState> {
        Arc::clone(&self.health)
    }

    pub fn wasi_policy(&self) -> Arc<RunnerWasiPolicy> {
        Arc::clone(&self.wasi_policy)
    }

    pub fn session_store(&self) -> DynSessionStore {
        Arc::clone(&self.session_store)
    }

    pub fn state_store(&self) -> DynStateStore {
        Arc::clone(&self.state_store)
    }

    pub fn session_host(&self) -> Arc<dyn SessionHost> {
        Arc::clone(&self.session_host)
    }

    pub fn state_host(&self) -> Arc<dyn StateHost> {
        Arc::clone(&self.state_host)
    }

    pub fn secrets_manager(&self) -> DynSecretsManager {
        Arc::clone(&self.secrets_manager)
    }

    pub fn tenant_configs(&self) -> HashMap<String, Arc<HostConfig>> {
        self.configs.clone()
    }

    /// Build a minimal `RunnerHost` suitable for unit tests. The host has no
    /// packs loaded into `active`, so `handle_activity` for any tenant will
    /// return a "not loaded" error — which is exactly the error path the
    /// `POST /agent/chat` handler tests exercise.
    #[cfg(test)]
    pub(crate) fn for_test() -> Arc<Self> {
        use crate::config::{
            FlowRetryConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
            WebhookPolicy,
        };
        let config = Arc::new(HostConfig {
            tenant: "test".into(),
            bindings_path: std::path::PathBuf::from("<test>"),
            flow_type_bindings: std::collections::HashMap::new(),
            rate_limits: RateLimits::default(),
            retry: FlowRetryConfig::default(),
            http_enabled: false,
            secrets_policy: SecretsPolicy::allow_all(),
            state_store_policy: StateStorePolicy::default(),
            webhook_policy: WebhookPolicy::default(),
            timers: Vec::new(),
            oauth: None,
            mocks: None,
            pack_bindings: Vec::new(),
            env_passthrough: Vec::new(),
            trace: crate::trace::TraceConfig::from_env(),
            validation: crate::validate::ValidationConfig::from_env(),
            operator_policy: OperatorPolicy::allow_all(),
            fast2flow: Default::default(),
            #[cfg(feature = "agentic-worker")]
            agents: std::collections::HashMap::new(),
            #[cfg(feature = "agentic-worker")]
            graphs: std::collections::HashMap::new(),
        });
        let session_store = new_session_store();
        let session_host = session_host_from(Arc::clone(&session_store));
        let state_store = new_state_store();
        let state_host = state_host_from(Arc::clone(&state_store));
        let secrets_manager = default_manager().expect("test secrets manager");
        Arc::new(Self {
            configs: std::collections::HashMap::from([("test".to_string(), config)]),
            active: Arc::new(ActivePacks::new()),
            health: Arc::new(HealthState::new()),
            session_store,
            state_store,
            session_host,
            state_host,
            wasi_policy: Arc::new(RunnerWasiPolicy::default()),
            secrets_manager,
            telemetry: None,
        })
    }

    async fn prepare_runtime(
        &self,
        tenant: &str,
        pack_path: &Path,
        archive_source: Option<&Path>,
    ) -> Result<Arc<TenantRuntime>> {
        let config = self
            .configs
            .get(tenant)
            .cloned()
            .with_context(|| format!("tenant {tenant} not registered"))?;
        if config.tenant != tenant {
            bail!(
                "tenant mismatch: config declares '{}' but '{tenant}' was requested",
                config.tenant
            );
        }
        let runtime = TenantRuntime::load(
            pack_path,
            Arc::clone(&config),
            None,
            archive_source,
            None,
            self.wasi_policy(),
            self.session_host(),
            self.session_store(),
            self.state_store(),
            self.state_host(),
            self.secrets_manager(),
        )
        .await?;
        let timers = adapt_timer::spawn_timers(Arc::clone(&runtime))?;
        runtime.register_timers(timers);
        Ok(runtime)
    }
}

impl TenantHandle {
    pub fn config(&self) -> Arc<HostConfig> {
        Arc::clone(self.runtime.config())
    }

    pub fn pack(&self) -> Arc<PackRuntime> {
        self.runtime.pack()
    }

    pub fn engine(&self) -> Arc<FlowEngine> {
        Arc::clone(self.runtime.engine())
    }

    pub fn overlays(&self) -> Vec<Arc<PackRuntime>> {
        self.runtime.overlays()
    }

    pub fn overlay_digests(&self) -> Vec<Option<String>> {
        self.runtime.overlay_digests()
    }
}

/// M1.5 welcome-flow override: swap the envelope's `(pack_id, flow_id,
/// flow_type)` to the producer-supplied [`WelcomeFlowHint`] when ALL of:
/// the hint is present, the envelope carries a `messaging_endpoint_id`,
/// the welcome-seen marker is absent for this `(tenant, env, eid, user)`
/// (set atomically on success), and `FlowResumeStore::fetch` finds no
/// active wait snapshot. Any missing precondition is a silent no-op.
///
/// **The welcome-seen marker is the durable first-contact gate.** Without
/// it, post-completion / no-wait / TTL-expired turns would re-fire welcome
/// because the wait-snapshot check is only positive while a flow is paused.
/// The marker lives in the shared session store under a synthetic scope
/// (`welcome-seen::ep=<eid>`) distinct from the flow's own conversation, so
/// flow-completion `clear_wait` does NOT drop it.
///
/// The wait-snapshot check is kept as a belt-and-braces safety net: the
/// marker check + write is two operations against the store with a small
/// race window (Phase D will add an atomic `register_wait_if_absent`).
/// The safety net guarantees an in-flight flow is never overridden even if
/// two concurrent first-ever turns both pass the marker probe.
///
/// `session_store` + `hint_flow_type` are passed as primitives so the logic
/// is unit-testable without a `TenantRuntime`; the caller does the engine
/// lookup that produces `hint_flow_type`.
fn apply_welcome_flow_override(
    session_store: &DynSessionStore,
    envelope: &mut IngressEnvelope,
    hint: Option<&WelcomeFlowHint>,
    hint_flow_type: Option<String>,
) -> Result<()> {
    let Some(hint) = hint else {
        return Ok(());
    };
    if envelope.messaging_endpoint_id.is_none() {
        return Ok(());
    }

    if !try_mark_welcome_first_contact(session_store, envelope)? {
        return Ok(());
    }

    let resume = FlowResumeStore::new(Arc::clone(session_store));
    let snapshot = resume
        .fetch(envelope)
        .map_err(|err| anyhow!("welcome-flow first-contact probe failed: {err}"))?;
    if snapshot.is_some() {
        return Ok(());
    }

    envelope.pack_id = Some(hint.pack_id.clone());
    envelope.flow_id = hint.flow_id.clone();
    envelope.flow_type = hint_flow_type;
    Ok(())
}

/// Persists a per-`(tenant, env, eid, user)` welcome-seen marker on first
/// contact and returns `true` only when this turn observed no marker AND
/// wrote one. Subsequent turns short-circuit to `false`.
///
/// Returns `false` (without writing) if the envelope lacks a
/// `messaging_endpoint_id` — no marker bucket is derivable.
///
/// Race window: check + mark is two store calls, not one atomic CAS. Two
/// concurrent first-ever turns can both observe "no marker" and both fire
/// welcome once — bounded harm, mitigated by the wait-snapshot safety net
/// in [`apply_welcome_flow_override`]. A real atomic primitive
/// (`register_wait_if_absent`) is Phase D.
fn try_mark_welcome_first_contact(
    store: &DynSessionStore,
    envelope: &IngressEnvelope,
) -> Result<bool> {
    let Some(scope) = welcome_marker_scope(envelope) else {
        return Ok(false);
    };
    let (ctx, user) = FlowResumeStore::contact_identity(envelope)
        .map_err(|e| anyhow!("welcome marker identity probe failed: {e}"))?;

    if store
        .find_wait_by_scope(&ctx, &user, &scope)
        .map_err(|e| anyhow!("welcome marker probe failed: {e}"))?
        .is_some()
    {
        return Ok(false);
    }

    let data = marker_session_data(&ctx, &user);
    let session_key = marker_session_key(&ctx, &user, &scope);
    store
        .register_wait(&ctx, &user, &scope, &session_key, data, None)
        .map_err(|e| anyhow!("welcome marker register failed: {e}"))?;
    Ok(true)
}

/// Stable, identity-scoped session key for the welcome marker.
///
/// **The session key is the store's per-entry identity.** Both backends
/// (in-memory + Redis) overwrite or reject an existing entry on a
/// `register_wait` collision, so a scope-only `SessionKey` would collapse
/// every `(tenant, env, user)` on the same endpoint onto one row:
/// in-memory's `ensure_ctx_preserved` would reject User B's first turn
/// outright; Redis's unconditional `SET` would overwrite User A's entry
/// and dangle User A's scope index to User B's data.
///
/// Fix: SHA-256 over `(env, tenant, team, user, conversation)`. The `v1`
/// prefix lets us bump the derivation without colliding on old markers.
fn marker_session_key(
    ctx: &greentic_types::TenantCtx,
    user: &greentic_types::UserId,
    scope: &greentic_types::ReplyScope,
) -> greentic_session::SessionKey {
    use sha2::{Digest, Sha256};
    let team = match ctx.team_id.as_ref().or(ctx.team.as_ref()) {
        Some(t) => t.as_str(),
        None => "<none>",
    };
    let digest = Sha256::digest(
        format!(
            "welcome-marker:v1\0{}\0{}\0team={team}\0{}\0{}",
            ctx.env.as_str(),
            ctx.tenant_id.as_str(),
            user.as_str(),
            scope.conversation,
        )
        .as_bytes(),
    );
    greentic_session::SessionKey::new(format!("welcome-marker::{}", hex::encode(digest)))
}

/// Synthetic [`ReplyScope`] keyed on `messaging_endpoint_id` so the marker
/// is partitioned per-endpoint AND disjoint from any real conversation
/// scope. Returns `None` when the envelope lacks an eid — the marker has
/// no meaningful bucket then, and the caller exits early.
fn welcome_marker_scope(envelope: &IngressEnvelope) -> Option<greentic_types::ReplyScope> {
    let eid = envelope.messaging_endpoint_id.as_deref()?;
    Some(greentic_types::ReplyScope {
        conversation: format!("welcome-seen::ep={eid}"),
        thread: None,
        reply_to: None,
        correlation: None,
    })
}

/// Minimal `SessionData` for the marker. The store accepts any record
/// aligned with `(ctx, user)`; the marker carries no flow semantics, so
/// the placeholder `flow_id`/`pack_id` is fixed and validates as an
/// identifier (ascii + `.`/`-`/`_`).
fn marker_session_data(
    ctx: &greentic_types::TenantCtx,
    user: &greentic_types::UserId,
) -> greentic_session::SessionData {
    use std::str::FromStr;
    use std::sync::LazyLock;
    static FLOW_ID: LazyLock<greentic_types::FlowId> =
        LazyLock::new(|| greentic_types::FlowId::from_str("welcome-marker").expect("valid id"));
    static PACK_ID: LazyLock<greentic_types::PackId> =
        LazyLock::new(|| greentic_types::PackId::from_str("welcome-marker").expect("valid id"));
    let cursor = greentic_types::SessionCursor::new("marker".to_string());
    let ctx = ctx.clone().with_user(Some(user.clone()));
    greentic_session::SessionData {
        tenant_ctx: ctx,
        flow_id: FLOW_ID.clone(),
        pack_id: Some(PACK_ID.clone()),
        cursor,
        context_json: "{}".to_string(),
    }
}

fn apply_fast2flow_routing(
    runtime: &TenantRuntime,
    tenant: &str,
    activity: Activity,
) -> Result<Activity> {
    let config = &runtime.config().fast2flow;
    if !config.enabled || activity.flow_id().is_some() {
        return Ok(activity);
    }
    apply_fast2flow_routing_enabled(runtime, tenant, activity, config)
}

#[cfg(feature = "greentic-x-provider")]
fn apply_fast2flow_routing_enabled(
    runtime: &TenantRuntime,
    tenant: &str,
    activity: Activity,
    config: &Fast2FlowRoutingConfig,
) -> Result<Activity> {
    let Some(text) = activity.payload().get("text").and_then(Value::as_str) else {
        return Ok(activity);
    };
    if text.trim().is_empty() {
        return Ok(activity);
    }

    let mut envelope = Fast2FlowMessageEnvelope::new(text.trim().to_owned());
    if let Some(channel) = activity.channel() {
        envelope = envelope.with_channel(channel.to_owned());
    }
    if let Some(provider) = activity.provider_id() {
        envelope = envelope.with_provider(provider.to_owned());
    }
    let request = Fast2FlowRouteRequest {
        scope: config.scope.clone().unwrap_or_else(|| tenant.to_owned()),
        envelope,
        session_active: activity.session_id().is_some(),
        input_locale: "en".to_owned(),
        time_budget_ms: config.time_budget_ms,
        registry_path: config.registry_path.clone(),
        indexes_path: config.indexes_path.clone(),
        now_unix_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
        metadata: Default::default(),
    };
    let provider = RunnerPackFast2FlowRoutingProvider::new(runtime.pack())
        .map_err(|err| anyhow!(err.to_string()))?
        .with_component_ref(config.component_ref.clone())
        .with_operation(config.operation.clone())
        .with_tenant(tenant.to_owned());
    let route = provider
        .route_intent(request)
        .map_err(|err| anyhow!(err.to_string()))?;

    match route.directive {
        Fast2FlowDirective::Continue => Ok(activity),
        Fast2FlowDirective::Dispatch {
            target, entities, ..
        } => apply_fast2flow_target(activity, &target, entities),
        Fast2FlowDirective::Respond { message } => Ok(Activity::custom(
            "response",
            serde_json::json!({ "messages": [{ "text": message }] }),
        )
        .ensure_tenant(tenant)),
        Fast2FlowDirective::Deny { reason } => Ok(Activity::custom(
            "response",
            serde_json::json!({ "messages": [{ "text": reason }] }),
        )
        .ensure_tenant(tenant)),
    }
}

#[cfg(not(feature = "greentic-x-provider"))]
fn apply_fast2flow_routing_enabled(
    _runtime: &TenantRuntime,
    _tenant: &str,
    _activity: Activity,
    _config: &Fast2FlowRoutingConfig,
) -> Result<Activity> {
    bail!("fast2flow routing requires the greentic-x-provider feature")
}

#[cfg(feature = "greentic-x-provider")]
fn apply_fast2flow_target(
    activity: Activity,
    target: &str,
    entities: Vec<greentic_x_runtime::Fast2FlowRoutingEntity>,
) -> Result<Activity> {
    let target = target.trim();
    if target.is_empty() {
        bail!("fast2flow dispatch target is empty");
    }
    if let Some((pack_id, flow_id)) = target.split_once('/') {
        if pack_id.trim().is_empty() || flow_id.trim().is_empty() {
            bail!("fast2flow dispatch target `{target}` must be `pack_id/flow_id` or `flow_id`");
        }
        return Ok(attach_fast2flow_entities(
            activity.with_pack(pack_id.trim()).with_flow(flow_id.trim()),
            entities,
        ));
    }
    Ok(attach_fast2flow_entities(
        activity.with_flow(target),
        entities,
    ))
}

#[cfg(feature = "greentic-x-provider")]
fn attach_fast2flow_entities(
    activity: Activity,
    entities: Vec<greentic_x_runtime::Fast2FlowRoutingEntity>,
) -> Activity {
    if entities.is_empty() {
        return activity;
    }
    activity.with_payload_field(
        "fast2flow",
        serde_json::json!({
            "entities": entities,
        }),
    )
}

fn resolve_flow_id(runtime: &TenantRuntime, activity: &Activity) -> Result<(String, String)> {
    let engine = runtime.engine();
    if let Some(flow_id) = activity.flow_id() {
        if let Some(pack_id) = activity.pack_id() {
            if engine.flow_by_key(pack_id, flow_id).is_none() {
                bail!("flow {flow_id} not registered for pack {pack_id}");
            }
            return Ok((pack_id.to_string(), flow_id.to_string()));
        }
        if let Some(flow) = engine.flow_by_id(flow_id) {
            return Ok((flow.pack_id.clone(), flow.id.clone()));
        }
        bail!("flow {flow_id} is ambiguous; pack_id is required");
    }

    if let Some(flow_type) = activity.flow_type() {
        if let Some(pack_id) = activity.pack_id() {
            if let Some(flow) = engine
                .flows()
                .iter()
                .find(|flow| flow.pack_id == pack_id && flow.flow_type == flow_type)
            {
                return Ok((pack_id.to_string(), flow.id.clone()));
            }
            bail!("flow type {flow_type} not registered for pack {pack_id}");
        }
        if let Some(flow) = engine.flow_by_type(flow_type) {
            return Ok((flow.pack_id.clone(), flow.id.clone()));
        }
        // More than one flow of this type exists. Provider ingress is routed by
        // type alone (no flow_id/pack_id), so a pack with one public entrypoint
        // plus internal helper flows of the same type would otherwise be
        // rejected as ambiguous. Narrow to entrypoint flows before giving up:
        // internal flows are only reachable via `flow.call` and must never be
        // selected for an inbound event.
        if let Some(flow) = engine.entry_flow_by_type(flow_type) {
            return Ok((flow.pack_id.clone(), flow.id.clone()));
        }
        bail!("flow type {flow_type} is ambiguous; pack_id is required");
    }

    let pack = runtime.pack();
    let flow_id = pack
        .metadata()
        .entry_flows
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("no entry flows registered for tenant {}", runtime.tenant()))?;
    Ok((pack.metadata().pack_id.clone(), flow_id))
}

fn normalize_replies(result: Value, tenant: &str) -> Vec<Activity> {
    result
        .as_array()
        .cloned()
        .unwrap_or_else(|| vec![result])
        .into_iter()
        .map(|payload| Activity::from_output(payload, tenant))
        .collect()
}

fn is_pack_archive(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("gtpack"))
        .unwrap_or(false)
}

#[cfg(test)]
mod welcome_flow_tests {
    use super::*;
    use crate::engine::runtime::IngressEnvelope;
    use crate::runner::engine::{ExecutionState, FlowSnapshot, FlowWait};
    use crate::storage::new_session_store;
    use greentic_types::ReplyScope;
    use serde_json::json;

    fn sample_envelope(endpoint_id: Option<&str>) -> IngressEnvelope {
        sample_envelope_for_user(endpoint_id, "user-1")
    }

    fn sample_envelope_for_user(endpoint_id: Option<&str>, user: &str) -> IngressEnvelope {
        IngressEnvelope {
            tenant: "demo".into(),
            env: Some("local".into()),
            pack_id: Some("pack.default".into()),
            flow_id: "flow.default".into(),
            flow_type: Some("messaging".into()),
            action: Some("messaging".into()),
            session_hint: None,
            provider: Some("teams".into()),
            messaging_endpoint_id: endpoint_id.map(String::from),
            channel: Some("chan".into()),
            conversation: Some(format!("conv-{user}")),
            user: Some(user.to_string()),
            entry_node: None,
            activity_id: None,
            timestamp: None,
            payload: json!({}),
            metadata: None,
            reply_scope: Some(ReplyScope {
                conversation: format!("conv-{user}"),
                thread: None,
                reply_to: None,
                correlation: None,
            }),
        }
        .canonicalize()
    }

    fn hint() -> WelcomeFlowHint {
        WelcomeFlowHint {
            pack_id: "pack.welcome".into(),
            flow_id: "flow.welcome".into(),
        }
    }

    fn seed_resume(store: &DynSessionStore, envelope: &IngressEnvelope) {
        // Plant a snapshot in the exact bucket `fetch` would query so the
        // next call resolves to a resume — proves the override skips when a
        // session already exists.
        let resume = FlowResumeStore::new(Arc::clone(store));
        let state: ExecutionState = serde_json::from_value(json!({
            "input": { "text": "hi" },
            "nodes": {},
            "egress": []
        }))
        .expect("state");
        let wait = FlowWait {
            reason: Some("await-user".into()),
            snapshot: FlowSnapshot {
                pack_id: envelope.pack_id.clone().expect("pack_id"),
                flow_id: envelope.flow_id.clone(),
                next_flow: None,
                next_node: "node-2".into(),
                awaiting_submit: false,
                state,
            },
        };
        resume.save(envelope, &wait).expect("seed save");
    }

    #[test]
    fn override_is_no_op_when_hint_absent() {
        // Pre-M1.5 producers don't attach a hint — flow resolution must
        // stay exactly the same.
        let store = new_session_store();
        let mut envelope = sample_envelope(Some("teams-legal"));
        let before = envelope.clone();
        apply_welcome_flow_override(&store, &mut envelope, None, None).expect("ok");
        assert_eq!(envelope.pack_id, before.pack_id);
        assert_eq!(envelope.flow_id, before.flow_id);
        assert_eq!(envelope.flow_type, before.flow_type);
    }

    #[test]
    fn override_is_no_op_when_endpoint_id_absent() {
        // Non-messaging traffic carries no endpoint id and must never hit
        // the welcome-flow path even if the hint is somehow set.
        let store = new_session_store();
        let mut envelope = sample_envelope(None);
        let before = envelope.clone();
        apply_welcome_flow_override(&store, &mut envelope, Some(&hint()), Some("welcome".into()))
            .expect("ok");
        assert_eq!(envelope.pack_id, before.pack_id);
        assert_eq!(envelope.flow_id, before.flow_id);
    }

    #[test]
    fn override_swaps_pack_flow_and_threads_flow_type_through() {
        // Both axes covered: when the caller pre-resolved the welcome
        // flow's type, it lands on the envelope; when the resolver
        // returned None (unknown flow in engine), it lands as None and
        // downstream resolution defaults take over.
        for hint_flow_type in [Some("welcome".to_string()), None] {
            let store = new_session_store();
            let mut envelope = sample_envelope(Some("teams-legal"));
            apply_welcome_flow_override(
                &store,
                &mut envelope,
                Some(&hint()),
                hint_flow_type.clone(),
            )
            .expect("ok");
            assert_eq!(envelope.pack_id.as_deref(), Some("pack.welcome"));
            assert_eq!(envelope.flow_id, "flow.welcome");
            assert_eq!(envelope.flow_type, hint_flow_type);
        }
    }

    #[test]
    fn override_is_no_op_on_repeat_turn_with_existing_session() {
        // Resume path: an already-active session in the same bucket means
        // this isn't first contact. The user must continue on the resumed
        // flow, NOT be redirected to the welcome flow.
        let store = new_session_store();
        let envelope_template = sample_envelope(Some("teams-legal"));
        seed_resume(&store, &envelope_template);

        let mut envelope = envelope_template.clone();
        apply_welcome_flow_override(&store, &mut envelope, Some(&hint()), Some("welcome".into()))
            .expect("ok");
        assert_eq!(envelope.pack_id, envelope_template.pack_id);
        assert_eq!(envelope.flow_id, envelope_template.flow_id);
        assert_eq!(envelope.flow_type, envelope_template.flow_type);
    }

    #[test]
    fn override_is_no_op_post_completion_when_marker_present() {
        // POST-COMPLETION REGRESSION GUARD (Codex #201): the welcome-seen
        // marker is durable and survives flow completion. After welcome
        // fires once + the flow finishes (wait cleared), the next turn
        // must NOT re-fire welcome — the marker is the gate, not the
        // active-wait snapshot.
        let store = new_session_store();
        let mut first = sample_envelope(Some("teams-legal"));
        apply_welcome_flow_override(&store, &mut first, Some(&hint()), Some("welcome".into()))
            .expect("first turn ok");
        assert_eq!(
            first.pack_id.as_deref(),
            Some("pack.welcome"),
            "first turn fires welcome"
        );

        // Simulate the welcome flow completing: the engine clears the
        // wait at end-of-flow (mirror `FlowResumeStore::clear`). The
        // marker must NOT be in the wait scope, so this clear has no
        // effect on the marker.
        let resume = FlowResumeStore::new(Arc::clone(&store));
        resume.clear(&first).expect("clear post-completion wait");

        // Second turn arrives — producer still attaches the hint (it
        // does not know flow-completion happened). The marker keeps the
        // override off.
        let mut second = sample_envelope(Some("teams-legal"));
        apply_welcome_flow_override(&store, &mut second, Some(&hint()), Some("welcome".into()))
            .expect("second turn ok");
        assert_eq!(
            second.pack_id.as_deref(),
            Some("pack.default"),
            "second turn must NOT re-fire welcome"
        );
        assert_eq!(second.flow_id, "flow.default");
    }

    #[test]
    fn override_is_no_op_on_second_turn_after_marker_set() {
        // No-wait variant of the post-completion test: a welcome flow
        // without `session.wait` leaves no snapshot AT ALL. Marker is the
        // only thing standing between turn 2 and a welcome re-fire.
        let store = new_session_store();
        let mut first = sample_envelope(Some("teams-legal"));
        apply_welcome_flow_override(&store, &mut first, Some(&hint()), Some("welcome".into()))
            .expect("first turn ok");
        assert_eq!(first.pack_id.as_deref(), Some("pack.welcome"));

        let mut second = sample_envelope(Some("teams-legal"));
        apply_welcome_flow_override(&store, &mut second, Some(&hint()), Some("welcome".into()))
            .expect("second turn ok");
        assert_eq!(
            second.pack_id.as_deref(),
            Some("pack.default"),
            "second turn must NOT re-fire welcome"
        );
    }

    #[test]
    fn override_partitions_marker_per_endpoint() {
        // The marker is keyed by `(tenant, env, eid, user)` — a user
        // marked seen on `teams-legal` is still first contact on
        // `teams-accounting`. Welcome must fire independently on each
        // endpoint.
        let store = new_session_store();
        let mut legal = sample_envelope(Some("teams-legal"));
        apply_welcome_flow_override(&store, &mut legal, Some(&hint()), Some("welcome".into()))
            .expect("legal first turn ok");
        assert_eq!(legal.pack_id.as_deref(), Some("pack.welcome"));

        let mut accounting = sample_envelope(Some("teams-accounting"));
        apply_welcome_flow_override(
            &store,
            &mut accounting,
            Some(&hint()),
            Some("welcome".into()),
        )
        .expect("accounting first turn ok");
        assert_eq!(
            accounting.pack_id.as_deref(),
            Some("pack.welcome"),
            "different endpoint = independent first contact"
        );
    }

    #[test]
    fn override_partitions_marker_per_user_on_same_endpoint() {
        // Codex adversarial review of #382 (high): a session-key derived
        // only from the eid collapses every user on that endpoint onto one
        // store row — in-memory rejects User B's first turn with a hard
        // error, Redis silently overwrites and lets User A re-welcome on
        // their next turn.
        //
        // Regression guard: two users on the same eid each get welcome on
        // their own first turn; the second user's first contact does NOT
        // fail; both subsequent turns are no-ops.
        let store = new_session_store();

        // User A's first turn
        let mut a1 = sample_envelope_for_user(Some("teams-legal"), "user-a");
        apply_welcome_flow_override(&store, &mut a1, Some(&hint()), Some("welcome".into()))
            .expect("user-a first ok");
        assert_eq!(a1.pack_id.as_deref(), Some("pack.welcome"));

        // User B's first turn — must independently fire welcome, NOT error.
        let mut b1 = sample_envelope_for_user(Some("teams-legal"), "user-b");
        apply_welcome_flow_override(&store, &mut b1, Some(&hint()), Some("welcome".into()))
            .expect("user-b first must not collide with user-a marker");
        assert_eq!(
            b1.pack_id.as_deref(),
            Some("pack.welcome"),
            "user-b is independent first contact"
        );

        // User A's second turn — marker still intact, no re-fire.
        let mut a2 = sample_envelope_for_user(Some("teams-legal"), "user-a");
        apply_welcome_flow_override(&store, &mut a2, Some(&hint()), Some("welcome".into()))
            .expect("user-a second ok");
        assert_eq!(
            a2.pack_id.as_deref(),
            Some("pack.default"),
            "user-a must not be re-welcomed after user-b joined"
        );

        // User B's second turn — same.
        let mut b2 = sample_envelope_for_user(Some("teams-legal"), "user-b");
        apply_welcome_flow_override(&store, &mut b2, Some(&hint()), Some("welcome".into()))
            .expect("user-b second ok");
        assert_eq!(b2.pack_id.as_deref(), Some("pack.default"));
    }

    #[test]
    fn marker_is_not_written_when_hint_absent() {
        // Marker writes are gated on the hint+eid preconditions — a
        // pre-M1.5 turn (no hint) MUST NOT leak a marker, otherwise a
        // producer that later enables welcome would treat that user as
        // already-contacted and never fire the override.
        //
        // Mid-conversation users (active-wait safety net path) DO get a
        // marker — that's deliberate: they don't get retroactive welcomes.
        // This test only guards the no-hint gate.
        let store = new_session_store();
        let mut envelope = sample_envelope(Some("teams-legal"));
        apply_welcome_flow_override(&store, &mut envelope, None, None).expect("ok");

        let mut next = sample_envelope(Some("teams-legal"));
        apply_welcome_flow_override(&store, &mut next, Some(&hint()), Some("welcome".into()))
            .expect("ok");
        assert_eq!(
            next.pack_id.as_deref(),
            Some("pack.welcome"),
            "no marker leaked from hint-absent path"
        );
    }
}

#[cfg(test)]
mod identify_endpoints_tests {
    use super::*;

    fn dummy_runner_host() -> RunnerHost {
        let session_store = new_session_store();
        let state_store = new_state_store();
        RunnerHost {
            configs: HashMap::new(),
            active: Arc::new(ActivePacks::new()),
            health: Arc::new(HealthState::new()),
            session_host: session_host_from(session_store.clone()),
            state_host: state_host_from(state_store.clone()),
            session_store,
            state_store,
            wasi_policy: Arc::new(RunnerWasiPolicy::new()),
            secrets_manager: default_manager().expect("default secrets manager"),
            telemetry: None,
        }
    }

    #[tokio::test]
    async fn empty_provider_types_returns_empty_map_without_loading_revision() {
        // No revision is loaded; this proves the fast-path short-circuits
        // before `load_revision` so the caller can ask "any types?" cheaply
        // when an env declares zero messaging endpoints.
        let host = dummy_runner_host();
        let map = host
            .identify_messaging_endpoints_for_revision(
                "demo",
                DeploymentId::new(),
                BundleId::new("anything"),
                RevisionId::new(),
                &[],
                b"{}",
            )
            .await
            .expect("empty types is the cheap fast path");
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn missing_revision_surfaces_clear_error() {
        // Non-empty types but the revision was never loaded — the error
        // chain must name the revision so operators can correlate it with
        // their dispatch log.
        let host = dummy_runner_host();
        let deployment = DeploymentId::new();
        let revision = RevisionId::new();
        let err = host
            .identify_messaging_endpoints_for_revision(
                "demo",
                deployment,
                BundleId::new("missing"),
                revision,
                &["teams"],
                b"{}",
            )
            .await
            .expect_err("missing revision must fail closed");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("revision runtime not loaded"),
            "error chain should name the failure mode, got: {msg}"
        );
        assert!(
            msg.contains(&deployment.to_string()),
            "error chain should name the deployment id, got: {msg}"
        );
        assert!(
            msg.contains(&revision.to_string()),
            "error chain should name the revision id, got: {msg}"
        );
    }

    #[tokio::test]
    async fn scoped_empty_provider_types_returns_empty_map() {
        let host = dummy_runner_host();
        let map = host
            .identify_messaging_endpoints_for_revision_scoped(
                "demo",
                DeploymentId::new(),
                BundleId::new("anything"),
                RevisionId::new(),
                &[],
                &[],
                &Value::Null,
            )
            .await
            .expect("empty types is the cheap fast path");
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn scoped_missing_revision_surfaces_clear_error() {
        let host = dummy_runner_host();
        let deployment = DeploymentId::new();
        let revision = RevisionId::new();
        let err = host
            .identify_messaging_endpoints_for_revision_scoped(
                "demo",
                deployment,
                BundleId::new("missing"),
                revision,
                &["teams"],
                &[],
                &Value::Null,
            )
            .await
            .expect_err("missing revision must fail closed");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("revision runtime not loaded"),
            "error chain should name the failure mode, got: {msg}"
        );
        assert!(
            msg.contains(&deployment.to_string()),
            "error chain should name the deployment id, got: {msg}"
        );
        assert!(
            msg.contains(&revision.to_string()),
            "error chain should name the revision id, got: {msg}"
        );
    }

    /// Regression guard: the futures returned by the per-revision identify
    /// APIs MUST be `Send`. Downstream consumers (greentic-start's hyper
    /// `service_fn`) spawn them through tokio; a non-`Send` future at this
    /// boundary breaks every spawned-service consumer with a confusing
    /// "implementation of Send is not general enough" diagnostic that
    /// surfaces far from the offending change.
    ///
    /// Concrete history: PR #394 routed all three identify entry points
    /// through a shared `fan_out_across_packs` helper bounded on
    /// `AsyncFnMut`. The HRTB inference for the resulting future
    /// destabilised `Send` proof for all three APIs, even the legacy
    /// `identify_messaging_endpoints_for_revision` that itself hadn't
    /// changed shape — greentic-start failed to compile on the next
    /// dev-publish bump. This test would have caught it.
    #[test]
    fn identify_futures_are_send() {
        fn assert_send<F: Send>(_: F) {}
        let host = dummy_runner_host();
        // Each call has to be wrapped in its own scope so the borrows
        // don't outlive the host's reference per call — the point is to
        // assert each returned future type is Send-clean in isolation.
        assert_send(host.identify_messaging_endpoints_for_revision(
            "demo",
            DeploymentId::new(),
            BundleId::new("anything"),
            RevisionId::new(),
            &["teams"],
            b"{}",
        ));
        assert_send(host.identify_messaging_endpoints_for_revision_scoped(
            "demo",
            DeploymentId::new(),
            BundleId::new("anything"),
            RevisionId::new(),
            &["teams"],
            &[("x-telegram-bot-api-secret-token".into(), "tok".into())],
            &Value::Null,
        ));
        assert_send(host.describe_identify_instances_for_revision(
            "demo",
            DeploymentId::new(),
            BundleId::new("anything"),
            RevisionId::new(),
            &["teams"],
        ));
        assert_send(host.invoke_provider_for_revision(
            "demo",
            DeploymentId::new(),
            BundleId::new("anything"),
            RevisionId::new(),
            "messaging.telegram.bot",
            "ingest_http",
            b"{}".to_vec(),
            None,
            None,
        ));
    }

    #[tokio::test]
    async fn invoke_provider_missing_revision_surfaces_clear_error() {
        // Non-empty types but the revision was never loaded — the error
        // chain must name the revision so operators can correlate it with
        // their dispatch log. Mirrors the identify-side sibling so a future
        // refactor can't quietly drop the lookup-context wrapper.
        let host = dummy_runner_host();
        let deployment = DeploymentId::new();
        let revision = RevisionId::new();
        let err = host
            .invoke_provider_for_revision(
                "demo",
                deployment,
                BundleId::new("missing"),
                revision,
                "messaging.telegram.bot",
                "ingest_http",
                b"{}".to_vec(),
                None,
                None,
            )
            .await
            .expect_err("missing revision must fail closed");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("revision runtime not loaded"),
            "error chain should name the failure mode, got: {msg}"
        );
        assert!(
            msg.contains(&deployment.to_string()),
            "error chain should name the deployment id, got: {msg}"
        );
        assert!(
            msg.contains(&revision.to_string()),
            "error chain should name the revision id, got: {msg}"
        );
    }
}

#[cfg(all(test, feature = "greentic-x-provider"))]
mod fast2flow_tests {
    use greentic_x_runtime::Fast2FlowRoutingEntity;

    use super::*;

    #[test]
    fn dispatch_target_attaches_prefill_entities_to_payload() {
        let activity = Activity::text("show traffic tomorrow");
        let routed = apply_fast2flow_target(
            activity,
            "telco-x/prefix-traffic",
            vec![Fast2FlowRoutingEntity::new("date", "20260611").with_format("iso", "2026-06-11")],
        )
        .expect("target should route");

        assert_eq!(routed.pack_id(), Some("telco-x"));
        assert_eq!(routed.flow_id(), Some("prefix-traffic"));
        assert_eq!(
            routed.payload()["fast2flow"]["entities"][0]["normalized"],
            "20260611"
        );
        assert_eq!(
            routed.payload()["fast2flow"]["entities"][0]["formats"]["iso"],
            "2026-06-11"
        );
    }
}
