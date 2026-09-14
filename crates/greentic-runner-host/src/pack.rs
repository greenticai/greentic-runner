use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::{ArtifactKey, CacheConfig, CacheManager, CpuPolicy, EngineProfile};
use crate::component_api::{
    self, node::ExecCtx as ComponentExecCtx, node::InvokeResult, node::NodeError,
};
use crate::identify_hint::IdentifyInstanceHint;
use crate::oauth::{OAuthBrokerConfig, ResourceTokenRequest};
use crate::provider::{ProviderBinding, ProviderRegistry};
use crate::provider_core::{
    schema_core::SchemaCorePre as LegacySchemaCorePre,
    schema_core_path::SchemaCorePre as PathSchemaCorePre,
    schema_core_schema::SchemaCorePre as SchemaSchemaCorePre,
};
use crate::provider_core_only;
use crate::runtime_refs::RuntimeRefsInjection;
use crate::runtime_wasmtime::{Component, Engine, Linker, ResourceTable};
use anyhow::{Context, Result, anyhow, bail};
use futures::executor::block_on;
use greentic_distributor_client::dist::{
    CachePolicy, DistClient, DistError, DistOptions, ResolvePolicy,
};
use greentic_interfaces_wasmtime::host_helpers::v1::{
    self as host_v1, HostFns, add_all_v1_to_linker,
    oauth_broker::OAuthBrokerHost as OAuthBrokerHostTrait,
    runner_host_http::RunnerHostHttp,
    runner_host_kv::RunnerHostKv,
    runtime_config::{ConfigError, RuntimeConfigHost},
    secrets_store::{SecretsError, SecretsErrorV1_1, SecretsStoreHost, SecretsStoreHostV1_1},
    state_store::{
        OpAck as StateOpAck, StateKey as HostStateKey, StateStoreError as StateError,
        StateStoreHost, TenantCtx as StateTenantCtx,
    },
    telemetry_logger::{
        OpAck as TelemetryAck, SpanContext as TelemetrySpanContext,
        TelemetryLoggerError as TelemetryError, TelemetryLoggerHost,
        TenantCtx as TelemetryTenantCtx,
    },
};
use greentic_interfaces_wasmtime::http_client_client_v1_1::greentic::http::http_client as http_client_client_alias;
use greentic_interfaces_wasmtime::instance_identity_instance_identity_describe_v0_1::InstanceIdentityDescribePre;
use greentic_interfaces_wasmtime::instance_identity_v0_1::InstanceIdentityPre;
use greentic_interfaces_wasmtime::{
    http_client_client_v1_0::greentic::interfaces_types::types as http_types_v1_0,
    http_client_client_v1_1::greentic::interfaces_types::types as http_types_v1_1,
};
use greentic_pack::builder as legacy_pack;
use greentic_types::flow::FlowHasher;
use greentic_types::{
    ArtifactLocationV1, ComponentId, ComponentManifest, ComponentSourceRef, ComponentSourcesV1,
    EXT_COMPONENT_SOURCES_V1, EnvId, ExtensionRef, Flow, FlowComponentRef, FlowId, FlowKind,
    FlowMetadata, InputMapping, Node, NodeId, OutputMapping, Routing, StateKey as StoreStateKey,
    TeamId, TelemetryHints, TenantCtx as TypesTenantCtx, TenantId, UserId, decode_pack_manifest,
    pack_manifest::ExtensionInline,
};
use host_v1::http_client as host_http_client;
use host_v1::http_client::{
    HttpClientError, HttpClientErrorV1_1, HttpClientHost, HttpClientHostV1_1,
    Request as HttpRequest, RequestOptionsV1_1 as HttpRequestOptionsV1_1,
    RequestV1_1 as HttpRequestV1_1, Response as HttpResponse, ResponseV1_1 as HttpResponseV1_1,
    TenantCtx as HttpTenantCtx, TenantCtxV1_1 as HttpTenantCtxV1_1,
};
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use reqwest::blocking::Client as BlockingClient;
use runner_core::normalize_under_root;
use serde::{Deserialize, Serialize};
use serde_cbor;
use serde_json::{self, Value};
use sha2::Digest;
use tempfile::TempDir;
use tokio::fs;
use wasmparser::{Parser, Payload};
use wasmtime::{Store, StoreContextMut};
use wasmtime_wasi_http::WasiHttpCtx;
use wasmtime_wasi_http::p2::{
    WasiHttpCtxView, WasiHttpView, add_only_http_to_linker_sync as add_wasi_http_to_linker,
};
use wasmtime_wasi_tls::p2::LinkOptions;
use wasmtime_wasi_tls::{WasiTlsCtx, WasiTlsCtxBuilder, WasiTlsCtxView, WasiTlsView};
use zip::ZipArchive;

use crate::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use crate::runner::flow_adapter::{FlowIR, flow_doc_to_ir, flow_ir_to_flow, is_native_op_key};
use crate::runner::mocks::{HttpDecision, HttpMockRequest, HttpMockResponse, MockLayer};
#[cfg(feature = "fault-injection")]
use crate::testing::fault_injection::{FaultContext, FaultPoint, maybe_fail};

use crate::config::HostConfig;
use crate::fault;
use crate::secrets::{
    DynSecretsManager, canonicalize_secret_key, read_secret_blocking, write_secret_blocking,
};
use crate::storage::state::STATE_PREFIX;
use crate::storage::{DynSessionStore, DynStateStore};
use crate::verify;
use crate::wasi::{PreopenSpec, RunnerWasiPolicy};
use tracing::warn;
use wasmtime_wasi::p2::add_to_linker_sync as add_wasi_to_linker;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use greentic_flow::model::FlowDoc;

#[allow(dead_code)]
pub struct PackRuntime {
    /// Component artifact path (wasm file).
    path: PathBuf,
    /// Optional archive (.gtpack) used to load flows/manifests.
    archive_path: Option<PathBuf>,
    config: Arc<HostConfig>,
    engine: Engine,
    metadata: PackMetadata,
    manifest: Option<greentic_types::PackManifest>,
    legacy_manifest: Option<Box<legacy_pack::PackManifest>>,
    component_manifests: HashMap<String, ComponentManifest>,
    mocks: Option<Arc<MockLayer>>,
    flows: Option<PackFlows>,
    components: HashMap<String, PackComponent>,
    http_client: Arc<BlockingClient>,
    session_store: Option<DynSessionStore>,
    state_store: Option<DynStateStore>,
    wasi_policy: Arc<RunnerWasiPolicy>,
    /// Lazily-parsed `assets/mcp-routes.json` sidecar — see
    /// [`PackRuntime::mcp_routes`]. Read on first use rather than at load so a
    /// pack with no MCP nodes never touches the archive for it.
    mcp_routes: std::sync::OnceLock<Option<crate::runner::mcp_pack_routes::PackMcpRoutes>>,
    assets_tempdir: Option<TempDir>,
    provider_registry: RwLock<Option<ProviderRegistry>>,
    /// Per-revision lazy cache of `describe-identify-instance` results,
    /// keyed by `component_ref`. `None` value means the component does not
    /// export the describe world (or the hint was malformed) — the
    /// caller falls back to passing input headers through unchanged. The
    /// outer `Option` distinguishes "not probed yet" from "probed and
    /// has no hint". `ArcSwap`-driven revision swaps allocate a fresh
    /// `PackRuntime` so this cache is naturally invalidated.
    identify_hint_cache: RwLock<HashMap<String, Option<IdentifyInstanceHint>>>,
    secrets: DynSecretsManager,
    oauth_config: Option<OAuthBrokerConfig>,
    cache: CacheManager,
    /// `pack-config.v1.non_secret` map plumbed into each `HostState` for the
    /// `greentic:runtime-config@1.0.0` host import. Defaults to `None` when no
    /// producer (greentic-start) has materialized a `PackConfig` yet; in that
    /// case all runtime-config lookups fall through to the secrets-store
    /// compat shim.
    runtime_config_non_secret: Option<Arc<BTreeMap<String, Value>>>,
    /// `pack-config.v1.runtime_refs` (C5): per-pack `key → URI` bindings plus
    /// the env-shared [`RuntimeRefResolver`]. Consulted by the
    /// `greentic:runtime-config@1.0.0` host import AFTER `non_secret` and
    /// BEFORE the compat shim. `None` when no producer set it yet.
    ///
    /// [`RuntimeRefResolver`]: crate::runtime_refs::RuntimeRefResolver
    runtime_refs: Option<RuntimeRefsInjection>,
}

struct PackComponent {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    version: String,
    component: Arc<Component>,
}

/// Outcome of calling a provider component's `identify-instance` export
/// (`greentic:provider-instance-identity@0.1.0`). Callers MUST treat the
/// three variants differently per the WIT contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentifyOutcome {
    /// Component does not export the world — caller falls back to the
    /// operator's statically-declared `provider_id`.
    Unsupported,
    /// Component exported the world and returned `None` — caller MUST
    /// fail closed (401/404), no fallback.
    NoMatch,
    /// Component identified the payload as belonging to this
    /// `provider_id` — caller routes to the matching `MessagingEndpoint`.
    Identified(String),
}

impl IdentifyOutcome {
    /// Merge `other` into `self` per the lattice
    /// `Identified > NoMatch > Unsupported`. Used by callers fanning the probe
    /// out over multiple packs (overlays) where the strongest signal across
    /// packs wins.
    pub fn merge_in(&mut self, other: IdentifyOutcome) {
        match (&*self, &other) {
            // Identified is the top — never gets overwritten.
            (IdentifyOutcome::Identified(_), _) => {}
            // Promote to Identified from anything else.
            (_, IdentifyOutcome::Identified(_)) => *self = other,
            // NoMatch promotes Unsupported but cannot downgrade itself.
            (IdentifyOutcome::Unsupported, IdentifyOutcome::NoMatch) => *self = other,
            _ => {}
        }
    }
}

fn run_on_wasi_thread<F, T>(task_name: &'static str, task: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let builder = std::thread::Builder::new().name(format!("greentic-wasmtime-{task_name}"));
    let handle = builder
        .spawn(move || {
            let pid = std::process::id();
            let thread_id = std::thread::current().id();
            let tokio_handle_present = tokio::runtime::Handle::try_current().is_ok();
            tracing::info!(
                event = "wasmtime.thread.start",
                task = task_name,
                pid,
                thread_id = ?thread_id,
                tokio_handle_present,
                "starting Wasmtime thread"
            );
            task()
        })
        .context("failed to spawn Wasmtime thread")?;
    handle
        .join()
        .map_err(|err| {
            let reason = if let Some(msg) = err.downcast_ref::<&str>() {
                msg.to_string()
            } else if let Some(msg) = err.downcast_ref::<String>() {
                msg.clone()
            } else {
                "unknown panic".to_string()
            };
            anyhow!("Wasmtime thread panicked: {reason}")
        })
        .and_then(|res| res)
}

#[derive(Debug, Default, Clone)]
pub struct ComponentResolution {
    /// Root of a materialized pack directory containing `manifest.cbor` and `components/`.
    pub materialized_root: Option<PathBuf>,
    /// Explicit overrides mapping component id -> wasm path.
    pub overrides: HashMap<String, PathBuf>,
    /// If true, do not fetch remote components; require cached artifacts.
    pub dist_offline: bool,
    /// Optional cache directory for resolved remote components.
    pub dist_cache_dir: Option<PathBuf>,
    /// Allow bundled components without wasm_sha256 (dev-only escape hatch).
    pub allow_missing_hash: bool,
}

fn build_blocking_client() -> BlockingClient {
    std::thread::spawn(|| {
        BlockingClient::builder()
            .no_proxy()
            .build()
            .expect("blocking client")
    })
    .join()
    .expect("client build thread panicked")
}

fn normalize_pack_path(path: &Path) -> Result<(PathBuf, PathBuf)> {
    let (root, candidate) = if path.is_absolute() {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("pack path {} has no parent", path.display()))?;
        let root = parent
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", parent.display()))?;
        let file = path
            .file_name()
            .ok_or_else(|| anyhow!("pack path {} has no file name", path.display()))?;
        (root, PathBuf::from(file))
    } else {
        let cwd = std::env::current_dir().context("failed to resolve current directory")?;
        let base = if let Some(parent) = path.parent() {
            cwd.join(parent)
        } else {
            cwd
        };
        let root = base
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", base.display()))?;
        let file = path
            .file_name()
            .ok_or_else(|| anyhow!("pack path {} has no file name", path.display()))?;
        (root, PathBuf::from(file))
    };
    let safe = normalize_under_root(&root, &candidate)?;
    Ok((root, safe))
}

static HTTP_CLIENT: Lazy<Arc<BlockingClient>> = Lazy::new(|| Arc::new(build_blocking_client()));

/// Default for [`FlowDescriptor::entry`]. A flow is treated as an entrypoint
/// unless it is explicitly tagged `internal`, so packs (and serialized
/// descriptors) that predate the `entry` field keep their prior behaviour of
/// every flow being externally routable.
fn default_flow_entry() -> bool {
    true
}

/// A flow is an *entry* flow — a valid target for an inbound provider event
/// resolved by flow type — unless it is tagged `internal`. Internal flows are
/// only reachable via `flow.call` (e.g. a dispatcher's sub-flows) and must
/// never be selected for type-only routing. Untagged flows default to entry,
/// preserving behaviour for packs that don't declare the distinction.
fn tags_indicate_entry<'a, I>(tags: I) -> bool
where
    I: IntoIterator<Item = &'a str>,
{
    !tags.into_iter().any(|tag| tag == "internal")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDescriptor {
    pub id: String,
    #[serde(rename = "type")]
    pub flow_type: String,
    pub pack_id: String,
    pub profile: String,
    pub version: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Whether this flow is an entrypoint (see [`tags_indicate_entry`]).
    #[serde(default = "default_flow_entry")]
    pub entry: bool,
}

pub struct HostState {
    #[allow(dead_code)]
    pack_id: String,
    config: Arc<HostConfig>,
    http_client: Arc<BlockingClient>,
    default_env: String,
    #[allow(dead_code)]
    session_store: Option<DynSessionStore>,
    state_store: Option<DynStateStore>,
    mocks: Option<Arc<MockLayer>>,
    secrets: DynSecretsManager,
    oauth_config: Option<OAuthBrokerConfig>,
    exec_ctx: Option<ComponentExecCtx>,
    component_ref: Option<String>,
    provider_core_component: bool,
    /// `pack-config.v1.non_secret` map for the `greentic:runtime-config@1.0.0`
    /// host import. Populated by the producer (greentic-start) from the
    /// deployed `PackConfig`; `None` when no PackConfig was published, in
    /// which case lookups fall back to the secrets-store compat shim with
    /// a once-per-process deprecation warning.
    runtime_config_non_secret: Option<Arc<BTreeMap<String, Value>>>,
    /// `pack-config.v1.runtime_refs` (C5) injection: per-pack `key → URI`
    /// bindings plus the env-shared resolver. The host import resolves the
    /// URI on every call so the value tracks `runtime.json` hot-reloads.
    runtime_refs: Option<RuntimeRefsInjection>,
}

impl HostState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pack_id: String,
        config: Arc<HostConfig>,
        http_client: Arc<BlockingClient>,
        mocks: Option<Arc<MockLayer>>,
        session_store: Option<DynSessionStore>,
        state_store: Option<DynStateStore>,
        secrets: DynSecretsManager,
        oauth_config: Option<OAuthBrokerConfig>,
        exec_ctx: Option<ComponentExecCtx>,
        component_ref: Option<String>,
        provider_core_component: bool,
        runtime_config_non_secret: Option<Arc<BTreeMap<String, Value>>>,
        runtime_refs: Option<RuntimeRefsInjection>,
    ) -> Result<Self> {
        let default_env = std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_string());
        Ok(Self {
            pack_id,
            config,
            http_client,
            default_env,
            session_store,
            state_store,
            mocks,
            secrets,
            oauth_config,
            exec_ctx,
            component_ref,
            provider_core_component,
            runtime_config_non_secret,
            runtime_refs,
        })
    }

    fn instantiate_component_result(
        linker: &mut Linker<ComponentState>,
        store: &mut Store<ComponentState>,
        component: &Component,
        ctx: &ComponentExecCtx,
        component_ref: &str,
        operation: &str,
        input_json: &str,
    ) -> Result<InvokeResult> {
        let pre_instance = linker.instantiate_pre(component)?;
        match component_api::v0_6::ComponentPre::new(pre_instance) {
            Ok(pre) => {
                let envelope = component_api::envelope_v0_6(ctx, component_ref, input_json)?;
                let operation_owned = operation.to_string();
                let result = block_on(async {
                    let bindings = pre.instantiate_async(&mut *store).await?;
                    let node = bindings.greentic_component_node();
                    node.call_invoke(&mut *store, &operation_owned, &envelope)
                })?;
                component_api::invoke_result_from_v0_6(result)
            }
            Err(err_v06) => {
                if !is_missing_node_export(&err_v06, "0.6.0") {
                    return Err(err_v06.into());
                }
                let pre_instance = linker.instantiate_pre(component)?;
                match component_api::v0_5::ComponentPre::new(pre_instance) {
                    Ok(pre) => {
                        let result = block_on(async {
                            let bindings = pre.instantiate_async(&mut *store).await?;
                            let node = bindings.greentic_component_node();
                            let ctx_v05 = component_api::exec_ctx_v0_5(ctx);
                            let operation_owned = operation.to_string();
                            let input_owned = input_json.to_string();
                            node.call_invoke(&mut *store, &ctx_v05, &operation_owned, &input_owned)
                        })?;
                        Ok(component_api::invoke_result_from_v0_5(result))
                    }
                    Err(err) => {
                        if !is_missing_node_export(&err, "0.5.0") {
                            return Err(err.into());
                        }
                        let pre_instance = linker.instantiate_pre(component)?;
                        match component_api::v0_4::ComponentPre::new(pre_instance) {
                            Ok(pre) => {
                                let result = block_on(async {
                                    let bindings = pre.instantiate_async(&mut *store).await?;
                                    let node = bindings.greentic_component_node();
                                    let ctx_v04 = component_api::exec_ctx_v0_4(ctx);
                                    let operation_owned = operation.to_string();
                                    let input_owned = input_json.to_string();
                                    node.call_invoke(
                                        &mut *store,
                                        &ctx_v04,
                                        &operation_owned,
                                        &input_owned,
                                    )
                                })?;
                                Ok(component_api::invoke_result_from_v0_4(result))
                            }
                            Err(err_v04) => {
                                if is_missing_node_export(&err_v04, "0.4.0") {
                                    Self::try_v06_runtime(linker, store, component, input_json)
                                } else {
                                    Err(err_v04.into())
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Fallback for v0.6 components that export `component-runtime::run(input, state)`
    /// instead of the legacy `node::invoke(ctx, op, input)`.
    fn try_v06_runtime(
        linker: &mut Linker<ComponentState>,
        store: &mut Store<ComponentState>,
        component: &Component,
        input_json: &str,
    ) -> Result<InvokeResult> {
        let pre_instance = linker.instantiate_pre(component)?;
        let pre = component_api::v0_6_runtime::ComponentV0V6RuntimePre::new(pre_instance).map_err(
            |err| err.context("component exports neither node@0.5/0.4 nor component-runtime@0.6"),
        )?;

        let result = block_on(async {
            let bindings = pre.instantiate_async(&mut *store).await?;
            let runtime = bindings.greentic_component_component_runtime();

            // Encode input as CBOR — the component's run() expects CBOR bytes.
            let input_value: Value = serde_json::from_str(input_json).unwrap_or(Value::Null);
            let input_cbor =
                serde_cbor::to_vec(&input_value).context("encode input as CBOR for v0.6")?;
            let empty_state = serde_cbor::to_vec(&Value::Object(Default::default()))
                .context("encode empty state")?;

            let run_result = runtime
                .call_run(&mut *store, &input_cbor, &empty_state)
                .map_err(|err| err.context("v0.6 component-runtime::run call failed"))?;

            // Decode output CBOR to JSON.
            let output_value: Value = serde_cbor::from_slice(&run_result.output)
                .context("decode v0.6 run output CBOR")?;
            let output_json = serde_json::to_string(&output_value)
                .context("serialize v0.6 run output to JSON")?;

            Ok::<_, anyhow::Error>(output_json)
        })?;

        Ok(InvokeResult::Ok(result))
    }

    fn convert_invoke_result(result: InvokeResult) -> Result<Value> {
        match result {
            InvokeResult::Ok(body) => {
                if body.is_empty() {
                    return Ok(Value::Null);
                }
                serde_json::from_str(&body).or_else(|_| Ok(Value::String(body)))
            }
            InvokeResult::Err(NodeError {
                code,
                message,
                retryable,
                backoff_ms,
                details,
            }) => {
                let mut obj = serde_json::Map::new();
                obj.insert("ok".into(), Value::Bool(false));
                let mut error = serde_json::Map::new();
                error.insert("code".into(), Value::String(code));
                error.insert("message".into(), Value::String(message));
                error.insert("retryable".into(), Value::Bool(retryable));
                if let Some(backoff) = backoff_ms {
                    error.insert("backoff_ms".into(), Value::Number(backoff.into()));
                }
                if let Some(details) = details {
                    error.insert(
                        "details".into(),
                        serde_json::from_str(&details).unwrap_or(Value::String(details)),
                    );
                }
                obj.insert("error".into(), Value::Object(error));
                Ok(Value::Object(obj))
            }
        }
    }

    /// Build a `TenantCtx` for secrets lookups that includes the team from the
    /// execution context. `config.tenant_ctx()` only populates env + tenant;
    /// without this, secrets scoped to a specific team are unreachable.
    fn secrets_tenant_ctx(&self) -> TypesTenantCtx {
        let mut ctx = self.config.tenant_ctx();
        if let Some(exec_ctx) = self.exec_ctx.as_ref()
            && let Some(team) = exec_ctx.tenant.team.as_ref()
            && let Ok(team_id) = TeamId::from_str(team)
        {
            ctx = ctx.with_team(Some(team_id));
        }
        ctx
    }

    pub fn get_secret(&self, key: &str) -> Result<String> {
        if provider_core_only::is_enabled() {
            bail!(provider_core_only::blocked_message("secrets"))
        }
        if !self.config.secrets_policy.is_allowed(key) {
            bail!("secret {key} is not permitted by bindings policy");
        }
        if let Some(mock) = &self.mocks
            && let Some(value) = mock.secrets_lookup(key)
        {
            return Ok(value);
        }
        let ctx = self.secrets_tenant_ctx();
        let canonical_key = canonicalize_secret_key(key);
        let bytes = read_secret_blocking(&self.secrets, &ctx, &self.pack_id, &canonical_key)
            .context("failed to read secret from manager")?;
        let value = String::from_utf8(bytes).context("secret value is not valid UTF-8")?;
        Ok(value)
    }

    fn allows_secret_write_in_provider_core_only(&self) -> bool {
        self.provider_core_component || self.component_ref.is_none()
    }

    fn tenant_ctx_from_v1(&self, ctx: Option<StateTenantCtx>) -> Result<TypesTenantCtx> {
        let tenant_raw = ctx
            .as_ref()
            .map(|ctx| ctx.tenant.clone())
            .or_else(|| self.exec_ctx.as_ref().map(|ctx| ctx.tenant.tenant.clone()))
            .unwrap_or_else(|| self.config.tenant.clone());
        let env_raw = ctx
            .as_ref()
            .map(|ctx| ctx.env.clone())
            .unwrap_or_else(|| self.default_env.clone());
        let tenant_id = TenantId::from_str(&tenant_raw)
            .with_context(|| format!("invalid tenant id `{tenant_raw}`"))?;
        let env_id = EnvId::from_str(&env_raw)
            .unwrap_or_else(|_| EnvId::from_str("local").expect("default env must be valid"));
        let mut tenant_ctx = TypesTenantCtx::new(env_id, tenant_id);
        if let Some(exec_ctx) = self.exec_ctx.as_ref() {
            if let Some(team) = exec_ctx.tenant.team.as_ref() {
                let team_id =
                    TeamId::from_str(team).with_context(|| format!("invalid team id `{team}`"))?;
                tenant_ctx = tenant_ctx.with_team(Some(team_id));
            }
            if let Some(user) = exec_ctx.tenant.user.as_ref() {
                let user_id =
                    UserId::from_str(user).with_context(|| format!("invalid user id `{user}`"))?;
                tenant_ctx = tenant_ctx.with_user(Some(user_id));
            }
            tenant_ctx = tenant_ctx.with_flow(exec_ctx.flow_id.clone());
            if let Some(node) = exec_ctx.node_id.as_ref() {
                tenant_ctx = tenant_ctx.with_node(node.clone());
            }
            if let Some(session) = exec_ctx.tenant.correlation_id.as_ref() {
                tenant_ctx = tenant_ctx.with_session(session.clone());
            }
            tenant_ctx.trace_id = exec_ctx.tenant.trace_id.clone();
        }

        if let Some(ctx) = ctx {
            if let Some(team) = ctx.team.or(ctx.team_id) {
                let team_id =
                    TeamId::from_str(&team).with_context(|| format!("invalid team id `{team}`"))?;
                tenant_ctx = tenant_ctx.with_team(Some(team_id));
            }
            if let Some(user) = ctx.user.or(ctx.user_id) {
                let user_id =
                    UserId::from_str(&user).with_context(|| format!("invalid user id `{user}`"))?;
                tenant_ctx = tenant_ctx.with_user(Some(user_id));
            }
            if let Some(flow) = ctx.flow_id {
                tenant_ctx = tenant_ctx.with_flow(flow);
            }
            if let Some(node) = ctx.node_id {
                tenant_ctx = tenant_ctx.with_node(node);
            }
            if let Some(provider) = ctx.provider_id {
                tenant_ctx = tenant_ctx.with_provider(provider);
            }
            if let Some(session) = ctx.session_id {
                tenant_ctx = tenant_ctx.with_session(session);
            }
            tenant_ctx.trace_id = ctx.trace_id;
        }
        Ok(tenant_ctx)
    }

    fn send_http_request(
        &mut self,
        req: HttpRequest,
        opts: Option<HttpRequestOptionsV1_1>,
        _ctx: Option<HttpTenantCtx>,
    ) -> Result<HttpResponse, HttpClientError> {
        if !self.config.http_enabled {
            return Err(HttpClientError {
                code: "denied".into(),
                message: "http client disabled by policy".into(),
            });
        }

        let mut mock_state = None;
        let raw_body = req.body.clone();
        if let Some(mock) = &self.mocks
            && let Ok(meta) = HttpMockRequest::new(&req.method, &req.url, raw_body.as_deref())
        {
            match mock.http_begin(&meta) {
                HttpDecision::Mock(response) => {
                    let headers = response
                        .headers
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    return Ok(HttpResponse {
                        status: response.status,
                        headers,
                        body: response.body.clone().map(|b| b.into_bytes()),
                    });
                }
                HttpDecision::Deny(reason) => {
                    return Err(HttpClientError {
                        code: "denied".into(),
                        message: reason,
                    });
                }
                HttpDecision::Passthrough { record } => {
                    mock_state = Some((meta, record));
                }
            }
        }

        let method = req.method.parse().unwrap_or(reqwest::Method::GET);
        let mut builder = self.http_client.request(method, &req.url);
        for (key, value) in req.headers {
            if let Ok(header) = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                && let Ok(header_value) = reqwest::header::HeaderValue::from_str(&value)
            {
                builder = builder.header(header, header_value);
            }
        }

        if let Some(body) = raw_body.clone() {
            builder = builder.body(body);
        }

        if let Some(opts) = opts {
            if let Some(timeout_ms) = opts.timeout_ms {
                builder = builder.timeout(Duration::from_millis(timeout_ms as u64));
            }
            if opts.allow_insecure == Some(true) {
                warn!(url = %req.url, "allow-insecure not supported; using default TLS validation");
            }
            if let Some(follow_redirects) = opts.follow_redirects
                && !follow_redirects
            {
                warn!(url = %req.url, "follow-redirects=false not supported; using default client behaviour");
            }
        }

        let response = match builder.send() {
            Ok(resp) => resp,
            Err(err) => {
                warn!(url = %req.url, error = %err, "http client request failed");
                return Err(HttpClientError {
                    code: "unavailable".into(),
                    message: err.to_string(),
                });
            }
        };

        let status = response.status().as_u16();
        let headers_vec = response
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    v.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let body_bytes = response.bytes().ok().map(|b| b.to_vec());

        if let Some((meta, true)) = mock_state.take()
            && let Some(mock) = &self.mocks
        {
            let recorded = HttpMockResponse::new(
                status,
                headers_vec.clone().into_iter().collect(),
                body_bytes
                    .as_ref()
                    .map(|b| String::from_utf8_lossy(b).into_owned()),
            );
            mock.http_record(&meta, &recorded);
        }

        Ok(HttpResponse {
            status,
            headers: headers_vec,
            body: body_bytes,
        })
    }
}

#[cfg(test)]
mod canonicalize_tests {
    use crate::secrets::canonicalize_secret_key;

    #[test]
    fn upper_snake_to_lower_snake() {
        assert_eq!(
            canonicalize_secret_key("TELEGRAM_BOT_TOKEN"),
            "telegram_bot_token"
        );
    }

    #[test]
    fn trim_and_replace_non_alphanumeric() {
        assert_eq!(
            canonicalize_secret_key("  webex-bot-token  "),
            "webex_bot_token"
        );
    }

    #[test]
    fn preserve_existing_lower_snake_with_extra_underscores() {
        assert_eq!(canonicalize_secret_key("MiXeD__Case"), "mixed__case");
    }
}

impl SecretsStoreHost for HostState {
    fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, SecretsError> {
        if provider_core_only::is_enabled() {
            warn!(secret = %key, "provider-core only mode enabled; blocking secrets store");
            return Err(SecretsError::Denied);
        }
        if !self.config.secrets_policy.is_allowed(&key) {
            return Err(SecretsError::Denied);
        }
        if let Some(mock) = &self.mocks
            && let Some(value) = mock.secrets_lookup(&key)
        {
            return Ok(Some(value.into_bytes()));
        }
        let ctx = self.secrets_tenant_ctx();
        let canonical_key = canonicalize_secret_key(&key);
        match read_secret_blocking(&self.secrets, &ctx, &self.pack_id, &canonical_key) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) => {
                warn!(secret = %key, canonical = %canonical_key, error = %err, "secret lookup failed");
                Err(SecretsError::NotFound)
            }
        }
    }
}

impl SecretsStoreHostV1_1 for HostState {
    fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, SecretsErrorV1_1> {
        if provider_core_only::is_enabled() {
            warn!(secret = %key, "provider-core only mode enabled; blocking secrets store");
            return Err(SecretsErrorV1_1::Denied);
        }
        if !self.config.secrets_policy.is_allowed(&key) {
            return Err(SecretsErrorV1_1::Denied);
        }
        if let Some(mock) = &self.mocks
            && let Some(value) = mock.secrets_lookup(&key)
        {
            return Ok(Some(value.into_bytes()));
        }
        let ctx = self.secrets_tenant_ctx();
        let canonical_key = canonicalize_secret_key(&key);
        match read_secret_blocking(&self.secrets, &ctx, &self.pack_id, &canonical_key) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) => {
                warn!(secret = %key, canonical = %canonical_key, error = %err, "secret lookup failed");
                Err(SecretsErrorV1_1::NotFound)
            }
        }
    }

    fn put(&mut self, key: String, value: Vec<u8>) {
        if key.trim().is_empty() {
            warn!(secret = %key, "secret write blocked: empty key");
            panic!("secret write denied for key {key}: invalid key");
        }
        if provider_core_only::is_enabled() && !self.allows_secret_write_in_provider_core_only() {
            warn!(
                secret = %key,
                component = self.component_ref.as_deref().unwrap_or("<pack>"),
                "provider-core only mode enabled; blocking secrets store write"
            );
            panic!("secret write denied for key {key}: provider-core-only mode");
        }
        if !self.config.secrets_policy.is_allowed(&key) {
            warn!(secret = %key, "secret write denied by bindings policy");
            panic!("secret write denied for key {key}: policy");
        }
        let ctx = self.secrets_tenant_ctx();
        let canonical_key = canonicalize_secret_key(&key);
        if let Err(err) =
            write_secret_blocking(&self.secrets, &ctx, &self.pack_id, &canonical_key, &value)
        {
            warn!(secret = %key, canonical = %canonical_key, error = %err, "secret write failed");
            panic!("secret write failed for key {key}");
        }
    }
}

/// Process-global set of `pack-config.v1` keys for which the compat shim has
/// already logged a deprecation warning. Used to debounce once-per-process
/// per-key so resolving the same legacy key from many invocations does not
/// spam the log.
static WARNED_COMPAT_KEYS: Lazy<Mutex<HashSet<String>>> = Lazy::new(|| Mutex::new(HashSet::new()));

fn warn_compat_fallback_once(key: &str) {
    let mut warned = WARNED_COMPAT_KEYS.lock();
    if warned.insert(key.to_string()) {
        warn!(
            key = %key,
            "runtime-config key resolved via secrets-store compat fallback; \
             move this value into pack-config.v1.non_secret"
        );
    }
}

impl RuntimeConfigHost for HostState {
    fn get(&mut self, key: String) -> Result<Option<String>, ConfigError> {
        if key.trim().is_empty() {
            return Err(ConfigError::InvalidKey);
        }

        // 1) Primary channel: pack-config.v1.non_secret. Values are stored as
        //    `serde_json::Value`; the WIT contract returns UTF-8 strings
        //    conventionally JSON-encoded, so stringify here.
        if let Some(map) = self.runtime_config_non_secret.as_ref()
            && let Some(value) = map.get(&key)
        {
            return serde_json::to_string(value).map(Some).map_err(|err| {
                warn!(key = %key, error = %err, "runtime-config value JSON-encode failed");
                ConfigError::Internal
            });
        }

        // 1b) C5 channel: pack-config.v1.runtime_refs. Resolved on every call
        //     so values track `runtime.json` hot-reloads. The per-pack `refs`
        //     map gates which keys this channel claims; non-bound keys fall
        //     through to the compat shim.
        if let Some(injection) = self.runtime_refs.as_ref()
            && let Some(uri) = injection.refs.get(&key)
        {
            use crate::runtime_refs::RuntimeRefResolverError;
            return match injection.resolver.resolve(uri) {
                Ok(Some(value)) => serde_json::to_string(&value).map(Some).map_err(|err| {
                    warn!(key = %key, error = %err, "runtime-ref value JSON-encode failed");
                    ConfigError::Internal
                }),
                Ok(None) => Ok(None),
                Err(err @ RuntimeRefResolverError::Invalid(_)) => {
                    warn!(key = %key, error = %err, "runtime-ref rejected");
                    Err(ConfigError::InvalidKey)
                }
                Err(err @ RuntimeRefResolverError::Internal(_)) => {
                    warn!(key = %key, error = %err, "runtime-ref resolution failed");
                    Err(ConfigError::Internal)
                }
            };
        }

        // 2) Compat fallback: try the secrets-store. Warn once per key per
        //    process so this stays visible without spamming the log.
        match SecretsStoreHost::get(self, key.clone()) {
            Ok(Some(bytes)) => match String::from_utf8(bytes) {
                Ok(value) => {
                    warn_compat_fallback_once(&key);
                    Ok(Some(value))
                }
                Err(_) => {
                    warn!(
                        key = %key,
                        "runtime-config compat fallback found non-UTF-8 secret bytes; \
                         returning not-found"
                    );
                    Err(ConfigError::Internal)
                }
            },
            Ok(None) => Ok(None),
            Err(SecretsError::NotFound) => Ok(None),
            Err(SecretsError::Denied) => Err(ConfigError::Denied),
            Err(SecretsError::InvalidKey) => Err(ConfigError::InvalidKey),
            Err(SecretsError::Internal) => Err(ConfigError::Internal),
        }
    }
}

impl HttpClientHost for HostState {
    fn send(
        &mut self,
        req: HttpRequest,
        ctx: Option<HttpTenantCtx>,
    ) -> Result<HttpResponse, HttpClientError> {
        self.send_http_request(req, None, ctx)
    }
}

impl HttpClientHostV1_1 for HostState {
    fn send(
        &mut self,
        req: HttpRequestV1_1,
        opts: Option<HttpRequestOptionsV1_1>,
        ctx: Option<HttpTenantCtxV1_1>,
    ) -> Result<HttpResponseV1_1, HttpClientErrorV1_1> {
        let legacy_req = HttpRequest {
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: req.body,
        };
        let legacy_ctx = ctx.map(|ctx| HttpTenantCtx {
            env: ctx.env,
            tenant: ctx.tenant,
            tenant_id: ctx.tenant_id,
            team: ctx.team,
            team_id: ctx.team_id,
            user: ctx.user,
            user_id: ctx.user_id,
            trace_id: ctx.trace_id,
            correlation_id: ctx.correlation_id,
            i18n_id: ctx.i18n_id,
            attributes: ctx.attributes,
            session_id: ctx.session_id,
            flow_id: ctx.flow_id,
            node_id: ctx.node_id,
            provider_id: ctx.provider_id,
            deadline_ms: ctx.deadline_ms,
            attempt: ctx.attempt,
            idempotency_key: ctx.idempotency_key,
            impersonation: ctx.impersonation.map(|imp| http_types_v1_0::Impersonation {
                actor_id: imp.actor_id,
                reason: imp.reason,
            }),
        });

        self.send_http_request(legacy_req, opts, legacy_ctx)
            .map(|resp| HttpResponseV1_1 {
                status: resp.status,
                headers: resp.headers,
                body: resp.body,
            })
            .map_err(|err| HttpClientErrorV1_1 {
                code: err.code,
                message: err.message,
            })
    }
}

impl StateStoreHost for HostState {
    fn read(
        &mut self,
        key: HostStateKey,
        ctx: Option<StateTenantCtx>,
    ) -> Result<Vec<u8>, StateError> {
        let store = match self.state_store.as_ref() {
            Some(store) => store.clone(),
            None => {
                return Err(StateError {
                    code: "unavailable".into(),
                    message: "state store not configured".into(),
                });
            }
        };
        let tenant_ctx = match self.tenant_ctx_from_v1(ctx) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(StateError {
                    code: "invalid-ctx".into(),
                    message: err.to_string(),
                });
            }
        };
        #[cfg(feature = "fault-injection")]
        {
            let exec_ctx = self.exec_ctx.as_ref();
            let flow_id = exec_ctx
                .map(|ctx| ctx.flow_id.as_str())
                .unwrap_or("unknown");
            let node_id = exec_ctx.and_then(|ctx| ctx.node_id.as_deref());
            let attempt = exec_ctx.map(|ctx| ctx.tenant.attempt).unwrap_or(1);
            let fault_ctx = FaultContext {
                pack_id: self.pack_id.as_str(),
                flow_id,
                node_id,
                attempt,
            };
            if let Err(err) = maybe_fail(FaultPoint::StateRead, fault_ctx) {
                return Err(StateError {
                    code: "internal".into(),
                    message: err.to_string(),
                });
            }
        }
        let key = StoreStateKey::from(key);
        match store.get_json(&tenant_ctx, STATE_PREFIX, &key, None) {
            Ok(Some(value)) => Ok(serde_json::to_vec(&value).unwrap_or_else(|_| Vec::new())),
            Ok(None) => Err(StateError {
                code: "not_found".into(),
                message: "state key not found".into(),
            }),
            Err(err) => Err(StateError {
                code: "internal".into(),
                message: err.to_string(),
            }),
        }
    }

    fn write(
        &mut self,
        key: HostStateKey,
        bytes: Vec<u8>,
        ctx: Option<StateTenantCtx>,
    ) -> Result<StateOpAck, StateError> {
        let store = match self.state_store.as_ref() {
            Some(store) => store.clone(),
            None => {
                return Err(StateError {
                    code: "unavailable".into(),
                    message: "state store not configured".into(),
                });
            }
        };
        let tenant_ctx = match self.tenant_ctx_from_v1(ctx) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(StateError {
                    code: "invalid-ctx".into(),
                    message: err.to_string(),
                });
            }
        };
        #[cfg(feature = "fault-injection")]
        {
            let exec_ctx = self.exec_ctx.as_ref();
            let flow_id = exec_ctx
                .map(|ctx| ctx.flow_id.as_str())
                .unwrap_or("unknown");
            let node_id = exec_ctx.and_then(|ctx| ctx.node_id.as_deref());
            let attempt = exec_ctx.map(|ctx| ctx.tenant.attempt).unwrap_or(1);
            let fault_ctx = FaultContext {
                pack_id: self.pack_id.as_str(),
                flow_id,
                node_id,
                attempt,
            };
            if let Err(err) = maybe_fail(FaultPoint::StateWrite, fault_ctx) {
                return Err(StateError {
                    code: "internal".into(),
                    message: err.to_string(),
                });
            }
        }
        let key = StoreStateKey::from(key);
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()));
        match store.set_json(&tenant_ctx, STATE_PREFIX, &key, None, &value, None) {
            Ok(()) => Ok(StateOpAck::Ok),
            Err(err) => Err(StateError {
                code: "internal".into(),
                message: err.to_string(),
            }),
        }
    }

    fn delete(
        &mut self,
        key: HostStateKey,
        ctx: Option<StateTenantCtx>,
    ) -> Result<StateOpAck, StateError> {
        let store = match self.state_store.as_ref() {
            Some(store) => store.clone(),
            None => {
                return Err(StateError {
                    code: "unavailable".into(),
                    message: "state store not configured".into(),
                });
            }
        };
        let tenant_ctx = match self.tenant_ctx_from_v1(ctx) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(StateError {
                    code: "invalid-ctx".into(),
                    message: err.to_string(),
                });
            }
        };
        let key = StoreStateKey::from(key);
        match store.del(&tenant_ctx, STATE_PREFIX, &key) {
            Ok(_) => Ok(StateOpAck::Ok),
            Err(err) => Err(StateError {
                code: "internal".into(),
                message: err.to_string(),
            }),
        }
    }
}

impl TelemetryLoggerHost for HostState {
    fn log(
        &mut self,
        span: TelemetrySpanContext,
        fields: Vec<(String, String)>,
        _ctx: Option<TelemetryTenantCtx>,
    ) -> Result<TelemetryAck, TelemetryError> {
        if let Some(mock) = &self.mocks
            && mock.telemetry_drain(&[("span_json", span.flow_id.as_str())])
        {
            return Ok(TelemetryAck::Ok);
        }
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert(k, Value::String(v));
        }
        tracing::info!(
            tenant = %span.tenant,
            flow_id = %span.flow_id,
            node = ?span.node_id,
            provider = %span.provider,
            fields = %serde_json::Value::Object(map.clone()),
            "telemetry log from pack"
        );
        Ok(TelemetryAck::Ok)
    }
}

impl RunnerHostHttp for HostState {
    fn request(
        &mut self,
        method: String,
        url: String,
        headers: Vec<String>,
        body: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, String> {
        let req = HttpRequest {
            method,
            url,
            headers: headers
                .chunks(2)
                .filter_map(|chunk| {
                    if chunk.len() == 2 {
                        Some((chunk[0].clone(), chunk[1].clone()))
                    } else {
                        None
                    }
                })
                .collect(),
            body,
        };
        match HttpClientHost::send(self, req, None) {
            Ok(resp) => Ok(resp.body.unwrap_or_default()),
            Err(err) => Err(err.message),
        }
    }
}

impl RunnerHostKv for HostState {
    fn get(&mut self, _ns: String, _key: String) -> Option<String> {
        None
    }

    fn put(&mut self, _ns: String, _key: String, _val: String) {}
}

impl OAuthBrokerHostTrait for HostState {
    /// Returns an access-token JSON string (`{"access_token":…,"expires_at":…}`) for the given
    /// provider and scopes, or an empty string on error.
    ///
    /// `subject` is informational for MVP — tenant, env, and team are taken from the host context
    /// (config + default_env + oauth_config.team), NOT derived from `subject`.
    fn get_token(
        &mut self,
        provider_id: wasmtime::component::__internal::String,
        _subject: wasmtime::component::__internal::String,
        scopes: wasmtime::component::__internal::Vec<wasmtime::component::__internal::String>,
    ) -> wasmtime::component::__internal::String {
        let Some(cfg) = self.oauth_config.clone() else {
            return String::new();
        };
        let req = ResourceTokenRequest {
            http_base_url: cfg.http_base_url,
            env: self.default_env.clone(),
            tenant: self.config.tenant.clone(),
            team: cfg.team.clone(),
            resource_id: provider_id.to_string(),
            scopes: scopes.into_iter().collect(),
        };
        match crate::oauth::request_resource_token_blocking(
            &self.http_client,
            &req,
            cfg.shared_secret.as_deref(),
        ) {
            Ok(resp) => serde_json::to_string(&resp).unwrap_or_default(),
            Err(e) => {
                tracing::warn!(
                    provider = %provider_id,
                    error = %e,
                    "oauth get-token proxy failed"
                );
                String::new()
            }
        }
    }

    /// Operator-time only — OAuth consent URL generation happens in the admin UI, not at runtime.
    fn get_consent_url(
        &mut self,
        _provider_id: wasmtime::component::__internal::String,
        _subject: wasmtime::component::__internal::String,
        _scopes: wasmtime::component::__internal::Vec<wasmtime::component::__internal::String>,
        _redirect_path: wasmtime::component::__internal::String,
        _extra_json: wasmtime::component::__internal::String,
    ) -> wasmtime::component::__internal::String {
        String::new()
    }

    /// Operator-time only — OAuth code exchange happens in the admin UI, not at runtime.
    fn exchange_code(
        &mut self,
        _provider_id: wasmtime::component::__internal::String,
        _subject: wasmtime::component::__internal::String,
        _code: wasmtime::component::__internal::String,
        _redirect_path: wasmtime::component::__internal::String,
    ) -> wasmtime::component::__internal::String {
        String::new()
    }
}

enum ManifestLoad {
    New {
        manifest: Box<greentic_types::PackManifest>,
        flows: PackFlows,
    },
    Legacy {
        manifest: Box<legacy_pack::PackManifest>,
        flows: PackFlows,
    },
}

fn load_manifest_and_flows(path: &Path) -> Result<ManifestLoad> {
    let mut archive = ZipArchive::new(File::open(path)?)
        .with_context(|| format!("{} is not a valid gtpack", path.display()))?;
    let bytes = read_entry(&mut archive, "manifest.cbor")
        .with_context(|| format!("missing manifest.cbor in {}", path.display()))?;
    match decode_pack_manifest(&bytes) {
        Ok(manifest) => {
            let cache = PackFlows::from_manifest(manifest.clone());
            Ok(ManifestLoad::New {
                manifest: Box::new(manifest),
                flows: cache,
            })
        }
        Err(err) => {
            tracing::debug!(
                error = %err,
                pack = %path.display(),
                "decode_pack_manifest failed for archive; falling back to legacy manifest"
            );
            let legacy: legacy_pack::PackManifest = serde_cbor::from_slice(&bytes)
                .context("failed to decode legacy pack manifest from manifest.cbor")?;
            let flows = load_legacy_flows_from_archive(&mut archive, &legacy)?;
            Ok(ManifestLoad::Legacy {
                manifest: Box::new(legacy),
                flows,
            })
        }
    }
}

fn load_manifest_and_flows_from_dir(root: &Path) -> Result<ManifestLoad> {
    let manifest_path = root.join("manifest.cbor");
    let bytes = std::fs::read(&manifest_path)
        .with_context(|| format!("missing manifest.cbor in {}", root.display()))?;
    match decode_pack_manifest(&bytes) {
        Ok(manifest) => {
            let cache = PackFlows::from_manifest(manifest.clone());
            Ok(ManifestLoad::New {
                manifest: Box::new(manifest),
                flows: cache,
            })
        }
        Err(err) => {
            tracing::debug!(
                error = %err,
                pack = %root.display(),
                "decode_pack_manifest failed for materialized pack; trying legacy manifest"
            );
            let legacy: legacy_pack::PackManifest = serde_cbor::from_slice(&bytes)
                .context("failed to decode legacy pack manifest from manifest.cbor")?;
            let flows = load_legacy_flows_from_dir(root, &legacy)?;
            Ok(ManifestLoad::Legacy {
                manifest: Box::new(legacy),
                flows,
            })
        }
    }
}

fn load_legacy_flows_from_dir(
    root: &Path,
    manifest: &legacy_pack::PackManifest,
) -> Result<PackFlows> {
    build_legacy_flows(manifest, |rel_path| {
        let path = root.join(rel_path);
        std::fs::read(&path).with_context(|| format!("missing flow json {}", path.display()))
    })
}

fn load_legacy_flows_from_archive(
    archive: &mut ZipArchive<File>,
    manifest: &legacy_pack::PackManifest,
) -> Result<PackFlows> {
    build_legacy_flows(manifest, |rel_path| {
        read_entry(archive, rel_path).with_context(|| format!("missing flow json {}", rel_path))
    })
}

fn build_legacy_flows(
    manifest: &legacy_pack::PackManifest,
    mut read_json: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<PackFlows> {
    let mut flows = HashMap::new();
    let mut descriptors = Vec::new();

    for entry in &manifest.flows {
        let bytes = read_json(&entry.file_json)
            .with_context(|| format!("missing flow json {}", entry.file_json))?;
        let doc = parse_flow_doc_with_legacy_aliases(&bytes)?;
        let normalized = normalize_flow_doc(doc);
        let flow_ir = flow_doc_to_ir(normalized)?;
        let flow = flow_ir_to_flow(flow_ir)?;

        descriptors.push(FlowDescriptor {
            id: entry.id.clone(),
            flow_type: entry.kind.clone(),
            pack_id: manifest.meta.pack_id.clone(),
            profile: manifest.meta.pack_id.clone(),
            version: manifest.meta.version.to_string(),
            description: None,
            // Legacy manifests carry no flow tags; treat every flow as an
            // entrypoint, matching prior behaviour.
            entry: true,
        });
        flows.insert(entry.id.clone(), flow);
    }

    let mut entry_flows = manifest.meta.entry_flows.clone();
    if entry_flows.is_empty() {
        entry_flows = manifest.flows.iter().map(|f| f.id.clone()).collect();
    }
    let metadata = PackMetadata {
        pack_id: manifest.meta.pack_id.clone(),
        version: manifest.meta.version.to_string(),
        entry_flows,
        secret_requirements: Vec::new(),
    };

    Ok(PackFlows {
        descriptors,
        flows,
        metadata,
    })
}

fn parse_flow_doc_with_legacy_aliases(bytes: &[u8]) -> Result<FlowDoc> {
    let mut value: Value =
        serde_json::from_slice(bytes).context("failed to decode flow doc JSON")?;
    if let Some(map) = value.as_object_mut()
        && !map.contains_key("type")
        && let Some(flow_type) = map.remove("flow_type")
    {
        map.insert("type".to_string(), flow_type);
    }
    serde_json::from_value(value).context("failed to decode flow doc structure")
}

pub struct ComponentState {
    pub host: HostState,
    wasi_ctx: WasiCtx,
    wasi_tls_ctx: WasiTlsCtx,
    wasi_http_ctx: WasiHttpCtx,
    resource_table: ResourceTable,
}

/// Install the process-default rustls [`rustls::crypto::CryptoProvider`] exactly once.
///
/// `wasmtime-wasi-tls` 45's `RustlsProvider::default()` builds its
/// `rustls::ClientConfig` via `ClientConfig::builder()`, which resolves the
/// process-default provider. Our dependency graph enables BOTH the `ring` and
/// `aws_lc_rs` rustls backends, so there is no unambiguous implicit default and
/// the builder panics ("no process-level CryptoProvider available") the first
/// time [`WasiTlsCtxBuilder::build`] constructs the default provider. Install
/// the workspace-selected aws-lc-rs provider before that ever happens.
/// Idempotent: a returned `Err` means a default was already installed.
fn install_default_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

impl ComponentState {
    pub fn new(host: HostState, policy: Arc<RunnerWasiPolicy>) -> Result<Self> {
        // Must run before `WasiTlsCtxBuilder::build()` below, which eagerly
        // constructs wasi-tls's default rustls provider.
        install_default_crypto_provider();
        let wasi_ctx = policy
            .instantiate()
            .context("failed to build WASI context")?;
        Ok(Self {
            host,
            wasi_ctx,
            wasi_tls_ctx: WasiTlsCtxBuilder::new().build(),
            wasi_http_ctx: WasiHttpCtx::new(),
            resource_table: ResourceTable::new(),
        })
    }

    fn host_mut(&mut self) -> &mut HostState {
        &mut self.host
    }

    fn should_cancel_host(&mut self) -> bool {
        false
    }

    fn yield_now_host(&mut self) {
        // no-op cooperative yield
    }
}

impl component_api::v0_4::greentic::component::control::Host for ComponentState {
    fn should_cancel(&mut self) -> bool {
        self.should_cancel_host()
    }

    fn yield_now(&mut self) {
        self.yield_now_host();
    }
}

impl component_api::v0_5::greentic::component::control::Host for ComponentState {
    fn should_cancel(&mut self) -> bool {
        self.should_cancel_host()
    }

    fn yield_now(&mut self) {
        self.yield_now_host();
    }
}

fn add_component_control_instance(
    linker: &mut Linker<ComponentState>,
    name: &str,
) -> wasmtime::Result<()> {
    let mut inst = linker.instance(name)?;
    inst.func_wrap(
        "should-cancel",
        |mut caller: StoreContextMut<'_, ComponentState>, (): ()| {
            let host = caller.data_mut();
            Ok((host.should_cancel_host(),))
        },
    )?;
    inst.func_wrap(
        "yield-now",
        |mut caller: StoreContextMut<'_, ComponentState>, (): ()| {
            let host = caller.data_mut();
            host.yield_now_host();
            Ok(())
        },
    )?;
    Ok(())
}

fn add_component_control_to_linker(linker: &mut Linker<ComponentState>) -> wasmtime::Result<()> {
    add_component_control_instance(linker, "greentic:component/control@0.5.0")?;
    add_component_control_instance(linker, "greentic:component/control@0.4.0")?;
    Ok(())
}

/// Reduced-authority linker for `identify-instance` and
/// `describe-identify-instance` probes (M1 IID Phase D).
///
/// Identity probes match an inbound webhook payload against known
/// per-endpoint discriminators (Telegram secret-token header, Slack
/// signing-secret, Teams JWT issuer, …) and return an endpoint id
/// or `none`. The WIT contract is a pure projection over `(headers,
/// body)` — no outbound HTTP, no persistent state, no secrets.
///
/// # Why this delegates to `register_all` (Wasmtime eager-import constraint)
///
/// Wasmtime's [`Linker::instantiate_pre`] type-checks the component's
/// **entire** import graph eagerly — not just the imports reachable from
/// the export the caller intends to invoke. A provider component that
/// exports `instance-identity-api` alongside `schema-core-api` typically
/// also imports `http-client`, `secrets-store`, etc. for the latter.
/// If the linker omits those imports, `instantiate_pre` fails with
/// `"a matching implementation was not found in the linker"` before the
/// identity export is even checked.
///
/// The ideal probe linker would register deny-shim handlers that satisfy
/// the import graph but trap on actual invocation. That requires
/// deny-shim support in `greentic-interfaces-wasmtime` (tracked as a
/// follow-up). Until then, probes use the same linker surface as normal
/// execution, with state-store disabled.
///
/// The reduced-authority boundary is enforced at the WASI policy layer
/// instead: probe call sites construct a locked-down
/// [`RunnerWasiPolicy`](crate::wasi::RunnerWasiPolicy) with no
/// preopens, no env passthrough, and no stdio inheritance. See
/// [`RunnerWasiPolicy::probe()`](crate::wasi::RunnerWasiPolicy::probe)
/// and the `probe_wasi_policy_is_locked_down` test.
pub fn register_identity_probe(linker: &mut Linker<ComponentState>) -> Result<()> {
    // Delegates to `register_all` with state-store disabled. See doc
    // comment above for the rationale (Wasmtime eager-import validation).
    register_all(linker, false)
}

#[cfg(test)]
mod register_identity_probe_tests {
    use super::*;

    /// Verify that [`register_identity_probe`] successfully links all
    /// imports needed by real provider components (wasi-core, wasi-tls,
    /// wasi-http, http-client, secrets-store, telemetry, etc.).
    ///
    /// Before this fix, the probe linker omitted most host imports.
    /// Wasmtime's `instantiate_pre` validates the **entire** import
    /// graph eagerly, so any provider with runtime imports (all real
    /// providers) would fail before the identity export was checked.
    #[test]
    fn register_identity_probe_links_successfully() {
        let engine = wasmtime::Engine::default();
        let mut linker = Linker::<ComponentState>::new(&engine);
        register_identity_probe(&mut linker).expect("probe linker registers all imports");
    }

    /// Verify that the probe WASI policy has no preopens, no env, and
    /// no stdio — the only reduced-authority boundary available today.
    #[test]
    fn probe_wasi_policy_is_locked_down() {
        let policy = RunnerWasiPolicy::probe();
        assert!(!policy.inherit_stdio, "probe WASI must not inherit stdio");
        assert!(
            policy.preopens.is_empty(),
            "probe WASI must have no preopens"
        );
        assert!(
            policy.env_allow.is_empty(),
            "probe WASI must not allow env vars"
        );
        assert!(
            policy.env_set.is_empty(),
            "probe WASI must not set env vars"
        );
    }
}

pub fn register_all(linker: &mut Linker<ComponentState>, allow_state_store: bool) -> Result<()> {
    install_default_crypto_provider();

    add_wasi_to_linker(linker)?;

    // Add wasi-tls types and turn on the feature in linker
    let mut opts = LinkOptions::default();
    opts.tls(true);
    wasmtime_wasi_tls::p2::add_to_linker(linker, &opts)?;

    // Add wasi-http types and turn on the feature in linker
    add_wasi_http_to_linker(linker)?;

    add_all_v1_to_linker(
        linker,
        HostFns {
            http_client_v1_1: Some(|state: &mut ComponentState| state.host_mut()),
            http_client: Some(|state: &mut ComponentState| state.host_mut()),
            oauth_broker: Some(|state: &mut ComponentState| state.host_mut()),
            runner_host_http: Some(|state: &mut ComponentState| state.host_mut()),
            runner_host_kv: Some(|state: &mut ComponentState| state.host_mut()),
            telemetry_logger: Some(|state: &mut ComponentState| state.host_mut()),
            state_store: allow_state_store.then_some(|state: &mut ComponentState| state.host_mut()),
            secrets_store_v1_1: Some(|state: &mut ComponentState| state.host_mut()),
            secrets_store: None,
            runtime_config: Some(|state: &mut ComponentState| state.host_mut()),
        },
    )?;
    add_http_client_client_world_aliases(linker)?;
    add_telemetry_logging_stub(linker)?;
    Ok(())
}

/// Some generated MCP components import `greentic:telemetry/logging` for guest
/// instrumentation (it's baked in by the generator's telemetry wiring). The
/// runner emits its own telemetry via the native pipeline and does not consume
/// these guest events, so we satisfy the import with no-ops — otherwise such a
/// component fails to instantiate ("matching implementation was not found in the
/// linker"). Registered dynamically (`func_new`) so we don't need generated
/// bindings for the interface; the signature is resolved at instantiation.
fn add_telemetry_logging_stub(linker: &mut Linker<ComponentState>) -> Result<()> {
    let mut inst = match linker.instance("greentic:telemetry/logging") {
        Ok(inst) => inst,
        // Already defined by another registration path — nothing to do.
        Err(_) => return Ok(()),
    };
    // log: func(lvl: level, message: string, fields: fields)
    inst.func_new("log", |_store, _ty, _params, _results| Ok(()))?;
    // span-start: func(name: string, fields: fields) -> u64
    inst.func_new("span-start", |_store, _ty, _params, results| {
        if let Some(slot) = results.get_mut(0) {
            *slot = wasmtime::component::Val::U64(0);
        }
        Ok(())
    })?;
    // span-end: func(id: u64)
    inst.func_new("span-end", |_store, _ty, _params, _results| Ok(()))?;
    Ok(())
}

fn add_http_client_client_world_aliases(linker: &mut Linker<ComponentState>) -> Result<()> {
    let mut inst_v1_1 = linker.instance("greentic:http/client@1.1.0")?;
    inst_v1_1.func_wrap(
        "send",
        move |mut caller: StoreContextMut<'_, ComponentState>,
              (req, opts, ctx): (
            http_client_client_alias::Request,
            Option<http_client_client_alias::RequestOptions>,
            Option<http_client_client_alias::TenantCtx>,
        )| {
            let host = caller.data_mut().host_mut();
            let result = HttpClientHostV1_1::send(
                host,
                alias_request_to_host(req),
                opts.map(alias_request_options_to_host),
                ctx.map(alias_tenant_ctx_to_host),
            );
            Ok((match result {
                Ok(resp) => Ok(alias_response_from_host(resp)),
                Err(err) => Err(alias_error_from_host(err)),
            },))
        },
    )?;
    let mut inst_v1_0 = linker.instance("greentic:http/client@1.0.0")?;
    inst_v1_0.func_wrap(
        "send",
        move |mut caller: StoreContextMut<'_, ComponentState>,
              (req, ctx): (
            host_http_client::Request,
            Option<host_http_client::TenantCtx>,
        )| {
            let host = caller.data_mut().host_mut();
            let result = HttpClientHost::send(host, req, ctx);
            Ok((result,))
        },
    )?;
    Ok(())
}

fn alias_request_to_host(req: http_client_client_alias::Request) -> host_http_client::RequestV1_1 {
    host_http_client::RequestV1_1 {
        method: req.method,
        url: req.url,
        headers: req.headers,
        body: req.body,
    }
}

fn alias_request_options_to_host(
    opts: http_client_client_alias::RequestOptions,
) -> host_http_client::RequestOptionsV1_1 {
    host_http_client::RequestOptionsV1_1 {
        timeout_ms: opts.timeout_ms,
        allow_insecure: opts.allow_insecure,
        follow_redirects: opts.follow_redirects,
    }
}

fn alias_tenant_ctx_to_host(
    ctx: http_client_client_alias::TenantCtx,
) -> host_http_client::TenantCtxV1_1 {
    host_http_client::TenantCtxV1_1 {
        env: ctx.env,
        tenant: ctx.tenant,
        tenant_id: ctx.tenant_id,
        team: ctx.team,
        team_id: ctx.team_id,
        user: ctx.user,
        user_id: ctx.user_id,
        trace_id: ctx.trace_id,
        correlation_id: ctx.correlation_id,
        i18n_id: ctx.i18n_id,
        attributes: ctx.attributes,
        session_id: ctx.session_id,
        flow_id: ctx.flow_id,
        node_id: ctx.node_id,
        provider_id: ctx.provider_id,
        deadline_ms: ctx.deadline_ms,
        attempt: ctx.attempt,
        idempotency_key: ctx.idempotency_key,
        impersonation: ctx.impersonation.map(|imp| http_types_v1_1::Impersonation {
            actor_id: imp.actor_id,
            reason: imp.reason,
        }),
    }
}

fn alias_response_from_host(
    resp: host_http_client::ResponseV1_1,
) -> http_client_client_alias::Response {
    http_client_client_alias::Response {
        status: resp.status,
        headers: resp.headers,
        body: resp.body,
    }
}

fn alias_error_from_host(
    err: host_http_client::HttpClientErrorV1_1,
) -> http_client_client_alias::HostError {
    http_client_client_alias::HostError {
        code: err.code,
        message: err.message,
    }
}

impl WasiView for ComponentState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}

impl WasiHttpView for ComponentState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.wasi_http_ctx,
            table: &mut self.resource_table,
            hooks: Default::default(),
        }
    }
}

impl WasiTlsView for ComponentState {
    fn tls(&mut self) -> WasiTlsCtxView<'_> {
        WasiTlsCtxView {
            ctx: &mut self.wasi_tls_ctx,
            table: &mut self.resource_table,
        }
    }
}

#[allow(unsafe_code)]
unsafe impl Send for ComponentState {}
#[allow(unsafe_code)]
unsafe impl Sync for ComponentState {}

impl PackRuntime {
    fn allows_state_store(&self, component_ref: &str) -> bool {
        if self.state_store.is_none() {
            return false;
        }
        if !self.config.state_store_policy.allow {
            return false;
        }
        let Some(manifest) = self.component_manifests.get(component_ref) else {
            // No manifest entry — allow state-store; Wasmtime rejects if not imported.
            return true;
        };
        // If manifest declares host.state capabilities, honour them.
        // If host.state is None (not declared in manifest), default to true so
        // components whose CBOR manifest omits the field still get state-store
        // linked — Wasmtime will reject at instantiation if not actually imported.
        manifest
            .capabilities
            .host
            .state
            .as_ref()
            .map(|caps| caps.read || caps.write)
            .unwrap_or(true)
    }

    pub fn contains_component(&self, component_ref: &str) -> bool {
        self.components.contains_key(component_ref)
    }

    /// Returns a clonable handle to the pack's state store, when one is
    /// configured. Used by the flow engine's built-in `state.get`/`state.set`
    /// operators which call into the same store that WASM components read
    /// through their `state.read`/`state.write` host imports.
    pub fn state_store_handle(&self) -> Option<crate::storage::DynStateStore> {
        self.state_store.clone()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load(
        path: impl AsRef<Path>,
        config: Arc<HostConfig>,
        mocks: Option<Arc<MockLayer>>,
        archive_source: Option<&Path>,
        session_store: Option<DynSessionStore>,
        state_store: Option<DynStateStore>,
        wasi_policy: Arc<RunnerWasiPolicy>,
        secrets: DynSecretsManager,
        oauth_config: Option<OAuthBrokerConfig>,
        verify_archive: bool,
        component_resolution: ComponentResolution,
    ) -> Result<Self> {
        let path = path.as_ref();
        let (_pack_root, safe_path) = normalize_pack_path(path)?;
        let path_meta = std::fs::metadata(&safe_path).ok();
        let is_dir = path_meta
            .as_ref()
            .map(|meta| meta.is_dir())
            .unwrap_or(false);
        let is_component = !is_dir
            && safe_path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("wasm"))
                .unwrap_or(false);
        let archive_hint_path = if let Some(source) = archive_source {
            let (_, normalized) = normalize_pack_path(source)?;
            Some(normalized)
        } else if is_component || is_dir {
            None
        } else {
            Some(safe_path.clone())
        };
        let archive_hint = archive_hint_path.as_deref();
        if verify_archive {
            if let Some(verify_target) = archive_hint.and_then(|p| {
                std::fs::metadata(p)
                    .ok()
                    .filter(|meta| meta.is_file())
                    .map(|_| p)
            }) {
                verify::verify_pack(verify_target).await?;
                tracing::info!(pack_path = %verify_target.display(), "pack verification complete");
            } else {
                tracing::debug!("skipping archive verification (no archive source)");
            }
        }
        let engine = Engine::default();
        let engine_profile =
            EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
        let cache = CacheManager::new(CacheConfig::default(), engine_profile);
        let mut metadata = PackMetadata::fallback(&safe_path);
        let mut manifest = None;
        let mut legacy_manifest: Option<Box<legacy_pack::PackManifest>> = None;
        let mut flows = None;
        let materialized_root = component_resolution.materialized_root.clone().or_else(|| {
            if is_dir {
                Some(safe_path.clone())
            } else {
                None
            }
        });
        let (pack_assets_dir, assets_tempdir) =
            locate_pack_assets(materialized_root.as_deref(), archive_hint)?;
        let setup_yaml_exists = pack_assets_dir
            .as_ref()
            .map(|dir| dir.join("setup.yaml").is_file())
            .unwrap_or(false);
        tracing::info!(
            pack_root = %safe_path.display(),
            assets_setup_yaml_exists = setup_yaml_exists,
            "pack unpack metadata"
        );

        if let Some(root) = materialized_root.as_ref() {
            match load_manifest_and_flows_from_dir(root) {
                Ok(ManifestLoad::New {
                    manifest: m,
                    flows: cache,
                }) => {
                    metadata = cache.metadata.clone();
                    manifest = Some(*m);
                    flows = Some(cache);
                }
                Ok(ManifestLoad::Legacy {
                    manifest: m,
                    flows: cache,
                }) => {
                    metadata = cache.metadata.clone();
                    legacy_manifest = Some(m);
                    flows = Some(cache);
                }
                Err(err) => {
                    warn!(error = %err, pack = %root.display(), "failed to parse materialized pack manifest");
                }
            }
        }

        if manifest.is_none()
            && legacy_manifest.is_none()
            && let Some(archive_path) = archive_hint
        {
            let manifest_load = load_manifest_and_flows(archive_path).with_context(|| {
                format!(
                    "failed to load manifest.cbor from {}",
                    archive_path.display()
                )
            })?;
            match manifest_load {
                ManifestLoad::New {
                    manifest: m,
                    flows: cache,
                } => {
                    metadata = cache.metadata.clone();
                    manifest = Some(*m);
                    flows = Some(cache);
                }
                ManifestLoad::Legacy {
                    manifest: m,
                    flows: cache,
                } => {
                    metadata = cache.metadata.clone();
                    legacy_manifest = Some(m);
                    flows = Some(cache);
                }
            }
        }
        #[cfg(feature = "fault-injection")]
        {
            let fault_ctx = FaultContext {
                pack_id: metadata.pack_id.as_str(),
                flow_id: "unknown",
                node_id: None,
                attempt: 1,
            };
            maybe_fail(FaultPoint::PackResolve, fault_ctx)
                .map_err(|err| anyhow!(err.to_string()))?;
        }
        let mut pack_lock = None;
        for root in find_pack_lock_roots(&safe_path, is_dir, archive_hint) {
            pack_lock = load_pack_lock(&root)?;
            if pack_lock.is_some() {
                break;
            }
        }
        let component_sources_payload = if pack_lock.is_none() {
            if let Some(manifest) = manifest.as_ref() {
                manifest
                    .get_component_sources_v1()
                    .context("invalid component sources extension")?
            } else {
                None
            }
        } else {
            None
        };
        let component_sources = if let Some(lock) = pack_lock.as_ref() {
            Some(component_sources_table_from_pack_lock(
                lock,
                component_resolution.allow_missing_hash,
            )?)
        } else {
            component_sources_table(component_sources_payload.as_ref())?
        };
        let components = if is_component {
            let wasm_bytes = fs::read(&safe_path).await?;
            metadata = PackMetadata::from_wasm(&wasm_bytes)
                .unwrap_or_else(|| PackMetadata::fallback(&safe_path));
            let name = safe_path
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "component".to_string());
            let component = compile_component_with_cache(&cache, &engine, None, wasm_bytes).await?;
            let mut map = HashMap::new();
            map.insert(
                name.clone(),
                PackComponent {
                    name,
                    version: metadata.version.clone(),
                    component,
                },
            );
            map
        } else {
            let specs = component_specs(
                manifest.as_ref(),
                legacy_manifest.as_deref(),
                component_sources_payload.as_ref(),
                pack_lock.as_ref(),
            );
            if specs.is_empty() {
                HashMap::new()
            } else {
                let mut loaded = HashMap::new();
                let mut missing: HashSet<String> =
                    specs.iter().map(|spec| spec.id.clone()).collect();
                let mut searched = Vec::new();

                if !component_resolution.overrides.is_empty() {
                    load_components_from_overrides(
                        &cache,
                        &engine,
                        &component_resolution.overrides,
                        &specs,
                        &mut missing,
                        &mut loaded,
                    )
                    .await?;
                    searched.push("override map".to_string());
                }

                if let Some(component_sources) = component_sources.as_ref() {
                    load_components_from_sources(
                        &cache,
                        &engine,
                        component_sources,
                        &component_resolution,
                        &specs,
                        &mut missing,
                        &mut loaded,
                        materialized_root.as_deref(),
                        archive_hint,
                    )
                    .await?;
                    searched.push(format!("extension {}", EXT_COMPONENT_SOURCES_V1));
                }

                if let Some(root) = materialized_root.as_ref() {
                    load_components_from_dir(
                        &cache,
                        &engine,
                        root,
                        &specs,
                        &mut missing,
                        &mut loaded,
                    )
                    .await?;
                    searched.push(format!("components dir {}", root.display()));
                }

                if let Some(archive_path) = archive_hint {
                    load_components_from_archive(
                        &cache,
                        &engine,
                        archive_path,
                        &specs,
                        &mut missing,
                        &mut loaded,
                    )
                    .await?;
                    searched.push(format!("archive {}", archive_path.display()));
                }

                if !missing.is_empty() {
                    let missing_list = missing.into_iter().collect::<Vec<_>>().join(", ");
                    let sources = if searched.is_empty() {
                        "no component sources".to_string()
                    } else {
                        searched.join(", ")
                    };
                    bail!(
                        "components missing: {}; looked in {}",
                        missing_list,
                        sources
                    );
                }

                loaded
            }
        };
        let http_client = Arc::clone(&HTTP_CLIENT);
        let mut component_manifests = HashMap::new();
        if let Some(manifest) = manifest.as_ref() {
            for component in &manifest.components {
                component_manifests.insert(component.id.as_str().to_string(), component.clone());
            }
        }
        let mut pack_policy = (*wasi_policy).clone();
        if let Some(dir) = pack_assets_dir {
            tracing::debug!(path = %dir.display(), "preopening pack assets directory for WASI /assets");
            pack_policy =
                pack_policy.with_preopen(PreopenSpec::new(dir, "/assets").read_only(true));
        }
        let wasi_policy = Arc::new(pack_policy);
        Ok(Self {
            path: safe_path,
            archive_path: archive_hint.map(Path::to_path_buf),
            config,
            engine,
            metadata,
            manifest,
            legacy_manifest,
            component_manifests,
            mocks,
            flows,
            components,
            http_client,
            session_store,
            state_store,
            wasi_policy,
            mcp_routes: std::sync::OnceLock::new(),
            assets_tempdir,
            provider_registry: RwLock::new(None),
            identify_hint_cache: RwLock::new(HashMap::new()),
            secrets,
            oauth_config,
            cache,
            runtime_config_non_secret: None,
            runtime_refs: None,
        })
    }

    /// Inject the `pack-config.v1.non_secret` map for this pack. Called by
    /// the producer (greentic-start, C4.3) after loading the deployed
    /// `PackConfig`. Passing `None` clears any previously-set map.
    pub fn set_runtime_config_non_secret(&mut self, map: Option<Arc<BTreeMap<String, Value>>>) {
        self.runtime_config_non_secret = map;
    }

    /// Read-only accessor for the injected `pack-config.v1.non_secret` map.
    /// Used by the revision loader's tests to assert producer plumbing.
    pub fn runtime_config_non_secret(&self) -> Option<&Arc<BTreeMap<String, Value>>> {
        self.runtime_config_non_secret.as_ref()
    }

    /// Inject the `pack-config.v1.runtime_refs` channel (C5): per-pack
    /// `key → URI` bindings plus the env-shared resolver. Called by
    /// greentic-start after loading the deployed `PackConfig`. Passing
    /// `None` clears any previously-set injection.
    pub fn set_runtime_refs(&mut self, injection: Option<RuntimeRefsInjection>) {
        self.runtime_refs = injection;
    }

    /// Read-only accessor for the injected runtime-refs channel. Used by
    /// the revision loader's tests to assert producer plumbing.
    pub fn runtime_refs(&self) -> Option<&RuntimeRefsInjection> {
        self.runtime_refs.as_ref()
    }

    pub async fn list_flows(&self) -> Result<Vec<FlowDescriptor>> {
        if let Some(cache) = &self.flows {
            return Ok(cache.descriptors.clone());
        }
        if let Some(manifest) = &self.manifest {
            let descriptors = manifest
                .flows
                .iter()
                .map(|flow| FlowDescriptor {
                    id: flow.id.as_str().to_string(),
                    flow_type: flow_kind_to_str(flow.kind).to_string(),
                    pack_id: manifest.pack_id.as_str().to_string(),
                    profile: manifest.pack_id.as_str().to_string(),
                    version: manifest.version.to_string(),
                    description: None,
                    entry: tags_indicate_entry(flow.tags.iter().map(String::as_str)),
                })
                .collect();
            return Ok(descriptors);
        }
        Ok(Vec::new())
    }

    #[allow(dead_code)]
    pub async fn run_flow(
        &self,
        flow_id: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let pack = Arc::new(
            PackRuntime::load(
                &self.path,
                Arc::clone(&self.config),
                self.mocks.clone(),
                self.archive_path.as_deref(),
                self.session_store.clone(),
                self.state_store.clone(),
                Arc::clone(&self.wasi_policy),
                self.secrets.clone(),
                self.oauth_config.clone(),
                false,
                ComponentResolution::default(),
            )
            .await?,
        );

        let engine = FlowEngine::new(vec![Arc::clone(&pack)], Arc::clone(&self.config)).await?;
        let retry_config = self.config.retry_config().into();
        let mocks = pack.mocks.as_deref();
        let tenant = self.config.tenant.as_str();

        let ctx = FlowContext {
            tenant,
            pack_id: pack.metadata().pack_id.as_str(),
            flow_id,
            node_id: None,
            tool: None,
            action: None,
            session_id: None,
            provider_id: None,
            reply_scope: None,
            retry_config,
            attempt: 1,
            observer: None,
            mocks,
        };

        let execution = engine.execute(ctx, input).await?;
        match execution.status {
            FlowStatus::Completed => Ok(execution.output),
            FlowStatus::Waiting(wait) => Ok(serde_json::json!({
                "status": "pending",
                "reason": wait.reason,
                "resume": wait.snapshot,
                "response": execution.output,
            })),
        }
    }

    pub async fn invoke_component(
        &self,
        component_ref: &str,
        ctx: ComponentExecCtx,
        operation: &str,
        config_json: Option<String>,
        input_json: String,
    ) -> Result<Value> {
        let component_ref = resolve_component_key(component_ref, operation, |key| {
            self.components.contains_key(key)
        });
        let pack_component = self
            .components
            .get(component_ref)
            .with_context(|| format!("component '{component_ref}' not found in pack"))?;
        let engine = self.engine.clone();
        let config = Arc::clone(&self.config);
        let http_client = Arc::clone(&self.http_client);
        let mocks = self.mocks.clone();
        let session_store = self.session_store.clone();
        let state_store = self.state_store.clone();
        let secrets = Arc::clone(&self.secrets);
        let oauth_config = self.oauth_config.clone();
        let wasi_policy = Arc::clone(&self.wasi_policy);
        let pack_id = self.metadata().pack_id.clone();
        let allow_state_store = self.allows_state_store(component_ref);
        let component = pack_component.component.clone();
        let component_ref_owned = component_ref.to_string();
        let operation_owned = operation.to_string();
        let input_owned =
            Self::merge_component_config_into_input_json(config_json.as_deref(), &input_json)
                .context("merge component config into invocation payload")?;
        let ctx_owned = ctx;
        let runtime_config_non_secret = self.runtime_config_non_secret.clone();
        let runtime_refs = self.runtime_refs.clone();

        run_on_wasi_thread("component.invoke", move || {
            let mut linker = Linker::new(&engine);
            register_all(&mut linker, allow_state_store)?;
            add_component_control_to_linker(&mut linker)?;

            let host_state = HostState::new(
                pack_id.clone(),
                config,
                http_client,
                mocks,
                session_store,
                state_store,
                secrets,
                oauth_config,
                Some(ctx_owned.clone()),
                Some(component_ref_owned.clone()),
                false,
                runtime_config_non_secret,
                runtime_refs,
            )?;
            let store_state = ComponentState::new(host_state, wasi_policy)?;
            let mut store = wasmtime::Store::new(&engine, store_state);

            let invoke_result = HostState::instantiate_component_result(
                &mut linker,
                &mut store,
                &component,
                &ctx_owned,
                &component_ref_owned,
                &operation_owned,
                &input_owned,
            )?;
            HostState::convert_invoke_result(invoke_result)
        })
    }

    /// Fold a provider instance's configured answers into its invocation
    /// payload, the same way [`Self::merge_component_config_into_input_json`]
    /// does for a component.
    ///
    /// `ProviderBinding` has carried `config_json` since providers gained
    /// instances, and `runner::operator` passes it whenever it reaches a
    /// provider through `invoke_component`. This path — the one
    /// `HostRuntime`'s webhook dispatch uses — took the payload and handed it
    /// over untouched, so a provider invoked here saw `request.config` as an
    /// empty object no matter what the operator had answered. Nothing failed:
    /// every setup answer simply was not there, which a provider reports (if
    /// at all) as its own feature being switched off.
    ///
    /// Two rules, and the second is why this is not just a call to the
    /// component version:
    ///
    /// - **No config, no change.** A provider instance with no configured
    ///   answers gets the byte-identical payload it gets today, so nothing
    ///   about an unconfigured provider moves.
    /// - **The payload must already be JSON.** A provider's input arrives as
    ///   bytes from a webhook body, not as a flow node's value, so it may not
    ///   be JSON at all. The component merge wraps a non-JSON input as a JSON
    ///   string, which for a provider would replace a binary body with an
    ///   envelope the component cannot read — trading a missing config for a
    ///   corrupted request. Here a payload that does not parse is passed
    ///   through untouched and the drop is logged with the provider named,
    ///   because silence is what this whole function exists to end.
    fn merge_provider_config_into_input(
        config_json: Option<&str>,
        input_json: Vec<u8>,
        provider_type: &str,
    ) -> Vec<u8> {
        let Some(config_json) = config_json else {
            return input_json;
        };
        let Ok(input_str) = std::str::from_utf8(&input_json) else {
            tracing::warn!(
                provider_type,
                "provider payload is not UTF-8; invoking without its configured answers"
            );
            return input_json;
        };
        if serde_json::from_str::<Value>(input_str).is_err() {
            tracing::warn!(
                provider_type,
                "provider payload is not JSON; invoking without its configured answers"
            );
            return input_json;
        }
        match Self::merge_component_config_into_input_json(Some(config_json), input_str) {
            Ok(merged) => merged.into_bytes(),
            Err(error) => {
                tracing::warn!(
                    provider_type,
                    %error,
                    "could not merge the provider's configured answers into its payload; \
                     invoking without them"
                );
                input_json
            }
        }
    }

    fn merge_component_config_into_input_json(
        config_json: Option<&str>,
        input_json: &str,
    ) -> Result<String> {
        let Some(config_json) = config_json else {
            return Ok(input_json.to_string());
        };

        let config_value: Value =
            serde_json::from_str(config_json).context("parse component config JSON")?;

        if let Ok(mut invocation) =
            serde_json::from_str::<greentic_types::InvocationEnvelope>(input_json)
        {
            let payload_value = serde_json::from_slice(&invocation.payload).unwrap_or_else(|_| {
                Value::String(String::from_utf8_lossy(&invocation.payload).into_owned())
            });
            invocation.payload = serde_json::to_vec(&serde_json::json!({
                "config": config_value,
                "input": payload_value,
            }))
            .context("serialize merged invocation payload")?;
            return serde_json::to_string(&invocation)
                .context("serialize merged invocation envelope");
        }

        let input_value = serde_json::from_str(input_json)
            .unwrap_or_else(|_| Value::String(input_json.to_string()));
        serde_json::to_string(&serde_json::json!({
            "config": config_value,
            "input": input_value,
        }))
        .context("serialize merged component input")
    }

    pub fn resolve_provider(
        &self,
        provider_id: Option<&str>,
        provider_type: Option<&str>,
    ) -> Result<ProviderBinding> {
        let registry = self.provider_registry()?;
        registry.resolve(provider_id, provider_type)
    }

    pub async fn invoke_provider(
        &self,
        binding: &ProviderBinding,
        ctx: ComponentExecCtx,
        op: &str,
        input_json: Vec<u8>,
    ) -> Result<Value> {
        let component_ref_owned = binding.component_ref.clone();
        let pack_component = self.components.get(&component_ref_owned).with_context(|| {
            format!("provider component '{component_ref_owned}' not found in pack")
        })?;
        let component = pack_component.component.clone();

        let engine = self.engine.clone();
        let config = Arc::clone(&self.config);
        let http_client = Arc::clone(&self.http_client);
        let mocks = self.mocks.clone();
        let session_store = self.session_store.clone();
        let state_store = self.state_store.clone();
        let secrets = Arc::clone(&self.secrets);
        let oauth_config = self.oauth_config.clone();
        let wasi_policy = Arc::clone(&self.wasi_policy);
        let pack_id = self.metadata().pack_id.clone();
        let allow_state_store = self.allows_state_store(&component_ref_owned);
        let input_owned = Self::merge_provider_config_into_input(
            binding.config_json.as_deref(),
            input_json,
            &binding.provider_type,
        );
        let op_owned = op.to_string();
        let ctx_owned = ctx;
        let world = binding.world.clone();
        let runtime_config_non_secret = self.runtime_config_non_secret.clone();
        let runtime_refs = self.runtime_refs.clone();

        run_on_wasi_thread("provider.invoke", move || {
            let mut linker = Linker::new(&engine);
            register_all(&mut linker, allow_state_store)?;
            add_component_control_to_linker(&mut linker)?;
            let host_state = HostState::new(
                pack_id.clone(),
                config,
                http_client,
                mocks,
                session_store,
                state_store,
                secrets,
                oauth_config,
                Some(ctx_owned.clone()),
                Some(component_ref_owned.clone()),
                true,
                runtime_config_non_secret,
                runtime_refs,
            )?;
            let store_state = ComponentState::new(host_state, wasi_policy)?;
            let mut store = wasmtime::Store::new(&engine, store_state);

            // Extension-provider worlds (greentic:extension-provider@0.2.0 /
            // @0.1.0) are introspection surfaces (list-channels,
            // describe-channel, *-schema, dry-run-encode) and deliberately do
            // NOT export a generic `invoke(op, input)` data-plane call. If a
            // pack declares such a world for the runtime data plane, dispatch
            // here would otherwise fall through to the legacy schema-core
            // bindings and fail with an opaque "no exported instance" wasmtime
            // error. Detect it up front (declared-world fast path, confirmed by
            // an instance-level probe) and surface a typed, downcastable
            // `ProviderInvokeError` instead. The data-plane routing for these
            // worlds is an open design question (see PR body NEEDS_DECISION);
            // legacy schema-core remains the default path below.
            if world.contains("extension-provider") {
                let pre_instance = linker.instantiate_pre(component.as_ref())?;
                let instance =
                    block_on(async { pre_instance.instantiate_async(&mut store).await })?;
                let detected =
                    crate::extension_provider::probe_provider_world(&mut store, &instance);
                let version = detected
                    .map(|w| w.version())
                    .unwrap_or("unknown")
                    .to_string();
                let typed = crate::extension_provider::ProviderInvokeError::Internal(format!(
                    "extension-provider@{version} world exposes no data-plane invoke; \
                     operation '{op_owned}' has no extension-provider equivalent (introspection \
                     surface only)"
                ));
                return Err(crate::extension_provider::into_anyhow(typed));
            }

            let use_schema_core_schema = world.contains("provider-schema-core");
            let use_schema_core_path = world.contains("provider/schema-core");
            let result = if use_schema_core_schema {
                let pre_instance = linker.instantiate_pre(component.as_ref())?;
                let pre: SchemaSchemaCorePre<ComponentState> =
                    SchemaSchemaCorePre::new(pre_instance)?;
                let bindings = block_on(async { pre.instantiate_async(&mut store).await })?;
                let provider = bindings.greentic_provider_schema_core_schema_core_api();
                provider.call_invoke(&mut store, &op_owned, &input_owned)?
            } else if use_schema_core_path {
                let pre_instance = linker.instantiate_pre(component.as_ref())?;
                let path_attempt = (|| -> Result<Vec<u8>> {
                    let pre: PathSchemaCorePre<ComponentState> =
                        PathSchemaCorePre::new(pre_instance)?;
                    let bindings = block_on(async { pre.instantiate_async(&mut store).await })?;
                    let provider = bindings.greentic_provider_schema_core_api();
                    Ok(provider.call_invoke(&mut store, &op_owned, &input_owned)?)
                })();
                match path_attempt {
                    Ok(value) => value,
                    Err(path_err)
                        if path_err.to_string().contains("no exported instance named") =>
                    {
                        let pre_instance = linker.instantiate_pre(component.as_ref())?;
                        let pre: SchemaSchemaCorePre<ComponentState> =
                            SchemaSchemaCorePre::new(pre_instance)?;
                        let bindings = block_on(async { pre.instantiate_async(&mut store).await })?;
                        let provider = bindings.greentic_provider_schema_core_schema_core_api();
                        provider.call_invoke(&mut store, &op_owned, &input_owned)?
                    }
                    Err(path_err) => return Err(path_err),
                }
            } else {
                let pre_instance = linker.instantiate_pre(component.as_ref())?;
                let pre: LegacySchemaCorePre<ComponentState> =
                    LegacySchemaCorePre::new(pre_instance)?;
                let bindings = block_on(async { pre.instantiate_async(&mut store).await })?;
                let provider = bindings.greentic_provider_core_schema_core_api();
                provider.call_invoke(&mut store, &op_owned, &input_owned)?
            };
            deserialize_json_bytes(result)
        })
    }

    /// Call the provider component's `identify-instance` export
    /// (`greentic:provider-instance-identity@0.1.0`) with the inbound
    /// payload bytes. Returns an [`IdentifyOutcome`] — see the variant
    /// docs for the per-case contract.
    ///
    /// # Payload shape (M1 IID.4d wrapper)
    ///
    /// `payload` is forwarded opaque to the component. The shape is set by
    /// the caller; the M1 IID.4d wrapper convention from `greentic-start`
    /// is `{headers: [{name,value}], body: <parsed-or-null>}` so providers
    /// whose discriminator lives in HTTP headers (Telegram via
    /// `x-telegram-bot-api-secret-token`) can identify the instance the
    /// same call shape that body-based providers (Teams, Slack, Webex,
    /// etc.) use. See the docstring on
    /// `greentic:provider-instance-identity/instance-identity-api.identify-instance`
    /// for the full contract; this host method does not parse or
    /// validate the bytes.
    ///
    /// # Host authority on identity probes
    ///
    /// The linker registers the full host import surface (Wasmtime
    /// validates all imports eagerly at `instantiate_pre`, not just
    /// those reachable from the invoked export). The WASI sandbox is
    /// locked down: no preopens, no env, no stdio. Deny-shim linker
    /// handlers (trap on call, satisfy at link time) are a follow-up
    /// in `greentic-interfaces-wasmtime`. See [`register_identity_probe`].
    pub async fn invoke_identify_instance(
        &self,
        binding: &ProviderBinding,
        payload: Vec<u8>,
    ) -> Result<IdentifyOutcome> {
        let component_ref_owned = binding.component_ref.clone();
        let pack_component = self.components.get(&component_ref_owned).with_context(|| {
            format!("provider component '{component_ref_owned}' not found in pack")
        })?;
        let component = pack_component.component.clone();

        let engine = self.engine.clone();
        let config = Arc::clone(&self.config);
        let http_client = Arc::clone(&self.http_client);
        let mocks = self.mocks.clone();
        let session_store = self.session_store.clone();
        let state_store = self.state_store.clone();
        let secrets = Arc::clone(&self.secrets);
        let oauth_config = self.oauth_config.clone();
        let pack_id = self.metadata().pack_id.clone();

        // Locked-down WASI policy: no preopens, no env, no stdio.
        // The linker registers all imports (Wasmtime requires it for
        // instantiate_pre), but the WASI sandbox is the tightest we
        // can enforce today. See [`register_identity_probe`] docs.
        let wasi_policy = Arc::new(RunnerWasiPolicy::probe());
        let runtime_config_non_secret = self.runtime_config_non_secret.clone();
        let runtime_refs = self.runtime_refs.clone();
        run_on_wasi_thread("provider.identify_instance", move || {
            let mut linker = Linker::new(&engine);
            register_identity_probe(&mut linker)?;
            let host_state = HostState::new(
                pack_id.clone(),
                config,
                http_client,
                mocks,
                session_store,
                state_store,
                secrets,
                oauth_config,
                None,
                Some(component_ref_owned.clone()),
                true,
                runtime_config_non_secret,
                runtime_refs,
            )?;
            let store_state = ComponentState::new(host_state, wasi_policy)?;
            let mut store = wasmtime::Store::new(&engine, store_state);

            let pre_instance = linker.instantiate_pre(component.as_ref())?;
            let pre = match InstanceIdentityPre::<ComponentState>::new(pre_instance) {
                Ok(pre) => pre,
                Err(err) if is_missing_export_error(&format!("{err:#}")) => {
                    return Ok(IdentifyOutcome::Unsupported);
                }
                Err(err) => return Err(err.into()),
            };
            let bindings = block_on(async { pre.instantiate_async(&mut store).await })?;
            let api = bindings.greentic_provider_instance_identity_instance_identity_api();
            let result = api.call_identify_instance(&mut store, &payload)?;
            Ok(match result {
                Some(id) => IdentifyOutcome::Identified(id),
                None => IdentifyOutcome::NoMatch,
            })
        })
    }

    /// Call the provider component's `describe-identify-instance` export
    /// (`greentic:provider-instance-identity/instance-identity-describe@0.1.0`)
    /// and parse the returned JSON into an [`IdentifyInstanceHint`].
    ///
    /// Returns `Ok(None)` for every "no hint available" case: the
    /// component does not export the describe world, the export returned
    /// `none`, the returned bytes are not valid JSON, or the `version`
    /// gate failed. The two malformed cases are warn-logged so a typo'd
    /// hint surfaces in operator logs without blocking ingest. Component
    /// traps and other infrastructure errors propagate as `Err`.
    ///
    /// This is the uncached probe — see [`resolve_identify_hint`] for the
    /// cached wrapper that callers SHOULD use on the inbound hot path.
    ///
    /// [`resolve_identify_hint`]: PackRuntime::resolve_identify_hint
    pub async fn invoke_describe_identify_instance(
        &self,
        binding: &ProviderBinding,
    ) -> Result<Option<IdentifyInstanceHint>> {
        let component_ref_owned = binding.component_ref.clone();
        let pack_component = self.components.get(&component_ref_owned).with_context(|| {
            format!("provider component '{component_ref_owned}' not found in pack")
        })?;
        let component = pack_component.component.clone();

        let engine = self.engine.clone();
        let config = Arc::clone(&self.config);
        let http_client = Arc::clone(&self.http_client);
        let mocks = self.mocks.clone();
        let session_store = self.session_store.clone();
        let state_store = self.state_store.clone();
        let secrets = Arc::clone(&self.secrets);
        let oauth_config = self.oauth_config.clone();
        let pack_id = self.metadata().pack_id.clone();

        // Locked-down WASI policy — same rationale as
        // `invoke_identify_instance`. See [`register_identity_probe`] docs.
        let wasi_policy = Arc::new(RunnerWasiPolicy::probe());
        let runtime_config_non_secret = self.runtime_config_non_secret.clone();
        let runtime_refs = self.runtime_refs.clone();
        run_on_wasi_thread("provider.describe_identify_instance", move || {
            let mut linker = Linker::new(&engine);
            register_identity_probe(&mut linker)?;
            let host_state = HostState::new(
                pack_id.clone(),
                config,
                http_client,
                mocks,
                session_store,
                state_store,
                secrets,
                oauth_config,
                None,
                Some(component_ref_owned.clone()),
                true,
                runtime_config_non_secret,
                runtime_refs,
            )?;
            let store_state = ComponentState::new(host_state, wasi_policy)?;
            let mut store = wasmtime::Store::new(&engine, store_state);

            let pre_instance = linker.instantiate_pre(component.as_ref())?;
            let pre = match InstanceIdentityDescribePre::<ComponentState>::new(pre_instance) {
                Ok(pre) => pre,
                Err(err) if is_missing_export_error(&format!("{err:#}")) => {
                    return Ok(None);
                }
                Err(err) => return Err(err.into()),
            };
            let bindings = block_on(async { pre.instantiate_async(&mut store).await })?;
            let api = bindings.greentic_provider_instance_identity_instance_identity_describe_api();
            let raw = api.call_describe_identify_instance(&mut store)?;
            let Some(bytes) = raw else {
                // Component exported the world but said "no hint right now".
                // Per the WIT contract this is equivalent to a missing
                // export — unhinted fallback at the caller.
                return Ok(None);
            };
            match IdentifyInstanceHint::from_json(&bytes) {
                Ok(hint) => Ok(Some(hint)),
                Err(err) => {
                    // Malformed hint or wrong version. Don't fail closed:
                    // the contract demands the host fall back to unhinted
                    // (invoke identify-instance with the global allowlist).
                    // Warn so the provider author can fix the hint.
                    warn!(
                        event = "provider.describe_identify_instance.malformed",
                        component_ref = %component_ref_owned,
                        error = %err,
                        "ignoring malformed describe-identify-instance hint; \
                         falling back to unhinted wrapper"
                    );
                    Ok(None)
                }
            }
        })
    }

    /// Cached wrapper around [`invoke_describe_identify_instance`]. The
    /// hint for a given `binding.component_ref` is invariant across
    /// inbound requests within a revision (it is a function of the
    /// component itself, not of the payload), so we probe lazily on
    /// first ask and reuse thereafter. `ArcSwap`-driven revision swaps
    /// allocate a fresh [`PackRuntime`], naturally invalidating the cache.
    ///
    /// Returns `None` when the component does not export the describe
    /// world, when the probe returns no hint, or when the probe fails
    /// (trap, timeout, instantiation error). Failures are warn-logged
    /// and cached — the same trap is logged once per revision per
    /// component, not per request.
    ///
    /// [`invoke_describe_identify_instance`]:
    ///     PackRuntime::invoke_describe_identify_instance
    pub async fn resolve_identify_hint(
        &self,
        binding: &ProviderBinding,
    ) -> Option<IdentifyInstanceHint> {
        if let Some(cached) = self.identify_hint_cache.read().get(&binding.component_ref) {
            return cached.clone();
        }
        let hint = match self.invoke_describe_identify_instance(binding).await {
            Ok(hint) => hint,
            Err(err) => {
                warn!(
                    event = "provider.describe_identify_instance.failed",
                    component_ref = %binding.component_ref,
                    error = %err,
                    "describe-identify-instance probe failed; \
                     falling back to unhinted wrapper for this component"
                );
                None
            }
        };
        // Tolerate a concurrent populate — `insert` is idempotent on the
        // same (component_ref, hint) shape and the probe is pure w.r.t.
        // the component, so re-probing on a write-race yields identical
        // bytes.
        self.identify_hint_cache
            .write()
            .insert(binding.component_ref.clone(), hint.clone());
        hint
    }

    /// Fan out [`resolve_identify_hint`] over each requested `provider_type`.
    /// Result map is keyed by `provider_type`; `None` value means the
    /// pack has no binding for that type OR the binding's component does
    /// not export the describe world (unhinted — caller forwards input
    /// headers unfiltered for back-compat).
    ///
    /// `provider_id`-collision errors from [`ProviderRegistry::resolve`]
    /// against a `provider_type` query are propagated (M1.1 invariant
    /// violation, malformed pack).
    ///
    /// Fan out [`resolve_identify_hint`] across requested types. `None` value
    /// means the pack has no binding for that type OR the binding's component
    /// does not export the describe world.
    ///
    /// The per-binding loop is inlined (rather than factored into a shared
    /// `AsyncFnMut`-based helper) deliberately: routing through an
    /// `AsyncFnMut` closure destabilises HRTB `Send` inference for the
    /// returned future, which propagates up to host-level fan-out APIs and
    /// from there to downstream spawned-service consumers. See the
    /// regression test `identify_futures_are_send` on the host.
    ///
    /// [`resolve_identify_hint`]: PackRuntime::resolve_identify_hint
    pub async fn describe_identify_hints_by_provider_type(
        &self,
        provider_types: &[&str],
    ) -> Result<HashMap<String, Option<IdentifyInstanceHint>>> {
        let mut out = HashMap::with_capacity(provider_types.len());
        let registry = match self.provider_registry_optional()? {
            Some(registry) => registry,
            None => {
                for ty in provider_types {
                    out.insert((*ty).to_string(), None);
                }
                return Ok(out);
            }
        };
        for ty in provider_types {
            let Some(binding) = registry.try_resolve(None, Some(ty))? else {
                out.insert((*ty).to_string(), None);
                continue;
            };
            let hint = self.resolve_identify_hint(&binding).await;
            out.insert((*ty).to_string(), hint);
        }
        Ok(out)
    }

    /// Unscoped legacy API: fan out [`invoke_identify_instance`] with the
    /// caller-supplied opaque `payload` bytes forwarded verbatim. No
    /// describe-identify-instance hint lookup, no per-provider header
    /// scoping. New callers should use the `_scoped` sibling for
    /// per-provider header allowlist scoping (Phase D).
    ///
    /// Loop inlined for the same reason as
    /// [`describe_identify_hints_by_provider_type`].
    ///
    /// [`invoke_identify_instance`]: PackRuntime::invoke_identify_instance
    /// [`describe_identify_hints_by_provider_type`]:
    ///     PackRuntime::describe_identify_hints_by_provider_type
    pub async fn identify_endpoints_by_provider_type(
        &self,
        provider_types: &[&str],
        payload: &[u8],
    ) -> Result<HashMap<String, IdentifyOutcome>> {
        let mut out = HashMap::with_capacity(provider_types.len());
        let registry = match self.provider_registry_optional()? {
            Some(registry) => registry,
            None => {
                for ty in provider_types {
                    out.insert((*ty).to_string(), IdentifyOutcome::Unsupported);
                }
                return Ok(out);
            }
        };
        for ty in provider_types {
            let Some(binding) = registry.try_resolve(None, Some(ty))? else {
                out.insert((*ty).to_string(), IdentifyOutcome::Unsupported);
                continue;
            };
            let outcome = self
                .invoke_identify_instance(&binding, payload.to_vec())
                .await?;
            out.insert((*ty).to_string(), outcome);
        }
        Ok(out)
    }

    /// Per-provider scoped variant of [`identify_endpoints_by_provider_type`].
    ///
    /// The wrapper payload is built per-binding from `(headers, body)` and
    /// the component's cached identify-instance hint (see
    /// [`resolve_identify_hint`]): hinted providers see only the headers
    /// their hint declares; unhinted providers see every header passed in.
    /// Result-map semantics match the unscoped variant.
    ///
    /// Loop inlined for the same reason as
    /// [`describe_identify_hints_by_provider_type`].
    ///
    /// [`identify_endpoints_by_provider_type`]:
    ///     PackRuntime::identify_endpoints_by_provider_type
    /// [`resolve_identify_hint`]: PackRuntime::resolve_identify_hint
    /// [`describe_identify_hints_by_provider_type`]:
    ///     PackRuntime::describe_identify_hints_by_provider_type
    pub async fn identify_endpoints_by_provider_type_scoped(
        &self,
        provider_types: &[&str],
        headers: &[(String, String)],
        body: &Value,
    ) -> Result<HashMap<String, IdentifyOutcome>> {
        let mut out = HashMap::with_capacity(provider_types.len());
        let registry = match self.provider_registry_optional()? {
            Some(registry) => registry,
            None => {
                for ty in provider_types {
                    out.insert((*ty).to_string(), IdentifyOutcome::Unsupported);
                }
                return Ok(out);
            }
        };
        for ty in provider_types {
            let Some(binding) = registry.try_resolve(None, Some(ty))? else {
                out.insert((*ty).to_string(), IdentifyOutcome::Unsupported);
                continue;
            };
            let hint = self.resolve_identify_hint(&binding).await;
            let payload = build_scoped_identify_payload(headers, body, hint.as_ref());
            let outcome = self.invoke_identify_instance(&binding, payload).await?;
            out.insert((*ty).to_string(), outcome);
        }
        Ok(out)
    }

    pub(crate) fn provider_registry(&self) -> Result<ProviderRegistry> {
        if let Some(registry) = self.provider_registry.read().clone() {
            return Ok(registry);
        }
        let manifest = self
            .manifest
            .as_ref()
            .context("pack manifest required for provider resolution")?;
        let env = std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_string());
        let registry = ProviderRegistry::new(
            manifest,
            self.state_store.clone(),
            &self.config.tenant,
            &env,
        )?;
        *self.provider_registry.write() = Some(registry.clone());
        Ok(registry)
    }

    pub(crate) fn provider_registry_optional(&self) -> Result<Option<ProviderRegistry>> {
        if self.manifest.is_none() {
            return Ok(None);
        }
        Ok(Some(self.provider_registry()?))
    }

    pub fn load_flow(&self, flow_id: &str) -> Result<Flow> {
        if let Some(cache) = &self.flows {
            return cache
                .flows
                .get(flow_id)
                .cloned()
                .ok_or_else(|| anyhow!("flow '{flow_id}' not found in pack"));
        }
        if let Some(manifest) = &self.manifest {
            let entry = manifest
                .flows
                .iter()
                .find(|f| f.id.as_str() == flow_id)
                .ok_or_else(|| anyhow!("flow '{flow_id}' not found in manifest"))?;
            return Ok(entry.flow.clone());
        }
        bail!("flow '{flow_id}' not available (pack exports disabled)")
    }

    pub fn metadata(&self) -> &PackMetadata {
        &self.metadata
    }

    /// Read an asset file from the pack's assets directory.
    ///
    /// Accepts paths like `assets/cards/card-a.json` or `cards/card-a.json`
    /// (the `assets/` prefix is stripped automatically).
    pub fn read_asset(&self, asset_path: &str) -> Result<Vec<u8>> {
        let normalized = asset_path
            .trim_start_matches("assets/")
            .trim_start_matches("/assets/");
        // Try assets tempdir first (extracted from archive).
        if let Some(tempdir) = &self.assets_tempdir {
            let full = tempdir.path().join("assets").join(normalized);
            if full.exists() {
                return std::fs::read(&full)
                    .with_context(|| format!("read asset {}", full.display()));
            }
        }
        // Try materialized directory.
        let full = self.path.join("assets").join(normalized);
        if full.exists() {
            return std::fs::read(&full).with_context(|| format!("read asset {}", full.display()));
        }
        bail!("asset not found: {}", asset_path)
    }

    pub fn component_manifest(&self, component_ref: &str) -> Option<&ComponentManifest> {
        self.component_manifests.get(component_ref)
    }

    /// Iterate every `(component_ref, manifest)` this pack holds. Used by the
    /// agentic-worker component tool source to enumerate the operations a
    /// worker may call as tools without exposing the internal manifest map.
    pub fn component_manifest_entries(&self) -> impl Iterator<Item = (&str, &ComponentManifest)> {
        self.component_manifests
            .iter()
            .map(|(component_ref, manifest)| (component_ref.as_str(), manifest))
    }

    /// Returns the raw agent config blobs embedded in this pack's manifest.
    ///
    /// Only present on the New (`greentic_types::PackManifest`) path; Legacy
    /// packs do not carry agent config and return an empty map. Callers
    /// (e.g. `TenantRuntime::from_packs`) deserialize these blobs into
    /// concrete `AgentConfig` structs via
    /// `agent_node::agent_configs_from_manifest`.
    pub fn manifest_agent_blobs(&self) -> std::collections::BTreeMap<String, serde_json::Value> {
        self.manifest
            .as_ref()
            .map(|m| m.agents.clone())
            .unwrap_or_default()
    }

    /// Read the optional `agent-graph.json` sidecar embedded at the pack root.
    ///
    /// Returns the raw bytes when present, or `None` when the pack carries no
    /// sidecar (the common case). Tries the materialized pack directory first,
    /// then the `.gtpack` archive — mirroring [`load_schema_json`]'s resolution.
    /// IO/zip errors are logged and treated as "absent" so a damaged or
    /// unreadable sidecar never aborts pack loading; the caller
    /// (`graph_node::graph_config_from_sidecar`) then validates the bytes.
    ///
    /// [`load_schema_json`]: PackRuntime::load_schema_json
    pub fn read_agent_graph_sidecar(&self) -> Option<Vec<u8>> {
        self.read_pack_file("agent-graph.json")
    }

    /// Raw agent-config blobs from the optional `dw-agents.json` sidecar.
    ///
    /// Designer-built packs (old greentic-pack, which cannot populate
    /// `manifest.agents`) embed their `AgentConfig` map here. Returns an empty
    /// map when the sidecar is absent or unparseable (lenient, mirroring
    /// [`PackRuntime::manifest_agent_blobs`]) so a damaged sidecar never aborts
    /// pack loading. `manifest.agents` remains authoritative; callers fill only
    /// the agent_ids the manifest did not carry.
    pub fn dw_agents_sidecar_blobs(&self) -> std::collections::BTreeMap<String, serde_json::Value> {
        let Some(bytes) = self.read_pack_file("dw-agents.json") else {
            return std::collections::BTreeMap::new();
        };
        match serde_json::from_slice::<std::collections::BTreeMap<String, serde_json::Value>>(
            &bytes,
        ) {
            Ok(map) => map,
            Err(error) => {
                tracing::warn!(error = %error, "ignoring malformed dw-agents.json sidecar");
                std::collections::BTreeMap::new()
            }
        }
    }

    /// Read a single named file from the pack by its archive-relative path,
    /// trying the materialized pack directory first and the `.gtpack` archive as
    /// a fallback. Returns `None` when the file is absent (or on a read error,
    /// which is logged). Used for sidecar files (`agent-graph.json`) and bundled
    /// assets (`knowledge_corpus.json`, `assets/knowledge/*.txt`).
    /// The secrets manager this pack was built with — the HOST's, not one
    /// derived from the environment.
    ///
    /// The MCP flow node needs it to resolve a pack-carried route's credential.
    /// Deriving its own from `SECRETS_BACKEND` cannot work: that only knows
    /// `env` and `broker`, while an operator booting a bundle runs on
    /// greentic-start's dev store, which the runner has no variant for. So the
    /// only manager that can read the credential is the one the host already
    /// handed us.
    pub fn secrets(&self) -> &crate::secrets::DynSecretsManager {
        &self.secrets
    }

    /// The pack's own MCP route sidecar, if it carries one.
    ///
    /// A pack's `mcp` node names only an opaque `server` id; resolving it
    /// otherwise needs the tenant's admin catalog, which a deployed runner has
    /// no credentials for. Parsed once and cached; every failure path yields
    /// `None` so the caller falls back to the admin catalog rather than taking
    /// the whole pack down.
    pub fn mcp_routes(&self) -> Option<&crate::runner::mcp_pack_routes::PackMcpRoutes> {
        self.mcp_routes
            .get_or_init(|| {
                self.read_pack_file(crate::runner::mcp_pack_routes::MCP_ROUTES_ENTRY)
                    .and_then(|bytes| {
                        crate::runner::mcp_pack_routes::PackMcpRoutes::from_sidecar_bytes(&bytes)
                    })
            })
            .as_ref()
    }

    pub fn read_pack_file(&self, name: &str) -> Option<Vec<u8>> {
        // Materialized pack directory (root holds manifest.cbor + sidecars/assets).
        if self.path.is_dir() {
            let candidate = self.path.join(name);
            if candidate.exists() {
                match std::fs::read(&candidate) {
                    Ok(bytes) => return Some(bytes),
                    Err(error) => {
                        tracing::warn!(
                            path = %candidate.display(),
                            error = %error,
                            "failed to read {name} from pack directory"
                        );
                        return None;
                    }
                }
            }
        }

        // `.gtpack` archive.
        let archive_path = self
            .archive_path
            .as_ref()
            .or_else(|| path_is_gtpack(&self.path).then_some(&self.path))?;
        let file = match File::open(archive_path) {
            Ok(file) => file,
            Err(error) => {
                tracing::warn!(
                    path = %archive_path.display(),
                    error = %error,
                    "failed to open pack archive while reading {name}"
                );
                return None;
            }
        };
        let mut archive = match ZipArchive::new(file) {
            Ok(archive) => archive,
            Err(error) => {
                tracing::warn!(
                    path = %archive_path.display(),
                    error = %error,
                    "failed to read pack archive while reading {name}"
                );
                return None;
            }
        };
        match archive.by_name(name) {
            Ok(mut entry) => {
                let mut bytes = Vec::new();
                if let Err(error) = entry.read_to_end(&mut bytes) {
                    tracing::warn!(
                        path = %archive_path.display(),
                        error = %error,
                        "failed to extract {name} from pack archive"
                    );
                    return None;
                }
                Some(bytes)
            }
            Err(zip::result::ZipError::FileNotFound) => None,
            Err(error) => {
                tracing::warn!(
                    path = %archive_path.display(),
                    error = %error,
                    "error reading {name} from pack archive"
                );
                None
            }
        }
    }

    pub fn describe_component_contract_v0_6(&self, component_ref: &str) -> Result<Option<Value>> {
        let pack_component = self
            .components
            .get(component_ref)
            .with_context(|| format!("component '{component_ref}' not found in pack"))?;
        let engine = self.engine.clone();
        let config = Arc::clone(&self.config);
        let http_client = Arc::clone(&self.http_client);
        let mocks = self.mocks.clone();
        let session_store = self.session_store.clone();
        let state_store = self.state_store.clone();
        let secrets = Arc::clone(&self.secrets);
        let oauth_config = self.oauth_config.clone();
        let wasi_policy = Arc::clone(&self.wasi_policy);
        let pack_id = self.metadata().pack_id.clone();
        let allow_state_store = self.allows_state_store(component_ref);
        let component = pack_component.component.clone();
        let component_ref_owned = component_ref.to_string();
        let runtime_config_non_secret = self.runtime_config_non_secret.clone();
        let runtime_refs = self.runtime_refs.clone();

        run_on_wasi_thread("component.describe", move || {
            let mut linker = Linker::new(&engine);
            register_all(&mut linker, allow_state_store)?;
            add_component_control_to_linker(&mut linker)?;

            let host_state = HostState::new(
                pack_id.clone(),
                config,
                http_client,
                mocks,
                session_store,
                state_store,
                secrets,
                oauth_config,
                None,
                Some(component_ref_owned),
                false,
                runtime_config_non_secret,
                runtime_refs,
            )?;
            let store_state = ComponentState::new(host_state, wasi_policy)?;
            let mut store = wasmtime::Store::new(&engine, store_state);
            let pre_instance = linker.instantiate_pre(&component)?;
            let pre = match component_api::v0_6_descriptor::ComponentV0V6V0Pre::new(pre_instance) {
                Ok(pre) => pre,
                Err(_) => return Ok(None),
            };
            let bytes = block_on(async {
                let bindings = pre.instantiate_async(&mut store).await?;
                let descriptor = bindings.greentic_component_component_descriptor();
                descriptor.call_describe(&mut store)
            })?;

            if bytes.is_empty() {
                return Ok(Some(Value::Null));
            }
            if let Ok(value) = serde_cbor::from_slice::<Value>(&bytes) {
                return Ok(Some(value));
            }
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                return Ok(Some(value));
            }
            if let Ok(text) = String::from_utf8(bytes) {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    return Ok(Some(value));
                }
                return Ok(Some(Value::String(text)));
            }
            Ok(Some(Value::Null))
        })
    }

    pub fn load_schema_json(&self, schema_ref: &str) -> Result<Option<Value>> {
        let rel = normalize_schema_ref(schema_ref)?;
        if self.path.is_dir() {
            let candidate = self.path.join(&rel);
            if candidate.exists() {
                let bytes = std::fs::read(&candidate).with_context(|| {
                    format!("failed to read schema file {}", candidate.display())
                })?;
                let value = serde_json::from_slice::<Value>(&bytes)
                    .with_context(|| format!("invalid schema JSON in {}", candidate.display()))?;
                return Ok(Some(value));
            }
        }

        if let Some(archive_path) = self
            .archive_path
            .as_ref()
            .or_else(|| path_is_gtpack(&self.path).then_some(&self.path))
        {
            let file = File::open(archive_path)
                .with_context(|| format!("failed to open {}", archive_path.display()))?;
            let mut archive = ZipArchive::new(file)
                .with_context(|| format!("failed to read pack {}", archive_path.display()))?;
            match archive.by_name(&rel) {
                Ok(mut entry) => {
                    let mut bytes = Vec::new();
                    entry.read_to_end(&mut bytes)?;
                    let value = serde_json::from_slice::<Value>(&bytes).with_context(|| {
                        format!("invalid schema JSON in {}:{}", archive_path.display(), rel)
                    })?;
                    Ok(Some(value))
                }
                Err(zip::result::ZipError::FileNotFound) => Ok(None),
                Err(err) => Err(anyhow!(err)).with_context(|| {
                    format!(
                        "failed to read schema `{}` from {}",
                        rel,
                        archive_path.display()
                    )
                }),
            }
        } else {
            Ok(None)
        }
    }

    pub fn required_secrets(&self) -> &[greentic_types::SecretRequirement] {
        &self.metadata.secret_requirements
    }

    pub fn missing_secrets(
        &self,
        tenant_ctx: &TypesTenantCtx,
    ) -> Vec<greentic_types::SecretRequirement> {
        let env = tenant_ctx.env.as_str().to_string();
        let tenant = tenant_ctx.tenant.as_str().to_string();
        let team = tenant_ctx.team.as_ref().map(|t| t.as_str().to_string());
        self.required_secrets()
            .iter()
            .filter(|req| {
                // scope must match current context if provided
                if let Some(scope) = &req.scope {
                    if scope.env != env {
                        return false;
                    }
                    if scope.tenant != tenant {
                        return false;
                    }
                    if let Some(ref team_req) = scope.team
                        && team.as_ref() != Some(team_req)
                    {
                        return false;
                    }
                }
                let ctx = self.config.tenant_ctx();
                read_secret_blocking(
                    &self.secrets,
                    &ctx,
                    &self.metadata.pack_id,
                    canonicalize_secret_key(req.key.as_str()).as_str(),
                )
                .is_err()
            })
            .cloned()
            .collect()
    }

    pub fn for_component_test(
        components: Vec<(String, PathBuf)>,
        flows: HashMap<String, FlowIR>,
        pack_id: &str,
        config: Arc<HostConfig>,
    ) -> Result<Self> {
        let engine = Engine::default();
        let engine_profile =
            EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
        let cache = CacheManager::new(CacheConfig::default(), engine_profile);
        let mut component_map = HashMap::new();
        for (name, path) in components {
            if !path.exists() {
                bail!("component artifact missing: {}", path.display());
            }
            let wasm_bytes = std::fs::read(&path)?;
            let component =
                Arc::new(Component::from_binary(&engine, &wasm_bytes).map_err(|err| {
                    anyhow!("failed to compile component {}: {err}", path.display())
                })?);
            component_map.insert(
                name.clone(),
                PackComponent {
                    name,
                    version: "0.0.0".into(),
                    component,
                },
            );
        }

        let mut flow_map = HashMap::new();
        let mut descriptors = Vec::new();
        for (id, ir) in flows {
            let flow_type = ir.flow_type.clone();
            let flow = flow_ir_to_flow(ir)?;
            flow_map.insert(id.clone(), flow);
            descriptors.push(FlowDescriptor {
                id: id.clone(),
                flow_type,
                pack_id: pack_id.to_string(),
                profile: "test".into(),
                version: "0.0.0".into(),
                description: None,
                entry: true,
            });
        }
        let entry_flows = descriptors.iter().map(|flow| flow.id.clone()).collect();
        let metadata = PackMetadata {
            pack_id: pack_id.to_string(),
            version: "0.0.0".into(),
            entry_flows,
            secret_requirements: Vec::new(),
        };
        let flows_cache = PackFlows {
            descriptors: descriptors.clone(),
            flows: flow_map,
            metadata: metadata.clone(),
        };

        Ok(Self {
            path: PathBuf::new(),
            archive_path: None,
            config,
            engine,
            metadata,
            manifest: None,
            legacy_manifest: None,
            component_manifests: HashMap::new(),
            mocks: None,
            flows: Some(flows_cache),
            components: component_map,
            http_client: Arc::clone(&HTTP_CLIENT),
            session_store: None,
            state_store: None,
            wasi_policy: Arc::new(RunnerWasiPolicy::new()),
            mcp_routes: std::sync::OnceLock::new(),
            assets_tempdir: None,
            provider_registry: RwLock::new(None),
            identify_hint_cache: RwLock::new(HashMap::new()),
            secrets: crate::secrets::default_manager()?,
            oauth_config: None,
            cache,
            runtime_config_non_secret: None,
            runtime_refs: None,
        })
    }
}

/// Resolve a flow node's component reference to the key under which the
/// component is actually registered, given the requested `operation` and a
/// `is_registered` membership predicate over the pack's component keys.
///
/// greentic-pack resolves a component node to a bare component symbol
/// (e.g. `ai.greentic.component-templates`) and carries the operation
/// separately, so the full reference is the registration key. Older,
/// hand-authored flows instead pack the operation into the node id
/// (`qa.process`) while registering the component under the bare name
/// (`qa`). For those, fall back to the segment before the last dot — but
/// ONLY when that trailing segment IS the requested operation. Without the
/// suffix check, a missing dotted component whose prefix happens to be a
/// *different* registered component (`ai.greentic.component-templates` absent,
/// `ai.greentic` present) would silently resolve to the wrong component and
/// run it with the caller's tenant/session/state/secrets. Returns the
/// reference unchanged when neither form matches, so the caller's
/// "not found" error names the original reference.
fn resolve_component_key<'a>(
    component_ref: &'a str,
    operation: &str,
    is_registered: impl Fn(&str) -> bool,
) -> &'a str {
    if is_registered(component_ref) {
        return component_ref;
    }
    if let Some((prefix, suffix)) = component_ref.rsplit_once('.')
        && suffix == operation
        && is_registered(prefix)
    {
        return prefix;
    }
    component_ref
}

#[cfg(test)]
mod resolve_component_key_tests {
    use super::resolve_component_key;
    use std::collections::HashSet;

    fn registered(keys: &[&'static str]) -> impl Fn(&str) -> bool {
        let set: HashSet<&'static str> = keys.iter().copied().collect();
        move |key: &str| set.contains(key)
    }

    #[test]
    fn full_reference_is_used_when_registered() {
        // greentic-pack's resolved symbol: full ref is the registration key.
        let is_reg = registered(&["ai.greentic.component-templates", "ai.greentic"]);
        assert_eq!(
            resolve_component_key("ai.greentic.component-templates", "handle_message", is_reg),
            "ai.greentic.component-templates"
        );
    }

    #[test]
    fn legacy_packed_id_falls_back_when_suffix_is_operation() {
        // `qa.process` packs op into the id; component registered as `qa`.
        let is_reg = registered(&["qa"]);
        assert_eq!(resolve_component_key("qa.process", "process", is_reg), "qa");
    }

    #[test]
    fn drifted_dotted_reference_does_not_fall_back_to_prefix() {
        // Full symbol absent, a *different* prefix component present, and the
        // trailing segment is NOT the requested operation -> must not silently
        // resolve to the prefix; return the original so the caller errors out.
        let is_reg = registered(&["ai.greentic"]);
        assert_eq!(
            resolve_component_key("ai.greentic.component-templates", "handle_message", is_reg),
            "ai.greentic.component-templates"
        );
    }

    #[test]
    fn unregistered_reference_is_returned_unchanged() {
        let is_reg = registered(&[]);
        assert_eq!(resolve_component_key("foo", "bar", is_reg), "foo");
    }
}

fn normalize_schema_ref(schema_ref: &str) -> Result<String> {
    let candidate = schema_ref.trim();
    if candidate.is_empty() {
        bail!("schema ref cannot be empty");
    }
    let path = Path::new(candidate);
    if path.is_absolute() {
        bail!("schema ref must be relative: {}", schema_ref);
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(part) => normalized.push(part),
            std::path::Component::CurDir => {}
            _ => bail!("schema ref must not contain traversal: {}", schema_ref),
        }
    }
    let normalized = normalized
        .to_str()
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("schema ref must be valid UTF-8"))?;
    if normalized.is_empty() {
        bail!("schema ref cannot normalize to empty path");
    }
    Ok(normalized)
}

fn path_is_gtpack(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("gtpack"))
        .unwrap_or(false)
}

fn is_missing_node_export(err: &wasmtime::Error, version: &str) -> bool {
    let message = err.to_string();
    message.contains("no exported instance named")
        && message.contains(&format!("greentic:component/node@{version}"))
}

struct PackFlows {
    descriptors: Vec<FlowDescriptor>,
    flows: HashMap<String, Flow>,
    metadata: PackMetadata,
}

const RUNTIME_FLOW_EXTENSION_IDS: [&str; 3] = [
    "greentic.pack.runtime_flow",
    "greentic.pack.flow_runtime",
    "greentic.pack.runtime_flows",
];

#[derive(Debug, Deserialize)]
struct RuntimeFlowBundle {
    flows: Vec<RuntimeFlow>,
}

#[derive(Debug, Deserialize)]
struct RuntimeFlow {
    id: String,
    #[serde(alias = "flow_type")]
    kind: FlowKind,
    #[serde(default)]
    schema_version: Option<String>,
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    entrypoints: BTreeMap<String, Value>,
    nodes: BTreeMap<String, RuntimeNode>,
    #[serde(default)]
    metadata: Option<FlowMetadata>,
}

#[derive(Debug, Deserialize)]
struct RuntimeNode {
    #[serde(alias = "component")]
    component_id: String,
    #[serde(default, alias = "operation")]
    operation_name: Option<String>,
    #[serde(default, alias = "payload", alias = "input")]
    operation_payload: Value,
    #[serde(default)]
    config: Value,
    #[serde(default)]
    routing: Option<Routing>,
    #[serde(default)]
    telemetry: Option<TelemetryHints>,
}

fn deserialize_json_bytes(bytes: Vec<u8>) -> Result<Value> {
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes).or_else(|_| {
        String::from_utf8(bytes)
            .map(Value::String)
            .map_err(|err| anyhow!(err))
    })
}

/// `wasmtime::component::bindgen!` returns this error shape when a
/// `*Pre::new(...)` call resolves a world whose required export is
/// absent on the component. We treat that as "component does not opt
/// in" and let the caller fall back to the operator's statically
/// declared instance. Mirrors the same pattern in `invoke_provider`
/// for the legacy/path schema-core fallback.
///
/// The match is intentionally narrow: the error must mention BOTH a
/// broad wasmtime marker (`"no exported instance named"` or
/// `"no exported function named"`) AND the identity-world-specific
/// name segment (`"instance-identity-api"`, `"identify-instance"`,
/// `"instance-identity-describe-api"`, or `"describe-identify-instance"`).
/// A component that exports the identity world with a malformed
/// signature or a typo'd function name will NOT be silently treated
/// as unsupported — it will surface as a hard error.
fn is_missing_export_error(message: &str) -> bool {
    let has_broad_marker = message.contains("no exported instance named")
        || message.contains("no exported function named");
    let has_identity_segment = message.contains("instance-identity-api")
        || message.contains("identify-instance")
        || message.contains("instance-identity-describe-api")
        || message.contains("describe-identify-instance");
    has_broad_marker && has_identity_segment
}

/// Build the M1 IID.4d wrapper payload (`{ headers, body }`) scoped per
/// the provider's [`IdentifyInstanceHint`].
///
/// - `Some(hint)` ⇒ headers are filtered to ONLY those whose lowercase
///   name appears in [`hint.header_names()`](IdentifyInstanceHint::header_names).
///   A hint with no `Header` sources yields `"headers": []` — the
///   component is declaring that it identifies from the body alone.
/// - `None` ⇒ headers pass through unfiltered. The caller is responsible
///   for prefiltering (greentic-start applies a global allowlist at the
///   ingress boundary), so back-compat with not-yet-hinted providers
///   matches the pre-PR-B2 behavior exactly: every probed component
///   receives every allowlisted header.
///
/// `body` is forwarded verbatim regardless of hint shape. Body-path
/// short-circuit (using the hint's `BodyPath { json_pointer }` to skip
/// invoking `identify-instance` entirely) is a deliberately-deferred
/// Phase D follow-up — the current pass scopes the header allowlist only.
fn build_scoped_identify_payload(
    headers: &[(String, String)],
    body: &Value,
    hint: Option<&IdentifyInstanceHint>,
) -> Vec<u8> {
    let scoped_headers: Vec<&(String, String)> = match hint {
        // Hints carry 1-3 source headers in practice; a linear scan beats
        // a HashSet for that size (no hash + no allocation).
        Some(hint) => {
            let allowed = hint.header_names();
            headers
                .iter()
                .filter(|(name, _)| allowed.contains(&name.as_str()))
                .collect()
        }
        None => headers.iter().collect(),
    };
    let wrapper = serde_json::json!({
        "headers": scoped_headers
            .iter()
            .map(|(name, value)| serde_json::json!({ "name": name, "value": value }))
            .collect::<Vec<_>>(),
        "body": body,
    });
    serde_json::to_vec(&wrapper).expect("wrapper payload always serializes")
}

#[cfg(test)]
mod build_scoped_identify_payload_tests {
    use super::*;
    use crate::identify_hint::HintSource;
    use serde_json::json;

    fn hint(sources: Vec<HintSource>) -> IdentifyInstanceHint {
        IdentifyInstanceHint { sources }
    }

    #[test]
    fn unhinted_passes_all_input_headers_through() {
        // Back-compat: components without describe-identify-instance must
        // continue to see every header the caller (greentic-start)
        // allowlisted. Pre-PR-B2 behavior verbatim.
        let headers = vec![
            (
                "x-telegram-bot-api-secret-token".into(),
                "telegram-tok".into(),
            ),
            ("x-future-routing-tag".into(), "abc".into()),
        ];
        let body = json!({ "update_id": 1 });
        let bytes = build_scoped_identify_payload(&headers, &body, None);
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            parsed["headers"],
            json!([
                { "name": "x-telegram-bot-api-secret-token", "value": "telegram-tok" },
                { "name": "x-future-routing-tag", "value": "abc" }
            ])
        );
        assert_eq!(parsed["body"], body);
    }

    #[test]
    fn header_hint_filters_to_declared_names_only() {
        // Telegram-shape hint: declares one header, sees only that one.
        // Other allowlisted headers (e.g. a future Slack signature) MUST
        // NOT leak into the Telegram probe.
        let h = hint(vec![HintSource::Header {
            name: "x-telegram-bot-api-secret-token".into(),
        }]);
        let headers = vec![
            (
                "x-telegram-bot-api-secret-token".into(),
                "telegram-tok".into(),
            ),
            ("x-slack-signature".into(), "v0=sig".into()),
        ];
        let body = json!({});
        let bytes = build_scoped_identify_payload(&headers, &body, Some(&h));
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            parsed["headers"],
            json!([
                { "name": "x-telegram-bot-api-secret-token", "value": "telegram-tok" }
            ])
        );
    }

    #[test]
    fn hints_without_header_sources_drop_all_headers() {
        // Body-path-only (Teams-shape) and degenerate-empty hints both yield
        // an empty `Header` source set; the wrapper MUST carry no headers
        // either way. Passing Telegram's secret token through to either is
        // the exact blast-radius bug PR-B2 closes.
        let headers = vec![(
            "x-telegram-bot-api-secret-token".into(),
            "should-not-leak".into(),
        )];
        let body = json!({ "anything": true });
        for h in [
            hint(vec![HintSource::BodyPath {
                json_pointer: "/recipient/id".into(),
            }]),
            hint(vec![]),
        ] {
            let bytes = build_scoped_identify_payload(&headers, &body, Some(&h));
            let parsed: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(parsed["headers"], json!([]), "hint={:?}", h.sources);
            assert_eq!(parsed["body"], body);
        }
    }

    #[test]
    fn header_filter_preserves_input_order_and_dups() {
        // Multi-value headers and ordering matter to debuggability
        // (operators reading the wrapper from a probe should see the
        // headers in the same order they arrived). Filter is a
        // retain-only operation; no sort, no dedup.
        let h = hint(vec![HintSource::Header {
            name: "x-route".into(),
        }]);
        let headers = vec![
            ("x-route".into(), "a".into()),
            ("x-other".into(), "skip".into()),
            ("x-route".into(), "b".into()),
        ];
        let body = json!({});
        let bytes = build_scoped_identify_payload(&headers, &body, Some(&h));
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            parsed["headers"],
            json!([
                { "name": "x-route", "value": "a" },
                { "name": "x-route", "value": "b" }
            ])
        );
    }
}

impl PackFlows {
    fn from_manifest(manifest: greentic_types::PackManifest) -> Self {
        if let Some(flows) = flows_from_runtime_extension(&manifest) {
            return flows;
        }
        let descriptors = manifest
            .flows
            .iter()
            .map(|entry| FlowDescriptor {
                id: entry.id.as_str().to_string(),
                flow_type: flow_kind_to_str(entry.kind).to_string(),
                pack_id: manifest.pack_id.as_str().to_string(),
                profile: manifest.pack_id.as_str().to_string(),
                version: manifest.version.to_string(),
                description: None,
                entry: tags_indicate_entry(entry.tags.iter().map(String::as_str)),
            })
            .collect();
        let mut flows = HashMap::new();
        for entry in &manifest.flows {
            flows.insert(entry.id.as_str().to_string(), entry.flow.clone());
        }
        Self {
            metadata: PackMetadata::from_manifest(&manifest),
            descriptors,
            flows,
        }
    }
}

fn flows_from_runtime_extension(manifest: &greentic_types::PackManifest) -> Option<PackFlows> {
    let extensions = manifest.extensions.as_ref()?;
    let extension = extensions.iter().find_map(|(key, ext)| {
        if RUNTIME_FLOW_EXTENSION_IDS
            .iter()
            .any(|candidate| candidate == key)
        {
            Some(ext)
        } else {
            None
        }
    })?;
    let runtime_flows = match decode_runtime_flow_extension(extension) {
        Some(flows) if !flows.is_empty() => flows,
        _ => return None,
    };

    let descriptors = runtime_flows
        .iter()
        .map(|flow| FlowDescriptor {
            id: flow.id.as_str().to_string(),
            flow_type: flow_kind_to_str(flow.kind).to_string(),
            pack_id: manifest.pack_id.as_str().to_string(),
            profile: manifest.pack_id.as_str().to_string(),
            version: manifest.version.to_string(),
            description: None,
            entry: tags_indicate_entry(flow.metadata.tags.iter().map(String::as_str)),
        })
        .collect::<Vec<_>>();
    let flows = runtime_flows
        .into_iter()
        .map(|flow| (flow.id.as_str().to_string(), flow))
        .collect();

    Some(PackFlows {
        metadata: PackMetadata::from_manifest(manifest),
        descriptors,
        flows,
    })
}

fn decode_runtime_flow_extension(extension: &ExtensionRef) -> Option<Vec<Flow>> {
    let value = match extension.inline.as_ref()? {
        ExtensionInline::Other(value) => value.clone(),
        _ => return None,
    };

    if let Ok(bundle) = serde_json::from_value::<RuntimeFlowBundle>(value.clone()) {
        return Some(collect_runtime_flows(bundle.flows));
    }

    if let Ok(flows) = serde_json::from_value::<Vec<RuntimeFlow>>(value.clone()) {
        return Some(collect_runtime_flows(flows));
    }

    if let Ok(flows) = serde_json::from_value::<Vec<Flow>>(value) {
        return Some(flows);
    }

    warn!(
        extension = %extension.kind,
        version = %extension.version,
        "runtime flow extension present but could not be decoded"
    );
    None
}

fn collect_runtime_flows(flows: Vec<RuntimeFlow>) -> Vec<Flow> {
    flows
        .into_iter()
        .filter_map(|flow| match runtime_flow_to_flow(flow) {
            Ok(flow) => Some(flow),
            Err(err) => {
                warn!(error = %err, "failed to decode runtime flow");
                None
            }
        })
        .collect()
}

fn runtime_flow_to_flow(runtime: RuntimeFlow) -> Result<Flow> {
    let flow_id = FlowId::from_str(&runtime.id)
        .with_context(|| format!("invalid flow id `{}`", runtime.id))?;
    let mut entrypoints = runtime.entrypoints;
    if entrypoints.is_empty()
        && let Some(start) = &runtime.start
    {
        entrypoints.insert("default".into(), Value::String(start.clone()));
    }

    let mut nodes: IndexMap<NodeId, Node, FlowHasher> = IndexMap::default();
    for (id, node) in runtime.nodes {
        let node_id = NodeId::from_str(&id).with_context(|| format!("invalid node id `{id}`"))?;
        let component_id = ComponentId::from_str(&node.component_id)
            .with_context(|| format!("invalid component id `{}`", node.component_id))?;
        let operation_payload = if node.config.is_null() {
            node.operation_payload
        } else {
            serde_json::json!({
                "input": node.operation_payload,
                "config": node.config,
            })
        };
        let component = FlowComponentRef {
            id: component_id,
            pack_alias: None,
            operation: node.operation_name,
        };
        let routing = node.routing.unwrap_or(Routing::End);
        let telemetry = node.telemetry.unwrap_or_default();
        nodes.insert(
            node_id.clone(),
            Node {
                id: node_id,
                component,
                input: InputMapping {
                    mapping: operation_payload,
                },
                output: OutputMapping {
                    mapping: Value::Null,
                },
                err_map: None,
                routing,
                telemetry,
                conversational: false,
            },
        );
    }

    Ok(Flow {
        schema_version: runtime.schema_version.unwrap_or_else(|| "1.0".to_string()),
        id: flow_id,
        kind: runtime.kind,
        entrypoints,
        nodes,
        metadata: runtime.metadata.unwrap_or_default(),
    })
}

fn flow_kind_to_str(kind: greentic_types::FlowKind) -> &'static str {
    match kind {
        greentic_types::FlowKind::Messaging => "messaging",
        greentic_types::FlowKind::Event => "event",
        greentic_types::FlowKind::ComponentConfig => "component-config",
        greentic_types::FlowKind::Job => "job",
        greentic_types::FlowKind::Http => "http",
    }
}

fn read_entry(archive: &mut ZipArchive<File>, name: &str) -> Result<Vec<u8>> {
    let mut file = archive
        .by_name(name)
        .with_context(|| format!("entry {name} missing from archive"))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

fn normalize_flow_doc(mut doc: FlowDoc) -> FlowDoc {
    for node in doc.nodes.values_mut() {
        let Some((component_ref, payload)) = node
            .raw
            .iter()
            .next()
            .map(|(key, value)| (key.clone(), value.clone()))
        else {
            continue;
        };
        // Runner-native op-keys (`emit.*`, `mcp`, `dw.agent`, `sorla.call`, …)
        // are dispatched by the engine directly off the `component` string, so
        // they must survive verbatim — wrapping them in a `component.exec` node
        // would misclassify them as `NodeKind::Exec`. The runtime-flow load
        // path (`flow_doc_to_ir`) already preserves them; mirror that here for
        // the legacy flow-JSON path using the shared op-key predicate.
        if is_native_op_key(&component_ref) {
            node.operation = Some(component_ref);
            node.payload = payload;
            node.raw.clear();
            continue;
        }
        let (target_component, operation, input, config) =
            infer_component_exec(&payload, &component_ref);
        let mut payload_obj = serde_json::Map::new();
        // component.exec is meta; ensure the payload carries the actual target component.
        payload_obj.insert("component".into(), Value::String(target_component));
        payload_obj.insert("operation".into(), Value::String(operation));
        payload_obj.insert("input".into(), input);
        if let Some(cfg) = config {
            payload_obj.insert("config".into(), cfg);
        }
        node.operation = Some("component.exec".to_string());
        node.payload = Value::Object(payload_obj);
        node.raw.clear();
    }
    doc
}

fn infer_component_exec(
    payload: &Value,
    component_ref: &str,
) -> (String, String, Value, Option<Value>) {
    let default_op = if component_ref.starts_with("templating.") {
        "render"
    } else {
        "invoke"
    }
    .to_string();

    if let Value::Object(map) = payload {
        let has_embedded_component =
            map.get("component").is_some() || map.get("component_ref").is_some();
        let op = map
            .get("op")
            .or_else(|| map.get("operation"))
            .and_then(Value::as_str)
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                if has_embedded_component {
                    component_ref.to_string()
                } else {
                    default_op.clone()
                }
            });

        let mut input = map.clone();
        let config = input.remove("config");
        let canonical_input = if has_embedded_component {
            input.get("input").cloned()
        } else {
            None
        };
        let component = input
            .get("component")
            .or_else(|| input.get("component_ref"))
            .and_then(Value::as_str)
            .map(|s| s.to_string())
            .unwrap_or_else(|| component_ref.to_string());
        input.remove("component");
        input.remove("component_ref");
        input.remove("op");
        input.remove("operation");
        let input = canonical_input.unwrap_or(Value::Object(input));
        return (component, op, input, config);
    }

    (component_ref.to_string(), default_op, payload.clone(), None)
}

#[derive(Clone, Debug)]
struct ComponentSpec {
    id: String,
    version: String,
    legacy_path: Option<String>,
}

#[derive(Clone, Debug)]
struct ComponentSourceInfo {
    digest: Option<String>,
    source: ComponentSourceRef,
    artifact: ComponentArtifactLocation,
    expected_wasm_sha256: Option<String>,
    skip_digest_verification: bool,
}

#[derive(Clone, Debug)]
enum ComponentArtifactLocation {
    Inline { wasm_path: String },
    Remote,
}

#[derive(Clone, Debug, Deserialize)]
struct PackLockV1 {
    schema_version: u32,
    components: Vec<PackLockComponent>,
}

#[derive(Clone, Debug, Deserialize)]
struct PackLockComponent {
    name: String,
    #[serde(default, rename = "source_ref")]
    source_ref: Option<String>,
    #[serde(default, rename = "ref")]
    legacy_ref: Option<String>,
    #[serde(default)]
    component_id: Option<ComponentId>,
    #[serde(default)]
    bundled: Option<bool>,
    #[serde(default, rename = "bundled_path")]
    bundled_path: Option<String>,
    #[serde(default, rename = "path")]
    legacy_path: Option<String>,
    #[serde(default)]
    wasm_sha256: Option<String>,
    #[serde(default, rename = "sha256")]
    legacy_sha256: Option<String>,
    #[serde(default)]
    resolved_digest: Option<String>,
    #[serde(default)]
    digest: Option<String>,
}

fn component_specs(
    manifest: Option<&greentic_types::PackManifest>,
    legacy_manifest: Option<&legacy_pack::PackManifest>,
    component_sources: Option<&ComponentSourcesV1>,
    pack_lock: Option<&PackLockV1>,
) -> Vec<ComponentSpec> {
    if let Some(manifest) = manifest {
        if !manifest.components.is_empty() {
            return manifest
                .components
                .iter()
                .map(|entry| ComponentSpec {
                    id: entry.id.as_str().to_string(),
                    version: entry.version.to_string(),
                    legacy_path: None,
                })
                .collect();
        }
        if let Some(lock) = pack_lock {
            let mut seen = HashSet::new();
            let mut specs = Vec::new();
            for entry in &lock.components {
                let id = entry
                    .component_id
                    .as_ref()
                    .map(|id| id.as_str())
                    .unwrap_or(entry.name.as_str());
                if seen.insert(id.to_string()) {
                    specs.push(ComponentSpec {
                        id: id.to_string(),
                        version: "0.0.0".to_string(),
                        legacy_path: None,
                    });
                }
            }
            return specs;
        }
        if let Some(sources) = component_sources {
            let mut seen = HashSet::new();
            let mut specs = Vec::new();
            for entry in &sources.components {
                let id = entry
                    .component_id
                    .as_ref()
                    .map(|id| id.as_str())
                    .unwrap_or(entry.name.as_str());
                if seen.insert(id.to_string()) {
                    specs.push(ComponentSpec {
                        id: id.to_string(),
                        version: "0.0.0".to_string(),
                        legacy_path: None,
                    });
                }
            }
            return specs;
        }
    }
    if let Some(legacy_manifest) = legacy_manifest {
        return legacy_manifest
            .components
            .iter()
            .map(|entry| ComponentSpec {
                id: entry.name.clone(),
                version: entry.version.to_string(),
                legacy_path: Some(entry.file_wasm.clone()),
            })
            .collect();
    }
    Vec::new()
}

fn component_sources_table(
    sources: Option<&ComponentSourcesV1>,
) -> Result<Option<HashMap<String, ComponentSourceInfo>>> {
    let Some(sources) = sources else {
        return Ok(None);
    };
    let mut table = HashMap::new();
    for entry in &sources.components {
        let artifact = match &entry.artifact {
            ArtifactLocationV1::Inline { wasm_path, .. } => ComponentArtifactLocation::Inline {
                wasm_path: wasm_path.clone(),
            },
            ArtifactLocationV1::Remote => ComponentArtifactLocation::Remote,
        };
        let info = ComponentSourceInfo {
            digest: Some(entry.resolved.digest.clone()),
            source: entry.source.clone(),
            artifact,
            expected_wasm_sha256: None,
            skip_digest_verification: false,
        };
        if let Some(component_id) = entry.component_id.as_ref() {
            table.insert(component_id.as_str().to_string(), info.clone());
        }
        table.insert(entry.name.clone(), info);
    }
    Ok(Some(table))
}

fn load_pack_lock(path: &Path) -> Result<Option<PackLockV1>> {
    let lock_path = if path.is_dir() {
        let candidate = path.join("pack.lock");
        if candidate.exists() {
            Some(candidate)
        } else {
            let candidate = path.join("pack.lock.json");
            candidate.exists().then_some(candidate)
        }
    } else {
        None
    };
    let Some(lock_path) = lock_path else {
        return Ok(None);
    };
    let raw = std::fs::read_to_string(&lock_path)
        .with_context(|| format!("failed to read {}", lock_path.display()))?;
    let lock: PackLockV1 = serde_json::from_str(&raw).context("failed to parse pack.lock")?;
    if lock.schema_version != 1 {
        bail!("pack.lock schema_version must be 1");
    }
    Ok(Some(lock))
}

fn find_pack_lock_roots(
    pack_path: &Path,
    is_dir: bool,
    archive_hint: Option<&Path>,
) -> Vec<PathBuf> {
    if is_dir {
        return vec![pack_path.to_path_buf()];
    }
    let mut roots = Vec::new();
    if let Some(archive_path) = archive_hint {
        if let Some(parent) = archive_path.parent() {
            roots.push(parent.to_path_buf());
            if let Some(grandparent) = parent.parent() {
                roots.push(grandparent.to_path_buf());
            }
        }
    } else if let Some(parent) = pack_path.parent() {
        roots.push(parent.to_path_buf());
        if let Some(grandparent) = parent.parent() {
            roots.push(grandparent.to_path_buf());
        }
    }
    roots
}

fn normalize_sha256(digest: &str) -> Result<String> {
    let trimmed = digest.trim();
    if trimmed.is_empty() {
        bail!("sha256 digest cannot be empty");
    }
    if let Some(stripped) = trimmed.strip_prefix("sha256:") {
        if stripped.is_empty() {
            bail!("sha256 digest must include hex bytes after sha256:");
        }
        return Ok(trimmed.to_string());
    }
    if trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(format!("sha256:{trimmed}"));
    }
    bail!("sha256 digest must be hex or sha256:<hex>");
}

fn component_sources_table_from_pack_lock(
    lock: &PackLockV1,
    allow_missing_hash: bool,
) -> Result<HashMap<String, ComponentSourceInfo>> {
    let mut table = HashMap::new();
    let mut names = HashSet::new();
    for entry in &lock.components {
        if !names.insert(entry.name.clone()) {
            bail!(
                "pack.lock contains duplicate component name `{}`",
                entry.name
            );
        }
        let source_ref = match (&entry.source_ref, &entry.legacy_ref) {
            (Some(primary), Some(legacy)) => {
                if primary != legacy {
                    bail!(
                        "pack.lock component {} has conflicting refs: {} vs {}",
                        entry.name,
                        primary,
                        legacy
                    );
                }
                primary.as_str()
            }
            (Some(primary), None) => primary.as_str(),
            (None, Some(legacy)) => legacy.as_str(),
            (None, None) => {
                bail!("pack.lock component {} missing source_ref", entry.name);
            }
        };
        let source: ComponentSourceRef = source_ref
            .parse()
            .with_context(|| format!("invalid component ref `{}`", source_ref))?;
        let bundled_path = match (&entry.bundled_path, &entry.legacy_path) {
            (Some(primary), Some(legacy)) => {
                if primary != legacy {
                    bail!(
                        "pack.lock component {} has conflicting bundled paths: {} vs {}",
                        entry.name,
                        primary,
                        legacy
                    );
                }
                Some(primary.clone())
            }
            (Some(primary), None) => Some(primary.clone()),
            (None, Some(legacy)) => Some(legacy.clone()),
            (None, None) => None,
        };
        let bundled = entry.bundled.unwrap_or(false) || bundled_path.is_some();
        let (artifact, digest, expected_wasm_sha256, skip_digest_verification) = if bundled {
            let wasm_path = bundled_path.ok_or_else(|| {
                anyhow!(
                    "pack.lock component {} marked bundled but bundled_path is missing",
                    entry.name
                )
            })?;
            let expected_raw = match (&entry.wasm_sha256, &entry.legacy_sha256) {
                (Some(primary), Some(legacy)) => {
                    if primary != legacy {
                        bail!(
                            "pack.lock component {} has conflicting wasm_sha256 values: {} vs {}",
                            entry.name,
                            primary,
                            legacy
                        );
                    }
                    Some(primary.as_str())
                }
                (Some(primary), None) => Some(primary.as_str()),
                (None, Some(legacy)) => Some(legacy.as_str()),
                (None, None) => None,
            };
            let expected = match expected_raw {
                Some(value) => Some(normalize_sha256(value)?),
                None => None,
            };
            if expected.is_none() && !allow_missing_hash {
                bail!(
                    "pack.lock component {} missing wasm_sha256 for bundled component",
                    entry.name
                );
            }
            (
                ComponentArtifactLocation::Inline { wasm_path },
                expected.clone(),
                expected,
                allow_missing_hash && expected_raw.is_none(),
            )
        } else {
            if source.is_tag() {
                bail!(
                    "component {} uses tag ref {} but is not bundled; rebuild the pack",
                    entry.name,
                    source
                );
            }
            let expected = entry
                .resolved_digest
                .as_deref()
                .or(entry.digest.as_deref())
                .ok_or_else(|| {
                    anyhow!(
                        "pack.lock component {} missing resolved_digest for remote component",
                        entry.name
                    )
                })?;
            (
                ComponentArtifactLocation::Remote,
                Some(normalize_digest(expected)),
                None,
                false,
            )
        };
        let info = ComponentSourceInfo {
            digest,
            source,
            artifact,
            expected_wasm_sha256,
            skip_digest_verification,
        };
        if let Some(component_id) = entry.component_id.as_ref() {
            let key = component_id.as_str().to_string();
            if table.contains_key(&key) {
                bail!(
                    "pack.lock contains duplicate component id `{}`",
                    component_id.as_str()
                );
            }
            table.insert(key, info.clone());
        }
        if entry.name
            != entry
                .component_id
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or("")
        {
            table.insert(entry.name.clone(), info);
        }
    }
    Ok(table)
}

fn component_path_for_spec(root: &Path, spec: &ComponentSpec) -> PathBuf {
    if let Some(path) = &spec.legacy_path {
        return root.join(path);
    }
    root.join("components").join(format!("{}.wasm", spec.id))
}

fn normalize_digest(digest: &str) -> String {
    if digest.starts_with("sha256:") || digest.starts_with("blake3:") {
        digest.to_string()
    } else {
        format!("sha256:{digest}")
    }
}

fn compute_digest_for(bytes: &[u8], digest: &str) -> Result<String> {
    if digest.starts_with("blake3:") {
        let hash = blake3::hash(bytes);
        return Ok(format!("blake3:{}", hash.to_hex()));
    }
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    Ok(format!("sha256:{}", to_hex(&hasher.finalize())))
}

fn compute_sha256_digest_for(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", to_hex(&hasher.finalize()))
}

fn build_artifact_key(cache: &CacheManager, digest: Option<&str>, bytes: &[u8]) -> ArtifactKey {
    let wasm_digest = digest
        .map(normalize_digest)
        .unwrap_or_else(|| compute_sha256_digest_for(bytes));
    ArtifactKey::new(cache.engine_profile_id().to_string(), wasm_digest)
}

async fn compile_component_with_cache(
    cache: &CacheManager,
    engine: &Engine,
    digest: Option<&str>,
    bytes: Vec<u8>,
) -> Result<Arc<Component>> {
    let key = build_artifact_key(cache, digest, &bytes);
    cache.get_component(engine, &key, || Ok(bytes)).await
}

fn verify_component_digest(component_id: &str, expected: &str, bytes: &[u8]) -> Result<()> {
    let normalized_expected = normalize_digest(expected);
    let actual = compute_digest_for(bytes, &normalized_expected)?;
    if normalize_digest(&actual) != normalized_expected {
        bail!(
            "component {component_id} digest mismatch: expected {normalized_expected}, got {actual}"
        );
    }
    Ok(())
}

fn verify_wasm_sha256(component_id: &str, expected: &str, bytes: &[u8]) -> Result<()> {
    let normalized_expected = normalize_sha256(expected)?;
    let actual = compute_sha256_digest_for(bytes);
    if actual != normalized_expected {
        bail!(
            "component {component_id} bundled digest mismatch: expected {normalized_expected}, got {actual}"
        );
    }
    Ok(())
}

fn to_hex(digest: &[u8]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod pack_lock_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn pack_lock_tag_ref_requires_bundle() {
        let lock = PackLockV1 {
            schema_version: 1,
            components: vec![PackLockComponent {
                name: "templates".to_string(),
                source_ref: Some("oci://registry.test/templates:latest".to_string()),
                legacy_ref: None,
                component_id: None,
                bundled: Some(false),
                bundled_path: None,
                legacy_path: None,
                wasm_sha256: None,
                legacy_sha256: None,
                resolved_digest: None,
                digest: None,
            }],
        };
        let err = component_sources_table_from_pack_lock(&lock, false).unwrap_err();
        assert!(
            err.to_string().contains("tag ref") && err.to_string().contains("rebuild the pack"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn bundled_hash_mismatch_errors() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let temp = TempDir::new().expect("temp dir");
        let engine = Engine::default();
        let engine_profile =
            EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
        let cache_config = CacheConfig {
            root: temp.path().join("cache"),
            ..CacheConfig::default()
        };
        let cache = CacheManager::new(cache_config, engine_profile);
        let wasm_path = temp.path().join("component.wasm");
        let fixture_wasm = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/packs/secrets_store_smoke/components/echo_secret.wasm");
        let bytes = std::fs::read(&fixture_wasm).expect("read fixture wasm");
        std::fs::write(&wasm_path, &bytes).expect("write temp wasm");

        let spec = ComponentSpec {
            id: "qa.process".to_string(),
            version: "0.0.0".to_string(),
            legacy_path: None,
        };
        let mut missing = HashSet::new();
        missing.insert(spec.id.clone());

        let mut sources = HashMap::new();
        sources.insert(
            spec.id.clone(),
            ComponentSourceInfo {
                digest: Some("sha256:deadbeef".to_string()),
                source: ComponentSourceRef::Oci("registry.test/qa.process@sha256:deadbeef".into()),
                artifact: ComponentArtifactLocation::Inline {
                    wasm_path: "component.wasm".to_string(),
                },
                expected_wasm_sha256: Some("sha256:deadbeef".to_string()),
                skip_digest_verification: false,
            },
        );

        let mut loaded = HashMap::new();
        let result = rt.block_on(load_components_from_sources(
            &cache,
            &engine,
            &sources,
            &ComponentResolution::default(),
            &[spec],
            &mut missing,
            &mut loaded,
            Some(temp.path()),
            None,
        ));
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("bundled digest mismatch"),
            "unexpected error: {err}"
        );
    }
}

#[cfg(test)]
mod pack_resolution_prop_tests {
    use super::*;
    use greentic_types::{ArtifactLocationV1, ComponentSourceEntryV1, ResolvedComponentV1};
    use proptest::prelude::*;
    use proptest::test_runner::{Config as ProptestConfig, RngAlgorithm, TestRng, TestRunner};
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::str::FromStr;

    #[derive(Clone, Debug)]
    enum ResolveRequest {
        ById(String),
        ByName(String),
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ResolvedComponent {
        key: String,
        source: String,
        artifact: String,
        digest: Option<String>,
        expected_wasm_sha256: Option<String>,
        skip_digest_verification: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ResolveError {
        code: String,
        message: String,
        context_key: String,
    }

    #[derive(Clone, Debug)]
    struct Scenario {
        pack_lock: Option<PackLockV1>,
        component_sources: Option<ComponentSourcesV1>,
        request: ResolveRequest,
        expected_sha256: Option<String>,
        bytes: Vec<u8>,
    }

    fn resolve_component_test(
        sources: Option<&ComponentSourcesV1>,
        lock: Option<&PackLockV1>,
        request: &ResolveRequest,
    ) -> Result<ResolvedComponent, ResolveError> {
        let table = if let Some(lock) = lock {
            component_sources_table_from_pack_lock(lock, false).map_err(|err| ResolveError {
                code: classify_pack_lock_error(err.to_string().as_str()).to_string(),
                message: err.to_string(),
                context_key: request_key(request).to_string(),
            })?
        } else {
            let sources = component_sources_table(sources).map_err(|err| ResolveError {
                code: "component_sources_error".to_string(),
                message: err.to_string(),
                context_key: request_key(request).to_string(),
            })?;
            sources.ok_or_else(|| ResolveError {
                code: "missing_component_sources".to_string(),
                message: "component sources not provided".to_string(),
                context_key: request_key(request).to_string(),
            })?
        };

        let key = request_key(request);
        let source = table.get(key).ok_or_else(|| ResolveError {
            code: "component_not_found".to_string(),
            message: format!("component {key} not found"),
            context_key: key.to_string(),
        })?;

        Ok(ResolvedComponent {
            key: key.to_string(),
            source: source.source.to_string(),
            artifact: match source.artifact {
                ComponentArtifactLocation::Inline { .. } => "inline".to_string(),
                ComponentArtifactLocation::Remote => "remote".to_string(),
            },
            digest: source.digest.clone(),
            expected_wasm_sha256: source.expected_wasm_sha256.clone(),
            skip_digest_verification: source.skip_digest_verification,
        })
    }

    fn request_key(request: &ResolveRequest) -> &str {
        match request {
            ResolveRequest::ById(value) => value.as_str(),
            ResolveRequest::ByName(value) => value.as_str(),
        }
    }

    fn classify_pack_lock_error(message: &str) -> &'static str {
        if message.contains("duplicate component name") {
            "duplicate_name"
        } else if message.contains("duplicate component id") {
            "duplicate_id"
        } else if message.contains("conflicting refs") {
            "conflicting_ref"
        } else if message.contains("conflicting bundled paths") {
            "conflicting_bundled_path"
        } else if message.contains("conflicting wasm_sha256") {
            "conflicting_wasm_sha256"
        } else if message.contains("missing source_ref") {
            "missing_source_ref"
        } else if message.contains("marked bundled but bundled_path is missing") {
            "missing_bundled_path"
        } else if message.contains("missing wasm_sha256") {
            "missing_wasm_sha256"
        } else if message.contains("tag ref") && message.contains("not bundled") {
            "tag_ref_requires_bundle"
        } else if message.contains("missing resolved_digest") {
            "missing_resolved_digest"
        } else if message.contains("invalid component ref") {
            "invalid_component_ref"
        } else if message.contains("sha256 digest") {
            "invalid_sha256"
        } else {
            "unknown_error"
        }
    }

    fn known_error_codes() -> BTreeSet<&'static str> {
        [
            "component_sources_error",
            "missing_component_sources",
            "component_not_found",
            "duplicate_name",
            "duplicate_id",
            "conflicting_ref",
            "conflicting_bundled_path",
            "conflicting_wasm_sha256",
            "missing_source_ref",
            "missing_bundled_path",
            "missing_wasm_sha256",
            "tag_ref_requires_bundle",
            "missing_resolved_digest",
            "invalid_component_ref",
            "invalid_sha256",
            "unknown_error",
        ]
        .into_iter()
        .collect()
    }

    fn proptest_config() -> ProptestConfig {
        let cases = std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(128);
        ProptestConfig {
            cases,
            failure_persistence: None,
            ..ProptestConfig::default()
        }
    }

    fn proptest_seed() -> Option<[u8; 32]> {
        let seed = std::env::var("PROPTEST_SEED")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())?;
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        Some(bytes)
    }

    fn run_cases(strategy: impl Strategy<Value = Scenario>, cases: u32, seed: Option<[u8; 32]>) {
        let config = ProptestConfig {
            cases,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = match seed {
            Some(bytes) => {
                TestRunner::new_with_rng(config, TestRng::from_seed(RngAlgorithm::ChaCha, &bytes))
            }
            None => TestRunner::new(config),
        };
        runner
            .run(&strategy, |scenario| {
                run_scenario(&scenario);
                Ok(())
            })
            .unwrap();
    }

    fn run_scenario(scenario: &Scenario) {
        let known_codes = known_error_codes();
        let first = resolve_component_test(
            scenario.component_sources.as_ref(),
            scenario.pack_lock.as_ref(),
            &scenario.request,
        );
        let second = resolve_component_test(
            scenario.component_sources.as_ref(),
            scenario.pack_lock.as_ref(),
            &scenario.request,
        );
        assert_eq!(normalize_result(&first), normalize_result(&second));

        if let Some(lock) = scenario.pack_lock.as_ref() {
            let lock_only = resolve_component_test(None, Some(lock), &scenario.request);
            assert_eq!(normalize_result(&first), normalize_result(&lock_only));
        }

        if let Err(err) = first.as_ref() {
            assert!(
                known_codes.contains(err.code.as_str()),
                "unexpected error code {}: {}",
                err.code,
                err.message
            );
        }

        if let Some(expected) = scenario.expected_sha256.as_deref() {
            let expected_ok =
                verify_wasm_sha256("test.component", expected, &scenario.bytes).is_ok();
            let actual = compute_sha256_digest_for(&scenario.bytes);
            if actual == normalize_sha256(expected).unwrap_or_default() {
                assert!(expected_ok, "expected sha256 match to succeed");
            } else {
                assert!(!expected_ok, "expected sha256 mismatch to fail");
            }
        }
    }

    fn normalize_result(
        result: &Result<ResolvedComponent, ResolveError>,
    ) -> Result<ResolvedComponent, ResolveError> {
        match result {
            Ok(value) => Ok(value.clone()),
            Err(err) => Err(err.clone()),
        }
    }

    fn scenario_strategy() -> impl Strategy<Value = Scenario> {
        let name = any::<u8>().prop_map(|n| format!("component{n}.core"));
        let alt_name = any::<u8>().prop_map(|n| format!("component_alt{n}.core"));
        let tag_ref = any::<bool>();
        let bundled = any::<bool>();
        let include_sha = any::<bool>();
        let include_component_id = any::<bool>();
        let request_by_id = any::<bool>();
        let use_lock = any::<bool>();
        let use_sources = any::<bool>();
        let bytes = prop::collection::vec(any::<u8>(), 1..64);

        (
            name,
            alt_name,
            tag_ref,
            bundled,
            include_sha,
            include_component_id,
            request_by_id,
            use_lock,
            use_sources,
            bytes,
        )
            .prop_map(
                |(
                    name,
                    alt_name,
                    tag_ref,
                    bundled,
                    include_sha,
                    include_component_id,
                    request_by_id,
                    use_lock,
                    use_sources,
                    bytes,
                )| {
                    let component_id_str = if include_component_id {
                        alt_name.clone()
                    } else {
                        name.clone()
                    };
                    let component_id = ComponentId::from_str(&component_id_str).ok();
                    let source_ref = if tag_ref {
                        format!("oci://registry.test/{name}:v1")
                    } else {
                        format!(
                            "oci://registry.test/{name}@sha256:{}",
                            hex::encode([0x11u8; 32])
                        )
                    };
                    let expected_sha256 = if bundled && include_sha {
                        Some(compute_sha256_digest_for(&bytes))
                    } else {
                        None
                    };

                    let lock_component = PackLockComponent {
                        name: name.clone(),
                        source_ref: Some(source_ref),
                        legacy_ref: None,
                        component_id,
                        bundled: Some(bundled),
                        bundled_path: if bundled {
                            Some(format!("components/{name}.wasm"))
                        } else {
                            None
                        },
                        legacy_path: None,
                        wasm_sha256: expected_sha256.clone(),
                        legacy_sha256: None,
                        resolved_digest: if bundled {
                            None
                        } else {
                            Some("sha256:deadbeef".to_string())
                        },
                        digest: None,
                    };

                    let pack_lock = if use_lock {
                        Some(PackLockV1 {
                            schema_version: 1,
                            components: vec![lock_component],
                        })
                    } else {
                        None
                    };

                    let component_sources = if use_sources {
                        Some(ComponentSourcesV1::new(vec![ComponentSourceEntryV1 {
                            name: name.clone(),
                            component_id: ComponentId::from_str(&name).ok(),
                            source: ComponentSourceRef::from_str(
                                "oci://registry.test/component@sha256:deadbeef",
                            )
                            .expect("component ref"),
                            resolved: ResolvedComponentV1 {
                                digest: "sha256:deadbeef".to_string(),
                                signature: None,
                                signed_by: None,
                            },
                            artifact: if bundled {
                                ArtifactLocationV1::Inline {
                                    wasm_path: format!("components/{name}.wasm"),
                                    manifest_path: None,
                                }
                            } else {
                                ArtifactLocationV1::Remote
                            },
                            licensing_hint: None,
                            metering_hint: None,
                        }]))
                    } else {
                        None
                    };

                    let request = if request_by_id {
                        ResolveRequest::ById(component_id_str.clone())
                    } else {
                        ResolveRequest::ByName(name.clone())
                    };

                    Scenario {
                        pack_lock,
                        component_sources,
                        request,
                        expected_sha256,
                        bytes,
                    }
                },
            )
    }

    #[test]
    fn pack_resolution_proptest() {
        let seed = proptest_seed();
        run_cases(scenario_strategy(), proptest_config().cases, seed);
    }

    #[test]
    fn pack_resolution_regression_seeds() {
        let seeds_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/proptest-seeds.txt");
        let raw = std::fs::read_to_string(&seeds_path).expect("read proptest seeds");
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let seed = line.parse::<u64>().expect("seed must be an integer");
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&seed.to_le_bytes());
            run_cases(scenario_strategy(), 1, Some(bytes));
        }
    }
}

fn locate_pack_assets(
    materialized_root: Option<&Path>,
    archive_hint: Option<&Path>,
) -> Result<(Option<PathBuf>, Option<TempDir>)> {
    if let Some(root) = materialized_root {
        let assets = root.join("assets");
        if assets.is_dir() {
            return Ok((Some(assets), None));
        }
    }
    if let Some(path) = archive_hint
        && let Some((tempdir, assets)) = extract_assets_from_archive(path)?
    {
        return Ok((Some(assets), Some(tempdir)));
    }
    Ok((None, None))
}

fn extract_assets_from_archive(path: &Path) -> Result<Option<(TempDir, PathBuf)>> {
    let file =
        File::open(path).with_context(|| format!("failed to open pack {}", path.display()))?;
    let mut archive =
        ZipArchive::new(file).with_context(|| format!("failed to read pack {}", path.display()))?;
    let temp = TempDir::new().context("failed to create temporary assets directory")?;
    let mut found = false;
    for idx in 0..archive.len() {
        let mut entry = archive.by_index(idx)?;
        let name = entry.name();
        if !name.starts_with("assets/") {
            continue;
        }
        let dest = temp.path().join(name);
        if name.ends_with('/') {
            std::fs::create_dir_all(&dest)?;
            found = true;
            continue;
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut outfile = std::fs::File::create(&dest)?;
        std::io::copy(&mut entry, &mut outfile)?;
        found = true;
    }
    if found {
        let assets_path = temp.path().join("assets");
        Ok(Some((temp, assets_path)))
    } else {
        Ok(None)
    }
}

fn dist_options_from(component_resolution: &ComponentResolution) -> DistOptions {
    let mut opts = DistOptions {
        allow_tags: true,
        ..DistOptions::default()
    };
    if let Some(cache_dir) = component_resolution.dist_cache_dir.clone() {
        opts.cache_dir = cache_dir;
    }
    if component_resolution.dist_offline {
        opts.offline = true;
    }
    opts
}

#[allow(clippy::too_many_arguments)]
async fn load_components_from_sources(
    cache: &CacheManager,
    engine: &Engine,
    component_sources: &HashMap<String, ComponentSourceInfo>,
    component_resolution: &ComponentResolution,
    specs: &[ComponentSpec],
    missing: &mut HashSet<String>,
    into: &mut HashMap<String, PackComponent>,
    materialized_root: Option<&Path>,
    archive_hint: Option<&Path>,
) -> Result<()> {
    let mut archive = if let Some(path) = archive_hint {
        Some(
            ZipArchive::new(File::open(path)?)
                .with_context(|| format!("{} is not a valid gtpack", path.display()))?,
        )
    } else {
        None
    };
    let mut dist_client: Option<DistClient> = None;

    for spec in specs {
        if !missing.contains(&spec.id) {
            continue;
        }
        let Some(source) = component_sources.get(&spec.id) else {
            continue;
        };

        let bytes = match &source.artifact {
            ComponentArtifactLocation::Inline { wasm_path } => {
                if let Some(root) = materialized_root {
                    let path = root.join(wasm_path);
                    if path.exists() {
                        std::fs::read(&path).with_context(|| {
                            format!(
                                "failed to read inline component {} from {}",
                                spec.id,
                                path.display()
                            )
                        })?
                    } else if archive.is_none() {
                        bail!("inline component {} missing at {}", spec.id, path.display());
                    } else {
                        read_entry(
                            archive.as_mut().expect("archive present when needed"),
                            wasm_path,
                        )
                        .with_context(|| {
                            format!(
                                "inline component {} missing at {} in pack archive",
                                spec.id, wasm_path
                            )
                        })?
                    }
                } else if let Some(archive) = archive.as_mut() {
                    read_entry(archive, wasm_path).with_context(|| {
                        format!(
                            "inline component {} missing at {} in pack archive",
                            spec.id, wasm_path
                        )
                    })?
                } else {
                    bail!(
                        "inline component {} missing and no pack source available",
                        spec.id
                    );
                }
            }
            ComponentArtifactLocation::Remote => {
                if source.source.is_tag() {
                    bail!(
                        "component {} uses tag ref {} but is not bundled; rebuild the pack",
                        spec.id,
                        source.source
                    );
                }
                let client = dist_client.get_or_insert_with(|| {
                    DistClient::new(dist_options_from(component_resolution))
                });
                let reference = source.source.to_string();
                fault::maybe_fail_asset(&reference)
                    .await
                    .with_context(|| format!("fault injection blocked asset {reference}"))?;
                let digest = source.digest.as_deref().ok_or_else(|| {
                    anyhow!(
                        "component {} missing expected digest for remote component",
                        spec.id
                    )
                })?;
                let cache_path = if let Ok(cache_path) = client.fetch_digest(digest).await {
                    cache_path
                } else if component_resolution.dist_offline {
                    client
                        .fetch_digest(digest)
                        .await
                        .map_err(|err| dist_error_for_component(err, &spec.id, &reference))?
                } else {
                    let source = client
                        .parse_source(&reference)
                        .map_err(|err| dist_error_for_component(err, &spec.id, &reference))?;
                    let descriptor = client
                        .resolve(source, ResolvePolicy)
                        .await
                        .map_err(|err| dist_error_for_component(err, &spec.id, &reference))?;
                    let resolved = client
                        .fetch(&descriptor, CachePolicy)
                        .await
                        .map_err(|err| dist_error_for_component(err, &spec.id, &reference))?;
                    let expected = normalize_digest(digest);
                    let actual = normalize_digest(&resolved.digest);
                    if expected != actual {
                        bail!(
                            "component {} digest mismatch after fetch: expected {}, got {}",
                            spec.id,
                            expected,
                            actual
                        );
                    }
                    resolved.cache_path.ok_or_else(|| {
                        anyhow!(
                            "component {} resolved from {} but cache path is missing",
                            spec.id,
                            reference
                        )
                    })?
                };
                std::fs::read(&cache_path).with_context(|| {
                    format!(
                        "failed to read cached component {} from {}",
                        spec.id,
                        cache_path.display()
                    )
                })?
            }
        };

        if let Some(expected) = source.expected_wasm_sha256.as_deref() {
            verify_wasm_sha256(&spec.id, expected, &bytes)?;
        } else if source.skip_digest_verification {
            let actual = compute_sha256_digest_for(&bytes);
            warn!(
                component_id = %spec.id,
                digest = %actual,
                "bundled component missing wasm_sha256; allowing due to flag"
            );
        } else {
            let expected = source.digest.as_deref().ok_or_else(|| {
                anyhow!(
                    "component {} missing expected digest for verification",
                    spec.id
                )
            })?;
            verify_component_digest(&spec.id, expected, &bytes)?;
        }
        let component =
            compile_component_with_cache(cache, engine, source.digest.as_deref(), bytes)
                .await
                .with_context(|| format!("failed to compile component {}", spec.id))?;
        into.insert(
            spec.id.clone(),
            PackComponent {
                name: spec.id.clone(),
                version: spec.version.clone(),
                component,
            },
        );
        missing.remove(&spec.id);
    }

    Ok(())
}

fn dist_error_for_component(err: DistError, component_id: &str, reference: &str) -> anyhow::Error {
    match err {
        DistError::NotFound { reference: missing } => anyhow!(
            "remote component {} is not cached for {}. Run `greentic-dist pull --lock <pack.lock>` or `greentic-dist pull {}`",
            component_id,
            missing,
            reference
        ),
        DistError::Offline { reference: blocked } => anyhow!(
            "offline mode blocked fetching component {} from {}; run `greentic-dist pull --lock <pack.lock>` or `greentic-dist pull {}`",
            component_id,
            blocked,
            reference
        ),
        DistError::Unauthorized { target } => anyhow!(
            "component {} requires authenticated source {}; run `greentic-dist pull --lock <pack.lock>` or `greentic-dist pull {}`",
            component_id,
            target,
            reference
        ),
        other => anyhow!(
            "failed to resolve component {} from {}: {}",
            component_id,
            reference,
            other
        ),
    }
}

async fn load_components_from_overrides(
    cache: &CacheManager,
    engine: &Engine,
    overrides: &HashMap<String, PathBuf>,
    specs: &[ComponentSpec],
    missing: &mut HashSet<String>,
    into: &mut HashMap<String, PackComponent>,
) -> Result<()> {
    for spec in specs {
        if !missing.contains(&spec.id) {
            continue;
        }
        let Some(path) = overrides.get(&spec.id) else {
            continue;
        };
        let bytes = std::fs::read(path)
            .with_context(|| format!("failed to read override component {}", path.display()))?;
        let component = compile_component_with_cache(cache, engine, None, bytes)
            .await
            .with_context(|| {
                format!(
                    "failed to compile component {} from override {}",
                    spec.id,
                    path.display()
                )
            })?;
        into.insert(
            spec.id.clone(),
            PackComponent {
                name: spec.id.clone(),
                version: spec.version.clone(),
                component,
            },
        );
        missing.remove(&spec.id);
    }
    Ok(())
}

async fn load_components_from_dir(
    cache: &CacheManager,
    engine: &Engine,
    root: &Path,
    specs: &[ComponentSpec],
    missing: &mut HashSet<String>,
    into: &mut HashMap<String, PackComponent>,
) -> Result<()> {
    for spec in specs {
        if !missing.contains(&spec.id) {
            continue;
        }
        let path = component_path_for_spec(root, spec);
        if !path.exists() {
            tracing::debug!(component = %spec.id, path = %path.display(), "materialized component missing; will try other sources");
            continue;
        }
        let bytes = std::fs::read(&path)
            .with_context(|| format!("failed to read component {}", path.display()))?;
        let component = compile_component_with_cache(cache, engine, None, bytes)
            .await
            .with_context(|| {
                format!(
                    "failed to compile component {} from {}",
                    spec.id,
                    path.display()
                )
            })?;
        into.insert(
            spec.id.clone(),
            PackComponent {
                name: spec.id.clone(),
                version: spec.version.clone(),
                component,
            },
        );
        missing.remove(&spec.id);
    }
    Ok(())
}

async fn load_components_from_archive(
    cache: &CacheManager,
    engine: &Engine,
    path: &Path,
    specs: &[ComponentSpec],
    missing: &mut HashSet<String>,
    into: &mut HashMap<String, PackComponent>,
) -> Result<()> {
    let mut archive = ZipArchive::new(File::open(path)?)
        .with_context(|| format!("{} is not a valid gtpack", path.display()))?;
    for spec in specs {
        if !missing.contains(&spec.id) {
            continue;
        }
        let file_name = spec
            .legacy_path
            .clone()
            .unwrap_or_else(|| format!("components/{}.wasm", spec.id));
        let bytes = match read_entry(&mut archive, &file_name) {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(component = %spec.id, pack = %path.display(), error = %err, "component entry missing in pack archive");
                continue;
            }
        };
        let component = compile_component_with_cache(cache, engine, None, bytes)
            .await
            .with_context(|| format!("failed to compile component {}", spec.id))?;
        into.insert(
            spec.id.clone(),
            PackComponent {
                name: spec.id.clone(),
                version: spec.version.clone(),
                component,
            },
        );
        missing.remove(&spec.id);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_flow::model::{FlowDoc, NodeDoc};
    use indexmap::IndexMap;
    use serde_json::json;

    /// The whole point: a configured provider used to be invoked with its
    /// answers dropped, so `request.config` arrived empty.
    #[test]
    fn a_configured_provider_receives_its_answers() {
        let merged = PackRuntime::merge_provider_config_into_input(
            Some(r#"{"auto_start_on_open":false}"#),
            br#"{"hello":"world"}"#.to_vec(),
            "webchat-gui",
        );
        let value: Value = serde_json::from_slice(&merged).expect("merged payload is JSON");
        assert_eq!(value["config"]["auto_start_on_open"], json!(false));
        assert_eq!(value["input"]["hello"], json!("world"));
    }

    /// An unconfigured provider must get the bytes it gets today, unchanged —
    /// so nothing about a provider nobody has answered for moves.
    #[test]
    fn an_unconfigured_provider_payload_is_untouched() {
        let original = br#"{"hello":"world"}"#.to_vec();
        let merged =
            PackRuntime::merge_provider_config_into_input(None, original.clone(), "webchat-gui");
        assert_eq!(merged, original);
    }

    /// A provider's input is a webhook body, not a flow value, so it may not be
    /// JSON. The component merge would wrap it as a JSON string; doing that
    /// here would swap a missing config for a corrupted request body.
    #[test]
    fn a_non_json_provider_payload_is_passed_through_rather_than_wrapped() {
        let original = vec![0x89u8, 0x50, 0x4e, 0x47];
        let merged = PackRuntime::merge_provider_config_into_input(
            Some(r#"{"k":1}"#),
            original.clone(),
            "some-provider",
        );
        assert_eq!(merged, original, "binary payloads must survive verbatim");

        let text = b"not json at all".to_vec();
        let merged =
            PackRuntime::merge_provider_config_into_input(Some(r#"{"k":1}"#), text.clone(), "p");
        assert_eq!(merged, text);
    }

    #[test]
    fn tags_indicate_entry_treats_internal_as_non_entry() {
        // `internal`-tagged flows are helpers reachable only via flow.call.
        assert!(!tags_indicate_entry(["internal"]));
        assert!(!tags_indicate_entry(["default", "internal"]));
        // The public entrypoint and untagged/other-tagged flows are entries.
        assert!(tags_indicate_entry(["default"]));
        assert!(tags_indicate_entry(["ui", "featured"]));
        assert!(tags_indicate_entry(std::iter::empty::<&str>()));
    }

    #[test]
    fn flow_descriptor_deserializes_missing_entry_as_true() {
        // Descriptors serialized before the `entry` field default to entry,
        // preserving prior routing behaviour.
        let desc: FlowDescriptor = serde_json::from_value(json!({
            "id": "default",
            "type": "messaging",
            "pack_id": "weatherapi-pack",
            "profile": "weatherapi-pack",
            "version": "0.1.0"
        }))
        .expect("descriptor without `entry` must deserialize");
        assert!(desc.entry);
    }

    /// Build a minimal `PackRuntime` rooted at `dir` so that `read_pack_file`
    /// resolves files from that directory via `self.path.is_dir()`.
    /// Mirrors the `for_component_test` constructor but sets `path` to the
    /// caller-supplied directory instead of `PathBuf::new()`.
    fn pack_runtime_for_dir(dir: &std::path::Path) -> PackRuntime {
        let engine = Engine::default();
        let engine_profile =
            EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
        let cache = CacheManager::new(CacheConfig::default(), engine_profile);
        let config = Arc::new(crate::config::HostConfig {
            tenant: "test-tenant".to_string(),
            bindings_path: std::path::PathBuf::from("/tmp/bindings.yaml"),
            flow_type_bindings: HashMap::new(),
            rate_limits: crate::config::RateLimits::default(),
            retry: crate::config::FlowRetryConfig::default(),
            http_enabled: false,
            secrets_policy: crate::config::SecretsPolicy::allow_all(),
            state_store_policy: crate::config::StateStorePolicy::default(),
            webhook_policy: crate::config::WebhookPolicy::default(),
            timers: Vec::new(),
            oauth: None,
            mocks: None,
            pack_bindings: Vec::new(),
            env_passthrough: Vec::new(),
            trace: crate::trace::TraceConfig::from_env(),
            validation: crate::validate::ValidationConfig::from_env(),
            operator_policy: crate::config::OperatorPolicy::allow_all(),
            fast2flow: Default::default(),
            #[cfg(feature = "agentic-worker")]
            agents: HashMap::new(),
            #[cfg(feature = "agentic-worker")]
            graphs: HashMap::new(),
        });
        PackRuntime {
            path: dir.to_path_buf(),
            archive_path: None,
            config,
            engine,
            metadata: PackMetadata {
                pack_id: "test-pack".to_string(),
                version: "0.0.0".to_string(),
                entry_flows: Vec::new(),
                secret_requirements: Vec::new(),
            },
            manifest: None,
            legacy_manifest: None,
            component_manifests: HashMap::new(),
            mocks: None,
            flows: None,
            components: HashMap::new(),
            http_client: Arc::clone(&HTTP_CLIENT),
            session_store: None,
            state_store: None,
            wasi_policy: Arc::new(crate::wasi::RunnerWasiPolicy::new()),
            mcp_routes: std::sync::OnceLock::new(),
            assets_tempdir: None,
            provider_registry: RwLock::new(None),
            identify_hint_cache: RwLock::new(HashMap::new()),
            secrets: crate::secrets::default_manager().expect("default secrets manager"),
            oauth_config: None,
            runtime_config_non_secret: None,
            runtime_refs: None,
            cache,
        }
    }

    #[test]
    fn dw_agents_sidecar_blobs_reads_map_from_pack_dir() {
        let dir = tempfile::tempdir().unwrap();
        let agents = serde_json::json!({
            "greeter": { "agent_id": "greeter", "system_prompt": "hi", "tools": [],
                         "llm": { "provider": "openai", "model": "gpt-4o-mini" } }
        });
        std::fs::write(
            dir.path().join("dw-agents.json"),
            serde_json::to_vec(&agents).unwrap(),
        )
        .unwrap();
        let pack = pack_runtime_for_dir(dir.path());
        let blobs = pack.dw_agents_sidecar_blobs();
        assert!(blobs.contains_key("greeter"));
        assert_eq!(blobs["greeter"]["agent_id"], "greeter");
    }

    #[test]
    fn dw_agents_sidecar_blobs_absent_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let pack = pack_runtime_for_dir(dir.path());
        assert!(pack.dw_agents_sidecar_blobs().is_empty());
    }

    #[test]
    fn dw_agents_sidecar_blobs_malformed_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("dw-agents.json"), b"not json").unwrap();
        let pack = pack_runtime_for_dir(dir.path());
        assert!(pack.dw_agents_sidecar_blobs().is_empty());
    }

    #[test]
    fn normalizes_raw_component_to_component_exec() {
        let mut nodes = IndexMap::new();
        let mut raw = IndexMap::new();
        raw.insert(
            "templating.handlebars".into(),
            json!({ "template": "Hi {{name}}" }),
        );
        nodes.insert(
            "start".into(),
            NodeDoc {
                raw,
                routing: json!([{"out": true}]),
                ..Default::default()
            },
        );
        let doc = FlowDoc {
            id: "welcome".into(),
            title: None,
            description: None,
            flow_type: "messaging".into(),
            start: Some("start".into()),
            parameters: json!({}),
            tags: Vec::new(),
            schema_version: None,
            entrypoints: IndexMap::new(),
            meta: None,
            slot_schema: None,
            nodes,
        };

        let normalized = normalize_flow_doc(doc);
        let node = normalized.nodes.get("start").expect("node exists");
        assert_eq!(node.operation.as_deref(), Some("component.exec"));
        assert!(node.raw.is_empty());
        let payload = node.payload.as_object().expect("payload object");
        assert_eq!(
            payload.get("component"),
            Some(&Value::String("templating.handlebars".into()))
        );
        assert_eq!(
            payload.get("operation"),
            Some(&Value::String("render".into()))
        );
        let input = payload.get("input").unwrap();
        assert_eq!(input, &json!({ "template": "Hi {{name}}" }));
    }

    #[test]
    fn normalizes_canonical_operation_node_to_component_exec_with_config() {
        let mut nodes = IndexMap::new();
        let mut raw = IndexMap::new();
        raw.insert(
            "handle_message".into(),
            json!({
                "component": "oci://ghcr.io/greenticai/component/component-llm-openai:stable",
                "config": {
                    "provider": "ollama",
                    "base_url": "http://127.0.0.1:11434/v1",
                    "default_model": "llama3.2"
                },
                "input": {
                    "messages": [{
                        "role": "user",
                        "content": "Say hello from Ollama."
                    }]
                }
            }),
        );
        nodes.insert(
            "llm".into(),
            NodeDoc {
                raw,
                routing: json!([{"out": true}]),
                ..Default::default()
            },
        );
        let doc = FlowDoc {
            id: "ollama-repro".into(),
            title: None,
            description: None,
            flow_type: "messaging".into(),
            start: Some("llm".into()),
            parameters: json!({}),
            tags: Vec::new(),
            schema_version: None,
            entrypoints: IndexMap::new(),
            meta: None,
            slot_schema: None,
            nodes,
        };

        let normalized = normalize_flow_doc(doc);
        let node = normalized.nodes.get("llm").expect("node exists");
        assert_eq!(node.operation.as_deref(), Some("component.exec"));
        assert!(node.raw.is_empty());
        let payload = node.payload.as_object().expect("payload object");
        assert_eq!(
            payload.get("component"),
            Some(&Value::String(
                "oci://ghcr.io/greenticai/component/component-llm-openai:stable".into()
            ))
        );
        assert_eq!(
            payload.get("operation"),
            Some(&Value::String("handle_message".into()))
        );
        assert_eq!(
            payload.get("config"),
            Some(&json!({
                "provider": "ollama",
                "base_url": "http://127.0.0.1:11434/v1",
                "default_model": "llama3.2"
            }))
        );
        assert_eq!(
            payload.get("input"),
            Some(&json!({
                "messages": [{
                    "role": "user",
                    "content": "Say hello from Ollama."
                }]
            }))
        );
    }

    #[test]
    fn missing_export_error_detection_recognises_bindgen_shapes() {
        // Positive: identity-world missing-instance error
        assert!(is_missing_export_error(
            "instantiation: no exported instance named \
             `greentic:provider-instance-identity/instance-identity-api@0.1.0`"
        ));
        // Positive: identity-world missing-function error
        assert!(is_missing_export_error(
            "instantiation: no exported function named `identify-instance`"
        ));
        // Negative: unrelated trap
        assert!(!is_missing_export_error(
            "Wasm trap: out of bounds memory access"
        ));
        // Negative: a DIFFERENT world's missing export must NOT match —
        // e.g. schema-core missing is a hard error, not "unsupported"
        assert!(!is_missing_export_error(
            "instantiation: no exported instance named \
             `greentic:provider-schema-core/schema-core-api@1.0.0`"
        ));
        // Negative: broad marker present but for a non-identity function
        assert!(!is_missing_export_error(
            "instantiation: no exported function named `invoke`"
        ));
    }

    #[test]
    fn identify_outcome_merge_in_follows_lattice() {
        let unsupported = || IdentifyOutcome::Unsupported;
        let no_match = || IdentifyOutcome::NoMatch;
        let id_a = || IdentifyOutcome::Identified("a".to_string());
        let id_b = || IdentifyOutcome::Identified("b".to_string());

        // Unsupported is the floor — every other variant promotes it.
        let mut x = unsupported();
        x.merge_in(unsupported());
        assert_eq!(x, unsupported());
        let mut x = unsupported();
        x.merge_in(no_match());
        assert_eq!(x, no_match());
        let mut x = unsupported();
        x.merge_in(id_a());
        assert_eq!(x, id_a());

        // NoMatch beats Unsupported but is overridable by Identified.
        let mut x = no_match();
        x.merge_in(unsupported());
        assert_eq!(x, no_match(), "NoMatch must not downgrade to Unsupported");
        let mut x = no_match();
        x.merge_in(no_match());
        assert_eq!(x, no_match());
        let mut x = no_match();
        x.merge_in(id_a());
        assert_eq!(x, id_a(), "Identified must override NoMatch");

        // Identified is the top — nothing overwrites it (first id wins).
        let mut x = id_a();
        x.merge_in(unsupported());
        assert_eq!(x, id_a());
        let mut x = id_a();
        x.merge_in(no_match());
        assert_eq!(x, id_a());
        let mut x = id_a();
        x.merge_in(id_b());
        assert_eq!(
            x,
            id_a(),
            "first Identified wins; later id does not replace"
        );
    }
}

#[cfg(test)]
mod identify_endpoints_pack_tests {
    use super::*;
    use crate::config::{
        FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
        WebhookPolicy,
    };
    use crate::trace::TraceConfig;
    use crate::validate::ValidationConfig;

    fn test_host_config() -> HostConfig {
        HostConfig {
            tenant: "test".to_string(),
            bindings_path: PathBuf::from("/tmp/bindings.yaml"),
            flow_type_bindings: HashMap::new(),
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
            trace: TraceConfig::from_env(),
            validation: ValidationConfig::from_env(),
            operator_policy: OperatorPolicy::allow_all(),
            fast2flow: Default::default(),
            #[cfg(feature = "agentic-worker")]
            agents: HashMap::new(),
            #[cfg(feature = "agentic-worker")]
            graphs: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn no_manifest_returns_unsupported_for_all_types() {
        // A PackRuntime with manifest: None (e.g. legacy single-component
        // packs or the for_component_test constructor) has no provider
        // registry. Every requested type must map to Unsupported — NOT
        // NoMatch — so the caller knows it can fall back to the static
        // provider_id rather than failing closed.
        let pack = PackRuntime::for_component_test(
            Vec::new(),
            HashMap::new(),
            "test-pack",
            Arc::new(test_host_config()),
        )
        .expect("empty pack construction");
        let result = pack
            .identify_endpoints_by_provider_type(&["teams", "slack", "telegram"], b"{}")
            .await
            .expect("no-manifest path must succeed");
        assert_eq!(result.len(), 3);
        for ty in &["teams", "slack", "telegram"] {
            assert_eq!(
                result.get(*ty),
                Some(&IdentifyOutcome::Unsupported),
                "type '{ty}' must be Unsupported when pack has no manifest"
            );
        }
    }

    #[tokio::test]
    async fn empty_provider_types_returns_empty_map() {
        let pack = PackRuntime::for_component_test(
            Vec::new(),
            HashMap::new(),
            "test-pack",
            Arc::new(test_host_config()),
        )
        .expect("empty pack construction");
        let result = pack
            .identify_endpoints_by_provider_type(&[], b"{}")
            .await
            .expect("empty types fast path");
        assert!(result.is_empty());
    }
}

#[cfg(test)]
mod legacy_flow_normalize_tests {
    use super::*;
    use greentic_flow::model::{FlowDoc, NodeDoc};
    use indexmap::IndexMap;
    use serde_json::{Value, json};

    /// Build a single-node `FlowDoc` whose only node carries the raw-YGTC
    /// op-key `op_key` with `payload`, mirroring the legacy flow-JSON shape the
    /// loader feeds into `normalize_flow_doc` -> `flow_doc_to_ir`.
    fn single_node_doc(op_key: &str, payload: Value) -> FlowDoc {
        let mut raw = IndexMap::new();
        raw.insert(op_key.to_string(), payload);
        let mut nodes = IndexMap::new();
        nodes.insert(
            "node".into(),
            NodeDoc {
                raw,
                routing: json!([{ "out": true }]),
                ..Default::default()
            },
        );
        FlowDoc {
            id: "legacy".into(),
            title: None,
            description: None,
            flow_type: "messaging".into(),
            start: Some("node".into()),
            parameters: json!({}),
            tags: Vec::new(),
            schema_version: None,
            entrypoints: IndexMap::new(),
            meta: None,
            slot_schema: None,
            nodes,
        }
    }

    /// REGRESSION: the legacy flow-JSON load path (`normalize_flow_doc` ->
    /// `flow_doc_to_ir`) must preserve a raw-YGTC `{ mcp: { ... }, routing }`
    /// node VERBATIM as `component == "mcp"` with its payload intact, exactly
    /// like the runtime-flow (packc) path. Before the fix `normalize_flow_doc`
    /// rewrote every non-`emit.*` op-key into a `component.exec` node, so the
    /// engine saw `NodeKind::Exec` instead of the MCP dispatch arm.
    #[test]
    fn normalize_preserves_mcp_op_key_through_legacy_path() {
        let doc = single_node_doc(
            "mcp",
            json!({
                "server": "github",
                "tool": "get_issue",
                "arguments": { "id": "{{ entry.issue_id }}" },
                "output": "issue"
            }),
        );

        let normalized = normalize_flow_doc(doc);
        let node = normalized.nodes.get("node").expect("node exists");
        // NOT rewritten into a `component.exec` wrapper.
        assert_eq!(
            node.operation.as_deref(),
            Some("mcp"),
            "the `mcp` op-key must survive normalization verbatim, not become component.exec"
        );
        assert!(node.raw.is_empty(), "raw op-key should be lowered");

        // Lowering through the real adapter yields `component == "mcp"` with
        // the LOCKED ENCODING v2 payload (server/tool/arguments/output) intact.
        let ir = flow_doc_to_ir(normalized).expect("flow_doc_to_ir");
        let lowered = ir.nodes.get("node").expect("lowered node exists");
        assert_eq!(lowered.component, "mcp");
        assert_eq!(lowered.payload_expr["server"], json!("github"));
        assert_eq!(lowered.payload_expr["tool"], json!("get_issue"));
        assert_eq!(
            lowered.payload_expr["arguments"]["id"],
            json!("{{ entry.issue_id }}")
        );
        assert_eq!(lowered.payload_expr["output"], json!("issue"));
    }

    /// No regression to the passthrough set: the other native op-keys-with-
    /// payload (`dw.agent`, `sorla.call`) must also survive `normalize_flow_doc`
    /// verbatim rather than being wrapped in `component.exec`.
    #[test]
    fn normalize_preserves_dw_agent_and_sorla_call_op_keys() {
        for op_key in ["dw.agent", "sorla.call"] {
            let doc = single_node_doc(op_key, json!({ "input": { "x": 1 } }));
            let normalized = normalize_flow_doc(doc);
            let node = normalized.nodes.get("node").expect("node exists");
            assert_eq!(
                node.operation.as_deref(),
                Some(op_key),
                "`{op_key}` must survive normalization verbatim, not become component.exec"
            );
            assert!(node.raw.is_empty());

            let ir = flow_doc_to_ir(normalized).expect("flow_doc_to_ir");
            let lowered = ir.nodes.get("node").expect("lowered node exists");
            assert_eq!(lowered.component, op_key);
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PackMetadata {
    pub pack_id: String,
    pub version: String,
    #[serde(default)]
    pub entry_flows: Vec<String>,
    #[serde(default)]
    pub secret_requirements: Vec<greentic_types::SecretRequirement>,
}

impl PackMetadata {
    fn from_wasm(bytes: &[u8]) -> Option<Self> {
        let parser = Parser::new(0);
        for payload in parser.parse_all(bytes) {
            let payload = payload.ok()?;
            match payload {
                Payload::CustomSection(section) => {
                    if section.name() == "greentic.manifest"
                        && let Ok(meta) = Self::from_bytes(section.data())
                    {
                        return Some(meta);
                    }
                }
                Payload::DataSection(reader) => {
                    for segment in reader.into_iter().flatten() {
                        if let Ok(meta) = Self::from_bytes(segment.data) {
                            return Some(meta);
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, serde_cbor::Error> {
        #[derive(Deserialize)]
        struct RawManifest {
            pack_id: String,
            version: String,
            #[serde(default)]
            entry_flows: Vec<String>,
            #[serde(default)]
            flows: Vec<RawFlow>,
            #[serde(default)]
            secret_requirements: Vec<greentic_types::SecretRequirement>,
        }

        #[derive(Deserialize)]
        struct RawFlow {
            id: String,
        }

        let manifest: RawManifest = serde_cbor::from_slice(bytes)?;
        let mut entry_flows = if manifest.entry_flows.is_empty() {
            manifest.flows.iter().map(|f| f.id.clone()).collect()
        } else {
            manifest.entry_flows.clone()
        };
        entry_flows.retain(|id| !id.is_empty());
        Ok(Self {
            pack_id: manifest.pack_id,
            version: manifest.version,
            entry_flows,
            secret_requirements: manifest.secret_requirements,
        })
    }

    pub fn fallback(path: &Path) -> Self {
        let pack_id = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unknown-pack".to_string());
        Self {
            pack_id,
            version: "0.0.0".to_string(),
            entry_flows: Vec::new(),
            secret_requirements: Vec::new(),
        }
    }

    pub fn from_manifest(manifest: &greentic_types::PackManifest) -> Self {
        let entry_flows = manifest
            .flows
            .iter()
            .map(|flow| flow.id.as_str().to_string())
            .collect::<Vec<_>>();
        Self {
            pack_id: manifest.pack_id.as_str().to_string(),
            version: manifest.version.to_string(),
            entry_flows,
            secret_requirements: manifest.secret_requirements.clone(),
        }
    }
}
