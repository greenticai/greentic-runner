use anyhow::{Context, Result, anyhow};
use greentic_pack::reader::{VerifyReport, open_pack};
use greentic_runner_host::RunnerWasiPolicy;
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, FlowDescriptor, PackMetadata, PackRuntime};
use greentic_runner_host::runner::engine::{ExecutionObserver, FlowContext, FlowEngine, NodeEvent};
pub use greentic_runner_host::runner::mocks::{
    HttpMock, HttpMockMode, KvMock, MocksConfig, SecretsMock, TelemetryMock, TimeMock, ToolsMock,
};
use greentic_runner_host::runner::mocks::{MockEventSink, MockLayer};
use greentic_runner_host::secrets::{DynSecretsManager, default_manager};
use greentic_runner_host::storage::{new_session_store, new_state_store};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use parking_lot::Mutex;
use runner_core::normalize_under_root;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::runtime::Runtime;
use tracing::{info, warn};
use uuid::Uuid;

const PROVIDER_ID_DEV: &str = "greentic-dev";

/// Hook invoked for each transcript event.
pub type TranscriptHook = Arc<dyn Fn(&Value) + Send + Sync>;

/// Configuration for emitting OTLP telemetry.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct OtlpHook {
    pub endpoint: String,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub sample_all: bool,
}

/// Execution profile.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Profile {
    Dev(DevProfile),
}

impl Default for Profile {
    fn default() -> Self {
        Self::Dev(DevProfile::default())
    }
}

/// Tunables for the developer profile.
#[derive(Clone, Debug)]
pub struct DevProfile {
    pub tenant_id: String,
    pub team_id: String,
    pub user_id: String,
    pub max_node_wall_time_ms: u64,
    pub max_run_wall_time_ms: u64,
}

impl Default for DevProfile {
    fn default() -> Self {
        Self {
            tenant_id: "local-dev".to_string(),
            team_id: "default".to_string(),
            user_id: "developer".to_string(),
            max_node_wall_time_ms: 30_000,
            max_run_wall_time_ms: 600_000,
        }
    }
}

/// Tenant context overrides supplied by callers.
#[derive(Clone, Debug, Default)]
pub struct TenantContext {
    pub tenant_id: Option<String>,
    pub team_id: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
}

impl TenantContext {
    pub fn default_local() -> Self {
        Self {
            tenant_id: Some("local-dev".into()),
            team_id: Some("default".into()),
            user_id: Some("developer".into()),
            session_id: None,
        }
    }
}

/// User supplied options for a run.
#[derive(Clone)]
pub struct RunOptions {
    pub profile: Profile,
    pub entry_flow: Option<String>,
    /// Start the run at this node instead of the flow's declared entrypoint.
    ///
    /// Card-driven messaging packs carry the id of the next node to run on the
    /// inbound activity. Without this the host can only restart at the
    /// entrypoint on every turn, so the nodes chained between two rendered
    /// cards never execute.
    ///
    /// A persisted resume snapshot WINS over this: it means a card is parked
    /// awaiting the user's submit, and only re-dispatching that card attaches
    /// the submitted `answers` to its output for the nodes downstream to read.
    /// So this drives the FIRST hop into a flow, and resume drives the rest.
    ///
    /// A node id the flow does not have is an error, never a silent fall back
    /// to the entrypoint.
    pub entry_node: Option<String>,
    pub input: Value,
    pub ctx: TenantContext,
    pub transcript: Option<TranscriptHook>,
    pub otlp: Option<OtlpHook>,
    pub mocks: MocksConfig,
    pub artifacts_dir: Option<PathBuf>,
    pub signing: SigningPolicy,
    pub components_dir: Option<PathBuf>,
    pub components_map: HashMap<String, PathBuf>,
    pub dist_offline: bool,
    pub dist_cache_dir: Option<PathBuf>,
    pub allow_missing_hash: bool,
    /// Optional secrets manager override used by embedded runtimes that need
    /// to share a caller-owned secrets backend with the runner.
    pub secrets_manager: Option<DynSecretsManager>,
    /// Optional cross-pack resolver for `provider.invoke` nodes that reference
    /// providers in other packs (e.g., OAuth broker from a capability pack).
    pub cross_pack_resolver:
        Option<Arc<dyn greentic_runner_host::runner::engine::CrossPackResolver>>,
    /// Directory where suspended `FlowSnapshot`s are persisted between
    /// activities so a conversational flow can resume at the node the
    /// previous run paused at (instead of restarting from the entry).
    ///
    /// Snapshots are keyed by `ctx.session_id`, so the caller must populate
    /// `ctx.session_id` with a stable per-conversation identifier (e.g.
    /// `{tenant}:{flow}:{conversation_id}`) for resume to engage. When
    /// either this directory or `session_id` is missing, the runner
    /// behaves exactly as before: a `FlowExecution::waiting` result is
    /// surfaced as an `Err` and the snapshot is dropped.
    pub session_state_dir: Option<PathBuf>,
}

impl Default for RunOptions {
    fn default() -> Self {
        desktop_defaults()
    }
}

impl fmt::Debug for RunOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunOptions")
            .field("profile", &self.profile)
            .field("entry_flow", &self.entry_flow)
            .field("entry_node", &self.entry_node)
            .field("input", &self.input)
            .field("ctx", &self.ctx)
            .field("transcript", &self.transcript.is_some())
            .field("otlp", &self.otlp)
            .field("mocks", &self.mocks)
            .field("artifacts_dir", &self.artifacts_dir)
            .field("signing", &self.signing)
            .field("components_dir", &self.components_dir)
            .field("components_map_len", &self.components_map.len())
            .field("dist_offline", &self.dist_offline)
            .field("dist_cache_dir", &self.dist_cache_dir)
            .field("allow_missing_hash", &self.allow_missing_hash)
            .field("secrets_manager", &self.secrets_manager.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SigningPolicy {
    Strict,
    DevOk,
}

/// Convenience builder that retains a baseline `RunOptions`.
#[derive(Clone, Debug)]
pub struct Runner {
    base: RunOptions,
}

impl Runner {
    pub fn new() -> Self {
        Self {
            base: desktop_defaults(),
        }
    }

    pub fn profile(mut self, profile: Profile) -> Self {
        self.base.profile = profile;
        self
    }

    pub fn with_mocks(mut self, mocks: MocksConfig) -> Self {
        self.base.mocks = mocks;
        self
    }

    pub fn configure(mut self, f: impl FnOnce(&mut RunOptions)) -> Self {
        f(&mut self.base);
        self
    }

    pub fn run_pack<P: AsRef<Path>>(&self, pack_path: P) -> Result<RunResult> {
        run_pack_with_options(pack_path, self.base.clone())
    }

    pub fn run_pack_with<P: AsRef<Path>>(
        &self,
        pack_path: P,
        f: impl FnOnce(&mut RunOptions),
    ) -> Result<RunResult> {
        let mut opts = self.base.clone();
        f(&mut opts);
        run_pack_with_options(pack_path, opts)
    }

    pub async fn run_pack_async<P: AsRef<Path>>(&self, pack_path: P) -> Result<RunResult> {
        run_pack_with_options_async(pack_path, self.base.clone()).await
    }

    pub async fn run_pack_with_async<P: AsRef<Path>>(
        &self,
        pack_path: P,
        f: impl FnOnce(&mut RunOptions),
    ) -> Result<RunResult> {
        let mut opts = self.base.clone();
        f(&mut opts);
        run_pack_with_options_async(pack_path, opts).await
    }
}

impl Default for Runner {
    fn default() -> Self {
        Self::new()
    }
}

/// Execute a pack with the provided options.
pub fn run_pack_with_options<P: AsRef<Path>>(pack_path: P, opts: RunOptions) -> Result<RunResult> {
    let runtime = Runtime::new().context("failed to create tokio runtime")?;
    runtime.block_on(run_pack_async(pack_path.as_ref(), opts))
}

/// Execute a pack with the provided options using the caller's async runtime.
pub async fn run_pack_with_options_async<P: AsRef<Path>>(
    pack_path: P,
    opts: RunOptions,
) -> Result<RunResult> {
    run_pack_async(pack_path.as_ref(), opts).await
}

/// Reasonable defaults for local desktop invocations.
pub fn desktop_defaults() -> RunOptions {
    let otlp = std::env::var("OTLP_ENDPOINT")
        .ok()
        .map(|endpoint| OtlpHook {
            endpoint,
            headers: Vec::new(),
            sample_all: true,
        });
    RunOptions {
        profile: Profile::Dev(DevProfile::default()),
        entry_flow: None,
        input: json!({}),
        ctx: TenantContext::default_local(),
        transcript: None,
        otlp,
        mocks: MocksConfig::default(),
        artifacts_dir: None,
        signing: SigningPolicy::DevOk,
        components_dir: None,
        components_map: HashMap::new(),
        dist_offline: false,
        dist_cache_dir: None,
        allow_missing_hash: false,
        secrets_manager: None,
        cross_pack_resolver: None,
        session_state_dir: None,
        entry_node: None,
    }
}

fn resolve_secrets_manager(opts: &RunOptions) -> Result<DynSecretsManager> {
    if let Some(manager) = opts.secrets_manager.clone() {
        Ok(manager)
    } else {
        default_manager().context("failed to initialise secrets backend")
    }
}

async fn run_pack_async(pack_path: &Path, opts: RunOptions) -> Result<RunResult> {
    let pack_path = normalize_pack_path(pack_path)?;
    let resolved_profile = resolve_profile(&opts.profile, &opts.ctx);
    if let Some(otlp) = &opts.otlp {
        apply_otlp_hook(otlp);
    }

    let directories = prepare_run_dirs(opts.artifacts_dir.clone())?;
    info!(run_dir = %directories.root.display(), "prepared desktop run directory");

    let mock_layer = Arc::new(MockLayer::new(opts.mocks.clone(), &directories.root)?);

    let recorder = Arc::new(RunRecorder::new(
        directories.clone(),
        &resolved_profile,
        None,
        PackMetadata::fallback(&pack_path),
        opts.transcript.clone(),
    )?);

    let mock_sink: Arc<dyn MockEventSink> = recorder.clone();
    mock_layer.register_sink(mock_sink);

    // Collect only the `host.http.client` booleans from the component manifest
    // rather than cloning full `ComponentManifest` values — we just need to know
    // whether any component opted in.
    let mut pack_http_flags: Vec<bool> = Vec::new();
    if pack_path.is_file() {
        match open_pack(&pack_path, to_reader_policy(opts.signing)) {
            Ok(load) => {
                let meta = &load.manifest.meta;
                recorder.update_pack_metadata(PackMetadata {
                    pack_id: meta.pack_id.clone(),
                    version: meta.version.to_string(),
                    entry_flows: meta.entry_flows.clone(),
                    secret_requirements: Vec::new(),
                });
                note_verify_outcome(&recorder, VerifyOutcome::Loaded(&load.report));
                if let Some(manifest) = load.gpack_manifest.as_ref() {
                    pack_http_flags = manifest
                        .components
                        .iter()
                        .map(|component| {
                            component
                                .capabilities
                                .host
                                .http
                                .as_ref()
                                .is_some_and(|http| http.client)
                        })
                        .collect();
                }
            }
            Err(err) => {
                recorder.record_verify_event("error", &err.message)?;
                if opts.signing == SigningPolicy::DevOk && is_signature_error(&err.message) {
                    note_verify_outcome(&recorder, VerifyOutcome::Downgraded(&err.message));
                } else {
                    return Err(anyhow!("pack verification failed: {}", err.message));
                }
            }
        }
    } else {
        note_verify_outcome(&recorder, VerifyOutcome::DirectorySkipped);
    }

    let kill_switch = desktop_http_kill_switch_active();
    let http_enabled = derive_http_enabled(&pack_http_flags, kill_switch);
    let http_component_count = pack_http_flags.iter().filter(|flag| **flag).count();
    if http_enabled {
        info!(
            total_component_count = pack_http_flags.len(),
            http_component_count,
            "enabling HTTP client gate based on component manifest capabilities"
        );
    } else if kill_switch {
        info!(
            env_var = DESKTOP_HTTP_DISABLE_ENV,
            "HTTP client gate forced off by desktop kill-switch"
        );
    } else {
        tracing::debug!(
            total_component_count = pack_http_flags.len(),
            "HTTP client gate disabled: no component manifest declares host.http.client"
        );
    }
    let host_config = Arc::new(build_host_config(
        &resolved_profile,
        &directories,
        http_enabled,
    ));
    let mut component_resolution = ComponentResolution::default();
    if let Some(dir) = opts.components_dir.clone() {
        component_resolution.materialized_root = Some(dir);
    } else if pack_path.is_dir() {
        component_resolution.materialized_root = Some(pack_path.clone());
    }
    component_resolution.overrides = opts.components_map.clone();
    component_resolution.dist_offline = opts.dist_offline;
    component_resolution.dist_cache_dir = opts.dist_cache_dir.clone();
    component_resolution.allow_missing_hash = opts.allow_missing_hash;
    let archive_source = if pack_path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("gtpack"))
        .unwrap_or(false)
    {
        Some(&pack_path)
    } else {
        None
    };

    let session_store = new_session_store();
    let state_store = new_state_store();
    let secrets_manager = resolve_secrets_manager(&opts)?;
    let pack = Arc::new(
        PackRuntime::load(
            &pack_path,
            Arc::clone(&host_config),
            Some(Arc::clone(&mock_layer)),
            archive_source.map(|p| p as &Path),
            Some(Arc::clone(&session_store)),
            Some(Arc::clone(&state_store)),
            Arc::new(RunnerWasiPolicy::default()),
            secrets_manager,
            host_config.oauth_broker_config(),
            false,
            component_resolution,
        )
        .await
        .with_context(|| format!("failed to load pack {}", pack_path.display()))?,
    );
    recorder.update_pack_metadata(pack.metadata().clone());

    let flows = pack
        .list_flows()
        .await
        .context("failed to enumerate flows")?;
    let entry_flow_id = resolve_entry_flow(opts.entry_flow.clone(), pack.metadata(), &flows)?;
    recorder.set_flow_id(&entry_flow_id);

    let mut engine = FlowEngine::new(vec![Arc::clone(&pack)], Arc::clone(&host_config))
        .await
        .context("failed to prime flow engine")?;
    if let Some(resolver) = opts.cross_pack_resolver.clone() {
        engine.set_cross_pack_resolver(resolver);
    }

    #[cfg(feature = "agentic-worker")]
    {
        #[cfg(feature = "desktop-agent-ephemeral")]
        use greentic_runner_host::runner::agent_node::build_agent_node_handler_ephemeral;
        use greentic_runner_host::runner::agent_node::{
            agent_configs_from_manifest, build_agent_node_handler, merge_agent_sources,
        };

        let blobs = pack.manifest_agent_blobs();
        if !blobs.is_empty() {
            let pack_agents = agent_configs_from_manifest(&pack.metadata().pack_id, &blobs);
            let merged = merge_agent_sources(pack_agents, HashMap::new());
            let tenant = resolved_profile.tenant_id.clone();
            let sm = resolve_secrets_manager(&opts)?;
            let redis_set = std::env::var("GREENTIC_AW_REDIS_URL")
                .map(|v| !v.is_empty())
                .unwrap_or(false);
            // `project_id` (billing) is `None` here: the desktop runner loads a
            // single local `.gtpack` directly, with no deployment revision and
            // therefore no pinned `bundle_id`. Billing omits the dimension
            // rather than falling back to an in-pack agent id, which is not
            // unique across packs.
            let handler = if redis_set {
                build_agent_node_handler(
                    merged,
                    tenant,
                    sm,
                    None,
                    vec![Arc::clone(&pack)],
                    None,
                    None,
                    None,
                )
                .await
            } else {
                #[cfg(feature = "desktop-agent-ephemeral")]
                {
                    build_agent_node_handler_ephemeral(
                        merged,
                        tenant,
                        sm,
                        None,
                        vec![Arc::clone(&pack)],
                        None,
                        None,
                        None,
                    )
                    .await
                }
                #[cfg(not(feature = "desktop-agent-ephemeral"))]
                {
                    let _ = (merged, tenant, sm);
                    tracing::info!(
                        "GREENTIC_AW_REDIS_URL unset and desktop-agent-ephemeral feature \
                         off; dw.agent nodes disabled"
                    );
                    None
                }
            };
            if let Some(handler) = handler {
                engine.set_agent_node_handler(handler);
                tracing::info!("DwAgent runtime wired into desktop FlowEngine");
            }
        }
    }

    let started_at = OffsetDateTime::now_utc();
    let tenant_str = host_config.tenant.clone();
    let session_id_owned = resolved_profile.session_id.clone();
    let provider_id_owned = resolved_profile.provider_id.clone();
    let recorder_ref: &RunRecorder = &recorder;
    let mock_ref: &MockLayer = &mock_layer;
    let ctx = FlowContext {
        tenant: &tenant_str,
        pack_id: pack.metadata().pack_id.as_str(),
        flow_id: &entry_flow_id,
        node_id: None,
        tool: None,
        action: Some("run_pack"),
        session_id: Some(session_id_owned.as_str()),
        provider_id: Some(provider_id_owned.as_str()),
        retry_config: host_config.retry_config().into(),
        attempt: 1,
        observer: Some(recorder_ref),
        mocks: Some(mock_ref),
        reply_scope: None,
        // The desktop runner drives a pack from the local CLI: there is no
        // messaging provider in the path, so nothing has verified a caller and
        // the turn is anonymous. `crate::caller_identity` explains why only a
        // provider may establish one.
        caller: None,
    };

    // Resume support: if the caller wired up a session_state_dir AND ctx
    // carries a session_id, load any persisted snapshot for that session
    // and resume the engine from there. Without either signal we fall
    // back to a fresh execution at the flow entry.
    let session_snapshot_path = session_snapshot_file(
        opts.session_state_dir.as_deref(),
        resolved_profile.session_id.as_str(),
    );
    let resume_snapshot = session_snapshot_path
        .as_deref()
        .and_then(load_session_snapshot);

    let execution = if let Some(snapshot) = resume_snapshot {
        // A parked snapshot OUTRANKS `entry_node`. A card parks awaiting the
        // user's submit, and only re-dispatching THAT node on resume attaches
        // the submitted `answers` to its output — which is what the nodes
        // downstream read (`{{node.<card>.answers.<field>}}`). Jumping to an
        // entry node instead skips the card, so the answers never exist and
        // every downstream binding resolves empty.
        engine.resume(ctx, snapshot, opts.input.clone()).await
    } else if let Some(entry_node) = opts.entry_node.as_deref() {
        // No parked run to continue, so this is the caller's first hop into
        // the flow: start at the node it named.
        engine
            .execute_from(ctx, opts.input.clone(), entry_node)
            .await
    } else {
        engine.execute(ctx, opts.input.clone()).await
    };
    let finished_at = OffsetDateTime::now_utc();

    let status = match execution {
        Ok(result) => match result.status {
            greentic_runner_host::runner::engine::FlowStatus::Completed => {
                // Flow ran to a real terminator — discard any stale resume
                // snapshot so the next activity starts fresh at the entry.
                if let Some(path) = session_snapshot_path.as_deref() {
                    let _ = std::fs::remove_file(path);
                }
                RunCompletion::Ok
            }
            greentic_runner_host::runner::engine::FlowStatus::Waiting(wait) => {
                let reason = wait
                    .reason
                    .clone()
                    .unwrap_or_else(|| "flow paused unexpectedly".to_string());
                if let Some(path) = session_snapshot_path.as_deref() {
                    if let Err(err) = save_session_snapshot(path, &wait.snapshot) {
                        // Persisting the snapshot is best-effort — losing
                        // it just means the next activity restarts at the
                        // entry, which is the pre-fix behaviour. Surface
                        // the failure to the operator log via the run
                        // result error and continue.
                        RunCompletion::Err(anyhow::anyhow!(
                            "{reason} (and failed to persist resume snapshot to {}: {err})",
                            path.display()
                        ))
                    } else {
                        // Successfully paused. Surface the run as Ok so
                        // greentic-start can emit the rendered card and
                        // wait for the user's next submission.
                        RunCompletion::Ok
                    }
                } else {
                    // No persistence configured — preserve the legacy
                    // behaviour (treat pause as an error so the caller at
                    // least sees that the run did not complete).
                    RunCompletion::Err(anyhow::anyhow!(reason))
                }
            }
        },
        Err(err) => RunCompletion::Err(err),
    };

    let result = recorder.finalise(status, started_at, finished_at)?;

    let run_json_path = directories.root.join("run.json");
    fs::write(&run_json_path, serde_json::to_vec_pretty(&result)?)
        .with_context(|| format!("failed to write run summary {}", run_json_path.display()))?;

    Ok(result)
}

/// Resolve the snapshot file path for a given session id.
///
/// Returns `None` if the caller didn't wire up `session_state_dir` or the
/// session_id is empty/non-resumable — both cases mean "no persistence,
/// run with fresh state".
fn session_snapshot_file(session_state_dir: Option<&Path>, session_id: &str) -> Option<PathBuf> {
    let dir = session_state_dir?;
    if session_id.is_empty() {
        return None;
    }
    let safe_id: String = session_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect();
    Some(dir.join(format!("{safe_id}.snapshot.json")))
}

/// Load a persisted `FlowSnapshot` from disk if one exists. Returns `None`
/// when the file is missing or unreadable — the caller falls back to a
/// fresh execution in that case (which is also the pre-resume behaviour).
fn load_session_snapshot(
    path: &Path,
) -> Option<greentic_runner_host::runner::engine::FlowSnapshot> {
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Persist a `FlowSnapshot` to disk for later resume. Errors propagate to
/// the caller so the run can surface "we paused but lost the snapshot" as
/// a hard error rather than silently looping back to the flow entry.
fn save_session_snapshot(
    path: &Path,
    snapshot: &greentic_runner_host::runner::engine::FlowSnapshot,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create session snapshot dir {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(snapshot)
        .with_context(|| "serialize session snapshot for persistence")?;
    fs::write(path, bytes).with_context(|| format!("write session snapshot {}", path.display()))?;
    Ok(())
}

fn apply_otlp_hook(hook: &OtlpHook) {
    info!(
        endpoint = %hook.endpoint,
        sample_all = hook.sample_all,
        headers = %hook.headers.len(),
        "OTLP hook requested (set OTEL_* env vars before invoking run_pack)"
    );
}

fn normalize_pack_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("pack path {} has no parent", path.display()))?;
        let root = parent
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", parent.display()))?;
        let file = path
            .file_name()
            .ok_or_else(|| anyhow!("pack path {} has no file name", path.display()))?;
        return normalize_under_root(&root, Path::new(file));
    }

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
    normalize_under_root(&root, Path::new(file))
}

fn prepare_run_dirs(root_override: Option<PathBuf>) -> Result<RunDirectories> {
    let root = if let Some(dir) = root_override {
        dir
    } else {
        let timestamp = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
            .replace(':', "-");
        let short_id = Uuid::new_v4()
            .to_string()
            .chars()
            .take(6)
            .collect::<String>();
        PathBuf::from(".greentic")
            .join("runs")
            .join(format!("{timestamp}_{short_id}"))
    };

    fs::create_dir_all(&root).with_context(|| format!("failed to create {}", root.display()))?;
    let logs = root.join("logs");
    let resolved = root.join("resolved_config");
    fs::create_dir_all(&logs).with_context(|| format!("failed to create {}", logs.display()))?;
    fs::create_dir_all(&resolved)
        .with_context(|| format!("failed to create {}", resolved.display()))?;

    Ok(RunDirectories {
        root,
        logs,
        resolved,
    })
}

fn resolve_entry_flow(
    override_id: Option<String>,
    metadata: &PackMetadata,
    flows: &[FlowDescriptor],
) -> Result<String> {
    if let Some(flow) = override_id {
        return Ok(flow);
    }
    if let Some(first) = metadata.entry_flows.first() {
        return Ok(first.clone());
    }
    flows
        .first()
        .map(|f| f.id.clone())
        .ok_or_else(|| anyhow!("pack does not declare any flows"))
}

fn is_signature_error(message: &str) -> bool {
    message.to_ascii_lowercase().contains("signature")
}

/// What a verification outcome is worth recording, if anything.
///
/// Kept separate from the call sites so the decision can be tested directly —
/// the call sites live deep inside `run_pack_with_options`, which needs a real
/// pack, a real profile and a temp directory to reach.
enum VerifyOutcome<'a> {
    /// A pack that loaded, carrying its verification report.
    Loaded(&'a VerifyReport),
    /// The input was a directory, so verification never ran at all.
    DirectorySkipped,
    /// An error was downgraded to a warning by `is_signature_error`.
    Downgraded(&'a str),
}

/// `None` when there is nothing worth recording; otherwise the event status and
/// its message.
fn verify_event(outcome: VerifyOutcome<'_>) -> Option<(&'static str, String)> {
    match outcome {
        VerifyOutcome::Loaded(report) if report.signature_ok => None,
        VerifyOutcome::Loaded(report) => {
            // Fall back to a fixed sentence rather than an empty message: an
            // empty event is as invisible as no event, which is the defect this
            // exists to close.
            let message = if report.warnings.is_empty() {
                "pack signature did not verify; the loader reported no detail".to_string()
            } else {
                report.warnings.join("; ")
            };
            Some(("unverified", message))
        }
        VerifyOutcome::DirectorySkipped => Some((
            "skipped",
            "pack verification skipped: input is a directory, not a signed archive".to_string(),
        )),
        VerifyOutcome::Downgraded(err) => Some((
            "downgraded",
            format!("continuing despite error, matched as signature-related by substring: {err}"),
        )),
    }
}

/// Record a verification outcome, if there is one worth recording.
///
/// Deliberately swallows a recording failure. All three call sites fire on paths
/// where the run continues, and observability added by this change must not be
/// able to turn a run that works into a run that fails. The pre-existing call on
/// the error path keeps its `?` — that run is already failing.
fn note_verify_outcome(recorder: &RunRecorder, outcome: VerifyOutcome<'_>) {
    let Some((status, message)) = verify_event(outcome) else {
        return;
    };
    warn!(status, message = %message, "pack verification");
    if let Err(err) = recorder.record_verify_event(status, &message) {
        warn!(error = %err, "could not record the pack verification event");
    }
}

fn to_reader_policy(policy: SigningPolicy) -> greentic_pack::reader::SigningPolicy {
    match policy {
        SigningPolicy::Strict => greentic_pack::reader::SigningPolicy::Strict,
        SigningPolicy::DevOk => greentic_pack::reader::SigningPolicy::DevOk,
    }
}

fn resolve_profile(profile: &Profile, ctx: &TenantContext) -> ResolvedProfile {
    match profile {
        Profile::Dev(dev) => ResolvedProfile {
            tenant_id: ctx
                .tenant_id
                .clone()
                .unwrap_or_else(|| dev.tenant_id.clone()),
            team_id: ctx.team_id.clone().unwrap_or_else(|| dev.team_id.clone()),
            user_id: ctx.user_id.clone().unwrap_or_else(|| dev.user_id.clone()),
            session_id: ctx
                .session_id
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
            provider_id: PROVIDER_ID_DEV.to_string(),
            max_node_wall_time_ms: dev.max_node_wall_time_ms,
            max_run_wall_time_ms: dev.max_run_wall_time_ms,
        },
    }
}

/// Environment variable that forces the HTTP client gate off regardless of
/// what component manifests declare. Operators set this to `1` / `true` to
/// disable outbound HTTP as a kill-switch when running the desktop profile.
const DESKTOP_HTTP_DISABLE_ENV: &str = "GREENTIC_DESKTOP_HTTP_DISABLE";

/// Derive whether the desktop profile must enable the HTTP client gate.
///
/// Returns `true` when at least one component flag is `true` and the
/// kill-switch is not active. Packs opt in to HTTP by declaring
/// `capabilities.host.http.client: true`; operators can force the gate off
/// via [`DESKTOP_HTTP_DISABLE_ENV`] regardless of what the manifest says.
fn derive_http_enabled(component_http_flags: &[bool], kill_switch: bool) -> bool {
    !kill_switch && component_http_flags.iter().any(|flag| *flag)
}

/// Parse the raw value of [`DESKTOP_HTTP_DISABLE_ENV`] into a kill-switch bool.
///
/// Truthy values (`1`, `true`, `yes`, `on`, case- and whitespace-insensitive)
/// activate the kill-switch. `None` or any other value leaves it inactive so
/// the gate remains controlled by the component manifest.
fn parse_kill_switch(raw: Option<&str>) -> bool {
    matches!(
        raw.map(|value| value.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// Read [`DESKTOP_HTTP_DISABLE_ENV`] from the process environment and
/// interpret it via [`parse_kill_switch`].
fn desktop_http_kill_switch_active() -> bool {
    parse_kill_switch(std::env::var(DESKTOP_HTTP_DISABLE_ENV).ok().as_deref())
}

fn build_host_config(
    profile: &ResolvedProfile,
    dirs: &RunDirectories,
    http_enabled: bool,
) -> HostConfig {
    HostConfig {
        tenant: profile.tenant_id.clone(),
        bindings_path: dirs.resolved.join("dev.bindings.yaml"),
        flow_type_bindings: HashMap::new(),
        rate_limits: RateLimits::default(),
        retry: FlowRetryConfig::default(),
        http_enabled,
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
        // The `agents` and `graphs` fields are present only when the host is
        // compiled with the `agentic-worker` feature, which this crate
        // propagates via its own same-named feature flag.
        #[cfg(feature = "agentic-worker")]
        agents: HashMap::new(),
        #[cfg(feature = "agentic-worker")]
        graphs: HashMap::new(),
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct ResolvedProfile {
    tenant_id: String,
    team_id: String,
    user_id: String,
    session_id: String,
    provider_id: String,
    max_node_wall_time_ms: u64,
    max_run_wall_time_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunResult {
    pub session_id: String,
    pub pack_id: String,
    pub pack_version: String,
    pub flow_id: String,
    pub started_at_utc: String,
    pub finished_at_utc: String,
    pub status: RunStatus,
    pub error: Option<String>,
    pub node_summaries: Vec<NodeSummary>,
    pub failures: BTreeMap<String, NodeFailure>,
    pub artifacts_dir: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RunStatus {
    Success,
    PartialFailure,
    Failure,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeSummary {
    pub node_id: String,
    pub component: String,
    pub status: NodeStatus,
    pub duration_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    Ok,
    Skipped,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeFailure {
    pub code: String,
    pub message: String,
    pub details: Value,
    pub transcript_offsets: (u64, u64),
    pub log_paths: Vec<PathBuf>,
}

#[derive(Clone)]
struct RunDirectories {
    root: PathBuf,
    logs: PathBuf,
    resolved: PathBuf,
}

enum RunCompletion {
    Ok,
    Err(anyhow::Error),
}

struct RunRecorder {
    directories: RunDirectories,
    profile: ResolvedProfile,
    flow_id: Mutex<String>,
    pack_meta: Mutex<PackMetadata>,
    transcript: Mutex<TranscriptWriter>,
    state: Mutex<RunRecorderState>,
}

impl RunRecorder {
    fn new(
        dirs: RunDirectories,
        profile: &ResolvedProfile,
        flow_id: Option<String>,
        pack_meta: PackMetadata,
        hook: Option<TranscriptHook>,
    ) -> Result<Self> {
        let transcript_path = dirs.root.join("transcript.jsonl");
        let file = File::create(&transcript_path)
            .with_context(|| format!("failed to open {}", transcript_path.display()))?;
        Ok(Self {
            directories: dirs,
            profile: profile.clone(),
            flow_id: Mutex::new(flow_id.unwrap_or_else(|| "unknown".into())),
            pack_meta: Mutex::new(pack_meta),
            transcript: Mutex::new(TranscriptWriter::new(BufWriter::new(file), hook)),
            state: Mutex::new(RunRecorderState::default()),
        })
    }

    fn finalise(
        &self,
        completion: RunCompletion,
        started_at: OffsetDateTime,
        finished_at: OffsetDateTime,
    ) -> Result<RunResult> {
        let status = match &completion {
            RunCompletion::Ok => RunStatus::Success,
            RunCompletion::Err(_) => RunStatus::Failure,
        };

        let mut runtime_error = None;
        if let RunCompletion::Err(err) = completion {
            warn!(error = %err, "pack execution failed");
            eprintln!("pack execution failed: {err}");
            runtime_error = Some(err.to_string());
        }

        let started = started_at
            .format(&Rfc3339)
            .unwrap_or_else(|_| started_at.to_string());
        let finished = finished_at
            .format(&Rfc3339)
            .unwrap_or_else(|_| finished_at.to_string());

        let state = self.state.lock();
        let mut summaries = Vec::new();
        let mut failures = BTreeMap::new();
        for node_id in &state.order {
            if let Some(record) = state.nodes.get(node_id) {
                let duration = record.duration_ms.unwrap_or(0);
                summaries.push(NodeSummary {
                    node_id: node_id.clone(),
                    component: record.component.clone(),
                    status: record.status.clone(),
                    duration_ms: duration,
                });
                if record.status == NodeStatus::Error {
                    let start_offset = record.transcript_start.unwrap_or(0);
                    let err_offset = record.transcript_error.unwrap_or(start_offset);
                    failures.insert(
                        node_id.clone(),
                        NodeFailure {
                            code: record
                                .failure_code
                                .clone()
                                .unwrap_or_else(|| "component-failed".into()),
                            message: record
                                .failure_message
                                .clone()
                                .unwrap_or_else(|| "node failed".into()),
                            details: record
                                .failure_details
                                .clone()
                                .unwrap_or_else(|| json!({ "node": node_id })),
                            transcript_offsets: (start_offset, err_offset),
                            log_paths: record.log_paths.clone(),
                        },
                    );
                }
            }
        }

        let mut final_status = status;
        if final_status == RunStatus::Success && !failures.is_empty() {
            final_status = RunStatus::PartialFailure;
        }

        if let Some(error) = &runtime_error {
            failures.insert(
                "_runtime".into(),
                NodeFailure {
                    code: "runtime-error".into(),
                    message: error.clone(),
                    details: json!({ "stage": "execute" }),
                    transcript_offsets: (0, 0),
                    log_paths: Vec::new(),
                },
            );
        }

        let pack_meta = self.pack_meta.lock();
        let flow_id = self.flow_id.lock().clone();
        Ok(RunResult {
            session_id: self.profile.session_id.clone(),
            pack_id: pack_meta.pack_id.clone(),
            pack_version: pack_meta.version.clone(),
            flow_id,
            started_at_utc: started,
            finished_at_utc: finished,
            status: final_status,
            error: runtime_error,
            node_summaries: summaries,
            failures,
            artifacts_dir: self.directories.root.clone(),
        })
    }

    fn set_flow_id(&self, flow_id: &str) {
        *self.flow_id.lock() = flow_id.to_string();
    }

    fn update_pack_metadata(&self, meta: PackMetadata) {
        *self.pack_meta.lock() = meta;
    }

    fn current_flow_id(&self) -> String {
        self.flow_id.lock().clone()
    }

    fn record_verify_event(&self, status: &str, message: &str) -> Result<()> {
        let timestamp = OffsetDateTime::now_utc();
        let event = json!({
            "ts": timestamp
                .format(&Rfc3339)
                .unwrap_or_else(|_| timestamp.to_string()),
            "session_id": self.profile.session_id,
            "flow_id": self.current_flow_id(),
            "node_id": Value::Null,
            "component": "verify.pack",
            "phase": "verify",
            "status": status,
            "inputs": Value::Null,
            "outputs": Value::Null,
            "error": json!({ "message": message }),
            "metrics": Value::Null,
            "schema_id": Value::Null,
            "defaults_applied": Value::Null,
            "redactions": Value::Array(Vec::new()),
        });
        self.transcript.lock().write(&event).map(|_| ())
    }

    fn write_mock_event(&self, capability: &str, provider: &str, payload: Value) -> Result<()> {
        let timestamp = OffsetDateTime::now_utc();
        let event = json!({
            "ts": timestamp
                .format(&Rfc3339)
                .unwrap_or_else(|_| timestamp.to_string()),
            "session_id": self.profile.session_id,
            "flow_id": self.current_flow_id(),
            "node_id": Value::Null,
            "component": format!("mock::{capability}"),
            "phase": "mock",
            "status": "ok",
            "inputs": json!({ "capability": capability, "provider": provider }),
            "outputs": payload,
            "error": Value::Null,
            "metrics": Value::Null,
            "schema_id": Value::Null,
            "defaults_applied": Value::Null,
            "redactions": Value::Array(Vec::new()),
        });
        self.transcript.lock().write(&event).map(|_| ())
    }

    fn handle_node_start(&self, event: &NodeEvent<'_>) -> Result<()> {
        let timestamp = OffsetDateTime::now_utc();
        let (redacted_payload, redactions) = redact_value(event.payload, "$.inputs.payload");
        let inputs = json!({
            "payload": redacted_payload,
            "context": {
                "tenant_id": self.profile.tenant_id.as_str(),
                "team_id": self.profile.team_id.as_str(),
                "user_id": self.profile.user_id.as_str(),
            }
        });
        let flow_id = self.current_flow_id();
        let event_json = build_transcript_event(TranscriptEventArgs {
            profile: &self.profile,
            flow_id: &flow_id,
            node_id: event.node_id,
            component: &event.node.component,
            phase: "start",
            status: "ok",
            timestamp,
            inputs,
            outputs: Value::Null,
            error: Value::Null,
            redactions,
        });
        let (start_offset, _) = self.transcript.lock().write(&event_json)?;

        let mut state = self.state.lock();
        let node_key = event.node_id.to_string();
        if !state.order.iter().any(|id| id == &node_key) {
            state.order.push(node_key.clone());
        }
        let entry = state.nodes.entry(node_key).or_insert_with(|| {
            NodeExecutionRecord::new(event.node.component.clone(), &self.directories)
        });
        entry.start_instant = Some(Instant::now());
        entry.status = NodeStatus::Ok;
        entry.transcript_start = Some(start_offset);
        Ok(())
    }

    fn handle_node_end(&self, event: &NodeEvent<'_>, output: &Value) -> Result<()> {
        let timestamp = OffsetDateTime::now_utc();
        let (redacted_output, redactions) = redact_value(output, "$.outputs");
        let flow_id = self.current_flow_id();
        let event_json = build_transcript_event(TranscriptEventArgs {
            profile: &self.profile,
            flow_id: &flow_id,
            node_id: event.node_id,
            component: &event.node.component,
            phase: "end",
            status: "ok",
            timestamp,
            inputs: Value::Null,
            outputs: redacted_output,
            error: Value::Null,
            redactions,
        });
        self.transcript.lock().write(&event_json)?;

        let mut state = self.state.lock();
        if let Some(entry) = state.nodes.get_mut(event.node_id)
            && let Some(started) = entry.start_instant.take()
        {
            entry.duration_ms = Some(started.elapsed().as_millis() as u64);
        }
        Ok(())
    }

    fn handle_node_error(
        &self,
        event: &NodeEvent<'_>,
        error: &dyn std::error::Error,
    ) -> Result<()> {
        let timestamp = OffsetDateTime::now_utc();
        let error_message = error.to_string();
        let error_json = json!({
            "code": "component-failed",
            "message": error_message,
            "details": {
                "node": event.node_id,
            }
        });
        let flow_id = self.current_flow_id();
        let event_json = build_transcript_event(TranscriptEventArgs {
            profile: &self.profile,
            flow_id: &flow_id,
            node_id: event.node_id,
            component: &event.node.component,
            phase: "error",
            status: "error",
            timestamp,
            inputs: Value::Null,
            outputs: Value::Null,
            error: error_json.clone(),
            redactions: Vec::new(),
        });
        let (_, end_offset) = self.transcript.lock().write(&event_json)?;

        let mut state = self.state.lock();
        if let Some(entry) = state.nodes.get_mut(event.node_id) {
            entry.status = NodeStatus::Error;
            if let Some(started) = entry.start_instant.take() {
                entry.duration_ms = Some(started.elapsed().as_millis() as u64);
            }
            entry.transcript_error = Some(end_offset);
            entry.failure_code = Some("component-failed".to_string());
            entry.failure_message = Some(error_message.clone());
            entry.failure_details = Some(error_json);
            let log_path = self
                .directories
                .logs
                .join(format!("{}.stderr.log", sanitize_id(event.node_id)));
            if let Ok(mut file) = File::create(&log_path) {
                let _ = writeln!(file, "{error_message}");
            }
            entry.log_paths.push(log_path);
        }
        Ok(())
    }
}

impl ExecutionObserver for RunRecorder {
    fn on_node_start(&self, event: &NodeEvent<'_>) {
        if let Err(err) = self.handle_node_start(event) {
            warn!(node = event.node_id, error = %err, "failed to record node start");
        }
    }

    fn on_node_end(&self, event: &NodeEvent<'_>, output: &Value) {
        if let Err(err) = self.handle_node_end(event, output) {
            warn!(node = event.node_id, error = %err, "failed to record node end");
        }
    }

    fn on_node_error(&self, event: &NodeEvent<'_>, error: &dyn std::error::Error) {
        if let Err(err) = self.handle_node_error(event, error) {
            warn!(node = event.node_id, error = %err, "failed to record node error");
        }
    }
}

impl MockEventSink for RunRecorder {
    fn on_mock_event(&self, capability: &str, provider: &str, payload: &Value) {
        if let Err(err) = self.write_mock_event(capability, provider, payload.clone()) {
            warn!(?capability, ?provider, error = %err, "failed to record mock event");
        }
    }
}

#[derive(Default)]
struct RunRecorderState {
    nodes: BTreeMap<String, NodeExecutionRecord>,
    order: Vec<String>,
}

#[derive(Clone)]
struct NodeExecutionRecord {
    component: String,
    status: NodeStatus,
    duration_ms: Option<u64>,
    transcript_start: Option<u64>,
    transcript_error: Option<u64>,
    log_paths: Vec<PathBuf>,
    failure_code: Option<String>,
    failure_message: Option<String>,
    failure_details: Option<Value>,
    start_instant: Option<Instant>,
}

impl NodeExecutionRecord {
    fn new(component: String, _dirs: &RunDirectories) -> Self {
        Self {
            component,
            status: NodeStatus::Ok,
            duration_ms: None,
            transcript_start: None,
            transcript_error: None,
            log_paths: Vec::new(),
            failure_code: None,
            failure_message: None,
            failure_details: None,
            start_instant: None,
        }
    }
}

struct TranscriptWriter {
    writer: BufWriter<File>,
    offset: u64,
    hook: Option<TranscriptHook>,
}

impl TranscriptWriter {
    fn new(writer: BufWriter<File>, hook: Option<TranscriptHook>) -> Self {
        Self {
            writer,
            offset: 0,
            hook,
        }
    }

    fn write(&mut self, value: &Value) -> Result<(u64, u64)> {
        let line = serde_json::to_vec(value)?;
        let start = self.offset;
        self.writer.write_all(&line)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        self.offset += line.len() as u64 + 1;
        if let Some(hook) = &self.hook {
            hook(value);
        }
        Ok((start, self.offset))
    }
}

struct TranscriptEventArgs<'a> {
    profile: &'a ResolvedProfile,
    flow_id: &'a str,
    node_id: &'a str,
    component: &'a str,
    phase: &'a str,
    status: &'a str,
    timestamp: OffsetDateTime,
    inputs: Value,
    outputs: Value,
    error: Value,
    redactions: Vec<String>,
}

fn build_transcript_event(args: TranscriptEventArgs<'_>) -> Value {
    let ts = args
        .timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| args.timestamp.to_string());
    json!({
        "ts": ts,
        "session_id": args.profile.session_id.as_str(),
        "flow_id": args.flow_id,
        "node_id": args.node_id,
        "component": args.component,
        "phase": args.phase,
        "status": args.status,
        "inputs": args.inputs,
        "outputs": args.outputs,
        "error": args.error,
        "metrics": {
            "duration_ms": null,
            "cpu_time_ms": null,
            "mem_peak_bytes": null,
        },
        "schema_id": Value::Null,
        "defaults_applied": Value::Array(Vec::new()),
        "redactions": args.redactions,
    })
}

fn redact_value(value: &Value, base: &str) -> (Value, Vec<String>) {
    let mut paths = Vec::new();
    let redacted = redact_recursive(value, base, &mut paths);
    (redacted, paths)
}

fn redact_recursive(value: &Value, path: &str, acc: &mut Vec<String>) -> Value {
    match value {
        Value::Object(map) => {
            let mut new_map = JsonMap::new();
            for (key, val) in map {
                let child_path = format!("{path}.{key}");
                if is_sensitive_key(key) {
                    acc.push(child_path);
                    new_map.insert(key.clone(), Value::String("__REDACTED__".into()));
                } else {
                    new_map.insert(key.clone(), redact_recursive(val, &child_path, acc));
                }
            }
            Value::Object(new_map)
        }
        Value::Array(items) => {
            let mut new_items = Vec::new();
            for (idx, item) in items.iter().enumerate() {
                let child_path = format!("{path}[{idx}]");
                new_items.push(redact_recursive(item, &child_path, acc));
            }
            Value::Array(new_items)
        }
        other => other.clone(),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    const MARKERS: [&str; 5] = ["secret", "token", "password", "authorization", "cookie"];
    MARKERS.iter().any(|marker| lower.contains(marker))
}

fn sanitize_id(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use async_trait::async_trait;
    use greentic_runner_host::secrets::DynSecretsManager;
    use greentic_secrets_lib::{SecretError, SecretsManager};
    use serde_json::json;
    use tempfile::TempDir;

    fn sample_metadata() -> PackMetadata {
        PackMetadata {
            pack_id: "pack.demo".into(),
            version: "1.0.0".into(),
            entry_flows: vec!["entry.flow".into()],
            secret_requirements: Vec::new(),
        }
    }

    fn sample_profile() -> ResolvedProfile {
        ResolvedProfile {
            tenant_id: "tenant".into(),
            team_id: "team".into(),
            user_id: "user".into(),
            session_id: "session-1".into(),
            provider_id: PROVIDER_ID_DEV.into(),
            max_node_wall_time_ms: 1000,
            max_run_wall_time_ms: 2000,
        }
    }

    struct NoopSecretsManager;

    #[async_trait]
    impl SecretsManager for NoopSecretsManager {
        async fn read(&self, path: &str) -> Result<Vec<u8>, SecretError> {
            Err(SecretError::NotFound(path.to_string()))
        }

        async fn write(&self, _path: &str, _bytes: &[u8]) -> Result<(), SecretError> {
            Ok(())
        }

        async fn delete(&self, _path: &str) -> Result<(), SecretError> {
            Ok(())
        }
    }

    #[test]
    fn resolve_entry_flow_prefers_override_then_metadata_then_flows() {
        let metadata = sample_metadata();
        let flows = vec![FlowDescriptor {
            id: "fallback.flow".into(),
            flow_type: "message".into(),
            pack_id: "pack.demo".into(),
            profile: "default".into(),
            version: "1.0.0".into(),
            description: None,
            entry: true,
        }];

        assert_eq!(
            resolve_entry_flow(Some("manual.flow".into()), &metadata, &flows).unwrap(),
            "manual.flow"
        );
        assert_eq!(
            resolve_entry_flow(None, &metadata, &flows).unwrap(),
            "entry.flow"
        );
        assert!(
            resolve_entry_flow(
                None,
                &PackMetadata {
                    entry_flows: Vec::new(),
                    ..metadata
                },
                &flows
            )
            .is_ok()
        );
    }

    #[test]
    fn normalize_pack_path_accepts_relative_files_inside_cwd() {
        let temp = tempfile::tempdir_in(std::env::current_dir().expect("cwd")).expect("tempdir");
        let pack = temp.path().join("demo.gtpack");
        fs::write(&pack, b"pack").expect("pack file");
        let relative = pack
            .strip_prefix(std::env::current_dir().expect("cwd"))
            .expect("relative")
            .to_path_buf();

        let normalized = normalize_pack_path(&relative).expect("normalized path");

        assert_eq!(normalized, pack.canonicalize().expect("canonical"));
    }

    #[test]
    fn prepare_run_dirs_creates_logs_and_resolved_subdirs() {
        let temp = TempDir::new().expect("tempdir");
        let root = temp.path().join("artifacts");
        let dirs = prepare_run_dirs(Some(root.clone())).expect("run dirs");

        assert_eq!(dirs.root, root);
        assert!(dirs.logs.is_dir());
        assert!(dirs.resolved.is_dir());
    }

    #[test]
    fn redact_value_masks_nested_sensitive_fields() {
        let (redacted, paths) = redact_value(
            &json!({
                "token": "abc",
                "nested": { "authorization_header": "secret" },
                "items": [{ "password_hint": "nope" }]
            }),
            "$",
        );

        assert_eq!(redacted["token"], "__REDACTED__");
        assert_eq!(redacted["nested"]["authorization_header"], "__REDACTED__");
        assert_eq!(redacted["items"][0]["password_hint"], "__REDACTED__");
        assert!(paths.contains(&"$.token".to_string()));
        assert!(paths.contains(&"$.nested.authorization_header".to_string()));
    }

    #[test]
    fn helper_functions_preserve_ids_and_transcript_shape() {
        assert_eq!(sanitize_id("flow demo/1"), "flow-demo-1");
        assert!(is_signature_error("Signature verification failed"));
        assert_eq!(
            to_reader_policy(SigningPolicy::DevOk),
            greentic_pack::reader::SigningPolicy::DevOk
        );

        let profile = sample_profile();
        let event = build_transcript_event(TranscriptEventArgs {
            profile: &profile,
            flow_id: "flow.demo",
            node_id: "node.demo",
            component: "component.demo",
            phase: "invoke",
            status: "ok",
            timestamp: OffsetDateTime::now_utc(),
            inputs: json!({"input": true}),
            outputs: json!({"output": true}),
            error: Value::Null,
            redactions: vec!["$.token".into()],
        });
        assert_eq!(event["session_id"], "session-1");
        assert_eq!(event["flow_id"], "flow.demo");
        assert_eq!(event["redactions"][0], "$.token");
    }

    fn report(signature_ok: bool, warnings: &[&str]) -> VerifyReport {
        VerifyReport {
            signature_ok,
            sbom_ok: true,
            warnings: warnings.iter().map(|w| (*w).to_string()).collect(),
        }
    }

    #[test]
    fn a_verified_pack_has_nothing_to_report() {
        // A report that verified is equally silent even if it carried unrelated
        // warnings — this function speaks only about signature state.
        assert!(verify_event(VerifyOutcome::Loaded(&report(true, &["noise"]))).is_none());
    }

    #[test]
    fn an_unverified_pack_reports_the_crates_own_diagnosis() {
        // greentic-pack already distinguishes missing / incomplete / invalid.
        // Surfacing its wording rather than re-deriving one keeps the two in step.
        let r = report(false, &["signature files missing; skipping verification"]);
        let (status, message) = verify_event(VerifyOutcome::Loaded(&r)).expect("reports");
        assert_eq!(status, "unverified");
        assert!(
            message.contains("signature files missing"),
            "the crate's own warning must survive verbatim: {message}"
        );
    }

    #[test]
    fn several_warnings_are_all_kept() {
        let r = report(false, &["first problem", "second problem"]);
        let (_, message) = verify_event(VerifyOutcome::Loaded(&r)).expect("reports");
        assert!(message.contains("first problem"), "{message}");
        assert!(message.contains("second problem"), "{message}");
    }

    #[test]
    fn an_unverified_pack_with_no_warnings_still_reports() {
        // Silence here would recreate the very defect this work closes.
        let r = report(false, &[]);
        let (status, message) = verify_event(VerifyOutcome::Loaded(&r)).expect("reports");
        assert_eq!(status, "unverified");
        assert!(
            !message.is_empty(),
            "an empty message is as invisible as no event"
        );
    }

    #[test]
    fn a_directory_input_reports_that_it_was_skipped() {
        let (status, message) = verify_event(VerifyOutcome::DirectorySkipped).expect("reports");
        assert_eq!(status, "skipped");
        assert!(message.contains("director"), "{message}");
    }

    #[test]
    fn note_verify_outcome_writes_verification_events_to_transcript() {
        let temp = TempDir::new().expect("tempdir");
        let root = temp.path().join("artifacts");
        let dirs = prepare_run_dirs(Some(root.clone())).expect("run dirs");
        let profile = sample_profile();

        let recorder = RunRecorder::new(
            dirs,
            &profile,
            None,
            PackMetadata::fallback(&PathBuf::from("test.gtpack")),
            None,
        )
        .expect("recorder");

        // Fire the event through the recorder-level helper, not just verify_event.
        note_verify_outcome(&recorder, VerifyOutcome::DirectorySkipped);

        // Read back and verify the event landed in the transcript with the expected shape.
        let transcript_path = root.join("transcript.jsonl");
        let transcript = std::fs::read_to_string(&transcript_path).expect("read transcript");

        // The event should be a JSON line with component == "verify.pack" and status == "skipped".
        let line = transcript
            .lines()
            .find(|line| line.contains("\"verify.pack\""))
            .expect("verify.pack event found");

        let parsed: serde_json::Value = serde_json::from_str(line).expect("parse transcript event");
        assert_eq!(parsed["component"], "verify.pack");
        assert_eq!(parsed["status"], "skipped");
    }

    #[test]
    fn a_downgraded_error_names_itself_and_how_it_was_matched() {
        let (status, message) =
            verify_event(VerifyOutcome::Downgraded("boom: bad signature")).expect("reports");
        assert_eq!(status, "downgraded");
        assert!(message.contains("boom: bad signature"), "{message}");
        // The substring match is the reason this downgrade happened at all, and
        // it is known to be an unreliable classifier — say so where it is seen.
        assert!(message.contains("substring"), "{message}");
    }

    #[test]
    fn resolve_profile_uses_context_overrides() {
        let profile = resolve_profile(
            &Profile::Dev(DevProfile::default()),
            &TenantContext {
                tenant_id: Some("tenant-x".into()),
                team_id: Some("team-x".into()),
                user_id: Some("user-x".into()),
                session_id: Some("session-x".into()),
            },
        );

        assert_eq!(profile.tenant_id, "tenant-x");
        assert_eq!(profile.team_id, "team-x");
        assert_eq!(profile.user_id, "user-x");
        assert_eq!(profile.session_id, "session-x");
        assert_eq!(profile.provider_id, PROVIDER_ID_DEV);
    }

    #[test]
    fn desktop_defaults_and_runner_builder_preserve_overrides() {
        let defaults = desktop_defaults();
        assert_eq!(defaults.signing, SigningPolicy::DevOk);
        assert_eq!(defaults.ctx.tenant_id.as_deref(), Some("local-dev"));

        let runner = Runner::new()
            .with_mocks(MocksConfig {
                telemetry: Some(TelemetryMock),
                ..MocksConfig::default()
            })
            .configure(|opts| {
                opts.entry_flow = Some("entry.flow".into());
                opts.input = json!({"demo": true});
            });

        let rendered = format!("{:?}", runner.base);
        assert!(rendered.contains("entry.flow"));
        assert!(runner.base.mocks.telemetry.is_some());
        assert_eq!(runner.base.input, json!({"demo": true}));
    }

    #[test]
    fn resolve_entry_flow_errors_when_pack_has_no_flows() {
        let metadata = PackMetadata {
            entry_flows: Vec::new(),
            ..sample_metadata()
        };
        assert!(resolve_entry_flow(None, &metadata, &[]).is_err());
    }

    #[test]
    fn build_host_config_enables_local_dev_defaults() {
        let temp = TempDir::new().expect("tempdir");
        let dirs = prepare_run_dirs(Some(temp.path().join("run"))).expect("dirs");
        let config = build_host_config(&sample_profile(), &dirs, false);

        assert_eq!(config.tenant, "tenant");
        assert!(!config.http_enabled);
        assert!(config.secrets_policy.is_allowed("any.secret"));
        assert!(
            config
                .operator_policy
                .allows_provider(Some("provider"), "provider")
        );
    }

    #[test]
    fn build_host_config_honours_http_enabled_flag() {
        let temp = TempDir::new().expect("tempdir");
        let dirs = prepare_run_dirs(Some(temp.path().join("run"))).expect("dirs");
        let enabled = build_host_config(&sample_profile(), &dirs, true);
        assert!(
            enabled.http_enabled,
            "http_enabled should propagate when derived from manifest"
        );

        let disabled = build_host_config(&sample_profile(), &dirs, false);
        assert!(!disabled.http_enabled);
    }

    #[test]
    fn derive_http_enabled_true_when_any_component_declares_http_client() {
        assert!(derive_http_enabled(&[false, true], false));
    }

    #[test]
    fn derive_http_enabled_false_when_no_component_declares_http_client() {
        assert!(!derive_http_enabled(&[false, false], false));
    }

    #[test]
    fn derive_http_enabled_false_for_empty_components() {
        assert!(!derive_http_enabled(&[], false));
    }

    #[test]
    fn derive_http_enabled_kill_switch_forces_disable() {
        assert!(
            !derive_http_enabled(&[true], true),
            "kill-switch must override positive manifest declaration"
        );
    }

    #[test]
    fn parse_kill_switch_matches_truthy_and_falsy_inputs() {
        // (raw input, expected kill-switch active)
        let cases: &[(Option<&str>, bool)] = &[
            (Some("1"), true),
            (Some("true"), true),
            (Some("YES"), true),
            (Some("  on  "), true),
            (Some("On"), true),
            (Some("0"), false),
            (Some("false"), false),
            (Some("bogus"), false),
            (Some(""), false),
            (None, false),
        ];

        for (raw, expected) in cases {
            assert_eq!(
                parse_kill_switch(*raw),
                *expected,
                "parse_kill_switch({raw:?}) should be {expected}"
            );
        }
    }

    #[test]
    fn redact_value_leaves_non_sensitive_payloads_unchanged() {
        let original = json!({"public": {"value": 1}});
        let (redacted, paths) = redact_value(&original, "$");
        assert_eq!(redacted, original);
        assert!(paths.is_empty());
    }

    /// Regression: `save_session_snapshot` + `load_session_snapshot` must
    /// round-trip an arbitrary `FlowSnapshot` byte-for-byte through JSON so
    /// that resume sees exactly what was persisted at pause time. If
    /// serialization ever drops a field (e.g. `next_flow` or any
    /// `ExecutionState` member) the resumed run would silently restart at
    /// the entry — which is the exact bug this whole change set fixes.
    #[test]
    fn session_snapshot_round_trip_through_disk() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let snapshot_path = tmp.path().join("session-1.snapshot.json");

        let snapshot_json = json!({
            "pack_id": "pack.demo",
            "flow_id": "flow.welcome",
            "next_node": "card-confirm",
            "state": {
                "entry": { "metadata": { "action": "confirm" } },
                "input": { "metadata": { "action": "confirm" } },
                "nodes": {},
                "egress": [],
                "redirect_count": 0
            }
        });
        let original: greentic_runner_host::runner::engine::FlowSnapshot =
            serde_json::from_value(snapshot_json.clone()).expect("deserialize seed snapshot");

        save_session_snapshot(&snapshot_path, &original).expect("save snapshot");
        let reloaded = load_session_snapshot(&snapshot_path).expect("load snapshot");

        let original_value = serde_json::to_value(&original).unwrap();
        let reloaded_value = serde_json::to_value(&reloaded).unwrap();
        assert_eq!(
            original_value, reloaded_value,
            "snapshot must round-trip byte-for-byte through disk"
        );
    }

    #[test]
    fn session_snapshot_file_sanitizes_session_id() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let path = session_snapshot_file(Some(tmp.path()), "tenant/team:user-1.session")
            .expect("snapshot path");
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .expect("file name");
        // `/` -> `_`, but `:`, `-`, `.` are preserved.
        assert_eq!(file_name, "tenant_team:user-1.session.snapshot.json");
        assert!(session_snapshot_file(None, "any").is_none());
        assert!(session_snapshot_file(Some(tmp.path()), "").is_none());
    }

    #[test]
    fn resolve_secrets_manager_prefers_injected_override_even_in_prod() {
        let previous_env = std::env::var("GREENTIC_ENV").ok();
        unsafe {
            std::env::set_var("GREENTIC_ENV", "prod");
        }

        let without_override = resolve_secrets_manager(&RunOptions::default());
        assert!(
            without_override.is_err(),
            "expected prod desktop default backend to be rejected"
        );

        let injected: DynSecretsManager = Arc::new(NoopSecretsManager);
        let with_override = resolve_secrets_manager(&RunOptions {
            secrets_manager: Some(Arc::clone(&injected)),
            ..RunOptions::default()
        });
        assert!(
            with_override.is_ok(),
            "expected injected secrets manager to bypass desktop default backend"
        );

        match previous_env {
            Some(value) => unsafe {
                std::env::set_var("GREENTIC_ENV", value);
            },
            None => unsafe {
                std::env::remove_var("GREENTIC_ENV");
            },
        }
    }
}
