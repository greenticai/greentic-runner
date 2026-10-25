#![deny(unsafe_code)]
//! Canonical Greentic host runtime.
//!
//! This crate owns tenant bindings, pack ingestion/watchers, ingress adapters,
//! Wasmtime glue, session/state storage, and the HTTP server used by the
//! `greentic-runner` CLI. Downstream crates embed it either through
//! [`RunnerConfig`] + [`run`] (HTTP host) or [`HostBuilder`] (direct API access).

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::secrets::SecretsBackend;
use anyhow::{Context, Result, anyhow};
use greentic_config::ResolvedConfig;
use greentic_config_types::TelemetryExporterKind;
use greentic_config_types::{
    NetworkConfig, PackSourceConfig, PacksConfig, PathsConfig, TelemetryConfig,
};
use greentic_telemetry::export::{ExportConfig as TelemetryExportConfig, ExportMode, Sampling};
use runner_core::env::PackConfig;
use serde_json::json;
use tokio::signal;

pub mod boot;
pub mod cache;
pub mod caller_identity;
pub mod component_api;
pub mod config;
pub mod engine;
pub mod extension_provider;
pub mod fault;
#[cfg(feature = "greentic-x-provider")]
pub mod greentic_x_provider;
pub mod gtbind;
pub mod http;
mod http_guard;
pub mod identify_hint;
pub mod metrics;
pub mod operator_metrics;
pub mod operator_registry;
pub mod pack;
pub mod provider;
pub mod provider_core;
pub mod provider_core_only;
pub mod routing;
pub mod run_outcome;
pub mod runner;
pub mod runtime;
pub mod runtime_refs;
pub mod runtime_wasmtime;
pub mod secrets;
pub(crate) mod secrets_broker;
pub mod sql;
pub mod storage;
pub mod telemetry;
pub mod telemetry_scan;
#[cfg(feature = "fault-injection")]
pub mod testing;
pub mod trace;
pub mod validate;
pub mod verify;
pub mod wasi;
mod wasm_limits;

/// The wasmtime engine and cache [`cache::EngineProfile`] the runtime itself
/// uses for compiled-component caching. CLI cache tools (`warmup`, `doctor`,
/// `prune`) must use this so they act on the same cache directory as the
/// runtime instead of a profile of a default-configured engine.
///
/// # Errors
/// Returns an error if the engine cannot be built.
pub fn runtime_cache_engine_and_profile() -> anyhow::Result<(wasmtime::Engine, cache::EngineProfile)>
{
    let engine = wasm_limits::build_engine()?;
    let profile = cache::EngineProfile::from_engine(
        &engine,
        cache::CpuPolicy::Native,
        wasm_limits::ENGINE_FINGERPRINT.to_string(),
    );
    Ok((engine, profile))
}
pub mod watcher;

mod activity;
mod host;
// `http_guard::GuardedHooks` supersedes `HttpTimeoutHooks` (same ceiling, plus the
// address guard and body cap); the timeout helpers it reuses stay here.
#[allow(dead_code)]
mod http_timeout_hooks;
pub mod oauth;

pub use activity::{Activity, ActivityKind, WelcomeFlowHint};
pub use config::HostConfig;
pub use gtbind::{PackBinding, TenantBindings};
pub use host::TelemetryCfg;
pub use host::{HostBuilder, RunnerHost, TenantHandle, TurnTrace};
pub use wasi::{PreopenSpec, RunnerWasiPolicy};

pub use greentic_types::{EnvId, FlowId, PackId, TenantCtx, TenantId};

pub use http::auth::AdminAuth;
pub use routing::RoutingConfig;
use routing::TenantRouting;
pub use runner::HostServer;

/// Build a worker's sorla tool source over exactly the given SoRs, each
/// resolved through its own route document at
/// `secrets://default/<tenant>/<team|_>/sorla/<sor>`. Re-exported for the
/// designer's in-process Test chat, which knows a worker's bound SoRs
/// directly and has no `PackRuntime` of its own to read a
/// `assets/sorla-routes.json` sidecar from — see
/// `runner::sorla_pack_source` for the pack-routed counterpart this crate
/// uses internally.
#[cfg(feature = "agentic-worker")]
pub use runner::sorla_pack_source::sorla_source_for_sors;

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::config::{OperatorPolicy, SecretsPolicy};
    use crate::runtime::TenantRuntime;
    use crate::secrets::default_manager;
    use crate::storage::{new_session_store, new_state_store, session_host_from, state_host_from};
    use crate::trace::TraceConfig;
    use crate::validate::ValidationConfig;
    use tempfile::TempDir;

    pub(crate) fn fixture_pack_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/packs/demo.gtpack")
            .canonicalize()
            .expect("fixture pack path")
    }

    fn minimal_config(workspace: &std::path::Path, timers: &str) -> Result<Arc<HostConfig>> {
        let bindings_path = workspace.join("bindings.yaml");
        std::fs::write(
            &bindings_path,
            format!(
                r#"
tenant: demo
flow_type_bindings: {{}}
rate_limits: {{}}
retry: {{}}
timers: {timers}
"#
            ),
        )?;
        let mut config =
            HostConfig::load_from_path(&bindings_path).context("load minimal host bindings")?;
        config.secrets_policy = SecretsPolicy::allow_all();
        config.operator_policy = OperatorPolicy::allow_all();
        config.trace = TraceConfig::from_env();
        config.validation = ValidationConfig::from_env();
        Ok(Arc::new(config))
    }

    pub(crate) async fn build_test_runtime() -> Result<(TempDir, Arc<TenantRuntime>)> {
        build_test_runtime_with_timers("[]").await
    }

    /// Same runtime, with `timers` (a YAML flow sequence) as its timer bindings.
    pub(crate) async fn build_test_runtime_with_timers(
        timers: &str,
    ) -> Result<(TempDir, Arc<TenantRuntime>)> {
        let workspace = TempDir::new().context("temp workspace")?;
        let config = minimal_config(workspace.path(), timers)?;
        let session_store = new_session_store();
        let session_host = session_host_from(Arc::clone(&session_store));
        let state_store = new_state_store();
        let state_host = state_host_from(Arc::clone(&state_store));
        let secrets = default_manager()?;
        let pack_path = fixture_pack_path();
        let runtime = TenantRuntime::load(
            &pack_path,
            config,
            None,
            Some(&pack_path),
            None,
            Arc::new(RunnerWasiPolicy::new()),
            session_host,
            Arc::clone(&session_store),
            Arc::clone(&state_store),
            state_host,
            secrets,
            #[cfg(feature = "agentic-worker")]
            None,
            #[cfg(feature = "agentic-worker")]
            None,
            #[cfg(feature = "agentic-worker")]
            None,
        )
        .await?;
        Ok((workspace, runtime))
    }
}

/// User-facing configuration for running the unified host.
#[derive(Clone)]
pub struct RunnerConfig {
    pub tenant_bindings: HashMap<String, TenantBindings>,
    pub pack: PackConfig,
    pub port: u16,
    pub refresh_interval: Duration,
    pub routing: RoutingConfig,
    pub admin: AdminAuth,
    pub telemetry: Option<TelemetryCfg>,
    pub secrets_backend: SecretsBackend,
    pub wasi_policy: RunnerWasiPolicy,
    pub resolved_config: ResolvedConfig,
    pub trace: trace::TraceConfig,
    pub validation: validate::ValidationConfig,
}

impl RunnerConfig {
    /// Build a [`RunnerConfig`] from a resolved greentic-config and the provided binding files.
    pub fn from_config(resolved_config: ResolvedConfig, bindings: Vec<PathBuf>) -> Result<Self> {
        Self::from_config_with_port(resolved_config, bindings, None)
    }

    /// Like [`RunnerConfig::from_config`], with an explicit port (the `--port`
    /// flag). When `port` is `Some`, the `PORT` environment variable is not
    /// read at all, so a malformed `PORT` cannot fail a run that overrides it.
    pub fn from_config_with_port(
        resolved_config: ResolvedConfig,
        bindings: Vec<PathBuf>,
        port: Option<u16>,
    ) -> Result<Self> {
        if bindings.is_empty() {
            anyhow::bail!("at least one gtbind file is required");
        }
        let tenant_bindings = gtbind::load_gtbinds(&bindings)?;
        if tenant_bindings.is_empty() {
            anyhow::bail!("no gtbind files loaded");
        }
        let mut pack = pack_config_from(
            &resolved_config.config.packs,
            &resolved_config.config.paths,
            &resolved_config.config.network,
        )?;
        maybe_write_gtbind_index(&tenant_bindings, &resolved_config.config.paths, &mut pack)?;
        let refresh = parse_refresh_interval(std::env::var("PACK_REFRESH_INTERVAL").ok())?;
        let port = resolve_port(port, || std::env::var("PORT").ok())?;
        let default_tenant = resolved_config
            .config
            .dev
            .as_ref()
            .map(|dev| dev.default_tenant.clone())
            .unwrap_or_else(|| crate::routing::DEFAULT_TENANT.into());
        let routing = RoutingConfig::from_env_with_default(default_tenant);
        routing.startup_check()?;
        let paths = &resolved_config.config.paths;
        ensure_paths_exist(paths)?;
        // The env allowlist is intentionally NOT the union of every tenant's
        // `env_passthrough`: `PackRuntime::load` adds only the owning tenant's
        // keys via `RunnerWasiPolicy::for_tenant`.
        let wasi_policy = default_wasi_policy(paths);

        let config_admin_token = resolved_config.config.services.as_ref().and_then(|s| {
            s.events
                .as_ref()
                .and_then(|svc| svc.headers.as_ref())
                .and_then(|headers| headers.get("x-admin-token").cloned())
        });
        let (admin_token, admin_source) =
            resolve_admin_token(std::env::var("ADMIN_TOKEN").ok(), config_admin_token);
        match admin_source {
            AdminTokenSource::Env => {
                tracing::info!("admin API auth: bearer token (ADMIN_TOKEN)")
            }
            AdminTokenSource::Config => {
                tracing::info!("admin API auth: bearer token (services.events.headers)")
            }
            AdminTokenSource::None => tracing::warn!(
                "admin API auth: no ADMIN_TOKEN configured; admin routes are loopback-only"
            ),
        }
        let admin = AdminAuth::new(admin_token);
        let secrets_backend = SecretsBackend::from_config(&resolved_config.config.secrets)?;
        Ok(Self {
            tenant_bindings,
            pack,
            port,
            refresh_interval: refresh,
            routing,
            admin,
            telemetry: telemetry_from(&resolved_config.config.telemetry),
            secrets_backend,
            wasi_policy,
            resolved_config,
            trace: trace::TraceConfig::from_env(),
            validation: validate::ValidationConfig::from_env(),
        })
    }

    /// Override the HTTP port used by the host server.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_wasi_policy(mut self, policy: RunnerWasiPolicy) -> Self {
        self.wasi_policy = policy;
        self
    }
}

fn maybe_write_gtbind_index(
    tenant_bindings: &HashMap<String, TenantBindings>,
    paths: &PathsConfig,
    pack: &mut PackConfig,
) -> Result<()> {
    let mut uses_locators = false;
    for binding in tenant_bindings.values() {
        for pack_binding in &binding.packs {
            if pack_binding.pack_locator.is_some() {
                uses_locators = true;
            }
        }
    }
    if !uses_locators {
        return Ok(());
    }

    let mut entries = serde_json::Map::new();
    for binding in tenant_bindings.values() {
        let mut packs = Vec::new();
        for pack_binding in &binding.packs {
            let locator = pack_binding.pack_locator.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "gtbind {} missing pack_locator for pack {}",
                    binding.tenant,
                    pack_binding.pack_id
                )
            })?;
            let (name, version_or_digest) =
                pack_binding.pack_ref.split_once('@').ok_or_else(|| {
                    anyhow::anyhow!(
                        "gtbind {} invalid pack_ref {} (expected name@version)",
                        binding.tenant,
                        pack_binding.pack_ref
                    )
                })?;
            if name != pack_binding.pack_id {
                anyhow::bail!(
                    "gtbind {} pack_ref {} does not match pack_id {}",
                    binding.tenant,
                    pack_binding.pack_ref,
                    pack_binding.pack_id
                );
            }
            let mut entry = serde_json::Map::new();
            entry.insert("name".to_string(), json!(name));
            if version_or_digest.contains(':') {
                entry.insert("digest".to_string(), json!(version_or_digest));
            } else {
                entry.insert("version".to_string(), json!(version_or_digest));
            }
            entry.insert("locator".to_string(), json!(locator));
            packs.push(serde_json::Value::Object(entry));
        }
        let main_pack = packs
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("gtbind {} has no packs", binding.tenant))?;
        let overlays = packs.into_iter().skip(1).collect::<Vec<_>>();
        entries.insert(
            binding.tenant.clone(),
            json!({
                "main_pack": main_pack,
                "overlays": overlays,
            }),
        );
    }

    let index_path = paths.greentic_root.join("packs").join("gtbind.index.json");
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let serialized = serde_json::to_vec_pretty(&serde_json::Value::Object(entries))?;
    fs::write(&index_path, serialized)
        .with_context(|| format!("failed to write {}", index_path.display()))?;
    pack.index_location = runner_core::env::IndexLocation::File(index_path);
    Ok(())
}

/// Parse the `PORT` value; an unset variable yields the default, an invalid
/// one is an error instead of a silent fallback.
/// An explicit port wins and the environment is never consulted.
fn resolve_port(explicit: Option<u16>, env: impl FnOnce() -> Option<String>) -> Result<u16> {
    match explicit {
        Some(port) => Ok(port),
        None => parse_port(env()),
    }
}

/// Unset, empty and whitespace-only `PORT` all mean the default (8080).
fn parse_port(value: Option<String>) -> Result<u16> {
    match value.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
        None => Ok(8080),
        Some(raw) => raw
            .parse::<u16>()
            .map_err(|err| anyhow!("invalid PORT `{raw}`: {err}")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminTokenSource {
    Env,
    Config,
    None,
}

/// `ADMIN_TOKEN` wins over the `x-admin-token` header from config.
fn resolve_admin_token(
    env_token: Option<String>,
    config_token: Option<String>,
) -> (Option<String>, AdminTokenSource) {
    if let Some(token) = env_token.filter(|value| !value.is_empty()) {
        return (Some(token), AdminTokenSource::Env);
    }
    if let Some(token) = config_token.filter(|value| !value.is_empty()) {
        return (Some(token), AdminTokenSource::Config);
    }
    (None, AdminTokenSource::None)
}

/// Resolve when the process receives ctrl-c or (on unix) SIGTERM.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "failed to install SIGTERM handler; using ctrl-c only");
                let _ = signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}

/// Whether strict pack verification is requested via config or environment.
fn pack_policy_from(resolved: &ResolvedConfig) -> runner_core::PackPolicy {
    let require_signatures = resolved
        .config
        .packs
        .as_ref()
        .and_then(|packs| packs.trust.as_ref())
        .map(|trust| trust.require_signatures)
        .unwrap_or(false);
    let policy = runner_core::PackPolicy::from_env();
    let strict = policy.verify_strict || require_signatures;
    policy.strict(strict)
}

fn parse_refresh_interval(value: Option<String>) -> Result<Duration> {
    let raw = value.unwrap_or_else(|| "30s".into());
    humantime::parse_duration(&raw).map_err(|err| anyhow!("invalid PACK_REFRESH_INTERVAL: {err}"))
}

fn default_wasi_policy(paths: &PathsConfig) -> RunnerWasiPolicy {
    let mut policy = RunnerWasiPolicy::default()
        .with_env("GREENTIC_ROOT", paths.greentic_root.display().to_string())
        .with_env("GREENTIC_STATE_DIR", paths.state_dir.display().to_string())
        .with_env("GREENTIC_CACHE_DIR", paths.cache_dir.display().to_string())
        .with_env("GREENTIC_LOGS_DIR", paths.logs_dir.display().to_string());
    policy = policy
        .with_preopen(PreopenSpec::new(&paths.state_dir, "/state"))
        .with_preopen(PreopenSpec::new(&paths.cache_dir, "/cache"))
        .with_preopen(PreopenSpec::new(&paths.logs_dir, "/logs"));
    policy
}

fn ensure_paths_exist(paths: &PathsConfig) -> Result<()> {
    for dir in [
        &paths.greentic_root,
        &paths.state_dir,
        &paths.cache_dir,
        &paths.logs_dir,
    ] {
        fs::create_dir_all(dir)
            .with_context(|| format!("failed to ensure directory {}", dir.display()))?;
    }
    Ok(())
}

fn pack_config_from(
    packs: &Option<PacksConfig>,
    paths: &PathsConfig,
    network: &NetworkConfig,
) -> Result<PackConfig> {
    if let Some(cfg) = packs {
        let cache_dir = cfg.cache_dir.clone();
        let index_location = match &cfg.source {
            PackSourceConfig::LocalIndex { path } => {
                runner_core::env::IndexLocation::File(path.clone())
            }
            PackSourceConfig::HttpIndex { url } => {
                runner_core::env::IndexLocation::from_value(url)?
            }
            PackSourceConfig::OciRegistry { reference } => {
                anyhow::bail!(
                    "OCI registry pack sources are not supported (reference `{reference}`); \
                     use a local or HTTP(S) index"
                )
            }
        };
        let public_key = runner_core::join_public_keys(
            cfg.trust.as_ref().map(|trust| trust.public_keys.as_slice()),
        );
        return Ok(PackConfig {
            source: runner_core::env::PackSource::Fs,
            index_location,
            cache_dir,
            public_key,
            network: Some(network.clone()),
        });
    }
    let mut cfg = PackConfig::default_for_paths(paths)?;
    cfg.network = Some(network.clone());
    Ok(cfg)
}

fn telemetry_from(cfg: &TelemetryConfig) -> Option<TelemetryCfg> {
    telemetry_from_env(
        cfg,
        std::env::var("OTLP_HEADERS").ok(),
        std::env::var("OTEL_SERVICE_NAME").ok(),
    )
}

/// The pure half of [`telemetry_from`]: the two environment variables are
/// parameters so this can be tested without mutating process-wide state
/// (`std::env::set_var` is `unsafe` under edition 2024, and this crate denies
/// `unsafe_code`).
fn telemetry_from_env(
    cfg: &TelemetryConfig,
    otlp_headers: Option<String>,
    service_name: Option<String>,
) -> Option<TelemetryCfg> {
    if !cfg.enabled || matches!(cfg.exporter, TelemetryExporterKind::None) {
        return None;
    }
    let mut export = TelemetryExportConfig::json_default();
    export.mode = match cfg.exporter {
        TelemetryExporterKind::Otlp => ExportMode::OtlpGrpc,
        TelemetryExporterKind::Stdout => ExportMode::JsonStdout,
        TelemetryExporterKind::Gcp => ExportMode::GcpCloudTrace,
        TelemetryExporterKind::Azure => ExportMode::AzureAppInsights,
        TelemetryExporterKind::Aws => ExportMode::AwsXRay,
        TelemetryExporterKind::None => return None,
    };
    export.endpoint = cfg.endpoint.clone();
    export.sampling = Sampling::TraceIdRatio(cfg.sampling as f64);

    // `OTLP_HEADERS` is how an authenticated collector is reached — Honeycomb,
    // Grafana Cloud and most hosted OTLP endpoints need an auth header, and
    // without one they reject every export.
    //
    // This config is built from `json_default()`, which starts with no headers,
    // and only `ExportConfig::from_env()` reads that variable — a function this
    // crate never calls. So the header was silently dropped and no exporter
    // that needs one could work at all. Callers that resolve a credential and
    // set the variable (greentic-designer resolves one through its admin
    // secret broker on every deploy) were doing that work for nothing.
    //
    // `from_env()` is deliberately NOT used here: it also runs preset detection
    // and would overwrite the mode, endpoint and sampling this function has
    // just derived from the operator's own configuration. The public
    // `parse_headers_from_env` is the same parser without that side effect.
    match greentic_telemetry::presets::parse_headers_from_env(otlp_headers) {
        Ok(headers) => {
            if !headers.is_empty() {
                export.headers = headers;
            }
        }
        // Malformed input must degrade to unauthenticated export rather than
        // killing telemetry outright, and it must say so: an exporter that
        // silently sends nothing is the failure this whole change is about.
        // The error names the offending ENTRY, never its value.
        Err(err) => {
            tracing::warn!(
                error = %err,
                "OTLP_HEADERS could not be parsed; exporting without headers"
            );
        }
    }

    Some(TelemetryCfg {
        config: greentic_telemetry::TelemetryConfig {
            // `OTEL_SERVICE_NAME` is the standard way to name a service, and a
            // hardcoded value here silently defeated it: the SDK's own
            // `SdkProvidedResourceDetector` reads that variable, but
            // `with_service_name` is applied afterwards and merges
            // last-writer-wins, so setting it had no effect and produced no
            // error to notice. Several runners deployed side by side were
            // therefore indistinguishable in the collector.
            //
            // The literal stays as the fallback, so nothing changes for a
            // deployment that does not set it.
            service_name: service_name
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "greentic-runner".into()),
        },
        export,
    })
}

/// Spawn the in-process agentic-worker NATS service once, when opted in.
///
/// Gated on `GREENTIC_AGENTIC_SERVE_INPROC` (truthy) AND `GREENTIC_EVENTS_NATS_URL`
/// (set) via [`runner::agent_node::should_serve_agentic_inproc`]. Loads
/// process-level base agent configs from `GREENTIC_AGENT_MANIFESTS_DIR`
/// (`<agent_id>.json` full [`greentic_aw_runtime::AgentConfig`] files) — the only
/// process-level agent source, since pack-embedded and per-tenant `HostConfig`
/// agents only exist inside `TenantRuntime::from_packs`.
///
/// Skips with a warning (continuing normal startup) when no agents are
/// configured; [`serve_agentic`] itself further degrades gracefully when the
/// runtime cannot be built (an explicit redis backend with no URL, an
/// unreachable Redis, or a failed extension runtime; a missing
/// `GREENTIC_AW_REDIS_URL` alone selects the in-memory store). The spawned
/// task owns the subscriber for the lifetime of the process.
#[cfg(feature = "agentic-worker")]
fn maybe_spawn_inproc_agentic_serve() {
    use crate::runner::agent_node::{
        load_process_agent_configs, serve_agentic, should_serve_agentic_inproc,
    };

    if !should_serve_agentic_inproc(|key| std::env::var(key).ok()) {
        return;
    }

    // Both env vars are guaranteed present/non-empty by the gate above.
    let nats_url = std::env::var("GREENTIC_EVENTS_NATS_URL").unwrap_or_default();
    let agents = load_process_agent_configs();
    if agents.is_empty() {
        tracing::warn!(
            "GREENTIC_AGENTIC_SERVE_INPROC set but no process-level agents found in \
             GREENTIC_AGENT_MANIFESTS_DIR; in-process agentic serve skipped"
        );
        return;
    }

    let agent_count = agents.len();
    tracing::info!(
        agent_count,
        nats_url = %greentic_aw_runtime::url_redact::redact_url(&nats_url),
        "starting in-process agentic serve (GREENTIC_AGENTIC_SERVE_INPROC)"
    );
    tokio::spawn(async move {
        if let Err(error) = serve_agentic(&nats_url, agents).await {
            tracing::warn!(error = %error, "in-process agentic serve stopped with error");
        }
    });
}

/// Run the unified Greentic runner host until shutdown.
pub async fn run(cfg: RunnerConfig) -> Result<()> {
    let RunnerConfig {
        tenant_bindings,
        pack,
        port,
        refresh_interval,
        routing,
        admin,
        telemetry,
        secrets_backend,
        wasi_policy,
        resolved_config,
        trace,
        validation,
    } = cfg;
    let pack_policy = pack_policy_from(&resolved_config);

    let mut builder = HostBuilder::new();
    for bindings in tenant_bindings.into_values() {
        let mut host_config = HostConfig::from_gtbind(bindings);
        // Fail fast on tenant ids / GREENTIC_ENV that are not valid identifiers
        // rather than silently running scoped lookups as `local`.
        host_config.try_tenant_ctx().with_context(|| {
            format!("invalid tenant configuration for `{}`", host_config.tenant)
        })?;
        host_config.trace = trace.clone();
        host_config.validation = validation.clone();
        builder = builder.with_config(host_config);
    }
    if let Some(telemetry) = telemetry.clone() {
        builder = builder.with_telemetry(telemetry);
    }
    // The standalone runner (the `greentic-runner` binary, also what the
    // designer's Test chat sidecar runs) is a single-tenant process, so it may
    // read the artifact door from GREENTIC_ARTIFACT_ENDPOINT / _TOKEN. A run
    // bound to several tenants still gets none (the host's tenant guard).
    #[cfg(feature = "agentic-worker")]
    {
        builder = builder.with_artifact_env_fallback(true);
    }
    builder = builder
        .with_wasi_policy(wasi_policy.clone())
        .with_secrets_manager(
            secrets_backend
                .build_manager()
                .context("failed to initialise secrets backend")?,
        );

    let host = Arc::new(builder.build()?);
    host.start().await?;

    // Opt-in, once-per-process co-host of the agentic-worker NATS service.
    // Default OFF: distributed deploys run a standalone `aw-serve` so the
    // service scales independently. When enabled, this spawns a SINGLE
    // subscriber for the whole process (NOT per-tenant — multiple subscribers
    // on `greentic.agentic.request.v1` would each handle every request).
    #[cfg(feature = "agentic-worker")]
    maybe_spawn_inproc_agentic_serve();

    let (watcher, reload_handle) = watcher::start_pack_watcher_with_policy(
        Arc::clone(&host),
        pack.clone(),
        pack_policy,
        refresh_interval,
    )
    .await?;

    let routing = TenantRouting::new(routing.clone());
    let server = HostServer::new(
        port,
        host.active_packs(),
        routing,
        host.health_state(),
        Some(reload_handle),
        admin.clone(),
        Arc::clone(&host),
    )?;

    tokio::select! {
        result = server.serve() => {
            result?;
        }
        _ = shutdown_signal() => {
            tracing::info!("received shutdown signal");
        }
    }

    drop(watcher);
    host.stop().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gtbind::{PackBinding, TenantBindings};
    use greentic_config_types::PackTrustConfig;
    use tempfile::TempDir;

    fn paths(temp: &TempDir) -> PathsConfig {
        PathsConfig {
            greentic_root: temp.path().join("greentic"),
            state_dir: temp.path().join("state"),
            cache_dir: temp.path().join("cache"),
            logs_dir: temp.path().join("logs"),
        }
    }

    #[test]
    fn parse_port_defaults_and_rejects_invalid_values() {
        assert_eq!(parse_port(None).expect("default"), 8080);
        assert_eq!(parse_port(Some("9090".into())).expect("valid"), 9090);
        assert_eq!(parse_port(Some(String::new())).expect("empty"), 8080);
        assert_eq!(parse_port(Some("  ".into())).expect("blank"), 8080);
        assert!(parse_port(Some("not-a-port".into())).is_err());
        assert!(parse_port(Some("70000".into())).is_err());
    }

    #[test]
    fn explicit_port_skips_the_port_env_entirely() {
        let port = resolve_port(Some(9000), || Some("garbage".into())).expect("explicit wins");
        assert_eq!(port, 9000);
        assert!(resolve_port(None, || Some("garbage".into())).is_err());
        assert_eq!(resolve_port(None, || None).expect("default"), 8080);
    }

    #[test]
    fn admin_token_prefers_env_then_config() {
        let (token, source) = resolve_admin_token(Some("env".into()), Some("cfg".into()));
        assert_eq!(
            (token.as_deref(), source),
            (Some("env"), AdminTokenSource::Env)
        );
        let (token, source) = resolve_admin_token(Some(String::new()), Some("cfg".into()));
        assert_eq!(
            (token.as_deref(), source),
            (Some("cfg"), AdminTokenSource::Config)
        );
        let (token, source) = resolve_admin_token(None, None);
        assert_eq!((token, source), (None, AdminTokenSource::None));
    }

    #[test]
    fn parse_refresh_interval_uses_default_and_rejects_invalid_values() {
        assert_eq!(
            parse_refresh_interval(None).expect("default interval"),
            Duration::from_secs(30)
        );
        assert!(parse_refresh_interval(Some("not-a-duration".into())).is_err());
    }

    #[test]
    fn ensure_paths_exist_creates_expected_directories() {
        let temp = TempDir::new().expect("tempdir");
        let paths = paths(&temp);
        ensure_paths_exist(&paths).expect("create directories");

        assert!(paths.greentic_root.is_dir());
        assert!(paths.state_dir.is_dir());
        assert!(paths.cache_dir.is_dir());
        assert!(paths.logs_dir.is_dir());
    }

    #[test]
    fn maybe_write_gtbind_index_writes_locator_index() {
        let temp = TempDir::new().expect("tempdir");
        let paths = paths(&temp);
        fs::create_dir_all(&paths.greentic_root).expect("greentic root");
        let mut pack = PackConfig::default_for_paths(&paths).expect("pack config");
        let tenant_bindings = HashMap::from([(
            "demo".to_string(),
            TenantBindings {
                tenant: "demo".into(),
                packs: vec![
                    PackBinding {
                        pack_id: "pack.main".into(),
                        pack_ref: "pack.main@1.0.0".into(),
                        pack_locator: Some("fs:///packs/main.gtpack".into()),
                        flows: vec!["main".into()],
                    },
                    PackBinding {
                        pack_id: "pack.overlay".into(),
                        pack_ref: "pack.overlay@sha256:abcd".into(),
                        pack_locator: Some("fs:///packs/overlay.gtpack".into()),
                        flows: vec![],
                    },
                ],
                env_passthrough: vec![],
            },
        )]);

        maybe_write_gtbind_index(&tenant_bindings, &paths, &mut pack).expect("write gtbind index");

        let index_path = paths.greentic_root.join("packs").join("gtbind.index.json");
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(&index_path).expect("read index")).expect("json");
        assert_eq!(
            json["demo"]["main_pack"]["locator"],
            "fs:///packs/main.gtpack"
        );
        assert_eq!(json["demo"]["overlays"][0]["digest"], "sha256:abcd");
        match pack.index_location {
            runner_core::env::IndexLocation::File(path) => assert_eq!(path, index_path),
            runner_core::env::IndexLocation::Remote(_) => panic!("expected generated file index"),
        }
    }

    #[test]
    fn pack_config_from_prefers_configured_pack_source() {
        let temp = TempDir::new().expect("tempdir");
        let paths = paths(&temp);
        let packs = Some(PacksConfig {
            source: PackSourceConfig::HttpIndex {
                url: "https://example.com/index.json".into(),
            },
            cache_dir: temp.path().join("packs-cache"),
            index_cache_ttl_secs: None,
            trust: Some(PackTrustConfig {
                public_keys: vec!["ed25519:test".into()],
                require_signatures: true,
            }),
        });
        let config = pack_config_from(&packs, &paths, &NetworkConfig::default())
            .expect("pack config from packs");

        match config.index_location {
            runner_core::env::IndexLocation::Remote(url) => {
                assert_eq!(url.as_str(), "https://example.com/index.json");
            }
            runner_core::env::IndexLocation::File(_) => panic!("expected remote index"),
        }
        assert_eq!(config.public_key.as_deref(), Some("ed25519:test"));
        assert!(config.network.is_some());
    }

    #[test]
    fn default_wasi_policy_sets_expected_env_and_preopens() {
        let temp = TempDir::new().expect("tempdir");
        let paths = paths(&temp);
        let policy = default_wasi_policy(&paths);

        assert_eq!(
            policy.env_set.get("GREENTIC_ROOT"),
            Some(&paths.greentic_root.display().to_string())
        );
        assert_eq!(policy.preopens.len(), 3);
        assert_eq!(policy.preopens[0].guest_path, "/state");
        assert_eq!(policy.preopens[1].guest_path, "/cache");
        assert_eq!(policy.preopens[2].guest_path, "/logs");
    }

    #[test]
    fn telemetry_from_is_disabled_without_exporter() {
        assert!(telemetry_from(&TelemetryConfig::default()).is_none());
    }

    #[test]
    fn telemetry_from_env_honours_the_telemetry_environment() {
        fn enabled() -> TelemetryConfig {
            TelemetryConfig {
                enabled: true,
                exporter: TelemetryExporterKind::Otlp,
                ..Default::default()
            }
        }

        // Neither variable set: a deployment that configures nothing must
        // behave exactly as it did before this change.
        let bare = telemetry_from_env(&enabled(), None, None).expect("enabled yields a config");
        assert!(bare.export.headers.is_empty());
        assert_eq!(bare.config.service_name, "greentic-runner");

        let set = telemetry_from_env(
            &enabled(),
            Some("authorization=Bearer tok,x-team=alpha".into()),
            Some("greentic-designer-worker".into()),
        )
        .expect("enabled yields a config");
        assert_eq!(
            set.export.headers.get("authorization").map(String::as_str),
            Some("Bearer tok"),
            "an authenticated collector cannot be reached without this header"
        );
        assert_eq!(
            set.export.headers.get("x-team").map(String::as_str),
            Some("alpha")
        );
        assert_eq!(set.config.service_name, "greentic-designer-worker");

        // A blank name falls back rather than registering an unnamed service.
        let blank = telemetry_from_env(&enabled(), None, Some("   ".into()))
            .expect("enabled yields a config");
        assert_eq!(blank.config.service_name, "greentic-runner");

        // Malformed headers degrade to unauthenticated export — never a panic,
        // and never telemetry dropped entirely.
        let malformed =
            telemetry_from_env(&enabled(), Some("this-has-no-equals-sign".into()), None)
                .expect("malformed headers must not disable telemetry");
        assert!(malformed.export.headers.is_empty());
    }
}
