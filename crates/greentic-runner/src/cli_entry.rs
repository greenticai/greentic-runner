//! The `greentic-runner` command line, as a library function so that a binary
//! can register agent-runtime extensions before running it (see
//! [`crate::cli_main`]). Moved verbatim from `main.rs`.

use crate::cli;
use crate::info;
use clap::{Parser, Subcommand, ValueEnum};
use greentic_config::{ConfigFileFormat, ConfigLayer, ConfigResolver};
use greentic_runner_host::cache::{
    ArtifactKey, CacheConfig, CacheManager, CpuPolicy, EngineProfile,
};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::PackRuntime;
use greentic_runner_host::secrets::default_manager;
use greentic_runner_host::storage::{new_session_store, new_state_store};
use greentic_runner_host::trace::{TraceConfig, TraceMode};
use greentic_runner_host::validate::{ValidationConfig, ValidationMode};
use greentic_runner_host::{RunnerConfig, RunnerWasiPolicy, run as run_host};
use greentic_types::ComponentSourceRef;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs as async_fs;

use anyhow::{Context, Result, bail};
use greentic_distributor_client::dist::{CachePolicy, DistClient, DistOptions, ResolvePolicy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Debug, Parser)]
#[command(name = "greentic-runner", version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    run: RunArgs,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(subcommand)]
    Cache(CacheCommand),
    Replay(cli::replay::ReplayArgs),
    Conformance(cli::conformance::ConformanceArgs),
    Contract(ContractArgs),
    /// Describe this runner binary: version, supported imports, features.
    Info {
        /// Emit the info report as JSON.
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Parser)]
struct ContractArgs {
    /// Pack or component path for authoritative WASM describe()
    #[arg(long, value_name = "PATH")]
    pack: Option<PathBuf>,

    /// Component id/reference from the pack manifest
    #[arg(long, value_name = "ID")]
    component: Option<String>,

    /// Operation id (defaults to run)
    #[arg(long, default_value = "run")]
    operation: String,

    /// Read describe artifact from file (CBOR or JSON)
    #[arg(long = "describe", alias = "contract", value_name = "PATH")]
    describe_path: Option<PathBuf>,

    /// Write describe artifact as canonical CBOR
    #[arg(long, value_name = "PATH")]
    emit_describe: Option<PathBuf>,

    /// Print resolved contract as stable JSON
    #[arg(long)]
    print_contract: bool,

    /// Allow unverified artifact-only inspection
    #[arg(long)]
    no_verify: bool,

    /// Explicit non-execution mode gate for artifact-only inspection
    #[arg(long)]
    validate_only: bool,
}

#[derive(Debug, Subcommand)]
enum CacheCommand {
    Warmup(CacheWarmupArgs),
    Doctor,
    Prune(CachePruneArgs),
}

#[derive(Debug, Parser)]
struct CacheWarmupArgs {
    /// Path to a pack.lock/pack.lock.json or pack.yaml
    #[arg(long, value_name = "PATH")]
    pack: PathBuf,

    /// Warmup mode
    #[arg(long, value_enum, default_value = "disk")]
    mode: CacheWarmupMode,
}

#[derive(Debug, Parser)]
struct CachePruneArgs {
    /// Report prune result without deleting artifacts
    #[arg(long)]
    dry_run: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum CacheWarmupMode {
    Disk,
    Memory,
}

#[derive(Debug, Parser)]
struct RunArgs {
    /// Optional path to a greentic config file (toml/json). Overrides project discovery.
    #[arg(long = "config", value_name = "PATH")]
    config: Option<PathBuf>,

    /// Allow dev-only settings in the config (use with caution in prod).
    #[arg(long = "allow-dev")]
    allow_dev: bool,

    /// Print the resolved config and exit.
    #[arg(long = "config-explain")]
    config_explain: bool,

    /// Pack bindings file or directory containing *.gtbind (repeatable)
    #[arg(long = "bindings", value_name = "PATH")]
    bindings: Vec<PathBuf>,

    /// Directory containing *.gtbind files (repeatable)
    #[arg(long = "bindings-dir", value_name = "DIR")]
    bindings_dir: Vec<PathBuf>,

    /// Port to serve the HTTP server on (default 8080)
    #[arg(long, default_value = "8080")]
    port: u16,

    /// Disable the component compilation cache
    #[arg(long)]
    no_cache: bool,

    /// Emit JSON errors on failure
    #[arg(long)]
    json: bool,

    /// Trace output path (default: trace.json)
    #[arg(long = "trace-out", value_name = "PATH")]
    trace_out: Option<PathBuf>,

    /// Trace emission mode
    #[arg(long = "trace", value_enum, default_value = "on")]
    trace: TraceArg,

    /// Capture invocation inputs into trace.json (default off)
    #[arg(long = "trace-capture-inputs", value_enum, default_value = "off")]
    trace_capture_inputs: TraceCaptureArg,

    /// Invocation envelope validation mode
    #[arg(long = "validation", value_enum)]
    validation: Option<ValidationArg>,

    /// Preferred locale override (highest precedence for diagnostics)
    #[arg(long = "locale", value_name = "LOCALE")]
    locale: Option<String>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum TraceArg {
    Off,
    On,
    Always,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum TraceCaptureArg {
    Off,
    On,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ValidationArg {
    Off,
    Warn,
    Error,
}

#[derive(Debug, Clone, Serialize)]
struct ContractReport {
    selected_operation: String,
    input_schema: Value,
    output_schema: Value,
    config_schema: Value,
    describe_hash: String,
    schema_hash: String,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypedDescribe {
    #[serde(default)]
    operations: Vec<TypedOperation>,
    #[serde(default)]
    config_schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TypedSchemaSide {
    #[serde(default)]
    schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypedOperation {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: TypedSchemaSide,
    #[serde(default)]
    output: TypedSchemaSide,
    #[serde(default)]
    input_schema: Option<Value>,
    #[serde(default)]
    output_schema: Option<Value>,
}

#[derive(Serialize)]
struct ContractDescribeHashMaterial<'a> {
    describe: &'a Value,
}

#[derive(Serialize)]
struct ContractSchemaHashMaterial<'a> {
    input_schema: &'a Value,
    output_schema: &'a Value,
    config_schema: &'a Value,
}

/// Parse the process arguments and run the stock CLI. On failure it reports
/// the error and exits the process with status 1, exactly as the binary did.
pub(crate) async fn run() {
    let cli = Cli::parse();
    let json_output = cli.run.json;
    if let Err(err) = run_with_cli(cli).await {
        if json_output {
            emit_json_error(&err);
        } else {
            tracing::error!(error = %err, "runner failed");
            eprintln!("error: {err}");
        }
        std::process::exit(1);
    }
}

// The two `set_var` calls below are the only unsafe code in this crate; each
// carries its own SAFETY note. The lib denies `unsafe_code` everywhere else.
#[allow(unsafe_code)]
async fn run_with_cli(cli: Cli) -> anyhow::Result<()> {
    if let Some(locale) = cli.run.locale.as_deref() {
        // SAFETY: process-local override for locale selection precedence.
        unsafe {
            std::env::set_var("GREENTIC_LOCALE_CLI", locale);
        }
    }
    if let Some(command) = cli.command {
        return match command {
            Command::Cache(cmd) => run_cache(cmd).await,
            Command::Replay(args) => cli::replay::run(args).await,
            Command::Conformance(args) => cli::conformance::run(args).await,
            Command::Contract(args) => run_contract(args).await,
            Command::Info { json } => {
                let report = info::collect();
                if json {
                    let pretty = serde_json::to_string_pretty(&report)
                        .context("serialize info report as JSON")?;
                    println!("{pretty}");
                } else {
                    print!("{}", info::human::render(&report));
                }
                Ok(())
            }
        };
    }
    let run = cli.run;
    if run.no_cache {
        // SAFETY: toggling the cache behavior is scoped to this process.
        unsafe {
            std::env::set_var("GREENTIC_NO_CACHE", "1");
        }
    }
    if run.bindings.is_empty() && run.bindings_dir.is_empty() {
        bail!("at least one --bindings path is required");
    }
    let (resolver, _) = build_resolver(run.config.as_deref(), run.allow_dev)?;
    let resolved = resolver.load()?;
    if run.config_explain {
        let report =
            greentic_config::explain(&resolved.config, &resolved.provenance, &resolved.warnings);
        println!("{}", report.text);
        return Ok(());
    }
    let bindings =
        greentic_runner_host::gtbind::collect_gtbind_paths(&run.bindings, &run.bindings_dir)?;
    let trace_out = std::env::var_os("GREENTIC_TRACE_OUT").map(PathBuf::from);
    let trace_config = TraceConfig::from_env()
        .with_overrides(
            match run.trace {
                TraceArg::Off => TraceMode::Off,
                TraceArg::On => TraceMode::On,
                TraceArg::Always => TraceMode::Always,
            },
            trace_out.or(run.trace_out.clone()),
        )
        .with_capture_inputs(matches!(run.trace_capture_inputs, TraceCaptureArg::On));
    let validation_mode = run.validation.map(|value| match value {
        ValidationArg::Off => ValidationMode::Off,
        ValidationArg::Warn => ValidationMode::Warn,
        ValidationArg::Error => ValidationMode::Error,
    });
    let validation_config = validation_mode
        .map(|mode| ValidationConfig::from_env().with_mode(mode))
        .unwrap_or_else(ValidationConfig::from_env);
    let mut cfg = RunnerConfig::from_config(resolved, bindings)?.with_port(run.port);
    cfg.trace = trace_config;
    cfg.validation = validation_config;
    run_host(cfg).await
}

fn emit_json_error(err: &anyhow::Error) {
    let chain = err
        .chain()
        .skip(1)
        .map(|source| source.to_string())
        .collect::<Vec<_>>();
    let payload = json!({
        "error": {
            "message": err.to_string(),
            "chain": chain,
        }
    });
    println!("{}", payload);
}

fn build_resolver(
    config_path: Option<&Path>,
    allow_dev: bool,
) -> anyhow::Result<(ConfigResolver, ConfigLayer)> {
    let mut resolver = ConfigResolver::new();
    if allow_dev {
        resolver = resolver.allow_dev(true);
    }
    let layer = if let Some(path) = config_path {
        let format = match path.extension().and_then(|ext| ext.to_str()) {
            Some("json") => ConfigFileFormat::Json,
            _ => ConfigFileFormat::Toml,
        };
        let contents = std::fs::read_to_string(path)?;
        let layer = match format {
            ConfigFileFormat::Toml => toml::from_str(&contents)?,
            ConfigFileFormat::Json => serde_json::from_str(&contents)?,
        };
        if let Some(parent) = path.parent() {
            resolver = resolver.with_project_root_opt(Some(parent.to_path_buf()));
        }
        layer
    } else {
        ConfigLayer::default()
    };
    resolver = resolver.with_cli_overrides(layer.clone());
    Ok((resolver, layer))
}

#[derive(Debug, Deserialize)]
struct PackLockV1 {
    schema_version: u32,
    components: Vec<PackLockComponent>,
}

#[derive(Debug, Deserialize)]
struct PackLockComponent {
    name: String,
    #[serde(default, rename = "source_ref")]
    source_ref: Option<String>,
    #[serde(default, rename = "ref")]
    legacy_ref: Option<String>,
    #[serde(default)]
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

impl PackLockComponent {
    fn source_ref(&self) -> Result<&str> {
        match (&self.source_ref, &self.legacy_ref) {
            (Some(primary), Some(legacy)) => {
                if primary != legacy {
                    bail!(
                        "pack.lock component {} has conflicting refs: {} vs {}",
                        self.name,
                        primary,
                        legacy
                    );
                }
                Ok(primary.as_str())
            }
            (Some(primary), None) => Ok(primary.as_str()),
            (None, Some(legacy)) => Ok(legacy.as_str()),
            (None, None) => bail!("pack.lock component {} missing source_ref", self.name),
        }
    }

    fn bundled_path(&self) -> Option<&str> {
        match (&self.bundled_path, &self.legacy_path) {
            (Some(primary), Some(legacy)) if primary == legacy => Some(primary.as_str()),
            (Some(primary), None) => Some(primary.as_str()),
            (None, Some(legacy)) => Some(legacy.as_str()),
            _ => None,
        }
    }

    fn wasm_digest(&self) -> Option<String> {
        match (&self.wasm_sha256, &self.legacy_sha256) {
            (Some(primary), Some(legacy)) if primary == legacy => Some(primary.clone()),
            (Some(primary), None) => Some(primary.clone()),
            (None, Some(legacy)) => Some(legacy.clone()),
            _ => None,
        }
    }
}

async fn run_cache(cmd: CacheCommand) -> Result<()> {
    match cmd {
        CacheCommand::Warmup(args) => warmup_cache(args).await,
        CacheCommand::Doctor => doctor_cache().await,
        CacheCommand::Prune(args) => prune_cache(args).await,
    }
}

async fn warmup_cache(args: CacheWarmupArgs) -> Result<()> {
    let (root, lock) = read_pack_lock(&args.pack).await?;
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let config = CacheConfig {
        memory_enabled: matches!(args.mode, CacheWarmupMode::Memory),
        ..CacheConfig::default()
    };
    let cache = CacheManager::new(config, profile);
    let dist_opts = DistOptions {
        allow_tags: true,
        ..DistOptions::default()
    };
    let dist_client = DistClient::new(dist_opts);

    for entry in lock.components {
        let source_ref = entry.source_ref()?;
        let wasm_digest = entry
            .wasm_digest()
            .or_else(|| entry.resolved_digest.clone())
            .or_else(|| entry.digest.clone())
            .ok_or_else(|| anyhow::anyhow!("pack.lock component {} missing digest", entry.name))?;
        let wasm_digest = normalize_digest(&wasm_digest);
        let key = ArtifactKey::new(cache.engine_profile_id().to_string(), wasm_digest);
        let bytes = resolve_component_bytes(&root, &entry, source_ref, &dist_client).await?;
        let _ = cache
            .get_component(&engine, &key, || Ok(bytes))
            .await
            .with_context(|| format!("failed to warm component {}", entry.name))?;
    }
    Ok(())
}

async fn doctor_cache() -> Result<()> {
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let cache = CacheManager::new(CacheConfig::default(), profile);
    let metrics = cache.metrics();
    let memory = cache.memory_stats();
    let disk = cache.disk_stats()?;

    println!("engine_profile_id: {}", cache.engine_profile_id());
    println!(
        "memory: entries={} bytes={} hits={} misses={}",
        memory.entries, memory.total_bytes, memory.hits, memory.misses
    );
    println!(
        "disk: artifacts={} bytes={} reads={} hits={}",
        disk.artifact_count, disk.artifact_bytes, metrics.disk_reads, metrics.disk_hits
    );
    Ok(())
}

async fn prune_cache(args: CachePruneArgs) -> Result<()> {
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let cache = CacheManager::new(CacheConfig::default(), profile);
    let report = cache.prune_disk(args.dry_run).await?;
    if args.dry_run {
        println!(
            "prune dry-run: would remove {} entries ({} bytes)",
            report.removed_entries, report.removed_bytes
        );
    } else {
        println!(
            "prune: removed {} entries ({} bytes)",
            report.removed_entries, report.removed_bytes
        );
    }
    Ok(())
}

async fn read_pack_lock(path: &Path) -> Result<(PathBuf, PackLockV1)> {
    let lock_path = if path.is_dir() {
        pick_lock_path(path)
            .ok_or_else(|| anyhow::anyhow!("pack.lock not found in {}", path.display()))?
    } else if is_pack_lock(path) {
        path.to_path_buf()
    } else if is_pack_yaml(path) {
        let root = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("pack.yaml has no parent directory"))?;
        pick_lock_path(root)
            .ok_or_else(|| anyhow::anyhow!("pack.lock not found in {}", root.display()))?
    } else {
        bail!("unsupported pack path {}", path.display());
    };
    let root = lock_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("pack.lock has no parent directory"))?
        .to_path_buf();
    let raw = async_fs::read_to_string(&lock_path)
        .await
        .with_context(|| format!("failed to read {}", lock_path.display()))?;
    let lock: PackLockV1 = serde_json::from_str(&raw).context("failed to parse pack.lock")?;
    if lock.schema_version != 1 {
        bail!("pack.lock schema_version must be 1");
    }
    Ok((root, lock))
}

fn pick_lock_path(root: &Path) -> Option<PathBuf> {
    let candidate = root.join("pack.lock");
    if candidate.exists() {
        return Some(candidate);
    }
    let candidate = root.join("pack.lock.json");
    candidate.exists().then_some(candidate)
}

fn is_pack_lock(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some("pack.lock") | Some("pack.lock.json")
    )
}

fn is_pack_yaml(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some("pack.yaml")
    )
}

async fn resolve_component_bytes(
    root: &Path,
    entry: &PackLockComponent,
    source_ref: &str,
    dist_client: &DistClient,
) -> Result<Vec<u8>> {
    if let Some(relative) = entry.bundled_path() {
        let path = root.join(relative);
        if path.exists() {
            return async_fs::read(&path)
                .await
                .with_context(|| format!("failed to read {}", path.display()));
        }
    }

    let source: ComponentSourceRef = source_ref
        .parse()
        .with_context(|| format!("invalid component ref {}", source_ref))?;
    if !matches!(source, ComponentSourceRef::Oci(_)) {
        bail!("unsupported component source {}", source_ref);
    }

    if let Some(digest) = entry.resolved_digest.as_deref().or(entry.digest.as_deref()) {
        let cache_path = dist_client.fetch_digest(digest).await?;
        return async_fs::read(&cache_path)
            .await
            .with_context(|| format!("failed to read {}", cache_path.display()));
    }

    let source = dist_client.parse_source(source_ref)?;
    let descriptor = dist_client.resolve(source, ResolvePolicy).await?;
    let resolved = dist_client.fetch(&descriptor, CachePolicy).await?;
    let cache_path = resolved
        .cache_path
        .ok_or_else(|| anyhow::anyhow!("component {} missing cache path", entry.name))?;
    async_fs::read(&cache_path)
        .await
        .with_context(|| format!("failed to read {}", cache_path.display()))
}

fn normalize_digest(digest: &str) -> String {
    if digest.starts_with("sha256:") || digest.starts_with("blake3:") {
        digest.to_string()
    } else {
        format!("sha256:{digest}")
    }
}

async fn run_contract(args: ContractArgs) -> Result<()> {
    let operation = normalize_operation_id(&args.operation);
    let artifact_payload = if let Some(path) = args.describe_path.as_ref() {
        Some(load_describe_artifact(path)?)
    } else {
        None
    };

    if args.pack.is_none() && artifact_payload.is_some() && (!args.no_verify || !args.validate_only)
    {
        bail!(
            "artifact-only contract inspection requires explicit --no-verify and --validate-only"
        );
    }
    if args.pack.is_none() && (args.component.is_some() || args.emit_describe.is_some()) {
        bail!("--pack is required for --component and --emit-describe");
    }
    if args.pack.is_none() && artifact_payload.is_none() {
        bail!("provide either --pack (authoritative WASM) or --describe/--contract artifact path");
    }

    let report = if let Some(pack_path) = args.pack.as_ref() {
        let component = args
            .component
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--component is required when --pack is provided"))?;
        let pack = load_pack_runtime_for_contract(pack_path).await?;
        let manifest = pack.component_manifest(component).ok_or_else(|| {
            anyhow::anyhow!(
                "component `{component}` not found in pack {}",
                pack_path.display()
            )
        })?;
        if !manifest.world.contains("greentic:component@0.6.0") {
            bail!(
                "component `{}` world `{}` is not greentic:component@0.6.0",
                component,
                manifest.world
            );
        }

        let wasm_describe_value = pack
            .describe_component_contract_v0_6(component)?
            .ok_or_else(|| anyhow::anyhow!("component `{component}` has no 0.6 describe()"))?;
        let wasm_describe = parse_typed_describe_from_value(
            wasm_describe_value,
            "authoritative WASM describe payload",
        )?;

        if let Some(path) = args.emit_describe.as_ref() {
            write_canonical_describe(path, &wasm_describe)?;
        }

        if let Some(artifact) = artifact_payload.as_ref() {
            if args.no_verify {
                eprintln!(
                    "warning: artifact provided with --no-verify; using WASM describe() as authoritative"
                );
            } else {
                let artifact_hash = describe_hash(artifact)?;
                let wasm_hash = describe_hash(&wasm_describe)?;
                if artifact_hash != wasm_hash {
                    bail!(
                        "artifact describe does not match authoritative WASM describe(); rerun with --no-verify only for validate-only inspection"
                    );
                }
            }
        }

        contract_from_describe(&operation, &wasm_describe, "wasm.describe".to_string())?
    } else {
        let describe =
            artifact_payload.ok_or_else(|| anyhow::anyhow!("missing describe artifact payload"))?;
        contract_from_describe(&operation, &describe, "artifact.unverified".to_string())?
    };

    if args.print_contract || args.pack.is_none() {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if let Some(path) = args.emit_describe.as_ref() {
        println!("wrote describe artifact to {}", path.display());
    }
    Ok(())
}

async fn load_pack_runtime_for_contract(path: &Path) -> Result<PackRuntime> {
    let config = Arc::new(HostConfig {
        tenant: "contract".to_string(),
        bindings_path: PathBuf::from("<contract>"),
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
        trace: TraceConfig::from_env(),
        validation: ValidationConfig::from_env(),
        operator_policy: OperatorPolicy::allow_all(),
        fast2flow: Default::default(),
        #[cfg(feature = "agentic-worker")]
        agents: std::collections::HashMap::new(),
        #[cfg(feature = "agentic-worker")]
        graphs: std::collections::HashMap::new(),
    });
    PackRuntime::load(
        path,
        config,
        None,
        Some(path),
        Some(new_session_store()),
        Some(new_state_store()),
        Arc::new(RunnerWasiPolicy::new()),
        default_manager()?,
        None,
        false,
        greentic_runner_host::pack::ComponentResolution::default(),
    )
    .await
}

fn load_describe_artifact(path: &Path) -> Result<TypedDescribe> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    if bytes.is_empty() {
        return Ok(TypedDescribe {
            operations: Vec::new(),
            config_schema: Value::Null,
        });
    }
    if let Ok(value) = serde_cbor::from_slice::<Value>(&bytes) {
        return parse_typed_describe_from_value(value, &format!("artifact {}", path.display()));
    }
    if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
        return parse_typed_describe_from_value(value, &format!("artifact {}", path.display()));
    }
    if let Ok(text) = String::from_utf8(bytes)
        && let Ok(value) = serde_json::from_str::<Value>(&text)
    {
        return parse_typed_describe_from_value(value, &format!("artifact {}", path.display()));
    }
    bail!(
        "unsupported describe artifact encoding in {}; expected CBOR or JSON",
        path.display()
    )
}

fn write_canonical_describe(path: &Path, value: &TypedDescribe) -> Result<()> {
    let canonical = canonical_describe_value(value)?;
    let bytes = serde_cbor::ser::to_vec_packed(&canonical)?;
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))
}

fn contract_from_describe(
    operation: &str,
    payload: &TypedDescribe,
    source: String,
) -> Result<ContractReport> {
    let selected_operation = select_operation_from_describe(payload, operation)
        .ok_or_else(|| anyhow::anyhow!("operation `{operation}` not found in describe payload"))?;
    let selected_operation_entry = payload
        .operations
        .iter()
        .find(|entry| operation_name(entry) == Some(selected_operation.as_str()));
    let input_schema = selected_operation_entry
        .map(TypedOperation::input_schema_value)
        .unwrap_or(Value::Null);
    let output_schema = selected_operation_entry
        .map(TypedOperation::output_schema_value)
        .unwrap_or(Value::Null);
    let config_schema = canonicalize_json(payload.config_schema.clone());

    let describe_hash = describe_hash(payload)?;

    let schema_material = ContractSchemaHashMaterial {
        input_schema: &input_schema,
        output_schema: &output_schema,
        config_schema: &config_schema,
    };
    let schema_hash = sha256_prefixed(&serde_cbor::to_vec(&schema_material)?);

    Ok(ContractReport {
        selected_operation,
        input_schema,
        output_schema,
        config_schema,
        describe_hash,
        schema_hash,
        source,
    })
}

fn select_operation_from_describe(
    payload: &TypedDescribe,
    requested_operation: &str,
) -> Option<String> {
    let ops = &payload.operations;
    let requested = ops
        .iter()
        .find_map(operation_name)
        .filter(|name| *name == requested_operation)
        .map(ToString::to_string);
    if requested.is_some() {
        return requested;
    }
    ops.iter()
        .find_map(operation_name)
        .filter(|name| *name == "run")
        .map(ToString::to_string)
        .or_else(|| {
            ops.first()
                .and_then(operation_name)
                .map(ToString::to_string)
        })
}

fn operation_name(value: &TypedOperation) -> Option<&str> {
    value.id.as_deref().or(value.name.as_deref())
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut ordered = serde_json::Map::new();
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                let normalized = map
                    .get(&key)
                    .cloned()
                    .map(canonicalize_json)
                    .unwrap_or(Value::Null);
                ordered.insert(key, normalized);
            }
            Value::Object(ordered)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonicalize_json).collect()),
        other => other,
    }
}

fn parse_typed_describe_from_value(value: Value, source: &str) -> Result<TypedDescribe> {
    let normalized = canonicalize_json(value);
    let mut typed: TypedDescribe = serde_json::from_value(normalized)
        .with_context(|| format!("failed to decode {source} as typed describe payload"))?;
    typed.config_schema = canonicalize_json(typed.config_schema);
    for op in &mut typed.operations {
        op.input.schema = canonicalize_json(op.input.schema.clone());
        op.output.schema = canonicalize_json(op.output.schema.clone());
        op.input_schema = op.input_schema.clone().map(canonicalize_json);
        op.output_schema = op.output_schema.clone().map(canonicalize_json);
    }
    Ok(typed)
}

fn canonical_describe_value(describe: &TypedDescribe) -> Result<Value> {
    Ok(canonicalize_json(serde_json::to_value(describe)?))
}

fn describe_hash(describe: &TypedDescribe) -> Result<String> {
    let canonical = canonical_describe_value(describe)?;
    let describe_material = ContractDescribeHashMaterial {
        describe: &canonical,
    };
    Ok(sha256_prefixed(&serde_cbor::to_vec(&describe_material)?))
}

impl TypedOperation {
    fn input_schema_value(&self) -> Value {
        if !self.input.schema.is_null() {
            return self.input.schema.clone();
        }
        self.input_schema.clone().unwrap_or(Value::Null)
    }

    fn output_schema_value(&self) -> Value {
        if !self.output.schema.is_null() {
            return self.output.schema.clone();
        }
        self.output_schema.clone().unwrap_or(Value::Null)
    }
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!("sha256:{}", to_hex(&digest))
}

fn to_hex(digest: &[u8]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn normalize_operation_id(value: &str) -> String {
    let op = value.trim();
    if op.is_empty() {
        "run".to_string()
    } else {
        op.to_string()
    }
}
