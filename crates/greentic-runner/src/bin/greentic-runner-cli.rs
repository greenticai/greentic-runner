use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64_STANDARD;
use clap::{Parser, ValueEnum};
use greentic_runner::desktop::{
    DevProfile, HttpMock, HttpMockMode, MocksConfig, OtlpHook, Profile, RunOptions, Runner,
    SigningPolicy, TenantContext, ToolsMock,
};
use greentic_types::flow::{Flow, Node, Routing};
use greentic_types::{PackManifest, decode_pack_manifest};
use serde::Deserialize;
use serde_json::Value;
use serde_yaml_bw as serde_yaml;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use zip::ZipArchive;

#[derive(Debug, Parser)]
#[command(
    name = "greentic-runner-cli",
    about = "Run a Greentic pack locally using the desktop harness"
)]
struct Cli {
    /// Path to the pack (.gtpack or directory)
    #[arg(long, value_name = "PATH")]
    pack: PathBuf,

    /// Optional directory containing materialized components (`components/<id>.wasm`)
    #[arg(long, value_name = "DIR")]
    components_dir: Option<PathBuf>,

    /// JSON file mapping component id -> local wasm path (sidecar map)
    #[arg(long, value_name = "FILE")]
    components_map: Option<PathBuf>,

    /// Optional entry flow id (defaults to pack manifest)
    #[arg(long, alias = "entry")]
    flow: Option<String>,

    /// Emit compact JSON output
    #[arg(long)]
    json: bool,

    /// JSON input payload (string form)
    #[arg(long, default_value = "{}")]
    input: String,

    /// Read JSON input from a file (overrides --input)
    #[arg(long, value_name = "FILE")]
    input_file: Option<PathBuf>,

    /// Override tenant id
    #[arg(long)]
    tenant: Option<String>,

    /// Override team id
    #[arg(long)]
    team: Option<String>,

    /// Override user id
    #[arg(long)]
    user: Option<String>,

    /// Override session id
    #[arg(long)]
    session: Option<String>,

    /// Write artifacts under this directory
    #[arg(long, value_name = "DIR", alias = "artifacts")]
    artifacts_dir: Option<PathBuf>,

    /// Do not fetch remote components (requires cached artifacts)
    #[arg(long)]
    offline: bool,

    /// Override the component cache directory
    #[arg(long, value_name = "DIR")]
    cache_dir: Option<PathBuf>,

    /// Disable the component compilation cache
    #[arg(long)]
    no_cache: bool,

    /// Allow bundled components without wasm_sha256 (dev-only)
    #[arg(long)]
    allow_missing_hash: bool,

    /// Enforce strict signing (default is DevOk)
    #[arg(long)]
    strict_signing: bool,

    /// Signing policy (mutually exclusive with --strict-signing)
    #[arg(long, value_enum)]
    policy: Option<RunPolicyArg>,

    /// OTLP collector endpoint (optional)
    #[arg(long)]
    otlp: Option<String>,

    /// Comma-separated list of allowed outbound hosts
    #[arg(long = "allow")]
    allow_hosts: Option<String>,

    /// Mocks toggle
    #[arg(long, default_value = "on", value_enum)]
    mocks: MockSettingArg,

    /// Use mock executor (internal/testing)
    #[arg(long = "mock-exec", hide = true)]
    mock_exec: bool,

    /// Allow external calls in mock executor (default: false)
    #[arg(long = "allow-external", hide = true)]
    allow_external: bool,

    /// Return mocked external responses when external calls are allowed (mock exec only)
    #[arg(long = "mock-external", hide = true)]
    mock_external: bool,

    /// Path to JSON payload used for mocked external responses (mock exec only)
    #[arg(long = "mock-external-payload", value_name = "FILE", hide = true)]
    mock_external_payload: Option<PathBuf>,

    /// Secrets seed file applied to the mock secrets store (mock exec only)
    #[arg(long = "secrets-seed", value_name = "FILE", hide = true)]
    secrets_seed: Option<PathBuf>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum RunPolicyArg {
    Strict,
    Devok,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum MockSettingArg {
    On,
    Off,
}

fn parse_input(cli: &Cli) -> Result<Value> {
    if let Some(path) = &cli.input_file {
        let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        return serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse JSON from {}", path.display()));
    }
    if cli.input.trim().is_empty() {
        return Ok(serde_json::json!({}));
    }
    serde_json::from_str(&cli.input).context("failed to parse JSON from --input")
}

fn parse_components_map(cli: &Cli) -> Result<HashMap<String, PathBuf>> {
    if let Some(path) = &cli.components_map {
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read components map {}", path.display()))?;
        let raw: HashMap<String, String> = serde_json::from_slice(&bytes).with_context(|| {
            format!(
                "failed to parse components map JSON from {}",
                path.display()
            )
        })?;
        let mut map = HashMap::new();
        for (id, value) in raw {
            map.insert(id, PathBuf::from(value));
        }
        return Ok(map);
    }
    Ok(HashMap::new())
}

fn parse_allow_hosts(cli: &Cli) -> Vec<String> {
    cli.allow_hosts
        .as_deref()
        .map(|raw| {
            raw.split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn build_profile(cli: &Cli) -> Profile {
    let mut dev = DevProfile::default();
    if let Some(tenant) = &cli.tenant {
        dev.tenant_id = tenant.clone();
    }
    if let Some(team) = &cli.team {
        dev.team_id = team.clone();
    }
    if let Some(user) = &cli.user {
        dev.user_id = user.clone();
    }
    Profile::Dev(dev)
}

fn build_ctx(cli: &Cli) -> TenantContext {
    let mut ctx = TenantContext::default_local();
    if let Some(tenant) = &cli.tenant {
        ctx.tenant_id = Some(tenant.clone());
    }
    if let Some(team) = &cli.team {
        ctx.team_id = Some(team.clone());
    }
    if let Some(user) = &cli.user {
        ctx.user_id = Some(user.clone());
    }
    if let Some(session) = &cli.session {
        ctx.session_id = Some(session.clone());
    }
    ctx
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.strict_signing && cli.policy.is_some() {
        bail!("--policy and --strict-signing are mutually exclusive");
    }
    let policy = resolve_policy(cli.policy, cli.strict_signing);

    if cli.mock_exec {
        let input = parse_input(&cli)?;
        let mock_payload = match &cli.mock_external_payload {
            Some(path) => {
                let raw = fs::read_to_string(path)
                    .with_context(|| format!("failed to read {}", path.display()))?;
                serde_json::from_str(&raw)
                    .with_context(|| format!("invalid JSON in {}", path.display()))?
            }
            None => serde_json::json!({ "mocked": true }),
        };
        let rendered = mock_execute_pack(
            &cli.pack,
            cli.flow.as_deref().unwrap_or("default"),
            &input,
            cli.offline,
            cli.allow_external,
            cli.mock_external,
            mock_payload,
            cli.secrets_seed.as_deref(),
        )?;
        let mut rendered = rendered;
        if let Some(map) = rendered.as_object_mut() {
            map.insert("exec_mode".to_string(), serde_json::json!("mock"));
        }
        if cli.json {
            println!("{}", serde_json::to_string(&rendered)?);
        } else {
            println!("{}", serde_json::to_string_pretty(&rendered)?);
        }
        let status = rendered
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if status != "ok" {
            bail!("pack run failed");
        }
        return Ok(());
    }

    let log_path = init_run_logging()?;
    log_runner_start();
    // stderr keeps stdout clean for machine-readable `--json` output.
    eprintln!("Run logs: {}", log_path.display());
    configure_wasmtime_home()?;
    configure_proxy_env();
    if cli.no_cache {
        // SAFETY: toggling the cache behavior is scoped to this process.
        unsafe {
            std::env::set_var("GREENTIC_NO_CACHE", "1");
        }
    }

    let input = parse_input(&cli)?;

    let signing = match policy {
        RunPolicyArg::Strict => SigningPolicy::Strict,
        RunPolicyArg::Devok => SigningPolicy::DevOk,
    };

    let profile = build_profile(&cli);
    let ctx = build_ctx(&cli);
    let components_map = parse_components_map(&cli)?;
    let allow_hosts = parse_allow_hosts(&cli);
    let mocks_config = build_mocks_config(cli.mocks, allow_hosts)?;
    let otlp_hook = if cli.offline {
        None
    } else {
        cli.otlp.clone().map(|endpoint| OtlpHook {
            endpoint,
            headers: Vec::new(),
            sample_all: true,
        })
    };
    let artifacts_dir = cli.artifacts_dir.clone();
    if let Some(dir) = &artifacts_dir {
        fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    }

    let runner = Runner::new().configure(|opts: &mut RunOptions| {
        opts.profile = profile.clone();
        opts.entry_flow = cli.flow.clone();
        opts.input = input.clone();
        opts.ctx = ctx.clone();
        opts.artifacts_dir = artifacts_dir.clone();
        opts.signing = signing;
        opts.components_dir = cli.components_dir.clone();
        opts.components_map = components_map.clone();
        opts.dist_offline = cli.offline;
        opts.dist_cache_dir = cli.cache_dir.clone();
        opts.allow_missing_hash = cli.allow_missing_hash;
        opts.mocks = mocks_config.clone();
        if let Some(hook) = otlp_hook.clone() {
            opts.otlp = Some(hook);
        }
    });

    let result = runner
        .run_pack_with(&cli.pack, |_opts| {})
        .with_context(|| format!("failed to run pack {}", cli.pack.display()))?;

    let mut value = serde_json::to_value(&result)
        .map_err(|err| anyhow!("failed to serialize run result: {err}"))?;
    if let Some(map) = value.as_object_mut() {
        map.insert("exec_mode".to_string(), serde_json::json!("runtime"));
    }
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let rendered = if cli.json {
        serde_json::to_string(&value)
            .map_err(|err| anyhow!("failed to serialize run result: {err}"))?
    } else {
        serde_json::to_string_pretty(&value)
            .map_err(|err| anyhow!("failed to serialize run result: {err}"))?
    };
    println!("{rendered}");

    if status == "Failure" || status == "PartialFailure" {
        let err = value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("pack run returned failure status");
        bail!("pack run failed: {err}");
    }

    Ok(())
}

fn init_run_logging() -> Result<PathBuf> {
    let workspace = std::env::current_dir().context("failed to resolve workspace root")?;
    let logs_dir = workspace.join(".greentic").join("logs");
    fs::create_dir_all(&logs_dir)
        .with_context(|| format!("failed to create {}", logs_dir.display()))?;
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let log_path = logs_dir.join(format!("pack-run-{timestamp}.log"));
    let make_writer = {
        let log_path = log_path.clone();
        move || open_log_writer(&log_path)
    };

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(make_writer)
        .with_ansi(false)
        .with_target(true);

    let filter = tracing_subscriber::filter::EnvFilter::new(
        "debug,cranelift_codegen=off,wasmtime=off,wasmtime_cranelift=off,cranelift=off",
    );

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .try_init();

    Ok(log_path)
}

/// Combine `--policy` and `--strict-signing` into the effective policy.
/// The two flags are rejected together earlier; `--strict-signing` alone selects `Strict`.
fn resolve_policy(policy: Option<RunPolicyArg>, strict_signing: bool) -> RunPolicyArg {
    match (policy, strict_signing) {
        (Some(policy), _) => policy,
        (None, true) => RunPolicyArg::Strict,
        (None, false) => RunPolicyArg::Devok,
    }
}

/// Open the run-log file; if that fails, note it on stderr and discard logs
/// rather than panicking inside the tracing writer.
fn open_log_writer(log_path: &Path) -> Box<dyn std::io::Write + Send> {
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        Ok(file) => Box::new(file),
        Err(err) => {
            eprintln!(
                "warning: failed to open run log {}: {err}; discarding logs",
                log_path.display()
            );
            Box::new(std::io::sink())
        }
    }
}

/// Environment values that must be set to scope wasmtime state to the
/// workspace. Only entries that are missing from the process environment are
/// returned, so a valid user `HOME` / `WASMTIME_CACHE_DIR` is never clobbered.
#[derive(Debug, Default, PartialEq, Eq)]
struct WasmtimeEnvPlan {
    home: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
}

fn plan_wasmtime_env(home_set: bool, cache_dir_set: bool, workspace: &Path) -> WasmtimeEnvPlan {
    let local_home = workspace.join(".greentic").join("wasmtime-home");
    WasmtimeEnvPlan {
        home: (!home_set).then(|| local_home.clone()),
        cache_dir: (!cache_dir_set).then(|| {
            local_home
                .join("Library")
                .join("Caches")
                .join("BytecodeAlliance.wasmtime")
        }),
    }
}

fn configure_wasmtime_home() -> Result<()> {
    let workspace = std::env::current_dir().context("failed to resolve workspace root")?;
    let plan = plan_wasmtime_env(
        std::env::var_os("HOME").is_some(),
        std::env::var_os("WASMTIME_CACHE_DIR").is_some(),
        &workspace,
    );
    if let Some(home) = &plan.home {
        let config_dir = home
            .join("Library")
            .join("Application Support")
            .join("wasmtime");
        fs::create_dir_all(&config_dir)
            .with_context(|| format!("failed to create {}", config_dir.display()))?;
    }
    if let Some(cache_dir) = &plan.cache_dir {
        fs::create_dir_all(cache_dir)
            .with_context(|| format!("failed to create {}", cache_dir.display()))?;
    }
    // SAFETY: scope HOME and cache dir to a workspace-local directory to avoid
    // writing outside the sandbox; this only affects the child Wasmtime engine.
    unsafe {
        if let Some(home) = &plan.home {
            std::env::set_var("HOME", home);
        }
        if let Some(cache_dir) = &plan.cache_dir {
            std::env::set_var("WASMTIME_CACHE_DIR", cache_dir);
        }
    }
    Ok(())
}

fn proxy_env_already_configured() -> bool {
    [
        "NO_PROXY",
        "no_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ]
    .iter()
    .any(|key| std::env::var_os(key).is_some())
}

fn configure_proxy_env() {
    // Respect any proxy settings the user already has.
    if proxy_env_already_configured() {
        return;
    }
    // Avoid system proxy discovery (reqwest on macOS can panic in sandboxed CI).
    unsafe {
        std::env::set_var("NO_PROXY", "*");
        std::env::set_var("HTTPS_PROXY", "");
        std::env::set_var("HTTP_PROXY", "");
        std::env::set_var("CFNETWORK_DISABLE_SYSTEM_PROXY", "1");
    }
}

fn build_mocks_config(setting: MockSettingArg, allow_hosts: Vec<String>) -> Result<MocksConfig> {
    let mut config = MocksConfig {
        net_allowlist: allow_hosts
            .into_iter()
            .map(|host| host.trim().to_ascii_lowercase())
            .filter(|host| !host.is_empty())
            .collect(),
        ..MocksConfig::default()
    };

    if matches!(setting, MockSettingArg::On) {
        config.http = Some(HttpMock {
            record_replay_dir: None,
            mode: HttpMockMode::RecordReplay,
            rewrites: Vec::new(),
        });

        let tools_dir = PathBuf::from(".greentic").join("mocks").join("tools");
        fs::create_dir_all(&tools_dir)
            .with_context(|| format!("failed to create {}", tools_dir.display()))?;
        config.mcp_tools = Some(ToolsMock {
            directory: None,
            script_dir: Some(tools_dir),
            short_circuit: true,
        });
    }

    Ok(config)
}

fn log_runner_start() {
    let pid = std::process::id();
    let thread_id = std::thread::current().id();
    let tokio_handle_present = tokio::runtime::Handle::try_current().is_ok();
    tracing::info!(
        event = "runner.start",
        pid,
        thread_id = ?thread_id,
        tokio_handle_present,
        "greentic-runner process started"
    );
}

#[allow(clippy::too_many_arguments)]
fn mock_execute_pack(
    path: &Path,
    flow_id: &str,
    input: &Value,
    offline: bool,
    allow_external: bool,
    mock_external: bool,
    mock_external_payload: Value,
    secrets_seed: Option<&Path>,
) -> Result<Value> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read pack {}", path.display()))?;
    let mut archive = ZipArchive::new(std::io::Cursor::new(bytes)).context("open pack zip")?;
    let mut manifest_bytes = Vec::new();
    archive
        .by_name("manifest.cbor")
        .context("manifest.cbor missing")?
        .read_to_end(&mut manifest_bytes)
        .context("read manifest")?;
    let manifest: PackManifest =
        decode_pack_manifest(&manifest_bytes).context("decode manifest")?;
    let flow = manifest
        .flows
        .iter()
        .find(|f| f.id.as_str() == flow_id)
        .ok_or_else(|| anyhow!("flow `{flow_id}` not found in pack"))?;
    let mut exec_builder = ExecOptionsBuilder::default();
    if let Some(seed_path) = secrets_seed {
        exec_builder = exec_builder
            .load_seed_file(seed_path)
            .context("failed to load secrets seed")?;
    }
    let exec_opts = exec_builder
        .offline(offline)
        .external_enabled(allow_external)
        .mock_external(mock_external)
        .mock_external_payload(mock_external_payload)
        .build()
        .context("build mock exec options")?;
    let exec = execute_with_options(&flow.flow, input, &exec_opts)?;
    Ok(exec)
}

#[derive(Clone)]
struct ExecOptions {
    offline: bool,
    external_enabled: bool,
    mock_external: bool,
    mock_external_payload: Value,
    secrets: HashMap<String, Vec<u8>>,
}

#[derive(Default)]
struct ExecOptionsBuilder {
    offline: bool,
    external_enabled: bool,
    mock_external: bool,
    mock_external_payload: Value,
    seeds: HashMap<String, Vec<u8>>,
}

impl ExecOptionsBuilder {
    fn offline(mut self, offline: bool) -> Self {
        self.offline = offline;
        self
    }

    fn external_enabled(mut self, enabled: bool) -> Self {
        self.external_enabled = enabled;
        self
    }

    fn mock_external(mut self, enabled: bool) -> Self {
        self.mock_external = enabled;
        self
    }

    fn mock_external_payload(mut self, payload: Value) -> Self {
        self.mock_external_payload = payload;
        self
    }

    fn load_seed_file(mut self, path: &Path) -> Result<Self> {
        let seeds = load_seed_file(path)?;
        self.seeds.extend(seeds);
        Ok(self)
    }

    fn build(self) -> Result<ExecOptions> {
        Ok(ExecOptions {
            offline: self.offline,
            external_enabled: self.external_enabled,
            mock_external: self.mock_external,
            mock_external_payload: self.mock_external_payload,
            secrets: self.seeds,
        })
    }
}

fn execute_with_options(flow: &Flow, input: &Value, opts: &ExecOptions) -> Result<Value> {
    let nodes: HashMap<_, _> = flow
        .nodes
        .iter()
        .map(|(id, node)| (id.clone(), node.clone()))
        .collect();
    let mut current = flow
        .ingress()
        .map(|(id, _)| id.clone())
        .ok_or_else(|| anyhow!("flow has no ingress"))?;
    let mut payload = input.clone();
    let mut trace = Vec::new();
    let mut last_status = String::from("ok");

    loop {
        let Some(node) = nodes.get(&current) else {
            bail!("node `{current}` missing");
        };
        let (status, next_payload) = exec_node(node, &payload, opts)?;
        trace.push(serde_json::json!({
            "node_id": node.id.as_str(),
            "component": node.component.id.as_str(),
            "status": status,
            "payload": next_payload
        }));
        payload = next_payload;
        if status == "error" || last_status == "error" {
            last_status = "error".to_string();
        } else {
            last_status = status.clone();
        }
        current = match &node.routing {
            Routing::Next { node_id } => node_id.clone(),
            Routing::Branch { on_status, default } => {
                if let Some(dest) = on_status.get(&status) {
                    dest.clone()
                } else if let Some(def) = default {
                    def.clone()
                } else {
                    bail!("no branch for status `{status}`");
                }
            }
            Routing::End | Routing::Reply | Routing::Custom(_) => break,
        };
    }

    Ok(serde_json::json!({
        "status": last_status,
        "output": payload,
        "trace": trace,
    }))
}

fn exec_node(node: &Node, payload: &Value, opts: &ExecOptions) -> Result<(String, Value)> {
    let component = node.component.id.as_str();
    match component {
        "component.start" => Ok(("ok".into(), payload.clone())),
        "component.tool.fixed" => {
            if payload
                .get("fail")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                Ok((
                    "error".into(),
                    serde_json::json!({
                        "error": "tool_failed",
                        "input": payload
                    }),
                ))
            } else {
                Ok((
                    "ok".into(),
                    serde_json::json!({
                        "query": payload.get("query").cloned().unwrap_or(Value::Null),
                        "result": "fixed",
                        "constant": 42
                    }),
                ))
            }
        }
        "component.template" => {
            let result_value = payload.get("result").cloned().unwrap_or(Value::Null);
            let result = result_value
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| serde_json::to_string(&result_value).unwrap_or_default());
            Ok((
                "ok".into(),
                serde_json::json!({
                    "answer": format!("Result: {result}"),
                    "source": "template",
                    "input": payload
                }),
            ))
        }
        "component.error.map" => Ok((
            "ok".into(),
            serde_json::json!({
                "message": "A friendly error occurred",
                "details": payload
            }),
        )),
        "component.tool.secret" => {
            let secret = read_secret(opts, "API_KEY")?;
            match secret {
                None => Ok((
                    "error".into(),
                    serde_json::json!({
                        "error": "missing_secret",
                        "key": "API_KEY",
                        "secret_lookup": {
                            "key": "API_KEY",
                            "status": "missing"
                        }
                    }),
                )),
                Some(bytes) => {
                    let prefix = String::from_utf8_lossy(&bytes);
                    let prefix = prefix.chars().take(3).collect::<String>();
                    Ok((
                        "ok".into(),
                        serde_json::json!({
                            "has_key": true,
                            "prefix": prefix,
                            "secret_lookup": {
                                "key": "API_KEY",
                                "status": "found"
                            }
                        }),
                    ))
                }
            }
        }
        "component.tool.external" => {
            if opts.offline || !opts.external_enabled {
                return Ok((
                    "error".into(),
                    serde_json::json!({
                        "error": "external_blocked",
                        "policy": {
                            "offline": opts.offline,
                            "external_enabled": opts.external_enabled,
                            "mock_external": opts.mock_external,
                        },
                        "policy_status": "blocked_by_policy"
                    }),
                ));
            }
            if opts.mock_external {
                return Ok((
                    "ok".into(),
                    serde_json::json!({
                        "policy_status": "mocked_external",
                        "policy": {
                            "offline": opts.offline,
                            "external_enabled": opts.external_enabled,
                            "mock_external": opts.mock_external,
                        },
                        "result": opts.mock_external_payload,
                    }),
                ));
            }
            Ok((
                "error".into(),
                serde_json::json!({
                    "error": "real_external_not_supported_in_tests",
                    "policy_status": "blocked_by_policy",
                    "policy": {
                        "offline": opts.offline,
                        "external_enabled": opts.external_enabled,
                        "mock_external": opts.mock_external,
                    }
                }),
            ))
        }
        _ => bail!("unknown component `{component}`"),
    }
}

fn read_secret(opts: &ExecOptions, key: &str) -> Result<Option<Vec<u8>>> {
    Ok(opts.secrets.get(key).cloned())
}

#[derive(Debug, Deserialize)]
struct SeedDoc {
    entries: Vec<SeedEntry>,
}

#[derive(Debug, Deserialize)]
struct SeedEntry {
    uri: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    json: Option<Value>,
    #[serde(default, rename = "bytes_b64")]
    bytes_b64: Option<String>,
    #[serde(default)]
    value: Option<Value>,
}

fn load_seed_file(path: &Path) -> Result<HashMap<String, Vec<u8>>> {
    let data =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    if let Ok(doc) = serde_yaml::from_str::<SeedDoc>(&data) {
        let mut map = HashMap::new();
        for entry in doc.entries {
            let (uri, bytes) = seed_entry_to_bytes(entry)?;
            map.insert(uri, bytes);
        }
        return Ok(map);
    }
    if let Ok(map) = serde_yaml::from_str::<HashMap<String, Value>>(&data) {
        let mut out = HashMap::new();
        for (uri, val) in map {
            let bytes = match val {
                Value::String(s) => s.into_bytes(),
                other => serde_json::to_vec(&other)
                    .context("failed to serialize seed value to JSON bytes")?,
            };
            out.insert(uri, bytes);
        }
        return Ok(out);
    }

    bail!("failed to parse secrets seed (unsupported format)")
}

fn seed_entry_to_bytes(entry: SeedEntry) -> Result<(String, Vec<u8>)> {
    let bytes = if let Some(text) = entry.text {
        text.into_bytes()
    } else if let Some(json) = entry.json {
        serde_json::to_vec(&json).context("failed to serialize seed json value")?
    } else if let Some(b64) = entry.bytes_b64 {
        B64_STANDARD
            .decode(b64.as_bytes())
            .context("failed to decode seed bytes_b64")?
    } else if let Some(val) = entry.value {
        match val {
            Value::String(s) => s.into_bytes(),
            other => serde_json::to_vec(&other).context("failed to serialize seed value")?,
        }
    } else {
        bail!("seed entry {} missing value", entry.uri);
    };
    Ok((entry.uri, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn strict_signing_selects_strict_policy() {
        assert!(matches!(resolve_policy(None, true), RunPolicyArg::Strict));
        assert!(matches!(resolve_policy(None, false), RunPolicyArg::Devok));
        assert!(matches!(
            resolve_policy(Some(RunPolicyArg::Devok), false),
            RunPolicyArg::Devok
        ));
    }

    #[test]
    fn wasmtime_env_plan_only_sets_missing_values() {
        let ws = Path::new("/ws");
        let both_set = plan_wasmtime_env(true, true, ws);
        assert_eq!(both_set, WasmtimeEnvPlan::default());

        let only_cache_missing = plan_wasmtime_env(true, false, ws);
        assert!(only_cache_missing.home.is_none());
        assert!(only_cache_missing.cache_dir.is_some());

        let only_home_missing = plan_wasmtime_env(false, true, ws);
        assert!(only_home_missing.home.is_some());
        assert!(only_home_missing.cache_dir.is_none());
    }

    #[test]
    fn open_log_writer_falls_back_to_sink() {
        let missing = Path::new("/nonexistent-dir-for-greentic-test/run.log");
        let mut writer = open_log_writer(missing);
        assert!(writer.write_all(b"ignored").is_ok());
    }
}
