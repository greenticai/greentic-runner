use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result};
use greentic_flow::flow_bundle::load_and_validate_bundle_with_flow;
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::{
    ExecutionObserver, FlowContext, FlowEngine, FlowStatus, NodeEvent, RetryConfig,
};
use greentic_runner_host::secrets::default_manager;
use greentic_runner_host::storage::DynStateStore;
use greentic_runner_host::trace::{TraceConfig, TraceMode};
use greentic_runner_host::validate::ValidationConfig;
use greentic_runner_host::wasi::RunnerWasiPolicy;
use greentic_state::StateStore;
use greentic_state::inmemory::InMemoryStateStore;
use greentic_types::{
    ComponentCapabilities, ComponentId, ComponentManifest, ComponentProfiles, FlowKind,
    HostCapabilities, PackFlowEntry, PackKind, PackManifest, ResourceHints, StateCapabilities,
    encode_pack_manifest,
};
use insta::Settings;
use semver::Version;
use serde_json::{Map as JsonMap, Value, json};
use tempfile::TempDir;
use zip::ZipWriter;
use zip::write::FileOptions;

const SNAPSHOT_DIR: &str = "tests/snapshots/expected";
const INPUTS_DIR: &str = "tests/snapshots/inputs";

#[derive(Default)]
struct RecordingObserver {
    events: Mutex<Vec<Value>>,
    errors: Mutex<Vec<String>>,
}

impl ExecutionObserver for RecordingObserver {
    fn on_node_start(&self, event: &NodeEvent<'_>) {
        let operation = extract_operation(event.payload).unwrap_or_default();
        self.events.lock().unwrap().push(json!({
            "node_id": event.node_id,
            "component_id": event.node.component_id(),
            "operation": operation,
            "payload": event.payload,
        }));
    }

    fn on_node_end(&self, _event: &NodeEvent<'_>, _output: &Value) {}

    fn on_node_error(&self, _event: &NodeEvent<'_>, error: &dyn std::error::Error) {
        self.errors.lock().unwrap().push(error.to_string());
    }
}

fn extract_operation(payload: &Value) -> Option<String> {
    payload
        .as_object()
        .and_then(|map| map.get("operation"))
        .and_then(Value::as_str)
        .map(|value| value.to_string())
}

fn host_config(tenant: &str, retry: FlowRetryConfig) -> HostConfig {
    HostConfig {
        tenant: tenant.to_string(),
        bindings_path: PathBuf::from("<snapshot>"),
        flow_type_bindings: HashMap::new(),
        rate_limits: RateLimits::default(),
        retry,
        http_enabled: false,
        secrets_policy: SecretsPolicy::allow_all(),
        state_store_policy: StateStorePolicy { allow: true },
        webhook_policy: WebhookPolicy::default(),
        timers: Vec::new(),
        oauth: None,
        mocks: None,
        pack_bindings: Vec::new(),
        env_passthrough: Vec::new(),
        trace: TraceConfig::from_env().with_overrides(TraceMode::Off, None),
        validation: ValidationConfig::from_env(),
        operator_policy: OperatorPolicy::allow_all(),
        fast2flow: Default::default(),
        #[cfg(feature = "agentic-worker")]
        agents: HashMap::new(),
        #[cfg(feature = "agentic-worker")]
        graphs: HashMap::new(),
    }
}

fn build_pack(
    pack_id: &str,
    flow_yaml: &str,
    components: Vec<ComponentFixture>,
) -> Result<(TempDir, PathBuf)> {
    let temp = TempDir::new()?;
    let pack_path = temp.path().join(format!("{pack_id}.gtpack"));
    let (_bundle, flow) = load_and_validate_bundle_with_flow(flow_yaml, None)?;

    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: pack_id.parse()?,
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: components
            .iter()
            .map(|fixture| fixture.manifest.clone())
            .collect(),
        flows: vec![PackFlowEntry {
            id: flow.id.clone(),
            kind: flow.kind,
            flow: flow.clone(),
            tags: Vec::new(),
            entrypoints: vec!["default".into()],
        }],
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        extensions: None,
    };

    let mut writer = ZipWriter::new(fs::File::create(&pack_path)?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    for component in components {
        writer.start_file(format!("components/{}.wasm", component.id), options)?;
        let bytes = fs::read(&component.wasm_path)
            .with_context(|| format!("read component {}", component.wasm_path.display()))?;
        writer.write_all(&bytes)?;
    }

    writer.finish()?;
    Ok((temp, pack_path))
}

#[derive(Clone)]
struct ComponentFixture {
    id: String,
    wasm_path: PathBuf,
    manifest: ComponentManifest,
}

fn qa_process_fixture() -> Result<ComponentFixture> {
    let id = "qa.process";
    let wasm_path = ensure_runner_component_wasm("qa_process", "qa_process.wasm")?;
    let manifest = ComponentManifest {
        id: ComponentId::from_str(id)?,
        version: Version::parse("0.1.0")?,
        supports: vec![FlowKind::Messaging],
        world: "greentic:component@0.4.0".into(),
        profiles: ComponentProfiles::default(),
        capabilities: ComponentCapabilities::default(),
        configurators: None,
        operations: Vec::new(),
        config_schema: None,
        resources: ResourceHints::default(),
        dev_flows: BTreeMap::new(),
    };
    Ok(ComponentFixture {
        id: id.to_string(),
        wasm_path,
        manifest,
    })
}

fn state_store_fixture() -> Result<ComponentFixture> {
    let id = "state.store";
    let wasm_path =
        ensure_runner_component_wasm("state_store_component", "state_store_component.wasm")?;
    let manifest = ComponentManifest {
        id: ComponentId::from_str(id)?,
        version: Version::parse("0.1.0")?,
        supports: vec![FlowKind::Messaging],
        world: "greentic:component@0.4.0".into(),
        profiles: ComponentProfiles::default(),
        capabilities: ComponentCapabilities {
            host: HostCapabilities {
                state: Some(StateCapabilities {
                    read: true,
                    write: true,
                }),
                ..HostCapabilities::default()
            },
            ..ComponentCapabilities::default()
        },
        configurators: None,
        operations: Vec::new(),
        config_schema: None,
        resources: ResourceHints::default(),
        dev_flows: BTreeMap::new(),
    };
    Ok(ComponentFixture {
        id: id.to_string(),
        wasm_path,
        manifest,
    })
}

fn fixture_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(relative)
}

fn ensure_runner_component_wasm(package: &str, file_name: &str) -> Result<PathBuf> {
    let fixtures_root = fixture_path("tests/fixtures/runner-components");
    let wasm_path = fixtures_root
        .join("target-test")
        .join("wasm32-wasip2")
        .join("release")
        .join(file_name);
    if wasm_path.exists() {
        return Ok(wasm_path);
    }

    let workspace_manifest = fixtures_root.join("Cargo.toml");
    let target_dir = fixtures_root.join("target-test");
    let offline = env::var("CARGO_NET_OFFLINE").ok();
    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("--release")
        .arg("--target")
        .arg("wasm32-wasip2")
        .arg("--package")
        .arg(package)
        .arg("--manifest-path")
        .arg(&workspace_manifest)
        .arg("--target-dir")
        .arg(&target_dir);
    if matches!(offline.as_deref(), Some("true")) {
        cmd.arg("--offline");
    }
    if let Some(val) = &offline {
        cmd.env("CARGO_NET_OFFLINE", val);
    }

    let status = cmd
        .status()
        .with_context(|| format!("build {}", workspace_manifest.display()))?;
    if !status.success() {
        anyhow::bail!("failed to build {package} fixture");
    }
    if !wasm_path.exists() {
        anyhow::bail!(
            "{file_name} missing after fixture build: {}",
            wasm_path.display()
        );
    }
    Ok(wasm_path)
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..")
}

fn inputs_root() -> PathBuf {
    repo_root().join(INPUTS_DIR)
}

fn snapshot_root() -> PathBuf {
    repo_root().join(SNAPSHOT_DIR)
}

fn load_input(name: &str) -> Result<Value> {
    let path = inputs_root().join(format!("{name}.json"));
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("read snapshot input {}", path.display()))?;
    Ok(serde_json::from_str(&raw)?)
}

fn snapshot_path(name: &str) -> PathBuf {
    snapshot_root().join(format!("{name}.snap"))
}

fn artifact_dir(name: &str) -> PathBuf {
    repo_root()
        .join("target")
        .join("snapshot-artifacts")
        .join(name)
}

fn normalize_for_snapshot(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut items: Vec<(String, Value)> = map.into_iter().collect();
            items.sort_by(|a, b| a.0.cmp(&b.0));
            let mut normalized = JsonMap::new();
            for (key, value) in items {
                if should_redact_key(&key) {
                    normalized.insert(key, Value::String("<redacted>".to_string()));
                } else {
                    normalized.insert(key, normalize_for_snapshot(value));
                }
            }
            Value::Object(normalized)
        }
        Value::Array(items) => {
            Value::Array(items.into_iter().map(normalize_for_snapshot).collect())
        }
        Value::String(value) => {
            if looks_like_uuid(&value) {
                Value::String("<uuid>".to_string())
            } else {
                Value::String(value)
            }
        }
        other => other,
    }
}

fn should_redact_key(key: &str) -> bool {
    matches!(
        key,
        "timestamp" | "trace_id" | "session_id" | "seed" | "uuid" | "started_at" | "finished_at"
    )
}

fn looks_like_uuid(value: &str) -> bool {
    if value.len() != 36 {
        return false;
    }
    let bytes = value.as_bytes();
    [8, 13, 18, 23].iter().all(|&idx| bytes[idx] == b'-')
        && bytes
            .iter()
            .enumerate()
            .filter(|(idx, _)| ![8, 13, 18, 23].contains(idx))
            .all(|(_, ch)| ch.is_ascii_hexdigit())
}

fn read_expected_snapshot(path: &Path) -> Option<Value> {
    let raw = fs::read_to_string(path).ok()?;
    let start = raw.find('{').or_else(|| raw.find('['))?;
    serde_json::from_str(&raw[start..]).ok()
}

fn write_snapshot_artifacts(
    name: &str,
    input: &Value,
    actual: &Value,
    expected: &Value,
) -> Result<()> {
    let dir = artifact_dir(name);
    fs::create_dir_all(&dir)?;
    fs::write(dir.join("input.json"), serde_json::to_vec_pretty(input)?)?;
    fs::write(dir.join("actual.json"), serde_json::to_vec_pretty(actual)?)?;
    fs::write(
        dir.join("expected.json"),
        serde_json::to_vec_pretty(expected)?,
    )?;
    Ok(())
}

fn assert_snapshot(name: &str, input: &Value, actual: Value) -> Result<()> {
    let normalized_actual = normalize_for_snapshot(actual);
    let expected_path = snapshot_path(name);
    let expected_value = read_expected_snapshot(&expected_path).unwrap_or(Value::Null);
    if expected_path.exists() {
        let normalized_expected = normalize_for_snapshot(expected_value.clone());
        if normalized_expected != normalized_actual {
            write_snapshot_artifacts(name, input, &normalized_actual, &normalized_expected)?;
        }
    }

    let mut settings = Settings::new();
    settings.set_snapshot_path(snapshot_root());
    settings.set_prepend_module_to_snapshot(false);
    settings.bind(|| {
        insta::assert_json_snapshot!(name, normalized_actual);
    });
    Ok(())
}

fn run_flow(
    pack_path: &Path,
    flow_id: &str,
    config: Arc<HostConfig>,
    observer: Option<&RecordingObserver>,
    state_store: Option<DynStateStore>,
) -> Result<Value> {
    let runtime = tokio::runtime::Runtime::new()?;
    let pack = Arc::new(runtime.block_on(PackRuntime::load(
        pack_path,
        Arc::clone(&config),
        None,
        Some(pack_path),
        None,
        state_store,
        Arc::new(RunnerWasiPolicy::default().inherit_stdio(false)),
        default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);

    let engine = runtime.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;
    let retry_config: RetryConfig = config.retry.clone().into();
    let observer = observer.map(|obs| obs as &dyn ExecutionObserver);
    let ctx = FlowContext {
        tenant: config.tenant.as_str(),
        pack_id: pack.metadata().pack_id.as_str(),
        flow_id,
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        retry_config,
        attempt: 1,
        observer,
        mocks: None,
        reply_scope: None,
        caller: None,
    };

    let execution = runtime.block_on(engine.execute(ctx, Value::Null));
    match execution {
        Ok(exec) => match exec.status {
            FlowStatus::Completed => Ok(exec.output),
            FlowStatus::Waiting(wait) => Ok(json!({
                "status": "waiting",
                "reason": wait.reason,
                "output": exec.output,
            })),
        },
        Err(err) => Err(err),
    }
}

#[test]
fn snapshot_minimal_flow_success() -> Result<()> {
    let flow_yaml = r#"
id: snapshot.flow
type: messaging
start: emit
nodes:
  emit:
    emit.response:
      status: "ok"
    routing:
      - out: true
"#;
    let (temp, pack_path) = build_pack("snapshot-a", flow_yaml, Vec::new())?;
    let input = load_input("scenario_a")?;
    let config = Arc::new(host_config("snapshot", FlowRetryConfig::default()));
    let output = run_flow(&pack_path, "snapshot.flow", config, None, None)?;

    let actual = json!({
        "status": "completed",
        "output": output,
        "state": Value::Null,
    });
    assert_snapshot("scenario_a", &input, actual)?;
    drop(temp);
    Ok(())
}

#[test]
fn snapshot_component_execution() -> Result<()> {
    let flow_yaml = r#"
id: snapshot.flow
type: messaging
start: qa
nodes:
  qa:
    component.exec:
      component: qa.process
      operation: process
      input:
        text: "hello"
    routing:
      - to: emit
  emit:
    emit.response:
      text: "Echo: {{node.qa.text}}"
    routing:
      - out: true
"#;
    let component = qa_process_fixture()?;
    let (temp, pack_path) = build_pack("snapshot-b", flow_yaml, vec![component])?;
    let input = load_input("scenario_b")?;
    let observer = RecordingObserver::default();
    let config = Arc::new(host_config("snapshot", FlowRetryConfig::default()));
    let output = run_flow(&pack_path, "snapshot.flow", config, Some(&observer), None)?;

    let events = observer.events.lock().unwrap().clone();
    let actual = json!({
        "status": "completed",
        "output": output,
        "component_events": events,
    });
    assert_snapshot("scenario_b", &input, actual)?;
    drop(temp);
    Ok(())
}

#[test]
fn snapshot_error_path() -> Result<()> {
    let flow_yaml = r#"
id: snapshot.flow
type: messaging
start: qa
nodes:
  qa:
    component.exec:
      component: missing.component
      operation: process
      input:
        text: "oops"
    routing:
      - out: true
"#;
    let (temp, pack_path) = build_pack("snapshot-c", flow_yaml, Vec::new())?;
    let input = load_input("scenario_c")?;
    let config = Arc::new(host_config("snapshot", FlowRetryConfig::default()));
    let result = run_flow(&pack_path, "snapshot.flow", config, None, None);

    let actual = match result {
        Ok(output) => json!({
            "status": "completed",
            "output": output,
            "diagnostics": [],
        }),
        Err(err) => json!({
            "status": "error",
            "error": err.to_string(),
            "diagnostics": [
                {
                    "code": "component_missing",
                    "message": err.to_string(),
                }
            ]
        }),
    };
    assert_snapshot("scenario_c", &input, actual)?;
    drop(temp);
    Ok(())
}

#[test]
fn snapshot_retry_once() -> Result<()> {
    let flow_yaml = r#"
id: snapshot.flow
type: messaging
start: write
nodes:
  write:
    component.exec:
      component: state.store
      operation: write
      input:
        key: "snapshot-retry"
        value:
          status: "ok"
    routing:
      - out: true
"#;
    let component = state_store_fixture()?;
    let (temp, pack_path) = build_pack("snapshot-d", flow_yaml, vec![component])?;
    let input = load_input("scenario_d")?;
    let observer = RecordingObserver::default();

    let retry = FlowRetryConfig {
        max_attempts: 2,
        base_delay_ms: 1,
    };
    let config = Arc::new(host_config("snapshot", retry));
    let flaky = Arc::new(FlakyStateStore::new());
    let output = run_flow(
        &pack_path,
        "snapshot.flow",
        config,
        Some(&observer),
        Some(flaky.clone()),
    )?;

    let state_value = flaky
        .inner
        .get_json(
            &flaky.tenant_ctx(),
            "runner",
            &flaky.state_key("snapshot-retry"),
            None,
        )
        .unwrap_or(None)
        .unwrap_or(Value::Null);

    let actual = json!({
        "status": "completed",
        "output": output,
        "retry_errors": observer.errors.lock().unwrap().clone(),
        "state": state_value,
    });
    assert_snapshot("scenario_d", &input, actual)?;
    drop(temp);
    Ok(())
}

#[derive(Clone)]
struct FlakyStateStore {
    inner: Arc<InMemoryStateStore>,
    failed: Arc<AtomicBool>,
}

impl FlakyStateStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryStateStore::new()),
            failed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn tenant_ctx(&self) -> greentic_types::TenantCtx {
        greentic_types::TenantCtx::new(
            greentic_types::EnvId::from_str("local").expect("env"),
            greentic_types::TenantId::from_str("snapshot").expect("tenant"),
        )
    }

    fn state_key(&self, key: &str) -> greentic_types::StateKey {
        greentic_types::StateKey::new(key)
    }
}

impl StateStore for FlakyStateStore {
    fn get_json(
        &self,
        tenant: &greentic_types::TenantCtx,
        prefix: &str,
        key: &greentic_types::StateKey,
        path: Option<&greentic_state::StatePath>,
    ) -> greentic_types::GResult<Option<Value>> {
        self.inner.get_json(tenant, prefix, key, path)
    }

    fn set_json(
        &self,
        tenant: &greentic_types::TenantCtx,
        prefix: &str,
        key: &greentic_types::StateKey,
        path: Option<&greentic_state::StatePath>,
        value: &Value,
        ttl_secs: Option<u32>,
    ) -> greentic_types::GResult<()> {
        if !self.failed.swap(true, Ordering::SeqCst) {
            return Err(greentic_types::GreenticError::new(
                greentic_types::ErrorCode::Unavailable,
                "transient state write",
            ));
        }
        self.inner
            .set_json(tenant, prefix, key, path, value, ttl_secs)
    }

    fn del(
        &self,
        tenant: &greentic_types::TenantCtx,
        prefix: &str,
        key: &greentic_types::StateKey,
    ) -> greentic_types::GResult<bool> {
        self.inner.del(tenant, prefix, key)
    }

    fn del_prefix(
        &self,
        tenant: &greentic_types::TenantCtx,
        prefix: &str,
    ) -> greentic_types::GResult<u64> {
        self.inner.del_prefix(tenant, prefix)
    }
}
