use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result, anyhow};
use greentic_flow::flow_bundle::load_and_validate_bundle_with_flow;
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use greentic_runner_host::runner::flow_adapter::{FlowIR, NodeIR, RouteIR};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, ExtensionInline, ExtensionRef,
    Flow, FlowComponentRef, FlowId, FlowKind, FlowMetadata, InputMapping, Node, NodeId,
    OutputMapping, PackFlowEntry, PackKind, PackManifest, ResourceHints, Routing, TelemetryHints,
    encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use tempfile::TempDir;
use zip::ZipArchive;
use zip::write::FileOptions;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
}

fn build_components() -> Result<Vec<(String, PathBuf)>> {
    let workspace = workspace_root().join("tests/fixtures/runner-components");
    let mut results = Vec::new();
    let crates = vec![
        ("qa.process", "qa_process"),
        ("templating.handlebars", "templating_handlebars"),
        ("state.store", "state_store_component"),
    ];
    let offline = std::env::var("CARGO_NET_OFFLINE").ok();
    for (name, krate) in &crates {
        let manifest = workspace.join(krate).join("Cargo.toml");
        let mut cmd = std::process::Command::new("cargo");
        if let Some(val) = &offline {
            cmd.env("CARGO_NET_OFFLINE", val);
        }
        let mut args: Vec<String> = vec![
            "build".into(),
            "--manifest-path".into(),
            manifest.to_str().unwrap().into(),
            "--target".into(),
            "wasm32-wasip2".into(),
            "--release".into(),
        ];
        if matches!(offline.as_deref(), Some("true")) {
            args.insert(1, "--offline".into());
        }

        let status = cmd
            .current_dir(&workspace)
            .args(args)
            .status()
            .with_context(|| format!("failed to build {krate} component"))?;
        if !status.success() {
            anyhow::bail!("component build failed for {krate}");
        }
        let artifact = workspace.join(format!("target/wasm32-wasip2/release/{}.wasm", krate));
        results.push((name.to_string(), artifact));
    }
    Ok(results)
}

fn demo_flow_ir() -> FlowIR {
    let mut nodes = indexmap::IndexMap::new();
    nodes.insert(
        "qa".into(),
        NodeIR {
            component: "component.exec".into(),
            payload_expr: serde_json::json!({
                "component": "qa.process",
                "operation": "process",
                "input": { "text": "hello" }
            }),
            routes: vec![RouteIR {
                to: Some("emit".into()),
                out: false,
            }],
        },
    );
    nodes.insert(
        "emit".into(),
        NodeIR {
            component: "emit.response".into(),
            payload_expr: serde_json::json!({
                "text": "Echo: {{node.qa.text}}"
            }),
            routes: vec![RouteIR {
                to: None,
                out: true,
            }],
        },
    );
    FlowIR {
        id: "demo.flow".into(),
        flow_type: "messaging".into(),
        start: Some("qa".into()),
        parameters: serde_json::Value::Object(Default::default()),
        nodes,
    }
}

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";

fn host_config(bindings_path: &Path) -> HostConfig {
    HostConfig {
        tenant: "demo".into(),
        bindings_path: bindings_path.to_path_buf(),
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
        agents: std::collections::HashMap::new(),
        #[cfg(feature = "agentic-worker")]
        graphs: std::collections::HashMap::new(),
    }
}

fn legacy_component_exec_flow(flow_id: &str, message: &str) -> Result<Flow> {
    let node_id = NodeId::from_str("exec")?;
    let mut nodes = HashMap::new();
    nodes.insert(
        node_id.clone(),
        Node {
            id: node_id.clone(),
            component: FlowComponentRef {
                id: "component.exec".parse()?,
                pack_alias: None,
                operation: None,
            },
            input: InputMapping {
                mapping: serde_json::json!({
                    "component": "qa.process",
                    "operation": "process",
                    "input": { "message": message }
                }),
            },
            output: OutputMapping {
                mapping: Value::Null,
            },
            err_map: None,
            routing: Routing::End,
            telemetry: TelemetryHints::default(),
            conversational: false,
        },
    );
    Ok(Flow {
        schema_version: "1.0".into(),
        id: FlowId::from_str(flow_id)?,
        kind: FlowKind::Messaging,
        entrypoints: BTreeMap::from([("default".to_string(), Value::String(node_id.to_string()))]),
        nodes: nodes.into_iter().collect(),
        metadata: Default::default(),
    })
}

fn component_artifact_path(temp_dir: &Path) -> Result<PathBuf> {
    let local =
        workspace_root().join("tests/fixtures/packs/runner-components/components/qa_process.wasm");
    if local.exists() {
        return Ok(local);
    }
    let archive_path =
        workspace_root().join("tests/fixtures/packs/runner-components/runner-components.gtpack");
    let mut archive = ZipArchive::new(File::open(&archive_path).context("open fixture gtpack")?)?;
    let mut wasm = archive
        .by_name("components/qa.process@0.1.0/component.wasm")
        .context("qa.process component missing from fixture pack")?;
    let out = temp_dir.join("qa_process.wasm");
    let mut buf = Vec::new();
    wasm.read_to_end(&mut buf)?;
    std::fs::write(&out, &buf)?;
    Ok(out)
}

fn build_pack(flow_yaml: &str, pack_path: &Path) -> Result<()> {
    let component_path = component_artifact_path(
        pack_path
            .parent()
            .expect("pack path should have parent for temp dir"),
    )?;
    let (_bundle, flow) = load_and_validate_bundle_with_flow(flow_yaml, None)?;
    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "component.exec.test".parse()?,
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "qa.process".parse()?,
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
        }],
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

    let mut zip = zip::ZipWriter::new(File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&manifest_bytes)?;

    zip.start_file("components/qa.process.wasm", options)?;
    let mut comp_file = File::open(&component_path)?;
    std::io::copy(&mut comp_file, &mut zip)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

fn build_component_exec_flow_with_input(flow_id: &str, input: Value) -> Result<Flow> {
    let node_id = NodeId::from_str("run").context("build node id")?;
    let mut nodes = HashMap::new();
    nodes.insert(
        node_id.clone(),
        Node {
            id: node_id.clone(),
            component: FlowComponentRef {
                id: "qa.process".parse()?,
                pack_alias: None,
                operation: Some("process".into()),
            },
            input: InputMapping { mapping: input },
            output: OutputMapping {
                mapping: Value::Null,
            },
            err_map: None,
            routing: Routing::End,
            telemetry: TelemetryHints::default(),
            conversational: false,
        },
    );
    Ok(Flow {
        schema_version: "1.0".into(),
        id: FlowId::from_str(flow_id)?,
        kind: FlowKind::Messaging,
        entrypoints: BTreeMap::from([("default".into(), Value::String(node_id.to_string()))]),
        nodes: nodes.into_iter().collect(),
        metadata: FlowMetadata::default(),
    })
}

fn build_pack_with_flow_id(pack_id: &str, flow: Flow, pack_path: &Path) -> Result<()> {
    let component_path = component_artifact_path(
        pack_path
            .parent()
            .expect("pack path should have parent for temp dir"),
    )?;
    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: pack_id.parse()?,
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "qa.process".parse()?,
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
        }],
        flows: vec![PackFlowEntry {
            id: flow.id.clone(),
            kind: flow.kind,
            flow,
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

    let mut zip = zip::ZipWriter::new(File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&manifest_bytes)?;

    zip.start_file("components/qa.process.wasm", options)?;
    let mut comp_file = File::open(&component_path)?;
    std::io::copy(&mut comp_file, &mut zip)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

fn build_pack_with_runtime_extension(
    manifest_flows: Vec<Flow>,
    runtime_extension: Value,
    pack_path: &Path,
) -> Result<()> {
    let component_path = component_artifact_path(
        pack_path
            .parent()
            .expect("pack path should have parent for temp dir"),
    )?;

    let flow_entries = manifest_flows
        .iter()
        .map(|flow| PackFlowEntry {
            id: flow.id.clone(),
            kind: flow.kind,
            flow: flow.clone(),
            tags: Vec::new(),
            entrypoints: vec!["default".into()],
        })
        .collect::<Vec<_>>();

    let mut extensions = BTreeMap::new();
    extensions.insert(
        RUNTIME_FLOW_EXTENSION_ID.to_string(),
        ExtensionRef {
            kind: RUNTIME_FLOW_EXTENSION_ID.to_string(),
            version: "2.0.0".into(),
            digest: None,
            location: None,
            inline: Some(ExtensionInline::Other(runtime_extension)),
        },
    );

    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "component.exec.runtime".parse()?,
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "qa.process".parse()?,
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
        }],
        flows: flow_entries,
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        extensions: Some(extensions),
    };

    let mut zip = zip::ZipWriter::new(File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&manifest_bytes)?;

    zip.start_file("components/qa.process.wasm", options)?;
    let mut comp_file = File::open(&component_path)?;
    std::io::copy(&mut comp_file, &mut zip)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

static RUNTIME: Lazy<&'static tokio::runtime::Runtime> = Lazy::new(|| {
    Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime"),
    ))
});

#[test]
fn component_exec_invokes_pack_component() -> Result<()> {
    let temp = TempDir::new()?;
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let components = build_components()?;
    let flow = demo_flow_ir();
    let mut flows = HashMap::new();
    flows.insert(flow.id.clone(), flow);

    let config = Arc::new(host_config(&bindings_path));
    let runtime = Arc::new(
        PackRuntime::for_component_test(
            components,
            flows.clone(),
            "test-pack",
            Arc::clone(&config),
        )
        .context("pack init")?,
    );
    // Ensure the runtime constructed successfully with components and flows in place.
    let _ = (runtime, config, flows);
    Ok(())
}

#[test]
fn exec_node_uses_inner_component_artifact() -> Result<()> {
    // Regression: component.exec is a meta-component and must call the referenced pack artifact.
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("component-exec.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    // Build a pack whose flow uses component.exec to call qa.process.
    let flow_yaml = r#"
id: exec.flow
type: messaging
start: exec
nodes:
  exec:
    component.exec:
      component: qa.process
      operation: process
      input:
        payload:
          text: "hello"
        metadata:
          __return_envelope: true
    routing:
      - out: true
"#;
    build_pack(flow_yaml, &pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        &pack_path,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);
    let engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;

    let retry_config = config.retry.clone().into();
    let tenant = config.tenant.clone();
    let flow_id = "exec.flow".to_string();
    let ctx = FlowContext {
        tenant: tenant.as_str(),
        pack_id: pack.metadata().pack_id.as_str(),
        flow_id: flow_id.as_str(),
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        reply_scope: None,
        retry_config,
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    };

    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("component.exec flow run")?;
    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => {
            anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason);
        }
    }

    let payload = envelope_payload(&execution.output)?;
    assert_eq!(payload, json!({ "text": "hello" }));
    Ok(())
}

#[test]
fn component_exec_preserves_flow_node_config_for_wasm_components() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("component-exec-config.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let flow_yaml = r#"
id: exec.config.flow
type: messaging
start: exec
nodes:
  exec:
    component.exec:
      component: qa.process
      operation: process
      config:
        provider: ollama
        default_model: llama3.2
        base_url: http://127.0.0.1:11434/v1
      input:
        messages:
          - role: user
            content: hello
    routing:
      - out: true
"#;
    build_pack(flow_yaml, &pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        &pack_path,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);
    let engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;

    let retry_config = config.retry.clone().into();
    let ctx = FlowContext {
        tenant: config.tenant.as_str(),
        pack_id: pack.metadata().pack_id.as_str(),
        flow_id: "exec.config.flow",
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        reply_scope: None,
        retry_config,
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    };

    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("component.exec flow with config run")?;
    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => {
            anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason);
        }
    }

    let payload = envelope_payload(&execution.output)?;
    assert_eq!(
        payload,
        json!({
            "config": {
                "provider": "ollama",
                "default_model": "llama3.2",
                "base_url": "http://127.0.0.1:11434/v1"
            },
            "input": {
                "messages": [{
                    "role": "user",
                    "content": "hello"
                }]
            }
        })
    );
    Ok(())
}

#[test]
fn component_exec_always_serializes_invocation_envelope() -> Result<()> {
    let rt = *RUNTIME;
    let config_temp = TempDir::new()?;
    let bindings_path = config_temp.path().join("bindings.yaml");
    std::fs::write(
        &bindings_path,
        b"tenant: demo\n\
flow_type_bindings: {}\n\
rate_limits: {}\n\
retry: {}\n\
timers: []\n",
    )?;
    let config = Arc::new(host_config(&bindings_path));
    let retry_config = config.retry.clone().into();
    let pack_dir = TempDir::new()?;

    struct InvocationCase {
        suffix: &'static str,
        input: Value,
        expected_payload: Value,
        expected_metadata: Value,
    }

    let cases = vec![
        InvocationCase {
            suffix: "envelope",
            input: json!({
                "envelope": {
                    "ctx": {
                        "tenant": "spoof",
                        "env": "spoof-env",
                        "flow_id": "spoof.flow",
                        "node_id": "spoof.node",
                        "provider_id": "spoof.provider"
                    },
                    "flow_id": "spoof.flow",
                    "node_id": "spoof.node",
                    "op": "process",
                    "payload": { "message": "from envelope" },
                    "metadata": { "source": "envelope", "__return_envelope": true }
                }
            }),
            expected_payload: json!({ "message": "from envelope" }),
            expected_metadata: json!({ "source": "envelope", "__return_envelope": true }),
        },
        InvocationCase {
            suffix: "partial",
            input: json!({
                "op": "process",
                "payload": { "message": "partial" },
                "metadata": { "source": "partial", "__return_envelope": true }
            }),
            expected_payload: json!({ "message": "partial" }),
            expected_metadata: json!({ "source": "partial", "__return_envelope": true }),
        },
    ];

    for case in cases {
        let flow_id = format!("ctx.flow.{}", case.suffix);
        let flow = build_component_exec_flow_with_input(&flow_id, case.input.clone())?;
        let pack_path = pack_dir
            .path()
            .join(format!("component.exec.ctx.{}.gtpack", case.suffix));
        build_pack_with_flow_id(
            &format!("component.exec.ctx.{}", case.suffix),
            flow.clone(),
            &pack_path,
        )?;

        let pack = Arc::new(rt.block_on(PackRuntime::load(
            &pack_path,
            Arc::clone(&config),
            None,
            Some(&pack_path),
            None,
            None,
            Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
            greentic_runner_host::secrets::default_manager()?,
            None,
            false,
            ComponentResolution::default(),
        ))?);
        let engine = rt.block_on(FlowEngine::new(
            vec![Arc::clone(&pack)],
            Arc::clone(&config),
        ))?;

        let ctx = FlowContext {
            tenant: config.tenant.as_str(),
            pack_id: pack.metadata().pack_id.as_str(),
            flow_id: flow.id.as_str(),
            node_id: None,
            tool: None,
            action: None,
            session_id: None,
            provider_id: None,
            reply_scope: None,
            retry_config,
            attempt: 1,
            observer: None,
            mocks: None,
            caller: None,
        };

        let execution = rt
            .block_on(engine.execute(ctx, Value::Null))
            .context("execute component.exec flow")?;
        match execution.status {
            FlowStatus::Completed => {}
            other => panic!("flow {} paused unexpectedly: {:?}", flow.id, other),
        }

        let envelope_value = execution.output;
        let envelope = envelope_value
            .as_object()
            .context("expected component output object")?;
        assert_eq!(
            envelope
                .get("flow_id")
                .and_then(Value::as_str)
                .context("envelope flow_id missing")?,
            flow.id.as_str()
        );
        assert_eq!(
            envelope
                .get("node_id")
                .and_then(Value::as_str)
                .context("envelope node_id missing")?,
            "run"
        );
        assert_eq!(
            envelope
                .get("op")
                .and_then(Value::as_str)
                .context("envelope op missing")?,
            "process"
        );

        let ctx_value = envelope
            .get("ctx")
            .and_then(Value::as_object)
            .context("ctx missing")?;
        assert_eq!(
            ctx_value
                .get("tenant")
                .and_then(Value::as_str)
                .context("ctx tenant missing")?,
            config.tenant.as_str()
        );
        assert_eq!(
            ctx_value
                .get("flow_id")
                .and_then(Value::as_str)
                .context("ctx flow_id missing")?,
            flow.id.as_str()
        );
        assert_eq!(
            ctx_value
                .get("node_id")
                .and_then(Value::as_str)
                .context("ctx node_id missing")?,
            "run"
        );

        let payload = decode_binary_value(envelope.get("payload").context("payload missing")?)?;
        assert_eq!(payload, case.expected_payload);

        let metadata = decode_binary_value(envelope.get("metadata").context("metadata missing")?)?;
        assert_eq!(metadata, case.expected_metadata);
    }

    Ok(())
}

#[test]
fn emit_log_is_builtin_not_pack_component() -> Result<()> {
    // Regression: emit.log should be treated as a built-in, not looked up as a pack artifact.
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("emit-log.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let flow_yaml = r#"
id: emit.flow
type: messaging
start: exec
nodes:
  exec:
    component.exec:
      component: qa.process
      operation: process
      input:
        text: "hello"
    routing:
      - to: log
  log:
    emit.log:
      message: "logged"
    routing:
      - out: true
"#;
    build_pack(flow_yaml, &pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        &pack_path,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);
    let engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;

    let retry_config = config.retry.clone().into();
    let tenant = config.tenant.clone();
    let flow_id = "emit.flow".to_string();
    let ctx = FlowContext {
        tenant: tenant.as_str(),
        pack_id: pack.metadata().pack_id.as_str(),
        flow_id: flow_id.as_str(),
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        reply_scope: None,
        retry_config,
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    };

    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("emit.log flow run")?;
    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => {
            anyhow::bail!("emit flow paused unexpectedly: {:?}", wait.reason);
        }
    }

    let output_str = execution.output.to_string();
    assert!(
        output_str.contains("logged"),
        "expected emit.log to produce output; got {output_str}"
    );
    Ok(())
}

#[test]
fn runtime_extension_flow_overrides_manifest_flow() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("runtime-extension.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let legacy_flow = legacy_component_exec_flow("exec.flow", "legacy")?;
    let runtime_flow = serde_json::json!({
        "id": "exec.flow",
        "flow_type": "messaging",
        "start": "exec",
        "nodes": {
            "exec": {
                "component_id": "qa.process",
                "operation_name": "process",
                "operation_payload": {
                    "payload": { "message": "resolved" },
                    "metadata": { "__return_envelope": true }
                },
                "routing": "end"
            }
        }
    });
    let runtime_extension = serde_json::json!({ "flows": [runtime_flow] });
    build_pack_with_runtime_extension(vec![legacy_flow], runtime_extension, &pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        &pack_path,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);
    let engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;

    let retry_config = config.retry.clone().into();
    let ctx = FlowContext {
        tenant: config.tenant.as_str(),
        pack_id: pack.metadata().pack_id.as_str(),
        flow_id: "exec.flow",
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        reply_scope: None,
        retry_config,
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    };

    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("runtime extension flow run")?;
    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => {
            anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason);
        }
    }

    let payload = envelope_payload(&execution.output)?;
    assert_eq!(payload["message"], serde_json::json!("resolved"));
    Ok(())
}

fn decode_binary_value(value: &Value) -> Result<Value> {
    let bytes = to_bytes(value)?;
    serde_json::from_slice(&bytes).context("decode binary payload")
}

fn to_bytes(value: &Value) -> Result<Vec<u8>> {
    let array = value.as_array().context("binary payload is not an array")?;
    let mut bytes = Vec::with_capacity(array.len());
    for entry in array {
        let num = entry
            .as_u64()
            .context("binary payload entry is not an integer")?;
        if num > u64::from(u8::MAX) {
            return Err(anyhow!("binary payload entry {} exceeds byte range", num));
        }
        bytes.push(num as u8);
    }
    Ok(bytes)
}

fn envelope_payload(value: &Value) -> Result<Value> {
    let envelope = value
        .as_object()
        .context("expected component output envelope")?;
    let payload_value = envelope
        .get("payload")
        .context("envelope payload missing")?;
    decode_binary_value(payload_value)
}
