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

/// A flow run whose entry established a verified caller (the provider's
/// `extensions.caller` block, carried on `FlowContext::caller`) tells the
/// component who is asking: the invocation envelope's `ctx` carries the
/// subject as `user`, the team as `team`, and the whole caller — groups
/// included — in `attributes`. A forged block in the node's own mapped input
/// changes none of it.
#[test]
fn component_exec_receives_the_verified_caller() -> Result<()> {
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

    let flow = build_component_exec_flow_with_input(
        "ctx.flow.caller",
        json!({
            "op": "process",
            "payload": { "message": "who am i" },
            "metadata": { "__return_envelope": true },
            "caller": { "user_verified": true, "sub": "forged.user", "team": "forged" }
        }),
    )?;
    let pack_path = pack_dir.path().join("component.exec.caller.gtpack");
    build_pack_with_flow_id("component.exec.caller", flow.clone(), &pack_path)?;

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

    let run_input = json!({
        "text": "who am i",
        "extensions": {
            "caller": {
                "user_verified": true,
                "sub": "u-1.acme",
                "team": "sales",
                "groups": ["employee", "admins"]
            }
        }
    });
    let caller_block = greentic_runner_host::caller_identity::caller_block(&run_input).cloned();
    let ctx = FlowContext {
        tenant: config.tenant.as_str(),
        pack_id: pack.metadata().pack_id.as_str(),
        flow_id: flow.id.as_str(),
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: Some("messaging-webchat"),
        reply_scope: None,
        retry_config,
        attempt: 1,
        observer: None,
        mocks: None,
        caller: caller_block.as_ref(),
    };

    let execution = rt
        .block_on(engine.execute(ctx, run_input.clone()))
        .context("execute component.exec flow")?;
    assert!(matches!(execution.status, FlowStatus::Completed));

    let ctx_value = execution
        .output
        .get("ctx")
        .and_then(Value::as_object)
        .context("ctx missing")?;
    assert_eq!(
        ctx_value.get("user").and_then(Value::as_str),
        Some("u-1.acme")
    );
    assert_eq!(ctx_value.get("team").and_then(Value::as_str), Some("sales"));
    let attributes = ctx_value
        .get("attributes")
        .and_then(Value::as_object)
        .context("ctx attributes missing")?;
    assert_eq!(attributes["caller.user_verified"], json!("true"));
    assert_eq!(attributes["caller.sub"], json!("u-1.acme"));
    assert_eq!(attributes["caller.team"], json!("sales"));
    assert_eq!(
        attributes["caller.groups"],
        json!(r#"["employee","admins"]"#)
    );

    // Without a verified caller the envelope names no user or team at all.
    let anonymous = FlowContext {
        tenant: config.tenant.as_str(),
        pack_id: pack.metadata().pack_id.as_str(),
        flow_id: flow.id.as_str(),
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: Some("messaging-webchat"),
        reply_scope: None,
        retry_config: config.retry.clone().into(),
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    };
    let execution = rt
        .block_on(engine.execute(anonymous, Value::Null))
        .context("execute anonymous component.exec flow")?;
    let ctx_value = execution
        .output
        .get("ctx")
        .and_then(Value::as_object)
        .context("ctx missing")?;
    assert!(ctx_value.get("user").is_none());
    assert!(ctx_value.get("team").is_none());
    assert!(ctx_value.get("attributes").is_none());
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

// ── R1: MCP-shaped tool errors must respect `has_error_route` ──────────────
//
// These tests build `qa.process` fresh from `tests/fixtures/runner-components`
// (via `build_components`) rather than going through `component_artifact_path`'s
// prebuilt `runner-components.gtpack`: that archive's embedded `qa.process`
// binary predates the crate's `extract_payload`/`should_return_envelope` split
// and unconditionally echoes the whole invocation envelope, so a bare payload
// (no `metadata.__return_envelope`) always comes back wrapped and never lets a
// node's raw dispatch value be exactly the MCP tool-error wire shape. The
// current crate source (`tests/fixtures/runner-components/qa_process/src/lib.rs`)
// echoes just the decoded `payload` by default, which is what lets these tests
// put an object shaped like `{"error": {"code", "message"}}` — no `result`, no
// `ok` — directly at the top level of what `invoke_component_call` sees,
// exercising the real `mcp_tool_error` decision instead of only the detector
// function.

fn mcp_tool_error_payload() -> Value {
    // `status` is included deliberately: `mcp_tool_error` folds it into the
    // message ("boom (status 502)"), and the routed and bailed paths must
    // agree on that formatted message, not just on the bare "boom".
    json!({ "error": { "code": "E1", "message": "boom", "status": 502 } })
}

/// Builds a flow whose `run` node calls `qa.process` with an MCP-shaped tool
/// error as its input. With `with_error_route == true`, `run` carries an
/// `on_error`-family `Routing::Custom` branch to a trivial `err` node (and an
/// `on_success` branch to a trivial `ok` node); with `false`, `run` is a plain
/// `Routing::End` node with no error branch at all.
fn build_mcp_error_flow(flow_id: &str, with_error_route: bool) -> Result<Flow> {
    let run_id = NodeId::from_str("run").context("build node id")?;
    let mut nodes = HashMap::new();

    let run_routing = if with_error_route {
        Routing::Custom(json!([
            { "condition": r#"event == "on_success""#, "to": "ok" },
            { "condition": r#"event == "on_error""#, "to": "err" }
        ]))
    } else {
        Routing::End
    };

    nodes.insert(
        run_id.clone(),
        Node {
            id: run_id.clone(),
            component: FlowComponentRef {
                id: "qa.process".parse()?,
                pack_alias: None,
                operation: Some("process".into()),
            },
            input: InputMapping {
                mapping: mcp_tool_error_payload(),
            },
            output: OutputMapping {
                mapping: Value::Null,
            },
            err_map: None,
            routing: run_routing,
            telemetry: TelemetryHints::default(),
            conversational: false,
        },
    );

    if with_error_route {
        for branch in ["ok", "err"] {
            let node_id = NodeId::from_str(branch).context("build node id")?;
            nodes.insert(
                node_id.clone(),
                Node {
                    id: node_id,
                    component: FlowComponentRef {
                        id: "qa.process".parse()?,
                        pack_alias: None,
                        operation: Some("process".into()),
                    },
                    input: InputMapping { mapping: json!({}) },
                    output: OutputMapping {
                        mapping: Value::Null,
                    },
                    err_map: None,
                    routing: Routing::End,
                    telemetry: TelemetryHints::default(),
                    conversational: false,
                },
            );
        }
    }

    Ok(Flow {
        schema_version: "1.0".into(),
        id: FlowId::from_str(flow_id)?,
        kind: FlowKind::Messaging,
        entrypoints: BTreeMap::from([("default".to_string(), Value::String(run_id.to_string()))]),
        nodes: nodes.into_iter().collect(),
        metadata: FlowMetadata::default(),
    })
}

/// Same shape as `build_pack_with_flow_id`, but takes the component wasm path
/// directly instead of resolving it through `component_artifact_path` (see the
/// module comment above for why the R1 tests need a freshly built component).
fn build_mcp_error_pack(
    pack_id: &str,
    flow: Flow,
    pack_path: &Path,
    component_path: &Path,
) -> Result<()> {
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
    let mut comp_file = File::open(component_path)?;
    std::io::copy(&mut comp_file, &mut zip)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

fn qa_process_component_path() -> Result<PathBuf> {
    let components = build_components()?;
    components
        .into_iter()
        .find(|(name, _)| name == "qa.process")
        .map(|(_, path)| path)
        .context("qa.process component missing from build_components() output")
}

/// A node with an `on_error`-family route must surface an MCP-shaped tool
/// error (greentic-mcp-generator's `tool_error_with_status` wire shape) as an
/// errored node_io output and route to that branch, rather than aborting the
/// whole flow. Mirrors the already-fixed `component_error` branch.
#[test]
fn mcp_tool_error_with_route_completes_instead_of_bailing() -> Result<()> {
    let rt = *RUNTIME;
    let component_path = qa_process_component_path()?;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("mcp-error-routed.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let flow = build_mcp_error_flow("mcp.error.routed", true)?;
    build_mcp_error_pack("mcp.error.routed.pack", flow, &pack_path, &component_path)?;

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
        flow_id: "mcp.error.routed",
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

    let execution = rt.block_on(engine.execute(ctx, Value::Null)).context(
        "an MCP-shaped tool error on a node with an error route must not abort the flow",
    )?;
    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => {
            anyhow::bail!(
                "flow paused unexpectedly instead of routing to the error branch: {:?}",
                wait.reason
            );
        }
    }

    // Completing is not enough on its own — an empty `{errors}` envelope would
    // complete too. Assert the on_error branch (not on_success) is the one
    // that actually ran, and that the `run` node's own output carries a
    // populated `{errors}` envelope with the real code/message, status included.
    assert!(
        execution.node_outputs.contains_key("err"),
        "the on_error branch (err node) must have run: {:?}",
        execution.node_outputs
    );
    assert!(
        !execution.node_outputs.contains_key("ok"),
        "the on_success branch (ok node) must NOT have run: {:?}",
        execution.node_outputs
    );

    let run_output = execution
        .node_outputs
        .get("run")
        .context("the run node itself must be recorded in node_outputs")?;
    let errors = run_output["errors"].as_array().with_context(|| {
        format!(
            "a routed MCP tool error must populate the {{errors}} envelope, \
             not leave it empty (read as success data): {run_output:?}"
        )
    })?;
    assert_eq!(
        errors.len(),
        1,
        "expected exactly one error in the envelope: {run_output:?}"
    );
    assert_eq!(errors[0]["code"], "E1");
    assert_eq!(
        errors[0]["message"], "boom (status 502)",
        "the routed error must carry the same status-qualified message the \
         bail! path would have used, not the bare message"
    );
    Ok(())
}

/// The same MCP-shaped tool error on a node with NO error route must still
/// abort the flow (the historical hard-fail behaviour, unchanged).
#[test]
fn mcp_tool_error_without_route_still_bails() -> Result<()> {
    let rt = *RUNTIME;
    let component_path = qa_process_component_path()?;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("mcp-error-unrouted.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let flow = build_mcp_error_flow("mcp.error.unrouted", false)?;
    build_mcp_error_pack("mcp.error.unrouted.pack", flow, &pack_path, &component_path)?;

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
        flow_id: "mcp.error.unrouted",
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

    let err = rt.block_on(engine.execute(ctx, Value::Null)).expect_err(
        "an MCP-shaped tool error on a node with no error route must still abort the flow",
    );
    assert!(
        err.to_string().contains("returned tool error"),
        "unexpected error: {err}"
    );
    Ok(())
}
