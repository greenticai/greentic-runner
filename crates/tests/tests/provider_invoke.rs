use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine};
use greentic_runner_host::secrets::default_manager;
use greentic_runner_host::{
    HostConfig, PreopenSpec, RunnerWasiPolicy,
    storage::{DynSessionStore, state::new_state_store},
};
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentOperation, ComponentProfiles, Flow,
    FlowComponentRef, FlowId, FlowKind, FlowMetadata, InputMapping, Node, NodeId, OutputMapping,
    PackFlowEntry, PackKind, PackManifest, ResourceHints, Routing, TelemetryHints,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use zip::write::FileOptions;
use zip::{ZipArchive, ZipWriter};

static WASI_POLICY: Lazy<Arc<RunnerWasiPolicy>> = Lazy::new(|| {
    Arc::new(
        RunnerWasiPolicy::new()
            .inherit_stdio(false)
            .with_preopen(PreopenSpec::new(".", "/").read_only(true)),
    )
});

#[tokio::test]
async fn provider_invoke_echoes_payload() -> Result<()> {
    let config = write_minimal_config()?;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("provider-dummy.gtpack");
    let component_path = build_dummy_component()?;
    let flows = vec![build_flow(
        "provider.echo",
        FlowKind::Job,
        json!({"echo": "/state/input/message"}),
        json!({"echoed": "/result/echo"}),
    )?];
    build_pack(&component_path, &pack_path, &flows)?;

    let pack = Arc::new(
        PackRuntime::load(
            &pack_path,
            Arc::clone(&config),
            None,
            Some(&pack_path),
            None::<DynSessionStore>,
            Some(new_state_store()),
            Arc::clone(&WASI_POLICY),
            default_manager()?,
            None,
            false,
            ComponentResolution::default(),
        )
        .await?,
    );
    let engine = FlowEngine::new(vec![Arc::clone(&pack)], Arc::clone(&config)).await?;
    let retry_config = config.retry_config().into();
    let ctx = FlowContext {
        tenant: config.tenant.as_str(),
        pack_id: pack.metadata().pack_id.as_str(),
        flow_id: "provider.echo",
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        retry_config,
        attempt: 1,
        observer: None,
        mocks: None,
        reply_scope: None,
        caller: None,
    };

    let input = json!({"message": "hello world"});
    let execution = engine.execute(ctx, input).await?;
    match execution.status {
        greentic_runner_host::runner::engine::FlowStatus::Completed => {}
        other => return Err(anyhow!("flow did not complete: {:?}", other)),
    }
    assert_eq!(
        execution.output,
        json!({"echoed": "hello world"}),
        "provider invoke should map output"
    );
    Ok(())
}

#[tokio::test]
async fn provider_invoke_supports_messaging_secrets_events() -> Result<()> {
    let _flag = EnvGuard::set("GREENTIC_PROVIDER_CORE_ONLY", "1");
    let config = write_minimal_config()?;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("provider-dummy.gtpack");
    let component_path = build_dummy_component()?;

    let flows = vec![
        build_flow(
            "provider.messaging",
            FlowKind::Messaging,
            json!({ "echo": "/state/input/message" }),
            json!({ "echoed": "/result/echo" }),
        )?,
        build_flow(
            "provider.secrets",
            FlowKind::Job,
            json!({ "echo": "/state/input/secret" }),
            json!({ "secret_echo": "/result/echo" }),
        )?,
        build_flow(
            "provider.events",
            FlowKind::Event,
            json!({ "echo": "/state/input/event" }),
            json!({ "event_echo": "/result/echo" }),
        )?,
    ];

    build_pack(&component_path, &pack_path, &flows)?;

    let pack = Arc::new(
        PackRuntime::load(
            &pack_path,
            Arc::clone(&config),
            None,
            Some(&pack_path),
            None::<DynSessionStore>,
            Some(new_state_store()),
            Arc::clone(&WASI_POLICY),
            default_manager()?,
            None,
            false,
            ComponentResolution::default(),
        )
        .await?,
    );
    let engine = FlowEngine::new(vec![Arc::clone(&pack)], Arc::clone(&config)).await?;
    let retry_config = config.retry_config().into();

    let cases = vec![
        (
            "provider.messaging",
            json!({"message": "hi"}),
            json!({"echoed": "hi"}),
            Some("messaging"),
        ),
        (
            "provider.secrets",
            json!({"secret": "keep-me"}),
            json!({"secret_echo": "keep-me"}),
            Some("secrets"),
        ),
        (
            "provider.events",
            json!({"event": "ping"}),
            json!({"event_echo": "ping"}),
            Some("events"),
        ),
    ];

    for (flow_id, input, expected, action) in cases {
        let ctx = FlowContext {
            tenant: config.tenant.as_str(),
            pack_id: pack.metadata().pack_id.as_str(),
            flow_id,
            node_id: None,
            tool: None,
            action,
            session_id: None,
            provider_id: None,
            retry_config,
            attempt: 1,
            observer: None,
            mocks: None,
            reply_scope: None,
            caller: None,
        };

        let execution = engine.execute(ctx, input).await?;
        match execution.status {
            greentic_runner_host::runner::engine::FlowStatus::Completed => {}
            other => return Err(anyhow!("flow {flow_id} did not complete: {:?}", other)),
        }
        assert_eq!(
            execution.output, expected,
            "{flow_id} should map provider-core output"
        );
    }

    Ok(())
}

#[tokio::test]
async fn component_exec_carries_operation_from_flow() -> Result<()> {
    let config = write_minimal_config()?;
    let temp = TempDir::new()?;
    let component_path = component_artifact_path(temp.path())?;

    let flow_a = build_component_exec_flow("pack-a.flow", "pack A")?;
    let flow_b = build_component_exec_flow("pack-b.flow", "pack B")?;
    let pack_a_path = temp.path().join("pack-a.gtpack");
    let pack_b_path = temp.path().join("pack-b.gtpack");
    build_component_pack("pack.a", &pack_a_path, &component_path, &[flow_a])?;
    build_component_pack(
        "pack.b",
        &pack_b_path,
        &component_path,
        std::slice::from_ref(&flow_b),
    )?;

    let pack_a = Arc::new(
        PackRuntime::load(
            &pack_a_path,
            Arc::clone(&config),
            None,
            Some(&pack_a_path),
            None::<DynSessionStore>,
            None,
            Arc::clone(&WASI_POLICY),
            default_manager()?,
            None,
            false,
            ComponentResolution::default(),
        )
        .await?,
    );
    let pack_b = Arc::new(
        PackRuntime::load(
            &pack_b_path,
            Arc::clone(&config),
            None,
            Some(&pack_b_path),
            None::<DynSessionStore>,
            None,
            Arc::clone(&WASI_POLICY),
            default_manager()?,
            None,
            false,
            ComponentResolution::default(),
        )
        .await?,
    );
    let engine = FlowEngine::new(vec![pack_a, Arc::clone(&pack_b)], Arc::clone(&config)).await?;
    let retry_config = config.retry_config().into();
    let ctx = FlowContext {
        tenant: config.tenant.as_str(),
        pack_id: pack_b.metadata().pack_id.as_str(),
        flow_id: flow_b.id.as_str(),
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        retry_config,
        attempt: 1,
        observer: None,
        mocks: None,
        reply_scope: None,
        caller: None,
    };

    let execution = engine.execute(ctx, json!({})).await?;
    match execution.status {
        greentic_runner_host::runner::engine::FlowStatus::Completed => {}
        other => panic!("flow should complete, got {other:?}"),
    }
    let payload = envelope_payload(&execution.output)?;
    assert_eq!(payload["message"], json!("pack B"));
    Ok(())
}

fn write_minimal_config() -> Result<Arc<HostConfig>> {
    let temp = TempDir::new()?;
    let path = temp.path().join("bindings.yaml");
    let contents = r#"
tenant: demo
flow_type_bindings: {}
rate_limits: {}
retry: {}
timers: []
"#;
    std::fs::write(&path, contents)?;
    let mut cfg = HostConfig::load_from_path(&path).context("load minimal host bindings")?;
    cfg.secrets_policy = greentic_runner_host::config::SecretsPolicy::allow_all();
    Ok(Arc::new(cfg))
}

fn build_pack(component_path: &Path, pack_path: &Path, flows: &[Flow]) -> Result<()> {
    let extensions = provider_extension();
    let flow_entries = flows
        .iter()
        .map(|flow| PackFlowEntry {
            id: flow.id.clone(),
            kind: flow.kind,
            flow: flow.clone(),
            tags: Vec::new(),
            entrypoints: vec!["default".into()],
        })
        .collect::<Vec<_>>();
    let supported_kinds = flows.iter().map(|flow| flow.kind).collect::<Vec<_>>();
    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "provider.test".parse()?,
        name: None,
        version: Version::parse("0.0.1")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "provider.dummy".parse()?,
            version: Version::parse("0.1.0")?,
            supports: supported_kinds,
            world: "greentic:provider-core@1.0.0".into(),
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

    let mut writer =
        ZipWriter::new(std::fs::File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = greentic_types::encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    writer.start_file("components/provider.dummy.wasm", options)?;
    let mut file = std::fs::File::open(component_path)
        .with_context(|| format!("open component {}", component_path.display()))?;
    std::io::copy(&mut file, &mut writer)?;
    writer.finish().context("finalise provider pack")?;
    Ok(())
}

fn build_component_pack(
    pack_id: &str,
    pack_path: &Path,
    component_path: &Path,
    flows: &[Flow],
) -> Result<()> {
    let flow_entries = flows
        .iter()
        .map(|flow| PackFlowEntry {
            id: flow.id.clone(),
            kind: flow.kind,
            flow: flow.clone(),
            tags: Vec::new(),
            entrypoints: vec!["default".into()],
        })
        .collect::<Vec<_>>();
    let supported_kinds = flows.iter().map(|flow| flow.kind).collect::<Vec<_>>();
    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: pack_id.parse()?,
        name: None,
        version: Version::parse("0.0.1")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "qa.process".parse()?,
            version: Version::parse("0.1.0")?,
            supports: supported_kinds,
            world: "greentic:component@0.4.0".into(),
            profiles: ComponentProfiles::default(),
            capabilities: ComponentCapabilities::default(),
            configurators: None,
            operations: vec![ComponentOperation {
                name: "process".into(),
                input_schema: Value::Null,
                output_schema: Value::Null,
            }],
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
        extensions: None,
    };

    let mut writer =
        ZipWriter::new(std::fs::File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = greentic_types::encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    writer.start_file("components/qa.process.wasm", options)?;
    let mut file = std::fs::File::open(component_path)
        .with_context(|| format!("open component {}", component_path.display()))?;
    std::io::copy(&mut file, &mut writer)?;
    writer.finish().context("finalise component pack")?;
    Ok(())
}

fn component_artifact_path(temp_dir: &Path) -> Result<PathBuf> {
    let local = fixture_path("tests/fixtures/packs/runner-components/components/qa_process.wasm");
    if local.exists() {
        return Ok(local);
    }

    let archive = fixture_path("tests/fixtures/packs/runner-components/runner-components.gtpack");
    let mut zip = ZipArchive::new(File::open(&archive).context("open fixture gtpack")?)?;
    let mut wasm = zip
        .by_name("components/qa.process@0.1.0/component.wasm")
        .context("qa.process component missing from fixture pack")?;
    let out = temp_dir.join("qa_process.wasm");
    let mut buf = Vec::new();
    wasm.read_to_end(&mut buf)?;
    std::fs::write(&out, &buf)?;
    Ok(out)
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

fn provider_extension() -> BTreeMap<String, greentic_types::ExtensionRef> {
    let mut exts = BTreeMap::new();
    let inline = greentic_types::ProviderExtensionInline {
        providers: vec![greentic_types::ProviderDecl {
            provider_type: "example.dummy".into(),
            capabilities: Vec::new(),
            ops: vec!["echo".into()],
            config_schema_ref: "schemas/config.schema.json".into(),
            state_schema_ref: Some("schemas/state.schema.json".into()),
            runtime: greentic_types::ProviderRuntimeRef {
                component_ref: "provider.dummy".into(),
                export: "provider-core".into(),
                world: "greentic:provider-core@1.0.0".into(),
            },
            docs_ref: Some("schemas/README.md".into()),
        }],
        ..Default::default()
    };
    exts.insert(
        greentic_types::PROVIDER_EXTENSION_ID.to_string(),
        greentic_types::ExtensionRef {
            kind: greentic_types::PROVIDER_EXTENSION_ID.to_string(),
            version: "1.0.0".into(),
            digest: None,
            location: None,
            inline: Some(greentic_types::ExtensionInline::Provider(inline)),
        },
    );
    exts
}

fn build_flow(flow_id: &str, flow_kind: FlowKind, in_map: Value, out_map: Value) -> Result<Flow> {
    let node_id = NodeId::from_str("provider").context("node id")?;
    let mut nodes = HashMap::new();
    nodes.insert(
        node_id.clone(),
        Node {
            id: node_id.clone(),
            component: FlowComponentRef {
                id: "provider.invoke".parse()?,
                pack_alias: None,
                operation: None,
            },
            input: InputMapping {
                mapping: json!({
                    "provider_type": "example.dummy",
                    "op": "echo",
                    "in_map": in_map,
                    "out_map": out_map
                }),
            },
            output: OutputMapping {
                mapping: Value::Object(serde_json::Map::new()),
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
        kind: flow_kind,
        entrypoints: BTreeMap::from([("default".to_string(), Value::String(node_id.to_string()))]),
        nodes: nodes.into_iter().collect(),
        metadata: FlowMetadata::default(),
    })
}

fn build_component_exec_flow(flow_id: &str, message: &str) -> Result<Flow> {
    let node_id = NodeId::from_str("exec").context("node id")?;
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
            input: InputMapping {
                mapping: json!({
                    "payload": { "message": message },
                    "metadata": { "__return_envelope": true }
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
        entrypoints: BTreeMap::from([("default".into(), Value::String(node_id.to_string()))]),
        nodes: nodes.into_iter().collect(),
        metadata: FlowMetadata::default(),
    })
}

fn build_dummy_component() -> Result<PathBuf> {
    let root = fixture_path("tests/assets/provider-core-dummy");
    let wasm = root.join("target/wasm32-wasip2/release/provider_core_dummy.wasm");
    if !wasm.exists() {
        let offline = std::env::var("CARGO_NET_OFFLINE").ok();
        let status = Command::new("cargo")
            .args([
                "build",
                "--release",
                "--target",
                "wasm32-wasip2",
                "--manifest-path",
                root.join("Cargo.toml").to_str().expect("manifest path"),
            ])
            .envs(offline.map(|val| ("CARGO_NET_OFFLINE", val)))
            .status()
            .context("build provider-core dummy component")?;
        if !status.success() {
            return Err(anyhow!("provider-core dummy build failed with {status}"));
        }
    }
    Ok(wasm)
}

fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(relative)
}

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(val) => unsafe {
                std::env::set_var(self.key, val);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}
