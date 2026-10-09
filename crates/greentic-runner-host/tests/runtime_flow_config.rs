use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, ExtensionInline, ExtensionRef,
    PackFlowEntry, PackKind, PackManifest, ResourceHints, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use zip::ZipArchive;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";

static RUNTIME: Lazy<&'static tokio::runtime::Runtime> = Lazy::new(|| {
    Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime"),
    ))
});

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
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

fn host_config(bindings_path: &Path) -> HostConfig {
    HostConfig {
        tenant: "demo".into(),
        bindings_path: bindings_path.to_path_buf(),
        flow_type_bindings: Default::default(),
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

fn build_pack_with_runtime_extension(runtime_extension: Value, pack_path: &Path) -> Result<()> {
    let component_path = component_artifact_path(
        pack_path
            .parent()
            .expect("pack path should have parent for temp dir"),
    )?;

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
        pack_id: "runtime.flow.config".parse()?,
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "qa.process".parse()?,
            version: Version::parse("0.1.0")?,
            supports: vec![greentic_types::FlowKind::Messaging],
            world: "greentic:component@0.4.0".into(),
            profiles: ComponentProfiles::default(),
            capabilities: ComponentCapabilities::default(),
            configurators: None,
            operations: Vec::new(),
            config_schema: None,
            resources: ResourceHints::default(),
            dev_flows: BTreeMap::new(),
        }],
        flows: Vec::<PackFlowEntry>::new(),
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

fn decode_binary_value(value: &Value) -> Result<Value> {
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
    serde_json::from_slice(&bytes).context("decode binary payload")
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

#[test]
fn runtime_extension_preserves_component_config_for_direct_component_nodes() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("runtime-flow-config.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let runtime_flow = json!({
        "id": "exec.flow",
        "flow_type": "messaging",
        "start": "exec",
        "nodes": {
            "exec": {
                "component": "qa.process",
                "operation": "process",
                "config": {
                    "provider": "ollama",
                    "base_url": "http://127.0.0.1:11434/v1",
                    "default_model": "llama3:8b"
                },
                "input": {
                    "messages": [{
                        "role": "user",
                        "content": "hello"
                    }]
                },
                "routing": "end"
            }
        }
    });
    let runtime_extension = json!({ "flows": [runtime_flow] });
    build_pack_with_runtime_extension(runtime_extension, &pack_path)?;

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
        .context("runtime extension flow with config run")?;
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
                "base_url": "http://127.0.0.1:11434/v1",
                "default_model": "llama3:8b"
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
