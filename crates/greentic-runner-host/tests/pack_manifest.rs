use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use greentic_flow::flow_bundle::load_and_validate_bundle_with_flow;
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::storage::new_state_store;
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_runner_host::wasi::RunnerWasiPolicy;
use greentic_types::{
    ArtifactLocationV1, ComponentCapabilities, ComponentManifest, ComponentProfiles,
    ComponentSourceEntryV1, ComponentSourceRef, ComponentSourcesV1, FlowKind, HostCapabilities,
    PackFlowEntry, PackKind, PackManifest, ResolvedComponentV1, ResourceHints, StateCapabilities,
    encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::json;
use sha2::Digest;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;
use zip::ZipWriter;
use zip::write::FileOptions;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
}

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

fn build_pack(pack_path: &Path) -> Result<()> {
    let fixtures = workspace_root().join("tests/fixtures/packs/runner-components");
    let flow_yaml =
        std::fs::read_to_string(fixtures.join("flows/demo.yaml")).context("read flow yaml")?;
    let (_bundle, flow) = load_and_validate_bundle_with_flow(&flow_yaml, None)?;

    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "runner.components.test".parse()?,
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![
            ComponentManifest {
                id: "qa.process".parse()?,
                version: Version::parse("0.1.0")?,
                supports: vec![FlowKind::Messaging],
                world: "greentic:component@0.4.0".into(),
                profiles: ComponentProfiles::default(),
                capabilities: ComponentCapabilities::default(),
                configurators: None,
                operations: vec![greentic_types::ComponentOperation {
                    name: "process".into(),
                    input_schema: json!({
                        "type": "object",
                        "properties": { "text": { "type": "string" } }
                    }),
                    output_schema: json!({ "type": "object" }),
                }],
                config_schema: None,
                resources: ResourceHints::default(),
                dev_flows: BTreeMap::new(),
            },
            ComponentManifest {
                id: "templating.handlebars".parse()?,
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
            },
        ],
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

    let mut writer =
        ZipWriter::new(std::fs::File::create(pack_path).context("create pack archive for test")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    for (id, artifact_path) in component_sources(&fixtures)? {
        writer.start_file(format!("components/{id}.wasm"), options)?;
        let mut file = std::fs::File::open(&artifact_path)
            .with_context(|| format!("open component {}", artifact_path.display()))?;
        std::io::copy(&mut file, &mut writer)?;
    }
    writer.finish().context("finalise test pack")?;
    Ok(())
}

fn build_pack_with_component_sources(
    pack_path: &Path,
    sources_payload: ComponentSourcesV1,
    include_components: bool,
) -> Result<()> {
    let fixtures = workspace_root().join("tests/fixtures/packs/runner-components");
    let flow_yaml =
        std::fs::read_to_string(fixtures.join("flows/demo.yaml")).context("read flow yaml")?;
    let (_bundle, flow) = load_and_validate_bundle_with_flow(&flow_yaml, None)?;

    let mut manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "runner.components.remote".parse()?,
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![
            ComponentManifest {
                id: "qa.process".parse()?,
                version: Version::parse("0.1.0")?,
                supports: vec![FlowKind::Messaging],
                world: "greentic:component@0.4.0".into(),
                profiles: ComponentProfiles::default(),
                capabilities: ComponentCapabilities::default(),
                configurators: None,
                operations: vec![greentic_types::ComponentOperation {
                    name: "process".into(),
                    input_schema: json!({
                        "type": "object",
                        "properties": { "text": { "type": "string" } }
                    }),
                    output_schema: json!({ "type": "object" }),
                }],
                config_schema: None,
                resources: ResourceHints::default(),
                dev_flows: BTreeMap::new(),
            },
            ComponentManifest {
                id: "templating.handlebars".parse()?,
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
            },
        ],
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
    manifest.set_component_sources_v1(sources_payload)?;

    let mut writer =
        ZipWriter::new(std::fs::File::create(pack_path).context("create pack archive for test")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    if include_components {
        for (id, artifact_path) in component_sources(&fixtures)? {
            writer.start_file(format!("components/{id}.wasm"), options)?;
            let mut file = std::fs::File::open(&artifact_path)
                .with_context(|| format!("open component {}", artifact_path.display()))?;
            std::io::copy(&mut file, &mut writer)?;
        }
    }

    writer.finish().context("finalise test pack")?;
    Ok(())
}

fn build_pack_with_component_sources_only(
    pack_path: &Path,
    sources_payload: ComponentSourcesV1,
    include_components: bool,
) -> Result<()> {
    let fixtures = workspace_root().join("tests/fixtures/packs/runner-components");
    let flow_yaml =
        std::fs::read_to_string(fixtures.join("flows/demo.yaml")).context("read flow yaml")?;
    let (_bundle, flow) = load_and_validate_bundle_with_flow(&flow_yaml, None)?;

    let mut manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "runner.components.sources-only".parse()?,
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: Vec::new(),
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
    manifest.set_component_sources_v1(sources_payload)?;

    let mut writer =
        ZipWriter::new(std::fs::File::create(pack_path).context("create pack archive for test")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    if include_components {
        for (id, artifact_path) in component_sources(&fixtures)? {
            writer.start_file(format!("components/{id}.wasm"), options)?;
            let mut file = std::fs::File::open(&artifact_path)
                .with_context(|| format!("open component {}", artifact_path.display()))?;
            std::io::copy(&mut file, &mut writer)?;
        }
    }

    writer.finish().context("finalise test pack")?;
    Ok(())
}

fn write_manifest_only_gtpack(
    pack_path: &Path,
    manifest: &PackManifest,
    extra_entries: &[(&str, &[u8])],
) -> Result<()> {
    let mut writer =
        ZipWriter::new(std::fs::File::create(pack_path).context("create pack archive for test")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    for (name, data) in extra_entries {
        writer.start_file(name, options)?;
        writer.write_all(data)?;
    }

    writer.finish().context("finalise test pack")?;
    Ok(())
}

fn component_sources(fixtures_root: &Path) -> Result<Vec<(String, PathBuf)>> {
    let workspace_root = workspace_root();
    let crates_root = workspace_root.join("tests/fixtures/runner-components");
    let target_root = crates_root.join("target-test");

    let components = [
        ("qa.process", "qa_process"),
        ("templating.handlebars", "templating_handlebars"),
    ];

    let mut sources = Vec::new();

    for (id, crate_name) in components {
        let prebuilt = fixtures_root
            .join("components")
            .join(format!("{crate_name}.wasm"));
        if prebuilt.exists() {
            sources.push((id.to_string(), prebuilt));
            continue;
        }

        let crate_dir = crates_root.join(crate_name);

        let mut cmd = Command::new("cargo");
        let offline = std::env::var("CARGO_NET_OFFLINE").ok();
        if let Some(val) = &offline {
            cmd.env("CARGO_NET_OFFLINE", val);
        }
        let mut args: Vec<String> = vec![
            "build".into(),
            "--target".into(),
            "wasm32-wasip2".into(),
            "--release".into(),
        ];
        if matches!(offline.as_deref(), Some("true")) {
            args.insert(1, "--offline".into());
        }
        let status = cmd
            .env("CARGO_TARGET_DIR", &target_root)
            .current_dir(&crate_dir)
            .args(args)
            .status()
            .with_context(|| format!("failed to build component crate {}", crate_name))?;

        if !status.success() {
            anyhow::bail!("component build failed for {}", crate_name);
        }

        let base = target_root.join("wasm32-wasip2").join("release");
        let candidates = [
            base.join(format!("{crate_name}.wasm")),
            base.join("deps").join(format!("{crate_name}.wasm")),
        ];

        let artifact = candidates
            .into_iter()
            .find(|path| path.exists())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "component artifact not found after build for {}",
                    crate_name
                )
            })?;

        sources.push((id.to_string(), artifact));
    }

    Ok(sources)
}

fn state_store_component_artifact() -> Result<PathBuf> {
    let workspace_root = workspace_root();
    let crates_root = workspace_root.join("tests/fixtures/runner-components");
    let target_root = crates_root.join("target-test");
    let crate_name = "state_store_component";
    let crate_dir = crates_root.join(crate_name);

    let status = Command::new("cargo")
        .env("CARGO_NET_OFFLINE", "true")
        .env("CARGO_TARGET_DIR", &target_root)
        .current_dir(&crate_dir)
        .args([
            "build",
            "--offline",
            "--target",
            "wasm32-wasip2",
            "--release",
        ])
        .status()
        .with_context(|| format!("failed to build component crate {}", crate_name))?;
    if !status.success() {
        anyhow::bail!("component build failed for {}", crate_name);
    }

    let base = target_root.join("wasm32-wasip2").join("release");
    let candidates = [
        base.join(format!("{crate_name}.wasm")),
        base.join("deps").join(format!("{crate_name}.wasm")),
    ];
    let artifact = candidates
        .into_iter()
        .find(|path| path.exists())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "component artifact not found after build for {}",
                crate_name
            )
        })?;
    Ok(artifact)
}

fn build_state_store_pack(pack_path: &Path, include_state_capability: bool) -> Result<()> {
    let component_path = state_store_component_artifact()?;
    let capabilities = ComponentCapabilities {
        host: HostCapabilities {
            state: Some(if include_state_capability {
                StateCapabilities {
                    read: true,
                    write: true,
                }
            } else {
                StateCapabilities {
                    read: false,
                    write: false,
                }
            }),
            ..HostCapabilities::default()
        },
        ..ComponentCapabilities::default()
    };

    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "state.store.test".parse()?,
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "state.store".parse()?,
            version: Version::parse("0.1.0")?,
            supports: vec![FlowKind::Messaging],
            world: "greentic:component@0.4.0".into(),
            profiles: ComponentProfiles::default(),
            capabilities,
            configurators: None,
            operations: Vec::new(),
            config_schema: None,
            resources: ResourceHints::default(),
            dev_flows: BTreeMap::new(),
        }],
        flows: Vec::new(),
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        extensions: None,
    };

    let mut writer = ZipWriter::new(std::fs::File::create(pack_path).context("create state pack")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    writer.start_file("components/state.store.wasm", options)?;
    let mut file = std::fs::File::open(&component_path)
        .with_context(|| format!("open component {}", component_path.display()))?;
    std::io::copy(&mut file, &mut writer)?;
    writer.finish().context("finalise state pack")?;
    Ok(())
}

fn digest_for_bytes(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", to_hex(&hasher.finalize()))
}

fn to_hex(digest: &[u8]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn write_component_cache(cache_root: &Path, digest: &str, bytes: &[u8]) -> Result<PathBuf> {
    let trimmed = digest.strip_prefix("sha256:").unwrap_or(digest);
    let dir = cache_root.join(trimmed);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("component.wasm");
    std::fs::write(&path, bytes)?;
    Ok(path)
}

fn demo_exec_ctx(node_id: &str) -> greentic_runner_host::component_api::node::ExecCtx {
    greentic_runner_host::component_api::node::ExecCtx {
        tenant: greentic_runner_host::component_api::node::TenantCtx {
            tenant: "demo".into(),
            team: None,
            user: None,
            trace_id: None,
            i18n_id: None,
            correlation_id: None,
            deadline_unix_ms: None,
            attempt: 0,
            idempotency_key: None,
        },
        i18n_id: None,
        flow_id: "demo.flow".into(),
        node_id: Some(node_id.into()),
    }
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
fn gtpack_manifest_fast_path_invokes_components() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("runner-components.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_pack(&gtpack)?;

    let config = Arc::new(host_config(&bindings_path));
    let runtime = Arc::new(rt.block_on(PackRuntime::load(
        &gtpack,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);

    let flows = rt.block_on(runtime.list_flows())?;
    assert_eq!(flows.len(), 1);
    assert_eq!(flows[0].id, "demo.flow");

    let flow = runtime.load_flow("demo.flow")?;
    assert!(
        flow.entrypoints.contains_key("default") || !flow.nodes.is_empty(),
        "flow should expose an entrypoint or nodes"
    );

    let ctx = demo_exec_ctx("qa");
    let result = rt.block_on(runtime.invoke_component(
        "qa.process",
        ctx,
        "process",
        None,
        serde_json::to_string(&json!({ "text": "hello" }))?,
    ))?;
    assert_eq!(result, json!({ "text": "hello" }));

    // Validate the templating component resolves and can render against simple state.
    let ctx = demo_exec_ctx("tmpl");
    let render = rt.block_on(runtime.invoke_component(
        "templating.handlebars",
        ctx,
        "render",
        None,
        serde_json::to_string(&json!({
            "template": "Echo: {{input.text}}",
            "input": { "text": "hi" }
        }))?,
    ))?;
    match render {
        serde_json::Value::String(s) => assert!(s.starts_with("Echo:")),
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(s)) = map.get("text") {
                assert!(s.starts_with("Echo:"));
            } else {
                panic!("templating output missing text: {map:?}");
            }
        }
        other => panic!("unexpected templating output: {other:?}"),
    }
    Ok(())
}

#[test]
fn gtpack_manifest_loads_remote_components_from_cache() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("runner-components-remote.gtpack");
    let cache_root = temp.path().join("dist-cache");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let fixtures = workspace_root().join("tests/fixtures/packs/runner-components");
    let mut entries = Vec::new();
    for (id, artifact_path) in component_sources(&fixtures)? {
        let bytes = std::fs::read(&artifact_path)?;
        let digest = digest_for_bytes(&bytes);
        write_component_cache(&cache_root, &digest, &bytes)?;
        let source_ref = ComponentSourceRef::Oci(format!("registry.test/{id}@{}", digest));
        entries.push(ComponentSourceEntryV1 {
            name: id,
            component_id: None,
            source: source_ref,
            resolved: ResolvedComponentV1 {
                digest,
                signature: None,
                signed_by: None,
            },
            artifact: ArtifactLocationV1::Remote,
            licensing_hint: None,
            metering_hint: None,
        });
    }

    let sources = ComponentSourcesV1::new(entries);
    build_pack_with_component_sources(&gtpack, sources, false)?;

    let config = Arc::new(host_config(&bindings_path));
    let runtime = Arc::new(rt.block_on(PackRuntime::load(
        &gtpack,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution {
            dist_cache_dir: Some(cache_root),
            ..ComponentResolution::default()
        },
    ))?);

    let ctx = demo_exec_ctx("qa");
    let result = rt.block_on(runtime.invoke_component(
        "qa.process",
        ctx,
        "process",
        None,
        serde_json::to_string(&json!({ "text": "hello" }))?,
    ))?;
    assert_eq!(result, json!({ "text": "hello" }));
    Ok(())
}

#[test]
fn gtpack_manifest_offline_errors_when_remote_component_missing() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("runner-components-offline.gtpack");
    let cache_root = temp.path().join("dist-cache");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let fixtures = workspace_root().join("tests/fixtures/packs/runner-components");
    let mut entries = Vec::new();
    for (id, artifact_path) in component_sources(&fixtures)? {
        let bytes = std::fs::read(&artifact_path)?;
        let digest = digest_for_bytes(&bytes);
        let source_ref = ComponentSourceRef::Oci(format!("registry.test/{id}@{}", digest));
        entries.push(ComponentSourceEntryV1 {
            name: id,
            component_id: None,
            source: source_ref,
            resolved: ResolvedComponentV1 {
                digest,
                signature: None,
                signed_by: None,
            },
            artifact: ArtifactLocationV1::Remote,
            licensing_hint: None,
            metering_hint: None,
        });
    }

    let sources = ComponentSourcesV1::new(entries);
    build_pack_with_component_sources(&gtpack, sources, false)?;

    let config = Arc::new(host_config(&bindings_path));
    let err = rt
        .block_on(PackRuntime::load(
            &gtpack,
            Arc::clone(&config),
            None,
            None,
            None,
            None,
            Arc::new(RunnerWasiPolicy::new()),
            greentic_runner_host::secrets::default_manager()?,
            None,
            false,
            ComponentResolution {
                dist_offline: true,
                dist_cache_dir: Some(cache_root),
                ..ComponentResolution::default()
            },
        ))
        .err()
        .expect("pack load should fail when offline and cache is empty");

    let message = err.to_string();
    assert!(
        message.contains("greentic-dist pull"),
        "error should suggest greentic-dist pull: {message}"
    );
    Ok(())
}

#[test]
fn gtpack_manifest_component_sources_only_loads_components() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("runner-components-sources-only.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let fixtures = workspace_root().join("tests/fixtures/packs/runner-components");
    let mut entries = Vec::new();
    for (id, artifact_path) in component_sources(&fixtures)? {
        let bytes = std::fs::read(&artifact_path)?;
        let digest = digest_for_bytes(&bytes);
        entries.push(ComponentSourceEntryV1 {
            name: format!("{id}-alias"),
            component_id: Some(id.clone().parse()?),
            source: ComponentSourceRef::Oci(format!("registry.test/{id}@{}", digest)),
            resolved: ResolvedComponentV1 {
                digest,
                signature: None,
                signed_by: None,
            },
            artifact: ArtifactLocationV1::Inline {
                wasm_path: format!("components/{id}.wasm"),
                manifest_path: None,
            },
            licensing_hint: None,
            metering_hint: None,
        });
    }

    let sources = ComponentSourcesV1::new(entries);
    build_pack_with_component_sources_only(&gtpack, sources, true)?;

    let config = Arc::new(host_config(&bindings_path));
    let runtime = Arc::new(rt.block_on(PackRuntime::load(
        &gtpack,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);

    let ctx = demo_exec_ctx("qa");
    let result = rt.block_on(runtime.invoke_component(
        "qa.process",
        ctx,
        "process",
        None,
        serde_json::to_string(&json!({ "text": "hello" }))?,
    ))?;
    assert_eq!(result, json!({ "text": "hello" }));
    Ok(())
}

#[test]
fn state_store_roundtrip_requires_capability() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("state-store.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    build_state_store_pack(&gtpack, true)?;

    let config = Arc::new(host_config(&bindings_path));
    let runtime = Arc::new(rt.block_on(PackRuntime::load(
        &gtpack,
        Arc::clone(&config),
        None,
        None,
        None,
        Some(new_state_store()),
        Arc::new(RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);

    let write_ctx = demo_exec_ctx("write");
    let write_payload = serde_json::to_string(&json!({
        "key": "demo",
        "value": { "count": 1 }
    }))?;
    rt.block_on(runtime.invoke_component("state.store", write_ctx, "write", None, write_payload))?;

    let read_ctx = demo_exec_ctx("read");
    let read_payload = serde_json::to_string(&json!({ "key": "demo" }))?;
    let read_result =
        rt.block_on(runtime.invoke_component("state.store", read_ctx, "read", None, read_payload))?;
    assert_eq!(read_result, json!({ "value": { "count": 1 } }));

    let delete_ctx = demo_exec_ctx("delete");
    let delete_payload = serde_json::to_string(&json!({ "key": "demo" }))?;
    rt.block_on(runtime.invoke_component(
        "state.store",
        delete_ctx,
        "delete",
        None,
        delete_payload,
    ))?;

    Ok(())
}

#[test]
fn state_store_is_gated_without_capability() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("state-store-denied.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    build_state_store_pack(&gtpack, false)?;

    let config = Arc::new(host_config(&bindings_path));
    let runtime = Arc::new(rt.block_on(PackRuntime::load(
        &gtpack,
        Arc::clone(&config),
        None,
        None,
        None,
        Some(new_state_store()),
        Arc::new(RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);

    let ctx = demo_exec_ctx("write");
    let payload = serde_json::to_string(&json!({
        "key": "demo",
        "value": { "count": 1 }
    }))?;
    let result = rt.block_on(runtime.invoke_component("state.store", ctx, "write", None, payload));
    assert!(
        result.is_err(),
        "state store should be gated without capability"
    );
    Ok(())
}

#[test]
fn gtpack_cbor_only_pack_loads() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("cbor-only.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "cbor.only.test".parse()?,
        name: None,
        version: Version::parse("1.0.0")?,
        kind: PackKind::Application,
        publisher: "demo".into(),
        components: Vec::new(),
        flows: Vec::new(),
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        extensions: None,
    };

    write_manifest_only_gtpack(&gtpack, &manifest, &[])?;

    let config = Arc::new(host_config(&bindings_path));
    let runtime = Arc::new(rt.block_on(PackRuntime::load(
        &gtpack,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);

    assert_eq!(runtime.metadata().pack_id, manifest.pack_id.as_str());
    let flows = rt.block_on(runtime.list_flows())?;
    assert!(flows.is_empty());
    Ok(())
}

#[test]
fn gtpack_missing_manifest_errors() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("missing-manifest.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let mut writer =
        ZipWriter::new(std::fs::File::create(&gtpack).context("create pack archive for test")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer.start_file("pack.yaml", options)?;
    writer.write_all(b"name: manifest missing")?;
    writer.finish().context("finalise test pack")?;

    let config = Arc::new(host_config(&bindings_path));
    let err = rt
        .block_on(PackRuntime::load(
            &gtpack,
            Arc::clone(&config),
            None,
            None,
            None,
            None,
            Arc::new(RunnerWasiPolicy::new()),
            greentic_runner_host::secrets::default_manager()?,
            None,
            false,
            ComponentResolution::default(),
        ))
        .err()
        .expect("missing manifest should error");

    let message = err.to_string();
    assert!(
        message.contains("manifest.cbor"),
        "error should mention manifest.cbor: {message}"
    );
    Ok(())
}

#[test]
fn gtpack_legacy_artifacts_are_ignored() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("legacy-artifacts.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "legacy.ignore.test".parse()?,
        name: None,
        version: Version::parse("1.0.0")?,
        kind: PackKind::Application,
        publisher: "demo".into(),
        components: Vec::new(),
        flows: Vec::new(),
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        extensions: None,
    };

    let extras = [
        ("pack.yaml", &b"name: legacy"[..]),
        ("secrets_requirements.json", &b"{not valid json"[..]),
        ("flows/ignored.ygtc", &b"invalid flow artifact"[..]),
    ];
    write_manifest_only_gtpack(&gtpack, &manifest, &extras)?;

    let config = Arc::new(host_config(&bindings_path));
    let runtime = Arc::new(rt.block_on(PackRuntime::load(
        &gtpack,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);

    assert_eq!(runtime.metadata().pack_id, manifest.pack_id.as_str());
    let flows = rt.block_on(runtime.list_flows())?;
    assert!(flows.is_empty());
    Ok(())
}

/// End-to-end proof that an agentic worker's component tool seam invokes a real
/// WASM component: `PackRuntimeComponentInvoker` enumerates the pack's component
/// operations and dispatches one through `PackRuntime::invoke_component`,
/// returning the component's actual output. This is the runtime verification of
/// the `component:` tool kind that unit tests (with a fake invoker) cannot give.
#[cfg(feature = "agentic-worker")]
#[test]
fn agentic_worker_component_invoker_lists_and_invokes() -> Result<()> {
    use greentic_aw_runtime::ComponentInvoker;
    use greentic_runner_host::runner::component_invoker::PackRuntimeComponentInvoker;

    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("runner-components.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_pack(&gtpack)?;

    let config = Arc::new(host_config(&bindings_path));
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        &gtpack,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);

    let invoker = PackRuntimeComponentInvoker::new(vec![Arc::clone(&pack)], "demo".into());

    // (a) Operations declared by the pack's component manifests are enumerated
    // as tools.
    let ops = invoker.list_operations();
    assert!(
        ops.iter()
            .any(|o| o.component_ref == "qa.process" && o.operation == "process"),
        "qa.process/process must be listed; got: {ops:?}"
    );

    // (b) A real WASM component is invoked and its output flows back. The args
    // are wrapped in an invocation envelope by the invoker; the host's
    // envelope_v0_6 unwraps the payload, so the echo component returns it.
    let out = rt.block_on(invoker.invoke("qa.process", "process", r#"{"text":"hello"}"#));
    assert_eq!(out, Ok(json!({ "text": "hello" })), "got: {out:?}");

    // (c) An unknown component fails as an Err (the catalog layer wraps it to an
    // error value for the LLM; never a panic).
    let missing = rt.block_on(invoker.invoke("does.not.exist", "process", "{}"));
    assert!(
        missing.is_err(),
        "unknown component must error; got: {missing:?}"
    );
    Ok(())
}
