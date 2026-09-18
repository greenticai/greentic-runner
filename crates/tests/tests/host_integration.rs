#![recursion_limit = "256"]
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::future::Future;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use greentic_config_types::{PackSourceConfig, PacksConfig};
use greentic_flow::flow_bundle::load_and_validate_bundle_with_flow;
use greentic_runner_host::watcher;
use greentic_runner_host::{Activity, HostBuilder, HostConfig, RunnerHost};
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, FlowKind, HostCapabilities,
    PackFlowEntry, PackKind, PackManifest, ResourceHints, StateCapabilities, encode_pack_manifest,
};
use runner_core::env::PackConfig;
use semver::Version;
use serial_test::serial;
use tempfile::TempDir;
use tokio::time::sleep;
use zip::ZipWriter;
use zip::write::FileOptions;

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: impl AsRef<str>) -> Self {
        let prev = env::var(key).ok();
        unsafe {
            env::set_var(key, value.as_ref());
        }
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(ref value) = self.prev {
            unsafe {
                env::set_var(self.key, value);
            }
        } else {
            unsafe {
                env::remove_var(self.key);
            }
        }
    }
}

#[tokio::test]
#[serial]
async fn host_executes_demo_pack_flow() -> Result<()> {
    let _secret_guard = EnvGuard::set("TELEGRAM_BOT_TOKEN", "test-token");
    let _backend_guard = EnvGuard::set("SECRETS_BACKEND", "env");

    let bindings = fixture_path("examples/bindings/default.bindings.yaml");
    let config = HostConfig::load_from_path(&bindings)?;
    let host = HostBuilder::new().with_config(config).build()?;
    host.start().await?;

    let _pack_temp = TempDir::new()?;
    let pack_path = _pack_temp.path().join("runner-components.gtpack");
    build_runner_components_pack(&pack_path)?;
    host.load_pack("acme", pack_path.as_path()).await?;

    let activity = Activity::text("hello from integration")
        .with_tenant("acme")
        .from_user("user-1");
    let replies = host.handle_activity("acme", activity).await?;
    assert!(
        !replies.is_empty(),
        "demo pack should emit at least one activity"
    );

    host.stop().await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn trace_written_on_failure() -> Result<()> {
    let temp = TempDir::new()?;
    let trace_path = temp.path().join("trace.json");
    let _trace_guard = EnvGuard::set("GREENTIC_TRACE_OUT", trace_path.display().to_string());
    let _backend_guard = EnvGuard::set("SECRETS_BACKEND", "env");

    let bindings = fixture_path("examples/bindings/default.bindings.yaml");
    let config = HostConfig::load_from_path(&bindings)?;
    let host = HostBuilder::new().with_config(config).build()?;
    host.start().await?;

    let pack_path = temp.path().join("runner-components-fail.gtpack");
    build_failing_runner_components_pack(&pack_path)?;
    host.load_pack("acme", pack_path.as_path()).await?;

    let activity = Activity::text("hello from integration")
        .with_tenant("acme")
        .from_user("user-1");
    let result = host.handle_activity("acme", activity).await;
    // Session flows surface node failures as Ok with an error envelope.
    assert!(result.is_ok(), "session flow should not Err: {result:?}");

    assert!(
        trace_path.exists(),
        "trace.json should be written on failure"
    );
    let bytes = fs::read(&trace_path).context("read trace.json")?;
    let trace: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(trace["trace_version"], 1);
    assert_eq!(trace["flow"]["id"], "demo.flow");
    assert!(
        trace["steps"]
            .as_array()
            .map(|steps| !steps.is_empty())
            .unwrap_or(false),
        "trace steps should include the failing node"
    );

    host.stop().await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn fault_injection_drops_state_write() -> Result<()> {
    let temp = TempDir::new()?;
    let trace_path = temp.path().join("trace.json");
    let _trace_guard = EnvGuard::set("GREENTIC_TRACE_OUT", trace_path.display().to_string());
    let _drop_guard = EnvGuard::set("GREENTIC_FAIL_DROP_STATE_WRITE", "1");
    let _seed_guard = EnvGuard::set("GREENTIC_FAIL_SEED", "1234");
    let _backend_guard = EnvGuard::set("SECRETS_BACKEND", "env");

    let bindings = fixture_path("examples/bindings/default.bindings.yaml");
    let config = HostConfig::load_from_path(&bindings)?;
    let host = HostBuilder::new().with_config(config).build()?;
    host.start().await?;

    ensure_state_store_component()?;
    let pack_path = temp.path().join("state-store-fault.gtpack");
    build_state_store_fault_pack(&pack_path)?;
    host.load_pack("acme", pack_path.as_path()).await?;

    let activity = Activity::text("hello from integration")
        .with_tenant("acme")
        .from_user("user-1");
    let result = host.handle_activity("acme", activity).await;
    // Session flows surface node failures as Ok with an error envelope.
    assert!(result.is_ok(), "session flow should not Err: {result:?}");

    let bytes = fs::read(&trace_path).context("read trace.json")?;
    let trace: serde_json::Value = serde_json::from_slice(&bytes)?;
    let last = trace["steps"]
        .as_array()
        .and_then(|steps| steps.last())
        .cloned()
        .unwrap_or_default();
    assert_eq!(last["node_id"], "read");

    host.stop().await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn pack_watcher_resolves_index_and_reloads() -> Result<()> {
    let cache_dir = TempDir::new()?;
    let _backend_guard = EnvGuard::set("SECRETS_BACKEND", "env");

    let pack_cfg = pack_config_with_index(cache_dir.path(), fixture_path("examples/index.json"));
    let bindings = fixture_path("examples/bindings/default.bindings.yaml");
    let config = HostConfig::load_from_path(&bindings)?;
    let host = Arc::new(HostBuilder::new().with_config(config).build()?);
    host.start().await?;

    let (watcher_guard, reload) =
        watcher::start_pack_watcher(Arc::clone(&host), pack_cfg, Duration::from_millis(250))
            .await?;

    wait_for(|| host.active_packs().len() == 1, Duration::from_secs(5)).await?;
    reload.trigger().await?;
    wait_for(
        || host.health_state().snapshot().last_reload.is_some(),
        Duration::from_secs(5),
    )
    .await?;

    drop(watcher_guard);
    host.stop().await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn pack_watcher_handles_overlays() -> Result<()> {
    let temp = TempDir::new()?;
    let cache_dir = temp.path().join("cache");
    fs::create_dir_all(&cache_dir)?;
    let index_path = temp.path().join("index-overlay.json");
    write_overlay_index(&index_path, true)?;

    let _backend_guard = EnvGuard::set("SECRETS_BACKEND", "env");

    let pack_cfg = pack_config_with_index(cache_dir.as_path(), index_path.as_path());
    let bindings = fixture_path("examples/bindings/default.bindings.yaml");
    let config = HostConfig::load_from_path(&bindings)?;
    let host = Arc::new(HostBuilder::new().with_config(config).build()?);
    host.start().await?;

    let (watcher_guard, reload) =
        watcher::start_pack_watcher(Arc::clone(&host), pack_cfg, Duration::from_millis(250))
            .await?;

    let host_for_initial = Arc::clone(&host);
    wait_for_async(
        move || {
            let host = Arc::clone(&host_for_initial);
            async move {
                tenant_overlay_count(&host, "acme")
                    .await
                    .map(|count| count == 1)
                    .unwrap_or(false)
            }
        },
        Duration::from_secs(5),
    )
    .await?;

    write_overlay_index(&index_path, false)?;
    reload.trigger().await?;
    let host_for_reload = Arc::clone(&host);
    wait_for_async(
        move || {
            let host = Arc::clone(&host_for_reload);
            async move {
                tenant_overlay_count(&host, "acme")
                    .await
                    .map(|count| count == 0)
                    .unwrap_or(false)
            }
        },
        Duration::from_secs(5),
    )
    .await?;

    drop(watcher_guard);
    host.stop().await?;
    Ok(())
}

async fn wait_for<F>(mut predicate: F, timeout: Duration) -> Result<()>
where
    F: FnMut() -> bool,
{
    let step = Duration::from_millis(50);
    let mut elapsed = Duration::ZERO;
    while elapsed < timeout {
        if predicate() {
            return Ok(());
        }
        sleep(step).await;
        elapsed += step;
    }
    bail!("condition not met within {:?}", timeout);
}

async fn wait_for_async<F, Fut>(mut predicate: F, timeout: Duration) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let step = Duration::from_millis(50);
    let mut elapsed = Duration::ZERO;
    while elapsed < timeout {
        if predicate().await {
            return Ok(());
        }
        sleep(step).await;
        elapsed += step;
    }
    bail!("condition not met within {:?}", timeout);
}

fn fixture_path(relative: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(relative)
}

fn ensure_state_store_component() -> Result<PathBuf> {
    let crates_root = fixture_path("tests/fixtures/runner-components");
    let target_root = crates_root.join("target-test");
    let crate_name = "state_store_component";
    let base = target_root.join("wasm32-wasip2").join("release");
    let candidates = [
        base.join(format!("{crate_name}.wasm")),
        base.join("deps").join(format!("{crate_name}.wasm")),
    ];
    if let Some(found) = candidates.iter().find(|p| p.exists()) {
        return Ok(found.clone());
    }

    let crate_dir = crates_root.join(crate_name);
    let status = Command::new("cargo")
        .env("CARGO_NET_OFFLINE", "true")
        .env("CARGO_TARGET_DIR", &target_root)
        // wasm32-wasip2 has no `profiler_builtins`; strip any inherited
        // coverage instrumentation flags so the spawned build does not try
        // to link it under `cargo llvm-cov`.
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("LLVM_PROFILE_FILE")
        .current_dir(&crate_dir)
        .args([
            "build",
            "--offline",
            "--target",
            "wasm32-wasip2",
            "--release",
        ])
        .status()
        .with_context(|| format!("failed to build component crate {crate_name}"))?;
    if !status.success() {
        anyhow::bail!("component build failed for {crate_name}");
    }

    candidates
        .into_iter()
        .find(|p| p.exists())
        .ok_or_else(|| anyhow::anyhow!("component artifact not found after build for {crate_name}"))
}

fn build_runner_components_pack(pack_path: &std::path::Path) -> Result<()> {
    let fixtures = fixture_path("tests/fixtures/packs/runner-components");
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
                operations: Vec::new(),
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

    let mut writer = ZipWriter::new(
        std::fs::File::create(pack_path).context("create pack archive for integration test")?,
    );
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    let components = fixture_components(&fixtures)?;
    for (id, artifact) in components {
        writer.start_file(format!("components/{id}.wasm"), options)?;
        let mut file = std::fs::File::open(&artifact as &Path)
            .with_context(|| format!("open component {}", artifact.display()))?;
        io::copy(&mut file, &mut writer)?;
    }
    writer.finish().context("finalise integration test pack")?;
    Ok(())
}

fn build_failing_runner_components_pack(pack_path: &std::path::Path) -> Result<()> {
    let flow_yaml = r#"
id: demo.flow
type: messaging
start: qa
nodes:
  qa:
    component.exec:
      component: qa.process
      operation: missing-op
      input:
        text: "hello"
    routing:
      - out: true
"#;
    let (_bundle, flow) = load_and_validate_bundle_with_flow(flow_yaml, None)?;

    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "runner.components.test".parse()?,
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

    let mut writer =
        ZipWriter::new(std::fs::File::create(pack_path).context("create failing pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    let components = fixture_components(&fixture_path("tests/fixtures/packs/runner-components"))?;
    for (id, artifact) in components {
        if id != "qa.process" {
            continue;
        }
        writer.start_file(format!("components/{id}.wasm"), options)?;
        let mut file = std::fs::File::open(&artifact as &Path)
            .with_context(|| format!("open component {}", artifact.display()))?;
        io::copy(&mut file, &mut writer)?;
    }
    writer.finish().context("finalise failing pack")?;
    Ok(())
}

fn build_state_store_fault_pack(pack_path: &std::path::Path) -> Result<()> {
    let flow_yaml = r#"
id: demo.flow
type: messaging
start: write
nodes:
  write:
    component.exec:
      component: state.store
      operation: write
      input:
        key: "fault-key"
        value:
          status: "ok"
    routing:
      - to: read
  read:
    component.exec:
      component: state.store
      operation: read
      input:
        key: "fault-key"
    routing:
      - to: emit
  emit:
    emit.response:
      text: "done"
    routing:
      - out: true
"#;
    let (_bundle, flow) = load_and_validate_bundle_with_flow(flow_yaml, None)?;

    let capabilities = ComponentCapabilities {
        host: HostCapabilities {
            state: Some(StateCapabilities {
                read: true,
                write: true,
            }),
            ..HostCapabilities::default()
        },
        ..ComponentCapabilities::default()
    };
    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "runner.state-store.fault".parse()?,
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
        ZipWriter::new(std::fs::File::create(pack_path).context("create state fault pack")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    let wasm_path = fixture_path(
        "tests/fixtures/runner-components/target-test/wasm32-wasip2/release/state_store_component.wasm",
    );
    writer.start_file("components/state.store.wasm", options)?;
    let mut file = std::fs::File::open(&wasm_path as &Path)
        .with_context(|| format!("open component {}", wasm_path.display()))?;
    io::copy(&mut file, &mut writer)?;

    writer.finish().context("finalise state fault pack")?;
    Ok(())
}

fn write_overlay_index(path: &std::path::Path, include_overlay: bool) -> Result<()> {
    const DEMO_DIGEST: &str =
        "sha256:a3195ff0a9befb0192ef4fa7f5aa7fea944c9a9fa58aa25a4e80e9e80b5c36c1";
    let pack_path = fixture_path("examples/packs/demo.gtpack");
    let mut overlays = Vec::new();
    if include_overlay {
        overlays.push(serde_json::json!({
            "name": "runner.components",
            "version": "0.1.0",
            "locator": pack_path.display().to_string(),
            "digest": DEMO_DIGEST
        }));
    }
    let index = serde_json::json!({
        "acme": {
            "main_pack": {
                "name": "runner.components",
                "version": "0.1.0",
                "locator": pack_path.display().to_string(),
                "digest": DEMO_DIGEST
            },
            "overlays": overlays
        }
    });
    fs::write(path, serde_json::to_vec_pretty(&index)?)?;
    Ok(())
}

fn pack_config_with_index(cache_root: &Path, index: impl AsRef<Path>) -> PackConfig {
    PackConfig::from_packs(&PacksConfig {
        source: PackSourceConfig::LocalIndex {
            path: index.as_ref().to_path_buf(),
        },
        cache_dir: cache_root.to_path_buf(),
        index_cache_ttl_secs: None,
        trust: None,
    })
    .expect("pack config")
}

fn fixture_components(fixtures_root: &Path) -> Result<Vec<(String, PathBuf)>> {
    let crates_root = fixture_path("tests/fixtures/runner-components");
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
        sources.push((id.to_string(), artifact));
    }

    Ok(sources)
}

async fn tenant_overlay_count(host: &Arc<RunnerHost>, tenant: &str) -> Option<usize> {
    host.tenant(tenant)
        .await
        .map(|handle| handle.overlays().len())
}
