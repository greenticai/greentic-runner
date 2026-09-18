#![recursion_limit = "256"]
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use greentic_flow::flow_bundle::load_and_validate_bundle_with_flow;
use greentic_runner::Activity;
use greentic_runner::host::{HostBuilder, HostConfig};
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, FlowKind, PackFlowEntry, PackKind,
    PackManifest, ResourceHints, encode_pack_manifest,
};
use semver::Version;
use serial_test::serial;
use tempfile::TempDir;
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
async fn replay_reproduces_failing_step() -> Result<()> {
    let temp = TempDir::new()?;
    let trace_path = temp.path().join("trace.json");
    let _trace_guard = EnvGuard::set("GREENTIC_TRACE_OUT", trace_path.display().to_string());
    let _capture_guard = EnvGuard::set("GREENTIC_TRACE_CAPTURE_INPUTS", "1");
    let _backend_guard = EnvGuard::set("SECRETS_BACKEND", "env");

    let bindings = fixture_path("examples/bindings/default.bindings.yaml");
    let config = HostConfig::load_from_path(&bindings)?;
    let host = HostBuilder::new().with_config(config).build()?;
    host.start().await?;

    let pack_path = temp.path().join("runner-components-fail.gtpack");
    build_failing_pack(&pack_path)?;
    host.load_pack("acme", pack_path.as_path()).await?;

    let activity = Activity::text("replay probe")
        .with_tenant("acme")
        .from_user("user-1");
    let result = host.handle_activity("acme", activity).await;
    // Session flows surface node failures as Ok with an error envelope; the
    // trace is captured regardless, which is what replay needs.
    assert!(result.is_ok(), "session flow should not Err: {result:?}");
    assert!(trace_path.exists(), "trace.json should be written");

    host.stop().await?;

    let status = Command::new(env!("CARGO_BIN_EXE_greentic-runner"))
        .arg("replay")
        .arg(&trace_path)
        .arg("--pack")
        .arg(&pack_path)
        .status()
        .context("run greentic-runner replay")?;
    assert!(status.success(), "replay command should exit 0");

    Ok(())
}

fn fixture_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(relative)
}

fn build_failing_pack(pack_path: &Path) -> Result<()> {
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
            dev_flows: std::collections::BTreeMap::new(),
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
        ZipWriter::new(fs::File::create(pack_path).context("create failing pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    let artifact = qa_component_artifact()?;
    writer.start_file("components/qa.process.wasm", options)?;
    let mut file = fs::File::open(&artifact)
        .with_context(|| format!("open component {}", artifact.display()))?;
    io::copy(&mut file, &mut writer)?;
    writer.finish().context("finalise failing pack")?;
    Ok(())
}

fn qa_component_artifact() -> Result<PathBuf> {
    let fixtures_root = fixture_path("tests/fixtures/runner-components");
    let target_test = fixtures_root
        .join("target-test")
        .join("wasm32-wasip2")
        .join("release")
        .join("qa_process.wasm");
    if target_test.exists() {
        return Ok(target_test);
    }
    let target_default = fixtures_root
        .join("target")
        .join("wasm32-wasip2")
        .join("release")
        .join("qa_process.wasm");
    if target_default.exists() {
        return Ok(target_default);
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
        .arg("qa_process")
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
        anyhow::bail!("failed to build qa_process fixture");
    }
    if target_test.exists() {
        return Ok(target_test);
    }
    anyhow::bail!(
        "qa_process.wasm missing after fixture build: {}",
        target_test.display()
    )
}
