use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use greentic_flow::flow_bundle::load_and_validate_bundle_with_flow;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, FlowKind, HostCapabilities,
    PackFlowEntry, PackKind, PackManifest, ResourceHints, StateCapabilities, encode_pack_manifest,
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
async fn conformance_levels_pass() -> Result<()> {
    let _backend_guard = EnvGuard::set("SECRETS_BACKEND", "env");
    let temp = TempDir::new()?;
    let packs_dir = temp.path().join("packs");
    fs::create_dir_all(&packs_dir)?;
    let pack_path = packs_dir.join("messaging-conformance.gtpack");
    build_conformance_pack(&pack_path)?;

    for level in ["l0", "l1", "l2"] {
        let report_path = temp.path().join(format!("report-{level}.json"));
        let value = run_conformance(&packs_dir, level, &report_path)?;
        assert_eq!(value["summary"]["failed"], 0, "{level} report: {value}");
        assert_eq!(value["summary"]["total"], 1, "{level} report: {value}");
        assert_eq!(
            value["packs"][0]["status"], "pass",
            "{level} report: {value}"
        );
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn conformance_fails_when_filter_matches_no_pack() -> Result<()> {
    let _backend_guard = EnvGuard::set("SECRETS_BACKEND", "env");
    let temp = TempDir::new()?;
    let packs_dir = temp.path().join("packs");
    fs::create_dir_all(&packs_dir)?;
    build_conformance_pack(&packs_dir.join("messaging-conformance.gtpack"))?;

    let status = Command::new(env!("CARGO_BIN_EXE_greentic-runner"))
        .arg("conformance")
        .arg("--packs")
        .arg(&packs_dir)
        .arg("--level")
        .arg("l1")
        .arg("--provider")
        .arg("no-such-provider")
        .status()
        .context("run conformance")?;
    assert!(
        !status.success(),
        "a filter matching nothing must exit non-zero"
    );
    Ok(())
}

fn run_conformance(packs_dir: &Path, level: &str, report: &Path) -> Result<serde_json::Value> {
    let status = Command::new(env!("CARGO_BIN_EXE_greentic-runner"))
        .arg("conformance")
        .arg("--packs")
        .arg(packs_dir)
        .arg("--level")
        .arg(level)
        .arg("--report")
        .arg(report)
        .status()
        .context("run conformance")?;
    let bytes = fs::read(report).context("read report")?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    if !status.success() {
        anyhow::bail!("conformance command failed at {level}: {value}");
    }
    Ok(value)
}

fn build_conformance_pack(pack_path: &Path) -> Result<()> {
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
        key: "conformance-key"
        value:
          status: "ok"
    routing:
      - to: read
  read:
    component.exec:
      component: state.store
      operation: read
      input:
        key: "conformance-key"
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
        pack_id: "messaging-conformance".parse()?,
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
        ZipWriter::new(fs::File::create(pack_path).context("create conformance pack")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    let wasm_path = ensure_state_store_component_wasm()?;
    writer.start_file("components/state.store.wasm", options)?;
    let mut file = fs::File::open(&wasm_path as &Path)
        .with_context(|| format!("open component {}", wasm_path.display()))?;
    io::copy(&mut file, &mut writer)?;
    writer.finish().context("finalise conformance pack")?;
    Ok(())
}

fn ensure_state_store_component_wasm() -> Result<PathBuf> {
    let wasm_path = fixture_path(
        "tests/fixtures/runner-components/target-test/wasm32-wasip2/release/state_store_component.wasm",
    );
    if wasm_path.exists() {
        return Ok(wasm_path);
    }

    let workspace_manifest = fixture_path("tests/fixtures/runner-components/Cargo.toml");
    let target_dir = fixture_path("tests/fixtures/runner-components/target-test");
    let offline = env::var("CARGO_NET_OFFLINE").ok();
    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("--release")
        .arg("--target")
        .arg("wasm32-wasip2")
        .arg("--package")
        .arg("state_store_component")
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
        anyhow::bail!("failed to build state_store_component fixture");
    }
    if !wasm_path.exists() {
        anyhow::bail!(
            "state_store_component wasm missing after build: {}",
            wasm_path.display()
        );
    }
    Ok(wasm_path)
}

fn fixture_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(relative)
}
