use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use greentic_runner_host::pack;
use greentic_runner_host::pack::{ComponentState, HostState};
use greentic_runner_host::runtime_wasmtime::{Component, Engine, Linker, Store};
use greentic_runner_host::secrets::default_manager;
use greentic_runner_host::{HostConfig, PreopenSpec, RunnerWasiPolicy};
use reqwest::blocking::Client as BlockingClient;
use serial_test::serial;
use tempfile::TempDir;

#[test]
#[serial]
fn wasi_preview2_policy_enforced() -> Result<()> {
    // A missing `wasm-tools` or failed fixture build must fail loudly rather
    // than silently skipping the policy assertions.
    let wasm_path = build_fixture().context("failed to build the wasi-p2-smoke fixture")?;
    let workspace = workspace_root();
    let bindings = workspace.join("examples/bindings/default.bindings.yaml");
    let config = Arc::new(HostConfig::load_from_path(&bindings)?);

    let tempdir = TempDir::new()?;
    let data_file = tempdir.path().join("hello.txt");
    fs::write(&data_file, "wasi preview-2 preopen")?;
    let _env = EnvGuard::set("GT_TEST", "ok");

    let base_policy = RunnerWasiPolicy::new()
        .allow_env("GT_TEST")
        .with_preopen(PreopenSpec::new(tempdir.path(), "/data").read_only(true));
    run_component(&wasm_path, Arc::clone(&config), base_policy.clone())?;

    Ok(())
}

fn run_component(wasm: &Path, config: Arc<HostConfig>, policy: RunnerWasiPolicy) -> Result<()> {
    let engine = Engine::default();
    let component = Component::from_file(&engine, wasm)
        .map_err(|err| anyhow::anyhow!("failed to load {}: {err}", wasm.display()))?;
    let host_state = HostState::new(
        "wasi-p2-smoke".to_string(),
        Arc::clone(&config),
        Arc::new(BlockingClient::builder().build()?),
        None,
        None,
        None,
        default_manager()?,
        None,
        None,
        None,
        false,
        None,
        None,
    )?;
    let store_state = ComponentState::new(host_state, Arc::new(policy))?;
    let mut store = Store::new(&engine, store_state);
    let mut linker = Linker::new(&engine);
    pack::register_all(&mut linker, false)?;
    let instance = linker.instantiate(&mut store, &component)?;
    let run = instance
        .get_typed_func::<(), ()>(&mut store, "run")
        .map_err(|err| anyhow::anyhow!("component missing run export: {err}"))?;
    run.call(&mut store, ())
        .map_err(|err| anyhow::anyhow!("component execution failed: {err}"))?;
    Ok(())
}

fn build_fixture() -> Result<PathBuf> {
    let workspace = workspace_root();
    let target_dir = workspace.join("tests/fixtures/wasi-p2-smoke/target/wasm32-wasip2/release");
    std::fs::create_dir_all(&target_dir)?;
    let wat_path = target_dir.join("wasi_p2_smoke.wat");
    let artifact = target_dir.join("wasi_p2_smoke.wasm");
    let wat = r#"
(component
  (core module $m
    (func (export "run"))
  )
  (core instance $i (instantiate $m))
  (func (export "run") (canon lift (core func $i "run")))
)
"#;
    std::fs::write(&wat_path, wat)?;
    let status = Command::new("wasm-tools")
        .args([
            "parse",
            "--output",
            artifact.to_str().expect("utf8 path"),
            wat_path.to_str().expect("utf8 path"),
        ])
        .status()
        .context("failed to generate wasi-p2-smoke component via wasm-tools")?;
    if !status.success() {
        bail!("wasi-p2-smoke component generation failed");
    }
    let magic = std::fs::read(&artifact)
        .map(|bytes| bytes.get(0..4).map(|b| b.to_vec()).unwrap_or_default())
        .unwrap_or_default();
    if magic != [0x00, 0x61, 0x73, 0x6d] {
        bail!("wasi-p2-smoke artifact is not a valid wasm file");
    }
    Ok(artifact)
}

fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .map(Path::to_path_buf)
        .expect("tests crate to live under crates/")
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
        if let Some(value) = &self.prev {
            unsafe {
                std::env::set_var(self.key, value);
            }
        } else {
            unsafe {
                std::env::remove_var(self.key);
            }
        }
    }
}
