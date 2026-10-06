//! The PRODUCTION host (`TenantRuntime::load`, the constructor the pack
//! watcher uses) lends its real `dw.agent` handler to flow-tool engines.
//!
//! The handler is the real `RuntimeAgentNodeHandler` built from
//! `HostConfig.agents`, with an LLM that cannot answer (no credential), so the
//! nested agent FAILS. What is asserted is WHICH failure: without lending, a
//! flow tool's `dw.agent` dies with "no AgentNodeHandler configured"; with it,
//! the real handler is reached and the error is the agent's own.
//!
//! What is NOT real: the test calls `pack.run_flow_for_tool` directly, with no
//! outer agent and no `ToolCallFrame`. So the caller is stripped (nothing is
//! stamped) and the nested agent runs under a fresh `flowtool::<flow>::<ULID>`
//! session, not one derived from a calling agent's session and call id.
//!
//! The tests mutate process env (dispatch mode, opt-out), so they hold one
//! lock, and every run first clears the variables the host reads for this
//! decision, so an operator's shell cannot change the outcome; this file is its
//! own test binary, so nothing else reads those vars.

#![cfg(feature = "agentic-worker")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::runtime::TenantRuntime;
use greentic_runner_host::storage::{
    new_session_store, new_state_store, session_host_from, state_host_from,
};
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
const PACK_ID: &str = "nested.flow.agent";

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
    let agent: greentic_aw_runtime::AgentConfig = serde_json::from_value(json!({
        "agent_id": "helper",
        "system_prompt": "be brief",
        "tools": [],
        "llm": { "provider": "openai", "model": "none" }
    }))
    .expect("agent config");
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
        agents: std::collections::HashMap::from([("helper".to_string(), agent)]),
        graphs: std::collections::HashMap::new(),
    }
}

/// `agent.flow`: `agent` (dw.agent `helper`) → end.
fn build_pack(pack_path: &Path) -> Result<()> {
    let flows = json!([
        {
            "id": "agent.flow",
            "flow_type": "messaging",
            "start": "agent",
            "nodes": {
                "agent": {
                    "component": "dw.agent",
                    "operation": "helper",
                    "input": { "user_text": "hi" },
                    "routing": "end"
                }
            }
        }
    ]);
    let mut extensions = BTreeMap::new();
    extensions.insert(
        RUNTIME_FLOW_EXTENSION_ID.to_string(),
        ExtensionRef {
            kind: RUNTIME_FLOW_EXTENSION_ID.to_string(),
            version: "2.0.0".into(),
            digest: None,
            location: None,
            inline: Some(ExtensionInline::Other(json!({ "flows": flows }))),
        },
    );
    let manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: PACK_ID.parse()?,
        name: Some("Nested flow agent".into()),
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
        agents: BTreeMap::new(),
        extensions: Some(extensions),
    };
    let mut zip = zip::ZipWriter::new(File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&encode_pack_manifest(&manifest)?)?;
    let component_path = component_artifact_path(
        pack_path
            .parent()
            .context("pack path should have a parent temp dir")?,
    )?;
    zip.start_file("components/qa.process.wasm", options)?;
    std::io::copy(&mut File::open(&component_path)?, &mut zip)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard(Vec<(&'static str, Option<String>)>);

/// Every variable the host reads to decide whether (and how) a `dw.agent`
/// runs and is lent: cleared before each run so the shell cannot decide it.
const HOST_ENV: &[&str] = &[
    "GREENTIC_AW_DISPATCH",
    "GREENTIC_AW_NESTED_FLOW_AGENTS",
    "GREENTIC_AW_REDIS_URL",
    "GREENTIC_AW_STATE_BACKEND",
    "GREENTIC_EVENTS_NATS_URL",
];

impl EnvGuard {
    /// Clear [`HOST_ENV`], then set `vars`; every prior value comes back on
    /// drop.
    fn set(vars: &[(&'static str, &str)]) -> Self {
        let prev = HOST_ENV
            .iter()
            .copied()
            .chain(vars.iter().map(|(k, _)| *k))
            .map(|k| (k, std::env::var(k).ok()))
            .collect();
        for key in HOST_ENV {
            unsafe { std::env::remove_var(key) };
        }
        for (key, value) in vars {
            unsafe { std::env::set_var(key, value) };
        }
        Self(prev)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // In reverse, so a key recorded twice ends on its ORIGINAL value.
        for (key, old) in self.0.iter().rev() {
            match old {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}

/// Build a production host over the pack and run `agent.flow` as a flow tool
/// through the host's own pack instance.
fn run_nested_through_production_host(env: &[(&'static str, &str)]) -> Result<String> {
    run_on_the_nth_host(env, 1)
}

/// Like [`run_nested_through_production_host`], but builds `hosts` hosts in a
/// row (each one dropped before the next, as a revision swap does) and runs
/// the flow tool on the LAST one.
fn run_on_the_nth_host(env: &[(&'static str, &str)], hosts: usize) -> Result<String> {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::set(env);
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("nested.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_pack(&pack_path)?;
    let mut runtime = None;
    for _ in 0..hosts {
        drop(runtime.take());
        let session_store = new_session_store();
        let session_host = session_host_from(Arc::clone(&session_store));
        let state_store = new_state_store();
        let state_host = state_host_from(Arc::clone(&state_store));
        runtime = Some(rt.block_on(TenantRuntime::load(
            &pack_path,
            Arc::new(host_config(&bindings_path)),
            None,
            Some(pack_path.as_path()),
            None,
            Arc::new(greentic_runner_host::RunnerWasiPolicy::new()),
            session_host,
            session_store,
            state_store,
            state_host,
            greentic_runner_host::secrets::default_manager()?,
            None,
            None,
            None,
        ))?);
    }
    let runtime = runtime.context("a host")?;
    let pack = runtime.all_packs().first().cloned().context("a pack")?;
    let out: Result<Value, String> = rt.block_on(pack.run_flow_for_tool("agent.flow", json!({})));
    Ok(match out {
        Ok(value) => value.to_string(),
        Err(error) => error,
    })
}

const NOT_WIRED: &str = "AgentNodeHandler";

#[test]
fn a_production_host_lends_its_real_agent_handler_to_flow_tools() -> Result<()> {
    let text = run_nested_through_production_host(&[])?;
    assert_reached_the_real_handler(&text);
    Ok(())
}

/// The real handler ran and failed (no LLM credential): its sanitised reply,
/// not the engine's "no handler" error.
fn assert_reached_the_real_handler(text: &str) {
    assert!(!text.contains(NOT_WIRED), "handler not reached: {text}");
    assert!(text.contains(r#""terminated_by":"error""#), "{text}");
}

#[test]
fn a_rebuilt_host_registers_on_its_own_packs_after_the_old_one_is_dropped() -> Result<()> {
    let text = run_on_the_nth_host(&[], 2)?;
    assert_reached_the_real_handler(&text);
    Ok(())
}

#[test]
fn over_nats_dispatch_a_flow_tools_agent_is_not_run_in_process() -> Result<()> {
    let text = run_nested_through_production_host(&[("GREENTIC_AW_DISPATCH", "nats")])?;
    assert!(text.contains(NOT_WIRED), "must keep failing loudly: {text}");
    Ok(())
}

#[test]
fn the_env_opt_out_restores_the_unwired_behaviour() -> Result<()> {
    for off in ["0", "false", "off"] {
        let text = run_nested_through_production_host(&[("GREENTIC_AW_NESTED_FLOW_AGENTS", off)])?;
        assert!(text.contains(NOT_WIRED), "{off}: {text}");
    }
    Ok(())
}
