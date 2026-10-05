//! A `dw.agent` inside a flow run as an agent's `flow:` tool. The flow runs on
//! a fresh engine per call (`PackRuntime::load_flow_engine`); the host hands it
//! the agent handler the top-level engine was wired with, held weakly.
//!
//! The agent handler is a recording stub, so no LLM runs. Pack-building
//! harness trimmed from `tests/flow_tool_interactive.rs`.

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
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::agent_node::AgentNodeHandler;
use greentic_runner_host::runner::nested_flow::NestedFlowHandlers;
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
        agents: std::collections::HashMap::new(),
        graphs: std::collections::HashMap::new(),
    }
}

/// - `agent.flow`: `agent` (dw.agent `helper`) → end.
/// - `park.flow`: `pre` (dw.agent) → `card` (emit.response) → `ask`
///   (session.wait) → `post` (dw.agent) → end. Parks after the card.
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
        },
        {
            "id": "park.flow",
            "flow_type": "messaging",
            "start": "pre",
            "nodes": {
                "pre": {
                    "component": "dw.agent",
                    "operation": "helper",
                    "input": { "user_text": "before" },
                    "routing": { "next": { "node_id": "card" } }
                },
                "card": {
                    "component": "emit.response",
                    "input": { "text": "pick a room" },
                    "routing": { "next": { "node_id": "ask" } }
                },
                "ask": {
                    "component": "session.wait",
                    "input": { "reason": "awaiting the room choice" },
                    "routing": { "next": { "node_id": "post" } }
                },
                "post": {
                    "component": "dw.agent",
                    "operation": "helper",
                    "input": { "user_text": "after" },
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

struct Harness {
    _temp: TempDir,
    pack: Arc<PackRuntime>,
}

fn harness() -> Result<Harness> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("nested.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_pack(&pack_path)?;
    let config = Arc::new(host_config(&bindings_path));
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        &pack_path,
        config,
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
    Ok(Harness { _temp: temp, pack })
}

/// Records what each nested agent step saw. `recurse` makes the agent call
/// its own flow tool again (Task 5); `fail` makes it error (Task 4).
#[derive(Default)]
struct Recorder {
    reply: Value,
    fail: bool,
    recurse: Option<Arc<PackRuntime>>,
    sessions: Mutex<Vec<String>>,
    saw_run_context: Mutex<Vec<bool>>,
}

impl Recorder {
    fn replying(reply: Value) -> Self {
        Self {
            reply,
            ..Self::default()
        }
    }
    fn sessions(&self) -> Vec<String> {
        self.sessions.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl AgentNodeHandler for Recorder {
    async fn execute(
        &self,
        _tenant_id: &str,
        _env_id: &str,
        _agent_id: &str,
        session_id: &str,
        _flow_input: &Value,
        _conversational: bool,
        _caller: Option<&Value>,
    ) -> anyhow::Result<Value> {
        self.sessions.lock().unwrap().push(session_id.to_string());
        self.saw_run_context
            .lock()
            .unwrap()
            .push(greentic_aw_runtime::RunContext::current().is_some());
        if self.fail {
            anyhow::bail!("stub agent failed on purpose");
        }
        if let Some(pack) = &self.recurse {
            let inner = pack.run_flow_for_tool("agent.flow", json!({})).await;
            return Ok(json!({
                "reply": "one level",
                "inner": match inner { Ok(value) => value, Err(error) => json!({ "error": error }) },
            }));
        }
        Ok(self.reply.clone())
    }
}

fn register(pack: &PackRuntime, handler: &Arc<dyn AgentNodeHandler>) {
    pack.set_nested_flow_handlers(NestedFlowHandlers::default().with_agent(handler));
}

/// The text of either result shape: the engine may report a node failure as
/// an `Err` or inside the flow's output.
fn result_text(out: &Result<Value, String>) -> String {
    match out {
        Ok(value) => value.to_string(),
        Err(error) => error.clone(),
    }
}

#[test]
fn a_registered_handler_runs_the_nested_agent() -> Result<()> {
    let h = harness()?;
    let recorder = Arc::new(Recorder::replying(json!({ "reply": "nested-says-hi" })));
    let handler: Arc<dyn AgentNodeHandler> = recorder.clone();
    register(&h.pack, &handler);

    let out = RUNTIME.block_on(h.pack.run_flow_for_tool("agent.flow", json!({})));

    assert!(result_text(&out).contains("nested-says-hi"), "{out:?}");
    assert_eq!(recorder.sessions().len(), 1);
    Ok(())
}

#[test]
fn with_no_registered_handler_the_nested_agent_still_fails_loudly() -> Result<()> {
    let h = harness()?;
    let out = RUNTIME.block_on(h.pack.run_flow_for_tool("agent.flow", json!({})));
    assert!(result_text(&out).contains("AgentNodeHandler"), "{out:?}");
    Ok(())
}

#[test]
fn a_dropped_handler_falls_back_to_the_loud_failure() -> Result<()> {
    let h = harness()?;
    let handler: Arc<dyn AgentNodeHandler> =
        Arc::new(Recorder::replying(json!({ "reply": "never" })));
    register(&h.pack, &handler);
    drop(handler);

    let out = RUNTIME.block_on(h.pack.run_flow_for_tool("agent.flow", json!({})));

    let text = result_text(&out);
    assert!(text.contains("AgentNodeHandler"), "{text}");
    assert!(!text.contains("never"), "{text}");
    Ok(())
}
