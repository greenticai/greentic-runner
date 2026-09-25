//! Deployed run audit (`greentic_runner_host::run_outcome`), end to end
//! through the production turn path: `StateMachineRuntime::handle` →
//! `PackFlowAdapter::call_traced` → `FlowEngine` → `FlowResumeStore`.
//!
//! Pins, against a real pack:
//! - a parked turn reports `in_progress` at the node it resumes at, and the
//!   resume reports `completed` under the SAME `run_id`;
//! - a new run after completion gets a NEW `run_id`;
//! - a failing node reports `technical_error` with the failing node and a
//!   short class, and no reported field carries message text or error text;
//! - an engine `Err` reports `technical_error`;
//! - a flow that runs a `dw.agent` node reports `agentic` only;
//! - a runtime with NO sink persists the exact resume record it always did.
//!
//! Harness copied from `tests/resume_characterization.rs`.

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
use greentic_runner_host::engine::runtime::{IngressEnvelope, StateMachineRuntime};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::run_outcome::{RunKind, RunOutcome, RunOutcomeSink, RunStatus};
use greentic_runner_host::runner::engine::FlowEngine;
use greentic_runner_host::storage::new_session_store;
use greentic_runner_host::storage::session::session_host_from;
use greentic_runner_host::storage::state::{new_state_store, state_host_from};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, ExtensionInline, ExtensionRef,
    PackFlowEntry, PackKind, PackManifest, ReplyScope, ResourceHints, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::json;
use tempfile::TempDir;
use zip::ZipArchive;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "run.outcome.emission";
const SECRET_TEXT: &str = "my password is hunter2";

static RUNTIME: Lazy<&'static tokio::runtime::Runtime> = Lazy::new(|| {
    Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime"),
    ))
});

#[derive(Default)]
struct RecordingSink {
    outcomes: Mutex<Vec<RunOutcome>>,
}

impl RunOutcomeSink for RecordingSink {
    fn record(&self, outcome: RunOutcome) {
        self.outcomes.lock().expect("sink lock").push(outcome);
    }
}

impl RecordingSink {
    fn take(&self) -> Vec<RunOutcome> {
        std::mem::take(&mut *self.outcomes.lock().expect("sink lock"))
    }
}

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

/// Three flows, all builtin or failing-by-construction, so no WASM runs:
/// - `wait.flow`: `ask` (session.wait) → `done` (emit.response) → end
/// - `fail.flow`: `boom` (a component the pack does not have) → end
/// - `agent.flow`: `agent` (dw.agent with no config) → end
fn build_pack(pack_path: &Path) -> Result<()> {
    let flows = json!([
        {
            "id": "wait.flow",
            "flow_type": "messaging",
            "start": "ask",
            "nodes": {
                "ask": {
                    "component": "session.wait",
                    "input": { "reason": "awaiting the user" },
                    "routing": { "next": { "node_id": "done" } }
                },
                "done": {
                    "component": "emit.response",
                    "input": { "text": "thanks" },
                    "routing": "end"
                }
            }
        },
        {
            "id": "fail.flow",
            "flow_type": "messaging",
            "start": "boom",
            "nodes": {
                "boom": {
                    "component": "missing.component",
                    "input": { "text": "{{entry.text}}" },
                    "routing": "end"
                }
            }
        },
        {
            "id": "agent.flow",
            "flow_type": "messaging",
            "start": "agent",
            "nodes": {
                "agent": {
                    "component": "dw.agent",
                    "operation": "helper",
                    "input": { "message": "{{entry.text}}" },
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
    runtime: StateMachineRuntime,
    sink: Arc<RecordingSink>,
}

fn harness(with_sink: bool) -> Result<Harness> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("run-outcome.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_pack(&pack_path)?;
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
    let engine = Arc::new(rt.block_on(FlowEngine::new(vec![pack], Arc::clone(&config)))?);
    let session_store = new_session_store();
    let session_host = session_host_from(Arc::clone(&session_store));
    let state_host = state_host_from(new_state_store());
    let sink = Arc::new(RecordingSink::default());
    let runtime = StateMachineRuntime::from_flow_engine_with_run_outcome_sink(
        config,
        engine,
        std::collections::HashMap::new(),
        session_host,
        session_store,
        state_host,
        greentic_runner_host::secrets::default_manager()?,
        None,
        None,
        with_sink.then(|| Arc::clone(&sink) as Arc<dyn RunOutcomeSink>),
    )?;
    Ok(Harness {
        _temp: temp,
        runtime,
        sink,
    })
}

fn envelope(flow_id: &str, text: &str) -> IngressEnvelope {
    IngressEnvelope {
        tenant: "demo".into(),
        env: Some("local".into()),
        pack_id: Some(PACK_ID.into()),
        flow_id: flow_id.into(),
        flow_type: Some("messaging".into()),
        action: Some("messaging".into()),
        session_hint: Some(format!("demo:webchat:chan:conv-{flow_id}:u-7")),
        provider: Some("webchat".into()),
        messaging_endpoint_id: None,
        channel: Some("chan".into()),
        conversation: Some(format!("conv-{flow_id}")),
        user: Some("u-7".into()),
        entry_node: None,
        activity_id: Some("activity-1".into()),
        timestamp: None,
        payload: json!({ "text": text }),
        metadata: None,
        reply_scope: Some(ReplyScope {
            conversation: format!("conv-{flow_id}"),
            thread: None,
            reply_to: None,
            correlation: None,
        }),
    }
}

/// Every string a reported outcome carries, flattened, for leak checks.
fn reported_text(outcome: &RunOutcome) -> String {
    format!(
        "{} {} {:?} {:?} {:?} {:?} {:?} {}",
        outcome.run_id,
        outcome.flow_id,
        outcome.last_step,
        outcome.user_ref,
        outcome.channel,
        outcome.outcome_json,
        outcome.error_code,
        outcome.started_at
    )
}

#[test]
fn a_parked_run_resumes_under_the_same_run_id_and_a_new_run_gets_a_new_one() -> Result<()> {
    let rt = *RUNTIME;
    let h = harness(true)?;

    rt.block_on(h.runtime.handle(envelope("wait.flow", "hello")))?;
    let first = h.sink.take();
    assert_eq!(first.len(), 1, "one outcome per turn");
    assert_eq!(first[0].status, RunStatus::InProgress);
    assert_eq!(first[0].kind, RunKind::Flow);
    assert_eq!(first[0].last_step.as_deref(), Some("done"));
    assert_eq!(first[0].flow_id, "wait.flow");
    assert_eq!(first[0].user_ref.as_deref(), Some("webchat:u-7"));
    assert_eq!(first[0].channel.as_deref(), Some("webchat"));
    let run_id = first[0].run_id.clone();

    rt.block_on(h.runtime.handle(envelope("wait.flow", "again")))?;
    let second = h.sink.take();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].status, RunStatus::Completed);
    assert_eq!(second[0].last_step.as_deref(), Some("done"));
    assert_eq!(second[0].run_id, run_id, "a resume reuses the run id");
    assert_eq!(second[0].started_at, first[0].started_at);

    rt.block_on(h.runtime.handle(envelope("wait.flow", "fresh")))?;
    let third = h.sink.take();
    assert_eq!(third[0].status, RunStatus::InProgress);
    assert_ne!(third[0].run_id, run_id, "a completed run is not resumed");
    Ok(())
}

#[test]
fn a_failing_node_is_a_technical_error_that_carries_no_message_or_error_text() -> Result<()> {
    let rt = *RUNTIME;
    let h = harness(true)?;
    let reply = rt.block_on(h.runtime.handle(envelope("fail.flow", SECRET_TEXT)))?;
    let outcomes = h.sink.take();
    assert_eq!(outcomes.len(), 1, "reply was {reply}");
    let outcome = &outcomes[0];
    assert_eq!(
        outcome.status,
        RunStatus::TechnicalError,
        "reply was {reply}"
    );
    assert_eq!(outcome.kind, RunKind::Flow);
    assert_eq!(outcome.last_step.as_deref(), Some("boom"));
    let code = outcome.error_code.as_deref().context("error code")?;
    assert!(
        code.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "error_code must be a short class, got {code:?}"
    );

    let text = reported_text(outcome);
    assert!(!text.contains("hunter2"), "message text leaked: {text}");
    assert!(
        !text.contains("missing.component"),
        "error text leaked: {text}"
    );
    Ok(())
}

#[test]
fn an_engine_error_is_reported_as_a_technical_error() -> Result<()> {
    let rt = *RUNTIME;
    let h = harness(true)?;
    let mut env = envelope("wait.flow", SECRET_TEXT);
    env.entry_node = Some("no_such_node".into());
    assert!(rt.block_on(h.runtime.handle(env)).is_err());
    let outcomes = h.sink.take();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].status, RunStatus::TechnicalError);
    assert_eq!(
        outcomes[0].error_code.as_deref(),
        Some("flow_execution_failed")
    );
    let text = reported_text(&outcomes[0]);
    assert!(!text.contains("hunter2") && !text.contains("no_such_node"));
    Ok(())
}

#[test]
fn a_run_that_executes_a_dw_agent_node_reports_agentic_only() -> Result<()> {
    let rt = *RUNTIME;
    let h = harness(true)?;
    let _ = rt.block_on(h.runtime.handle(envelope("agent.flow", SECRET_TEXT)));
    let outcomes = h.sink.take();
    assert_eq!(outcomes.len(), 1);
    let outcome = &outcomes[0];
    assert_eq!(outcome.kind, RunKind::Agentic);
    assert_eq!(outcome.status, RunStatus::Agentic);
    assert_eq!(outcome.last_step, None);
    assert_eq!(outcome.error_code, None);
    assert!(!reported_text(outcome).contains("hunter2"));
    Ok(())
}

#[test]
fn without_a_sink_turns_still_park_and_resume() -> Result<()> {
    let rt = *RUNTIME;
    let h = harness(false)?;
    let parked = rt.block_on(h.runtime.handle(envelope("wait.flow", "hello")))?;
    let done = rt.block_on(h.runtime.handle(envelope("wait.flow", "again")))?;
    assert!(h.sink.take().is_empty());
    assert_ne!(parked, done);
    Ok(())
}
