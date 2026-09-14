//! Characterizes the native `operala.call` and `agentic.call` flow nodes.
//!
//! Both nodes are runtime-dispatch nodes that reuse the same
//! [`RemoteDispatchHandler`] seam as `sorla.call`, differing only in the
//! `runtime` name they pass to the handler.  Two modes are exercised for each
//! node through the real flow engine + the in-memory `.gtpack` harness:
//!
//!   1. fire-and-forget (`await=false`) -> flow COMPLETES, node output carries
//!      `{"dispatched": true}`; the handler saw `FireAndForget`, the expected
//!      `runtime` name, and the target.
//!   2. await (`await=true`) -> flow PAUSES (`FlowStatus::Waiting`); the handler
//!      saw `Await`, the expected `runtime` name, and the session hint as the
//!      correlation id.
//!
//! A stub [`RemoteDispatchHandler`] that captures the full [`RemoteDispatch`]
//! (including `runtime`) is used so no NATS broker is required.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use async_trait::async_trait;
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use greentic_runner_host::runner::remote_dispatch::{
    RemoteDispatch, RemoteDispatchAction, RemoteDispatchHandler,
};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, DispatchMode, ExtensionInline,
    ExtensionRef, PackFlowEntry, PackKind, PackManifest, ResourceHints, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "runtime.call.nodes.test";
const FLOW_ID: &str = "dispatch.flow";
const SESSION_HINT: &str = "demo:provider:chan:conv:user::pack=p";

static RUNTIME: Lazy<&'static tokio::runtime::Runtime> = Lazy::new(|| {
    Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime"),
    ))
});

/// Records the full [`RemoteDispatch`] including the `runtime` field so tests
/// can assert that `operala.call` / `agentic.call` route to the correct runtime
/// name without requiring a live NATS broker.
#[derive(Default)]
struct RuntimeCapturingStub {
    last: Mutex<Option<CapturedDispatch>>,
}

#[derive(Clone, Debug)]
struct CapturedDispatch {
    runtime: String,
    target: String,
    operation: String,
    mode: DispatchMode,
    correlation_id: String,
}

#[async_trait]
impl RemoteDispatchHandler for RuntimeCapturingStub {
    async fn dispatch(&self, request: RemoteDispatch) -> Result<RemoteDispatchAction> {
        let action = match request.mode {
            DispatchMode::Await => RemoteDispatchAction::AwaitingResponse {
                correlation_id: request.correlation_id.clone(),
            },
            DispatchMode::FireAndForget => RemoteDispatchAction::Dispatched,
        };
        *self.last.lock().unwrap() = Some(CapturedDispatch {
            runtime: request.runtime,
            target: request.target,
            operation: request.operation,
            mode: request.mode,
            correlation_id: request.correlation_id,
        });
        Ok(action)
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
    let mut archive =
        zip::ZipArchive::new(File::open(&archive_path).context("open fixture gtpack")?)?;
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

/// Build a `.gtpack` whose only flow is a single node using `component_id` as
/// the component (e.g. `"operala.call"` or `"agentic.call"`), with `operation =
/// "dep-1"` as the runtime target.  An optional `done` emit node is wired for
/// the await variant so the engine has a valid resume target.
fn build_dispatch_pack(
    pack_path: &Path,
    component_id: &str,
    node_input: Value,
    await_mode: bool,
) -> Result<()> {
    let routing = if await_mode {
        json!({ "next": { "node_id": "done" } })
    } else {
        json!("end")
    };
    let mut nodes = serde_json::Map::new();
    nodes.insert(
        "call".to_string(),
        json!({
            "component": component_id,
            "operation": "dep-1",
            "input": node_input,
            "routing": routing,
        }),
    );
    if await_mode {
        nodes.insert(
            "done".to_string(),
            json!({
                "component": "emit.response",
                "input": { "text": "resumed" },
                "routing": "end",
            }),
        );
    }

    let runtime_flow = json!({
        "id": FLOW_ID,
        "flow_type": "messaging",
        "start": "call",
        "nodes": Value::Object(nodes),
    });
    let runtime_extension = json!({ "flows": [runtime_flow] });

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
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&manifest_bytes)?;
    let component_path = component_artifact_path(
        pack_path
            .parent()
            .expect("pack path should have a parent temp dir"),
    )?;
    zip.start_file("components/qa.process.wasm", options)?;
    let mut comp_file = File::open(&component_path)?;
    std::io::copy(&mut comp_file, &mut zip)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

fn build_engine(
    pack_path: &Path,
    config: Arc<HostConfig>,
    handler: Arc<RuntimeCapturingStub>,
) -> Result<(Arc<PackRuntime>, FlowEngine)> {
    let rt = *RUNTIME;
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        pack_path,
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
    let mut engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;
    engine.set_remote_dispatch_handler(handler);
    Ok((pack, engine))
}

fn flow_ctx<'a>(config: &'a HostConfig, pack_id: &'a str) -> FlowContext<'a> {
    FlowContext {
        tenant: config.tenant.as_str(),
        pack_id,
        flow_id: FLOW_ID,
        node_id: None,
        tool: None,
        action: Some("messaging"),
        session_id: Some(SESSION_HINT),
        provider_id: Some("provider"),
        reply_scope: None,
        retry_config: config.retry.clone().into(),
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    }
}

/// Recursively search the finalized output envelope for a `dispatched: true`
/// marker, since the engine wraps node outputs in nested state envelopes.
fn output_has_dispatched(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if map.get("dispatched") == Some(&Value::Bool(true)) {
                return true;
            }
            map.values().any(output_has_dispatched)
        }
        Value::Array(items) => items.iter().any(output_has_dispatched),
        _ => false,
    }
}

// ── operala.call tests ────────────────────────────────────────────────────────

#[test]
fn operala_call_fire_and_forget_completes_and_dispatches() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("operala-fire.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    build_dispatch_pack(
        &pack_path,
        "operala.call",
        json!({ "await": false, "operation": "ping", "input": {} }),
        false,
    )?;

    let config = Arc::new(host_config(&bindings_path));
    let handler = Arc::new(RuntimeCapturingStub::default());
    let (pack, engine) = build_engine(&pack_path, Arc::clone(&config), Arc::clone(&handler))?;

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("fire-and-forget operala.call run")?;

    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => {
            anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason)
        }
    }
    assert!(
        output_has_dispatched(&execution.output),
        "expected dispatched=true in output, got {:?}",
        execution.output
    );

    let recorded = handler
        .last
        .lock()
        .unwrap()
        .clone()
        .expect("handler should have recorded a dispatch");
    assert_eq!(
        recorded.runtime, "operala",
        "runtime name must be 'operala', got '{}'",
        recorded.runtime
    );
    assert_eq!(recorded.mode, DispatchMode::FireAndForget);
    assert_eq!(recorded.target, "dep-1");
    assert_eq!(recorded.operation, "ping");
    Ok(())
}

#[test]
fn operala_call_await_pauses_with_session_hint_correlation() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("operala-await.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    build_dispatch_pack(
        &pack_path,
        "operala.call",
        json!({ "await": true, "operation": "create", "input": {} }),
        true,
    )?;

    let config = Arc::new(host_config(&bindings_path));
    let handler = Arc::new(RuntimeCapturingStub::default());
    let (pack, engine) = build_engine(&pack_path, Arc::clone(&config), Arc::clone(&handler))?;

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("await operala.call run")?;

    match execution.status {
        FlowStatus::Waiting(_) => {}
        FlowStatus::Completed => {
            anyhow::bail!("flow completed but should have paused awaiting the runtime response")
        }
    }

    let recorded = handler
        .last
        .lock()
        .unwrap()
        .clone()
        .expect("handler should have recorded a dispatch");
    assert_eq!(
        recorded.runtime, "operala",
        "runtime name must be 'operala', got '{}'",
        recorded.runtime
    );
    assert_eq!(recorded.mode, DispatchMode::Await);
    assert_eq!(recorded.target, "dep-1");
    assert_eq!(recorded.operation, "create");
    let bare_hint = SESSION_HINT.split("::").next().unwrap();
    let expected_correlation = format!(
        "{}::pack={}::flow={}",
        bare_hint,
        pack.metadata().pack_id.as_str(),
        FLOW_ID
    );
    assert_eq!(recorded.correlation_id, expected_correlation);
    assert!(recorded.correlation_id.starts_with(bare_hint));
    assert!(
        recorded
            .correlation_id
            .contains(&format!("::pack={}", pack.metadata().pack_id.as_str()))
    );
    assert!(
        recorded
            .correlation_id
            .contains(&format!("::flow={FLOW_ID}"))
    );
    Ok(())
}

// ── agentic.call tests ────────────────────────────────────────────────────────

#[test]
fn agentic_call_fire_and_forget_completes_and_dispatches() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("agentic-fire.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    build_dispatch_pack(
        &pack_path,
        "agentic.call",
        json!({ "await": false, "operation": "ping", "input": {} }),
        false,
    )?;

    let config = Arc::new(host_config(&bindings_path));
    let handler = Arc::new(RuntimeCapturingStub::default());
    let (pack, engine) = build_engine(&pack_path, Arc::clone(&config), Arc::clone(&handler))?;

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("fire-and-forget agentic.call run")?;

    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => {
            anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason)
        }
    }
    assert!(
        output_has_dispatched(&execution.output),
        "expected dispatched=true in output, got {:?}",
        execution.output
    );

    let recorded = handler
        .last
        .lock()
        .unwrap()
        .clone()
        .expect("handler should have recorded a dispatch");
    assert_eq!(
        recorded.runtime, "agentic",
        "runtime name must be 'agentic', got '{}'",
        recorded.runtime
    );
    assert_eq!(recorded.mode, DispatchMode::FireAndForget);
    assert_eq!(recorded.target, "dep-1");
    assert_eq!(recorded.operation, "ping");
    Ok(())
}

#[test]
fn agentic_call_await_pauses_with_session_hint_correlation() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("agentic-await.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    build_dispatch_pack(
        &pack_path,
        "agentic.call",
        json!({ "await": true, "operation": "create", "input": {} }),
        true,
    )?;

    let config = Arc::new(host_config(&bindings_path));
    let handler = Arc::new(RuntimeCapturingStub::default());
    let (pack, engine) = build_engine(&pack_path, Arc::clone(&config), Arc::clone(&handler))?;

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("await agentic.call run")?;

    match execution.status {
        FlowStatus::Waiting(_) => {}
        FlowStatus::Completed => {
            anyhow::bail!("flow completed but should have paused awaiting the runtime response")
        }
    }

    let recorded = handler
        .last
        .lock()
        .unwrap()
        .clone()
        .expect("handler should have recorded a dispatch");
    assert_eq!(
        recorded.runtime, "agentic",
        "runtime name must be 'agentic', got '{}'",
        recorded.runtime
    );
    assert_eq!(recorded.mode, DispatchMode::Await);
    assert_eq!(recorded.target, "dep-1");
    assert_eq!(recorded.operation, "create");
    let bare_hint = SESSION_HINT.split("::").next().unwrap();
    let expected_correlation = format!(
        "{}::pack={}::flow={}",
        bare_hint,
        pack.metadata().pack_id.as_str(),
        FLOW_ID
    );
    assert_eq!(recorded.correlation_id, expected_correlation);
    assert!(recorded.correlation_id.starts_with(bare_hint));
    assert!(
        recorded
            .correlation_id
            .contains(&format!("::pack={}", pack.metadata().pack_id.as_str()))
    );
    assert!(
        recorded
            .correlation_id
            .contains(&format!("::flow={FLOW_ID}"))
    );
    Ok(())
}
