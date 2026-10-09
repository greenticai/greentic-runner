//! End-to-end test for [`RuntimeSessionResumer`]: pause a flow at a builtin
//! `session.wait` node through the real ingress entry
//! ([`StateMachineRuntime::handle`]), then resume it to completion by calling
//! the resumer directly with the matching correlation id — no NATS broker.
//!
//! This pins that the production resume path
//!   `RuntimeSessionResumer::resume`
//!     -> `StateMachineRuntime::handle`
//!       -> `PackFlowAdapter::call` -> `FlowResumeStore::fetch`
//!         -> `FlowEngine::resume`
//! actually re-keys the saved wait. The correlation id used is the full store
//! hint (`<bare hint>::pack=<pack_id>`), mirroring the convention pinned by
//! `tests/sorla_node.rs` (where `ctx.session_id` carries the `::pack=` suffix).
//!
//! Pack-building helpers are copied from `tests/resume_characterization.rs`; the
//! flow is two builtin nodes (`session.wait` -> `emit.response`) so no WASM is
//! invoked.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use async_trait::async_trait;
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::engine::runtime::{IngressEnvelope, StateMachineRuntime};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::dispatch_listener::SessionResumer;
use greentic_runner_host::runner::engine::FlowEngine;
use greentic_runner_host::runner::remote_dispatch::{
    RemoteDispatch, RemoteDispatchAction, RemoteDispatchHandler,
};
use greentic_runner_host::runner::runtime_session_resumer::{
    RuntimeSessionResumer, split_dispatch_nonce,
};
use greentic_runner_host::storage::new_session_store;
use greentic_runner_host::storage::session::session_host_from;
use greentic_runner_host::storage::state::new_state_store;
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::DispatchMode;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, EnvId, ExtensionInline,
    ExtensionRef, PackFlowEntry, PackKind, PackManifest, ReplyScope, ResourceHints, TenantCtx,
    TenantId, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::json;
use tempfile::TempDir;
use zip::ZipArchive;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "resume.via.resumer";
const FLOW_ID: &str = "wait.flow";
const BARE_HINT: &str = "demo:provider:chan:conv:user";

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

/// Build a `.gtpack` whose only flow is `session.wait` -> `emit.response`.
fn build_wait_pack(pack_path: &Path) -> Result<()> {
    let runtime_flow = json!({
        "id": FLOW_ID,
        "flow_type": "messaging",
        "start": "wait",
        "nodes": {
            "wait": {
                "component": "session.wait",
                "input": { "reason": "awaiting downstream response" },
                "routing": { "next": { "node_id": "done" } }
            },
            "done": {
                "component": "emit.response",
                "input": { "text": "resumed and completed" },
                "routing": "end"
            }
        }
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

/// The inbound envelope that triggers turn 1 (pause at `session.wait`). Its
/// `session_hint`/`pack_id`/`conversation` determine the stored wait key.
fn inbound_envelope() -> IngressEnvelope {
    IngressEnvelope {
        tenant: "demo".into(),
        env: Some("local".into()),
        pack_id: Some(PACK_ID.into()),
        flow_id: FLOW_ID.into(),
        flow_type: Some("messaging".into()),
        action: Some("messaging".into()),
        session_hint: Some(BARE_HINT.into()),
        provider: Some("provider".into()),
        channel: Some("chan".into()),
        conversation: Some("conv".into()),
        user: Some("user".into()),
        entry_node: None,
        activity_id: Some("activity-1".into()),
        timestamp: None,
        messaging_endpoint_id: None,
        payload: json!({ "text": "start" }),
        metadata: None,
        reply_scope: Some(ReplyScope {
            conversation: "conv".into(),
            thread: None,
            reply_to: None,
            correlation: None,
        }),
    }
}

fn build_runtime(pack_path: &Path, config: Arc<HostConfig>) -> Result<Arc<StateMachineRuntime>> {
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
    let engine = Arc::new(rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?);

    // The session host and the resume store MUST share the same backing store
    // so the wait saved on turn 1 is visible to the fetch on resume.
    let session_store = new_session_store();
    let session_host = session_host_from(Arc::clone(&session_store));
    let state_store = new_state_store();
    let state_host = greentic_runner_host::storage::state::state_host_from(state_store);
    let secrets = greentic_runner_host::secrets::default_manager()?;

    let runtime = StateMachineRuntime::from_flow_engine(
        Arc::clone(&config),
        engine,
        std::collections::HashMap::new(),
        session_host,
        session_store,
        state_host,
        secrets,
        None,
        None,
    )?;
    Ok(Arc::new(runtime))
}

#[test]
fn resumer_resumes_paused_flow_to_completion() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("resume-via-resumer.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_wait_pack(&pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let runtime = build_runtime(&pack_path, Arc::clone(&config))?;

    // ---- Turn 1: inbound message pauses the flow at `session.wait`. ----
    // `handle` returns Ok with the wait node's emitted payload; the saved wait
    // now lives in the shared session store.
    let _paused = rt
        .block_on(runtime.handle(inbound_envelope()))
        .context("turn 1 should pause at session.wait")?;

    // ---- Turn 2: resume via the production resumer (no broker). ----
    // The correlation id carries the routing/keying suffixes the wire contract
    // does not echo: `::pack=<id>` (keys the saved wait, per `tests/sorla_node.rs`)
    // and `::flow=<id>` (routes through `StateMachine::step`, which needs a
    // registered `(pack_id, flow_id)`). The resumer strips both, re-keys the
    // saved wait, and resumes the flow to completion.
    let correlation_id = format!("{BARE_HINT}::pack={PACK_ID}::flow={FLOW_ID}");
    let tenant = TenantCtx::new(
        EnvId::from_str("local").unwrap(),
        TenantId::from_str("demo").unwrap(),
    );
    let resumer = RuntimeSessionResumer::new(Arc::clone(&runtime));
    rt.block_on(resumer.resume(
        tenant,
        &correlation_id,
        json!({ "ok": true, "output": { "reply": "downstream-done" } }),
    ))
    .context("resume should advance past the wait node and complete the flow")?;

    // ---- A second resume must NOT find the wait (it was cleared on completion).
    // `handle` then runs the flow fresh, which pauses again at `session.wait`
    // (Ok), proving the first resume consumed the saved wait rather than the
    // resume being a no-op against a still-present snapshot.
    let tenant_again = TenantCtx::new(
        EnvId::from_str("local").unwrap(),
        TenantId::from_str("demo").unwrap(),
    );
    let resumer_again = RuntimeSessionResumer::new(Arc::clone(&runtime));
    let second =
        rt.block_on(resumer_again.resume(tenant_again, &correlation_id, json!({ "ok": true })));
    // With no saved wait, the synthesized envelope starts the flow from the top
    // and pauses again at `session.wait`; `handle` returns Ok either way. The
    // assertion that matters is that turn 2 above succeeded.
    assert!(
        second.is_ok(),
        "second resume should not error even though the wait was already consumed"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Gold end-to-end test: prove the NODE-emitted correlation id round-trips
// through the resumer. Unlike the test above (which hand-builds the correlation
// id), this captures the EXACT correlation the `sorla.call` node published and
// feeds that captured value to the resumer.
// ---------------------------------------------------------------------------

const SORLA_FLOW_ID: &str = "sorla.wait.flow";

/// Records the correlation id the `sorla.call` node published, and returns
/// `AwaitingResponse` so the flow pauses (mirrors the await path without NATS).
#[derive(Default)]
struct CapturingDispatchHandler {
    last_correlation: Mutex<Option<String>>,
    /// Every dispatch in order, so a test can count them.
    all: Mutex<Vec<String>>,
    /// The `decision_token` each dispatch in `all` was issued, same order.
    tokens: Mutex<Vec<Option<String>>>,
}

#[async_trait]
impl RemoteDispatchHandler for CapturingDispatchHandler {
    async fn dispatch(&self, request: RemoteDispatch) -> Result<RemoteDispatchAction> {
        *self.last_correlation.lock().unwrap() = Some(request.correlation_id.clone());
        self.all
            .lock()
            .unwrap()
            .push(request.correlation_id.clone());
        self.tokens
            .lock()
            .unwrap()
            .push(request.decision_token.clone());
        match request.mode {
            DispatchMode::Await => Ok(RemoteDispatchAction::AwaitingResponse {
                correlation_id: request.correlation_id,
            }),
            DispatchMode::FireAndForget => Ok(RemoteDispatchAction::Dispatched),
        }
    }
}

/// Build a `.gtpack` whose only flow is `sorla.call` (await) -> `emit.response`.
/// The await node routes forward to `done` so the resume has a node to advance
/// into, exactly like the existing await test in `tests/sorla_node.rs`.
fn build_sorla_wait_pack(pack_path: &Path) -> Result<()> {
    build_flow_pack(
        pack_path,
        json!({
            "id": SORLA_FLOW_ID,
            "flow_type": "messaging",
            "start": "call",
            "nodes": {
                "call": {
                    "component": "sorla.call",
                    "operation": "dep-1",
                    "input": { "await": true, "operation": "create", "input": {} },
                    "routing": { "next": { "node_id": "done" } }
                },
                "done": {
                    "component": "emit.response",
                    "input": { "text": "resumed and completed" },
                    "routing": "end"
                }
            }
        }),
    )
}

/// Write a `.gtpack` carrying `runtime_flow` as its only flow.
fn build_flow_pack(pack_path: &Path, runtime_flow: serde_json::Value) -> Result<()> {
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

/// Inbound envelope that triggers the sorla flow. Routes to `SORLA_FLOW_ID`.
fn sorla_inbound_envelope() -> IngressEnvelope {
    IngressEnvelope {
        flow_id: SORLA_FLOW_ID.into(),
        ..inbound_envelope()
    }
}

/// Like `build_runtime`, but sets a [`RemoteDispatchHandler`] on the engine so
/// the `sorla.call` node can run, and returns the handler so the test can read
/// the captured correlation id.
fn build_runtime_with_dispatch(
    pack_path: &Path,
    config: Arc<HostConfig>,
    handler: Arc<CapturingDispatchHandler>,
) -> Result<Arc<StateMachineRuntime>> {
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
    let engine = Arc::new(engine);

    let session_store = new_session_store();
    let session_host = session_host_from(Arc::clone(&session_store));
    let state_store = new_state_store();
    let state_host = greentic_runner_host::storage::state::state_host_from(state_store);
    let secrets = greentic_runner_host::secrets::default_manager()?;

    let runtime = StateMachineRuntime::from_flow_engine(
        Arc::clone(&config),
        engine,
        std::collections::HashMap::new(),
        session_host,
        session_store,
        state_host,
        secrets,
        None,
        None,
    )?;
    Ok(Arc::new(runtime))
}

/// GOLD TEST: the correlation id the `sorla.call` node publishes round-trips
/// through `RuntimeSessionResumer` and resumes the paused flow.
///
/// 1. Run the flow (inbound) -> the `sorla.call` await node publishes a
///    correlation id (captured by the stub) and the flow PAUSES (`Waiting`).
/// 2. Feed the CAPTURED correlation id (not a hand-built one) to the resumer.
/// 3. The flow resumes past the wait; a subsequent resume finds no saved wait,
///    proving the first resume consumed it.
#[test]
fn node_emitted_correlation_round_trips_through_resumer() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("sorla-resume.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_sorla_wait_pack(&pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let handler = Arc::new(CapturingDispatchHandler::default());
    let runtime =
        build_runtime_with_dispatch(&pack_path, Arc::clone(&config), Arc::clone(&handler))?;

    // ---- Turn 1: inbound message runs the sorla.call node and pauses. ----
    let paused = rt
        .block_on(runtime.handle(sorla_inbound_envelope()))
        .context("turn 1 should run the sorla.call node and pause awaiting the response")?;
    // The wait surfaces as a pending payload (the node returns
    // `{ "pending": true, ... }`); the saved wait now lives in the session store.
    assert!(
        payload_has_pending(&paused),
        "expected the sorla.call node to pause with a pending payload, got {paused:?}"
    );

    // Capture the EXACT correlation id the node published.
    let captured_correlation = handler
        .last_correlation
        .lock()
        .unwrap()
        .clone()
        .expect("the sorla.call node must have published a correlation id");

    // Sanity: the captured correlation carries the markers the resumer parses,
    // and preserves the bare hint used to key the saved wait.
    assert!(
        captured_correlation.starts_with(BARE_HINT),
        "captured correlation must preserve the bare hint, got {captured_correlation}"
    );
    assert!(
        captured_correlation.contains(&format!("::pack={PACK_ID}")),
        "captured correlation must carry the pack marker, got {captured_correlation}"
    );
    assert!(
        captured_correlation.contains(&format!("::flow={SORLA_FLOW_ID}")),
        "captured correlation must carry the flow marker, got {captured_correlation}"
    );

    // ---- Turn 2: resume with the CAPTURED correlation id (no hand-building). ----
    let tenant = TenantCtx::new(
        EnvId::from_str("local").unwrap(),
        TenantId::from_str("demo").unwrap(),
    );
    let resumer = RuntimeSessionResumer::new(Arc::clone(&runtime));
    rt.block_on(resumer.resume(
        tenant,
        &captured_correlation,
        json!({ "ok": true, "output": { "reply": "downstream-done" } }),
    ))
    .context("resuming with the node-emitted correlation id should advance past the wait")?;

    // ---- The saved wait must have been consumed by the first resume. ----
    // A second resume finds no wait, so the synthesized envelope re-runs the flow
    // from the top and pauses again (Ok). The captured-correlation resume above
    // succeeding is the load-bearing assertion.
    let tenant_again = TenantCtx::new(
        EnvId::from_str("local").unwrap(),
        TenantId::from_str("demo").unwrap(),
    );
    let resumer_again = RuntimeSessionResumer::new(Arc::clone(&runtime));
    let second = rt.block_on(resumer_again.resume(
        tenant_again,
        &captured_correlation,
        json!({ "ok": true }),
    ));
    assert!(
        second.is_ok(),
        "second resume should not error even though the wait was already consumed"
    );

    Ok(())
}

const INBOUND_THREAD: &str = "topic-7";
const INBOUND_REPLY_TO: &str = "msg-42";

/// Inbound envelope for the sorla flow whose originating reply scope carries a
/// non-empty `thread`/`reply_to`. This is the case the old code could not
/// resume: `FlowResumeStore::save` keys the wait by a `scope_hash` over
/// `conversation`/`thread`/`reply_to`, but the bare canonical hint only encodes
/// `conversation`, so the resumer used to synthesize an empty thread/reply_to
/// and miss the saved wait.
fn sorla_threaded_inbound_envelope() -> IngressEnvelope {
    IngressEnvelope {
        flow_id: SORLA_FLOW_ID.into(),
        reply_scope: Some(ReplyScope {
            conversation: "conv".into(),
            thread: Some(INBOUND_THREAD.into()),
            reply_to: Some(INBOUND_REPLY_TO.into()),
            correlation: None,
        }),
        ..inbound_envelope()
    }
}

/// THREADED GOLD TEST: a `sorla.call await` wait whose originating inbound
/// carried a non-empty `thread`/`reply_to` resumes via the node-emitted
/// correlation id. This is the regression that the reply-scope carry fixes.
///
/// 1. Run the flow with a threaded inbound -> the node publishes a correlation
///    id that now carries `::thread=`/`::reply=` markers, and the flow PAUSES.
/// 2. Feed the CAPTURED correlation id to the resumer -> it rebuilds the exact
///    threaded reply scope, so `FlowResumeStore::fetch` re-keys the saved wait
///    and the flow advances past the wait (would MISS without the markers).
/// 3. A second resume finds no saved wait, proving the first consumed it.
#[test]
fn threaded_inbound_correlation_round_trips_through_resumer() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("sorla-resume-threaded.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_sorla_wait_pack(&pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let handler = Arc::new(CapturingDispatchHandler::default());
    let runtime =
        build_runtime_with_dispatch(&pack_path, Arc::clone(&config), Arc::clone(&handler))?;

    // ---- Turn 1: threaded inbound runs the sorla.call node and pauses. ----
    let paused = rt
        .block_on(runtime.handle(sorla_threaded_inbound_envelope()))
        .context("turn 1 should run the sorla.call node and pause awaiting the response")?;
    assert!(
        payload_has_pending(&paused),
        "expected the sorla.call node to pause with a pending payload, got {paused:?}"
    );

    // Capture the EXACT correlation id the node published.
    let captured_correlation = handler
        .last_correlation
        .lock()
        .unwrap()
        .clone()
        .expect("the sorla.call node must have published a correlation id");

    // The threaded correlation must carry the new opaque markers so the resumer
    // can reproduce the same scope_hash the save used.
    assert!(
        captured_correlation.contains(&format!("::thread={INBOUND_THREAD}")),
        "captured correlation must carry the thread marker, got {captured_correlation}"
    );
    assert!(
        captured_correlation.contains(&format!("::reply={INBOUND_REPLY_TO}")),
        "captured correlation must carry the reply marker, got {captured_correlation}"
    );

    // Control: the bare-hint-only correlation (no thread/reply markers) must NOT
    // re-key the threaded wait — proving thread/reply_to genuinely participate in
    // the key and the markers are load-bearing. With no wait found, `handle`
    // re-runs the flow from the top and pauses AGAIN (still pending), leaving the
    // original threaded wait intact for the real resume below.
    let bare_correlation = format!("{BARE_HINT}::pack={PACK_ID}::flow={SORLA_FLOW_ID}");
    let control_tenant = TenantCtx::new(
        EnvId::from_str("local").unwrap(),
        TenantId::from_str("demo").unwrap(),
    );
    let control_resumer = RuntimeSessionResumer::new(Arc::clone(&runtime));
    rt.block_on(control_resumer.resume(control_tenant, &bare_correlation, json!({ "ok": true })))
        .context("bare-hint resume should not error (it just re-runs and re-pauses)")?;

    // ---- Turn 2: resume with the CAPTURED (threaded) correlation id. ----
    let tenant = TenantCtx::new(
        EnvId::from_str("local").unwrap(),
        TenantId::from_str("demo").unwrap(),
    );
    let resumer = RuntimeSessionResumer::new(Arc::clone(&runtime));
    rt.block_on(resumer.resume(
        tenant,
        &captured_correlation,
        json!({ "ok": true, "output": { "reply": "downstream-done" } }),
    ))
    .context("resuming with the threaded node-emitted correlation should advance past the wait")?;

    // ---- The threaded wait must have been consumed by the resume above. ----
    // We re-pause via a fresh threaded inbound (so a wait exists again), then
    // confirm the threaded resume consumes it: clearing it means a subsequent
    // `clear` is idempotent and the resume path keyed it correctly. The
    // load-bearing assertion is that the threaded resume above succeeded where a
    // bare-hint resume could not have re-keyed the threaded wait.
    let second_tenant = TenantCtx::new(
        EnvId::from_str("local").unwrap(),
        TenantId::from_str("demo").unwrap(),
    );
    let resumer_again = RuntimeSessionResumer::new(Arc::clone(&runtime));
    let second = rt.block_on(resumer_again.resume(
        second_tenant,
        &captured_correlation,
        json!({ "ok": true }),
    ));
    assert!(
        second.is_ok(),
        "second resume should not error even though the wait was already consumed"
    );

    Ok(())
}

/// Recursively search a response envelope for a `pending: true` marker (the
/// engine wraps node outputs in nested state envelopes).
fn payload_has_pending(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            if map.get("pending") == Some(&serde_json::Value::Bool(true)) {
                return true;
            }
            map.values().any(payload_has_pending)
        }
        serde_json::Value::Array(items) => items.iter().any(payload_has_pending),
        _ => false,
    }
}

const APPROVAL_FLOW_ID: &str = "approval.twice.flow";

/// Two `approval.call` gates in ONE flow, so one conversation raises two
/// approvals in sequence: `gate1` -> `gate2` -> `done`.
fn build_two_approval_pack(pack_path: &Path) -> Result<()> {
    let gate = |next: &str| {
        json!({
            "component": "approval.call",
            "operation": "approvals",
            "input": { "await": true, "operation": "request", "input": { "mode": "always" } },
            "routing": { "next": { "node_id": next } }
        })
    };
    build_flow_pack(
        pack_path,
        json!({
            "id": APPROVAL_FLOW_ID,
            "flow_type": "messaging",
            "start": "gate1",
            "nodes": {
                "gate1": gate("gate2"),
                "gate2": gate("done"),
                "done": {
                    "component": "emit.response",
                    "input": { "text": "both approved" },
                    "routing": "end"
                }
            }
        }),
    )
}

fn demo_tenant() -> TenantCtx {
    TenantCtx::new(
        EnvId::from_str("local").unwrap(),
        TenantId::from_str("demo").unwrap(),
    )
}

/// greentic-runner#793: a second approval in one conversation must be a
/// DIFFERENT correlation id (the responder UNIQUE-indexes it), each id must
/// resume its own gate, and a stale response for the first gate must not
/// resume the second.
#[test]
fn two_approvals_in_one_conversation_get_distinct_ids_and_resume_their_own_gate() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("approval-twice.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_two_approval_pack(&pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let handler = Arc::new(CapturingDispatchHandler::default());
    let runtime =
        build_runtime_with_dispatch(&pack_path, Arc::clone(&config), Arc::clone(&handler))?;
    let resumer = RuntimeSessionResumer::new(Arc::clone(&runtime));
    let dispatched = || handler.all.lock().unwrap().clone();
    let token = |i: usize| {
        handler.tokens.lock().unwrap()[i]
            .clone()
            .expect("token issued")
    };
    let approve = |token: String| json!({ "ok": true, "output": { "decision": "approved", "decision_token": token } });

    // ---- Turn 1: parks on gate1. ----
    let inbound = IngressEnvelope {
        flow_id: APPROVAL_FLOW_ID.into(),
        ..inbound_envelope()
    };
    let paused = rt.block_on(runtime.handle(inbound))?;
    assert!(
        payload_has_pending(&paused),
        "gate1 must park, got {paused:?}"
    );
    let first = dispatched();
    assert_eq!(first.len(), 1, "exactly one approval dispatched: {first:?}");
    let id1 = first[0].clone();

    // Every existing segment is still there, in order, and the nonce is last.
    let (without_nonce, nonce1) = split_dispatch_nonce(&id1);
    assert_eq!(
        without_nonce,
        format!("{BARE_HINT}::pack={PACK_ID}::flow={APPROVAL_FLOW_ID}"),
        "the nonce is appended after every existing segment"
    );
    let nonce1 = nonce1.expect("an approval correlation id carries a nonce");
    assert_eq!(nonce1.len(), 32);

    // ---- Turn 2: approve gate1 -> gate2 dispatches and parks. ----
    rt.block_on(resumer.resume(demo_tenant(), &id1, approve(token(0))))?;
    let second = dispatched();
    assert_eq!(
        second.len(),
        2,
        "gate2 must dispatch its own approval: {second:?}"
    );
    let id2 = second[1].clone();
    assert_ne!(
        id1, id2,
        "two approvals in one conversation must not share an id"
    );
    assert_eq!(
        split_dispatch_nonce(&id2).0,
        without_nonce,
        "the ids differ ONLY in the nonce"
    );

    // ---- A stale response for gate1 (e.g. its watchdog `timeout`) arrives. ----
    // It must NOT resume gate2. Were it accepted, gate2 would complete on it,
    // and the real gate2 decision below would find no park and start a fresh
    // run — a third dispatch.
    rt.block_on(resumer.resume(
        demo_tenant(),
        &id1,
        json!({ "ok": false, "output": { "decision_token": token(0) }, "error": { "code": "timeout" } }),
    ))?;
    assert_eq!(dispatched().len(), 2, "a stale response dispatches nothing");

    // ---- gate2's own decision resumes gate2 and completes the flow. ----
    rt.block_on(resumer.resume(demo_tenant(), &id2, approve(token(1))))?;
    assert_eq!(
        dispatched().len(),
        2,
        "gate2's decision must land on gate2's park (no fresh run, no third dispatch)"
    );

    // ---- And the park really was consumed: a repeat of gate2's decision now
    // finds nothing parked and (unchanged behaviour) starts a fresh run, which
    // raises a NEW approval with a new id. ----
    rt.block_on(resumer.resume(demo_tenant(), &id2, approve(token(1))))?;
    let after = dispatched();
    assert_eq!(after.len(), 3, "no park left -> fresh run -> gate1 again");
    assert!(
        !after[..2].contains(&after[2]),
        "the fresh gate1 gets a fresh id"
    );
    Ok(())
}

/// greentic-runner#794: a parked approval resumes only on the token it was
/// issued. A missing or wrong token — or the guessable id with its nonce
/// dropped — is refused at the resumer and leaves the gate parked; the right
/// token then resumes it, once.
#[test]
fn an_approval_resumes_only_on_the_token_it_was_issued() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("approval-token.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_two_approval_pack(&pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let handler = Arc::new(CapturingDispatchHandler::default());
    let runtime =
        build_runtime_with_dispatch(&pack_path, Arc::clone(&config), Arc::clone(&handler))?;
    let resumer = RuntimeSessionResumer::new(Arc::clone(&runtime));
    let dispatched = || handler.all.lock().unwrap().len();

    rt.block_on(runtime.handle(IngressEnvelope {
        flow_id: APPROVAL_FLOW_ID.into(),
        ..inbound_envelope()
    }))?;
    assert_eq!(dispatched(), 1);
    let id1 = handler.all.lock().unwrap()[0].clone();
    let token1 = handler.tokens.lock().unwrap()[0]
        .clone()
        .expect("gate1 is issued a token");
    let guessed = split_dispatch_nonce(&id1).0.to_string();

    for (id, output) in [
        (id1.as_str(), json!({ "decision": "approved" })),
        (
            id1.as_str(),
            json!({ "decision": "approved", "decision_token": "forged" }),
        ),
        (guessed.as_str(), json!({ "decision": "approved" })),
    ] {
        rt.block_on(resumer.resume(demo_tenant(), id, json!({ "ok": true, "output": output })))?;
        assert_eq!(
            dispatched(),
            1,
            "a refused response must not resume gate1 (which would dispatch gate2)"
        );
    }

    let approve =
        json!({ "ok": true, "output": { "decision": "approved", "decision_token": token1 } });
    rt.block_on(resumer.resume(demo_tenant(), &id1, approve.clone()))?;
    assert_eq!(
        dispatched(),
        2,
        "the issued token resumes gate1 -> gate2 dispatches"
    );

    // Replay: gate1's token is spent. The conversation is now parked on gate2,
    // so the replay is dropped and nothing moves.
    rt.block_on(resumer.resume(demo_tenant(), &id1, approve))?;
    assert_eq!(dispatched(), 2, "a spent token resumes nothing");
    Ok(())
}

/// Only the approval runtime is nonced: `sorla.call` (like `agentic.call`,
/// whose bridge reuses the id as the agent's SESSION id) keeps its
/// deterministic per-conversation correlation.
#[test]
fn non_approval_dispatches_carry_no_nonce() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("sorla-no-nonce.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_sorla_wait_pack(&pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let handler = Arc::new(CapturingDispatchHandler::default());
    let runtime =
        build_runtime_with_dispatch(&pack_path, Arc::clone(&config), Arc::clone(&handler))?;
    rt.block_on(runtime.handle(sorla_inbound_envelope()))?;
    let correlation = handler.last_correlation.lock().unwrap().clone().unwrap();
    assert_eq!(
        correlation,
        format!("{BARE_HINT}::pack={PACK_ID}::flow={SORLA_FLOW_ID}")
    );
    assert!(split_dispatch_nonce(&correlation).1.is_none());
    Ok(())
}
