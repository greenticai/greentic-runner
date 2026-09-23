//! Characterization test for the production flow pause/resume path.
//!
//! This test pins the EXACT entry points a background response-listener must
//! call to resume a waiting flow session, so future async-dispatch code targets
//! the real methods rather than an invented harness.
//!
//! Production path (verified against `engine/runtime.rs`):
//!   inbound trigger -> `StateMachineRuntime::handle`
//!     -> `Runner::run_flow` -> `PackFlowAdapter::call`
//!       -> on first turn:  `FlowEngine::execute(ctx, input)` -> `FlowStatus::Waiting`
//!                          then `FlowResumeStore::save(envelope, &wait)` persists the snapshot
//!       -> on next turn:   `FlowResumeStore::fetch(envelope)` loads the snapshot
//!                          then `FlowEngine::resume(ctx, snapshot, input)` -> `FlowStatus::Completed`
//!
//! `engine/state_machine.rs::StateMachine::step` is a SEPARATE, legacy linear
//! `FlowStep`/`AdapterCall` model that does NOT execute `.gtpack` `HostFlow`
//! DAGs. Real pack flows reach the DAG interpreter only through
//! `FlowEngine::execute` / `FlowEngine::resume`, which is what this test
//! exercises directly.
//!
//! The harness (pack-building helpers) is copied from
//! `tests/runtime_flow_config.rs`; the only change is the inline flow, which
//! uses builtin `session.wait` + `emit.response` nodes (no WASM invocation),
//! plus a `FlowResumeStore` round-trip to characterize snapshot persistence.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::engine::runtime::{FlowResumeStore, IngressEnvelope};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::{
    FlowContext, FlowEngine, FlowSnapshot, FlowStatus, RetryConfig,
};
use greentic_runner_host::storage::new_session_store;
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
const PACK_ID: &str = "resume.characterization";
const FLOW_ID: &str = "wait.flow";

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

/// Returns a valid `qa.process` WASM artifact, extracting it from the shared
/// fixture gtpack when the loose file is absent. Copied verbatim from
/// `tests/runtime_flow_config.rs`. The component is declared in the manifest so
/// the pack loader compiles it, but this flow never invokes it.
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

/// Build a `.gtpack` whose only flow is two builtin nodes:
///   `wait`  = `session.wait` -> routes to `done`
///   `done`  = `emit.response` -> `end`
///
/// No WASM component is invoked: `session.wait` and `emit.response` are builtin
/// operators handled entirely by the flow engine. The manifest still declares a
/// component because the runtime-flow extension pipeline expects one, but it is
/// never executed by this flow.
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
    // A valid component artifact is required so the pack loader can compile the
    // declared component, but this flow never invokes it: the builtin nodes
    // (`session.wait`, `emit.response`) execute entirely in the flow engine.
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
    rt: &tokio::runtime::Runtime,
    pack_path: &Path,
    config: Arc<HostConfig>,
) -> Result<(Arc<PackRuntime>, FlowEngine)> {
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
    let engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;
    Ok((pack, engine))
}

fn flow_ctx<'a>(
    tenant: &'a str,
    pack_id: &'a str,
    flow_id: &'a str,
    session_id: &'a str,
    retry_config: RetryConfig,
) -> FlowContext<'a> {
    FlowContext {
        tenant,
        pack_id,
        flow_id,
        node_id: None,
        tool: None,
        action: Some("messaging"),
        session_id: Some(session_id),
        provider_id: Some("provider"),
        reply_scope: None,
        retry_config,
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    }
}

fn ingress_envelope() -> IngressEnvelope {
    IngressEnvelope {
        tenant: "demo".into(),
        env: Some("local".into()),
        pack_id: Some(PACK_ID.into()),
        flow_id: FLOW_ID.into(),
        flow_type: Some("messaging".into()),
        action: Some("messaging".into()),
        session_hint: Some("demo:provider:chan:conv:user".into()),
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
    .canonicalize()
}

/// Characterizes the full production pause/resume cycle:
///   1. `FlowEngine::execute` pauses at the `session.wait` node (status Waiting).
///   2. `FlowResumeStore::save` persists the snapshot keyed by the inbound
///      envelope's reply scope (the in-memory `greentic-session` store).
///   3. `FlowResumeStore::fetch` reloads that snapshot for the next inbound turn.
///   4. `FlowEngine::resume` advances PAST the wait node and completes the flow.
#[test]
fn flow_pauses_at_wait_then_resumes_to_completion() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("resume-characterization.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_wait_pack(&pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let (pack, engine) = build_engine(rt, &pack_path, Arc::clone(&config))?;
    let pack_id = pack.metadata().pack_id.to_string();
    let session_id = "demo:provider:chan:conv:user";
    let retry_config: RetryConfig = config.retry.clone().into();

    // ---- Turn 1: run to the wait. Expect status == Waiting. ----
    let ctx = flow_ctx("demo", &pack_id, FLOW_ID, session_id, retry_config);
    let first = rt
        .block_on(engine.execute(ctx, json!({ "text": "start" })))
        .context("first execute should reach the wait node")?;

    let wait = match first.status {
        FlowStatus::Waiting(wait) => *wait,
        FlowStatus::Completed => {
            anyhow::bail!("flow completed but should have paused at session.wait")
        }
    };
    // The snapshot's resume target is the node AFTER the wait (`done`).
    assert_eq!(
        wait.snapshot.next_node, "done",
        "wait snapshot should point at the post-wait node"
    );
    assert_eq!(wait.snapshot.flow_id, FLOW_ID);
    assert_eq!(wait.snapshot.pack_id, pack_id);

    // ---- Persist + reload the snapshot via the production resume store. ----
    let envelope = ingress_envelope();
    let resume_store = FlowResumeStore::new(new_session_store());
    let _reply_scope = rt
        .block_on(resume_store.save(&envelope, &wait))
        .context("resume store should persist the wait snapshot")?;
    let loaded: FlowSnapshot = rt
        .block_on(resume_store.fetch(&envelope))
        .context("resume store fetch failed")?
        .context("snapshot should be retrievable after save")?;
    assert_eq!(loaded.next_node, wait.snapshot.next_node);
    assert_eq!(loaded.flow_id, wait.snapshot.flow_id);

    // ---- Turn 2: resume with the loaded snapshot. Expect Completed. ----
    let resume_ctx = flow_ctx("demo", &pack_id, FLOW_ID, session_id, retry_config);
    let second = rt
        .block_on(engine.resume(resume_ctx, loaded, json!({ "text": "downstream-response" })))
        .context("resume should advance past the wait node")?;

    match second.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(_) => {
            anyhow::bail!("resume should complete the flow, not pause again")
        }
    }

    // After completion, the resume store entry is no longer needed; clearing it
    // mirrors what `PackFlowAdapter::call` does on `FlowStatus::Completed`.
    rt.block_on(resume_store.clear(&envelope))
        .context("clear after completion")?;
    assert!(
        rt.block_on(resume_store.fetch(&envelope))?.is_none(),
        "snapshot should be gone after clear"
    );

    Ok(())
}
