//! Entering a flow at an explicit node instead of its entrypoint.
//!
//! Card-driven messaging packs express "what happens next" as a node id carried
//! on the inbound activity (the designer emits it as `nextCardId`). A host that
//! can only call `FlowEngine::execute` restarts such a flow at its entrypoint on
//! every turn, so the capture nodes between two cards never run.
//!
//! `FlowEngine::resume` already enters mid-graph, but only from a persisted
//! `FlowSnapshot` — and a snapshot exists only if the flow previously parked at
//! `session.wait` / a `dw.agent` park-loop. A pack whose pause points are cards
//! never parks, so it never has a snapshot to resume from.
//!
//! `FlowEngine::execute_from` closes that gap: a fresh execution whose cursor
//! starts at a caller-chosen node.
//!
//! The harness (pack-building helpers) is copied from
//! `tests/resume_characterization.rs`; the only change is the inline flow, which
//! is two builtin `emit.response` nodes chained entrypoint -> second so a run
//! can be told apart by which node's text reaches the output.

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
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine, FlowStatus, RetryConfig};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, ExtensionInline, ExtensionRef,
    PackFlowEntry, PackKind, PackManifest, ResourceHints, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::json;
use tempfile::TempDir;
use zip::ZipArchive;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "entry.node";
const FLOW_ID: &str = "card.flow";
const FIRST_TEXT: &str = "first-node-ran";
const SECOND_TEXT: &str = "second-node-ran";

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
/// fixture gtpack when the loose file is absent. The component is declared in
/// the manifest so the pack loader compiles it, but this flow never invokes it.
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

/// Build a `.gtpack` whose only flow is two builtin `emit.response` nodes:
///   `first`  -> routes to `second`   (the flow's declared entrypoint)
///   `second` -> `end`
///
/// Each node emits a distinct text, so the output alone says which node the
/// run started at.
fn build_two_node_pack(pack_path: &Path) -> Result<()> {
    let runtime_flow = json!({
        "id": FLOW_ID,
        "flow_type": "messaging",
        "start": "first",
        "nodes": {
            "first": {
                "component": "emit.response",
                "input": { "text": FIRST_TEXT },
                "routing": { "next": { "node_id": "second" } }
            },
            "second": {
                "component": "emit.response",
                "input": { "text": SECOND_TEXT },
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

struct Fixture {
    _temp: TempDir,
    engine: FlowEngine,
    pack_id: String,
    retry_config: RetryConfig,
}

fn fixture(rt: &tokio::runtime::Runtime) -> Result<Fixture> {
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("entry-node.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_two_node_pack(&pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let (pack, engine) = build_engine(rt, &pack_path, Arc::clone(&config))?;
    let pack_id = pack.metadata().pack_id.to_string();
    let retry_config: RetryConfig = config.retry.clone().into();
    Ok(Fixture {
        _temp: temp,
        engine,
        pack_id,
        retry_config,
    })
}

/// Baseline that makes the `execute_from` test below discriminating: plain
/// `execute` starts at the flow's declared entrypoint, so BOTH nodes run.
/// If this ever stopped emitting `FIRST_TEXT`, the entry-node assertion would
/// pass for the wrong reason.
#[test]
fn execute_starts_at_the_declared_entrypoint() -> Result<()> {
    let rt = *RUNTIME;
    let f = fixture(rt)?;

    let ctx = flow_ctx("demo", &f.pack_id, FLOW_ID, "sess-baseline", f.retry_config);
    let run = rt
        .block_on(f.engine.execute(ctx, json!({ "text": "start" })))
        .context("execute from the entrypoint")?;

    assert!(
        matches!(run.status, FlowStatus::Completed),
        "flow has no wait node, so it must complete: {:?}",
        run.status
    );
    let output = serde_json::to_string(&run.output)?;
    assert!(
        output.contains(FIRST_TEXT),
        "entrypoint run must include the entry node's output, got {output}"
    );
    assert!(
        output.contains(SECOND_TEXT),
        "entrypoint run must walk on to the second node, got {output}"
    );
    Ok(())
}

/// The behaviour this change exists for: start the run at a node the caller
/// names, skipping everything upstream of it.
#[test]
fn execute_from_starts_at_the_named_node_and_skips_upstream() -> Result<()> {
    let rt = *RUNTIME;
    let f = fixture(rt)?;

    let ctx = flow_ctx("demo", &f.pack_id, FLOW_ID, "sess-entry", f.retry_config);
    let run = rt
        .block_on(
            f.engine
                .execute_from(ctx, json!({ "text": "start" }), "second"),
        )
        .context("execute_from should enter the flow at `second`")?;

    assert!(
        matches!(run.status, FlowStatus::Completed),
        "flow has no wait node, so it must complete: {:?}",
        run.status
    );
    let output = serde_json::to_string(&run.output)?;
    assert!(
        output.contains(SECOND_TEXT),
        "run must include the named entry node's output, got {output}"
    );
    assert!(
        !output.contains(FIRST_TEXT),
        "the entrypoint node is upstream of `second` and must NOT run, got {output}"
    );
    Ok(())
}

/// A node id that isn't in the flow must fail loudly rather than silently
/// falling back to the entrypoint — a silent fallback is exactly the bug this
/// API exists to remove.
#[test]
fn execute_from_rejects_an_unknown_node() -> Result<()> {
    let rt = *RUNTIME;
    let f = fixture(rt)?;

    let ctx = flow_ctx("demo", &f.pack_id, FLOW_ID, "sess-unknown", f.retry_config);
    let err = rt
        .block_on(
            f.engine
                .execute_from(ctx, json!({ "text": "start" }), "no_such_node"),
        )
        .expect_err("an unknown entry node must be an error, not a silent restart");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("no_such_node"),
        "error should name the missing node, got {msg}"
    );
    Ok(())
}
