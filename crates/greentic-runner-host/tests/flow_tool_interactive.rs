//! Interactive `flow:` tools (the in-process `dw.agent` loop's path), against
//! a real pack: `PackRuntime::run_flow_for_tool_interactive` answers a parked
//! flow with its snapshot and presentation instead of an error, and
//! `PackRuntime::resume_flow_for_tool` finishes it from that snapshot — both
//! directly and through the `FlowInvoker` seam the agent runtime calls.
//!
//! The parking step is a `session.wait` behind an `emit.response`, which parks
//! exactly as a card awaiting its submit does (`FlowStatus::Waiting` with the
//! emitted message as the turn output) while running no WASM.
//!
//! Pack-building harness trimmed from `tests/run_outcome_emission.rs`.

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
use greentic_runner_host::pack::{ComponentResolution, PackRuntime, ToolFlowOutcome};
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
const PACK_ID: &str = "flow.tool.interactive";

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

/// - `form.flow`: `card` (emit.response) → `ask` (session.wait) → `done`
///   (emit.response) → end. Parks after showing the "card".
/// - `plain.flow`: `hello` (emit.response) → end. Never parks.
fn build_pack(pack_path: &Path) -> Result<()> {
    let flows = json!([
        {
            "id": "form.flow",
            "flow_type": "messaging",
            "start": "card",
            "nodes": {
                "card": {
                    "component": "emit.response",
                    "input": { "text": "pick a room" },
                    "routing": { "next": { "node_id": "ask" } }
                },
                "ask": {
                    "component": "session.wait",
                    "input": { "reason": "awaiting the room choice" },
                    "routing": { "next": { "node_id": "done" } }
                },
                "done": {
                    "component": "emit.response",
                    "input": { "text": "booked" },
                    "routing": "end"
                }
            }
        },
        {
            "id": "plain.flow",
            "flow_type": "messaging",
            "start": "hello",
            "nodes": {
                "hello": {
                    "component": "emit.response",
                    "input": { "text": "hello" },
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
        name: Some("Flow tool interactive".into()),
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
    let pack_path = temp.path().join("flow-tool.gtpack");
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

fn mentions(value: &Value, needle: &str) -> bool {
    value.to_string().contains(needle)
}

#[test]
fn a_parking_tool_flow_answers_waiting_and_resumes_to_completion() -> Result<()> {
    let rt = *RUNTIME;
    let h = harness()?;

    let first = rt
        .block_on(h.pack.run_flow_for_tool_interactive("form.flow", json!({})))
        .map_err(anyhow::Error::msg)?;
    let (snapshot, presentation) = match first {
        ToolFlowOutcome::Waiting {
            snapshot,
            presentation,
        } => (snapshot, presentation),
        other => panic!("a parking flow must answer Waiting, got {other:?}"),
    };
    assert!(
        mentions(&presentation, "pick a room"),
        "the presentation is what the flow showed before parking: {presentation}"
    );
    assert!(
        !mentions(&presentation, "booked"),
        "nothing after the park point has run: {presentation}"
    );
    assert_eq!(snapshot["flow_id"], "form.flow");
    assert_eq!(snapshot["next_node"], "done");

    let resumed = rt
        .block_on(
            h.pack
                .resume_flow_for_tool("form.flow", snapshot, json!({ "text": "101" })),
        )
        .map_err(anyhow::Error::msg)?;
    match resumed {
        ToolFlowOutcome::Completed(output) => assert!(
            mentions(&output, "booked"),
            "the resumed flow runs past its park point: {output}"
        ),
        other => panic!("the resumed flow must complete, got {other:?}"),
    }
    Ok(())
}

#[test]
fn a_non_parking_tool_flow_completes_on_the_interactive_path() -> Result<()> {
    let rt = *RUNTIME;
    let h = harness()?;
    let out = rt
        .block_on(
            h.pack
                .run_flow_for_tool_interactive("plain.flow", json!({})),
        )
        .map_err(anyhow::Error::msg)?;
    assert!(
        matches!(&out, ToolFlowOutcome::Completed(v) if mentions(v, "hello")),
        "got {out:?}"
    );
    Ok(())
}

#[test]
fn the_non_interactive_path_still_refuses_a_parking_flow() -> Result<()> {
    let rt = *RUNTIME;
    let h = harness()?;
    let err = rt
        .block_on(h.pack.run_flow_for_tool("form.flow", json!({})))
        .expect_err("graph tool nodes and deep workers cannot suspend");
    assert!(err.contains("non-interactive"), "got {err}");
    Ok(())
}

#[test]
fn a_snapshot_of_another_flow_is_refused() -> Result<()> {
    let rt = *RUNTIME;
    let h = harness()?;
    let snapshot = match rt
        .block_on(h.pack.run_flow_for_tool_interactive("form.flow", json!({})))
        .map_err(anyhow::Error::msg)?
    {
        ToolFlowOutcome::Waiting { snapshot, .. } => snapshot,
        other => panic!("expected Waiting, got {other:?}"),
    };
    let err = rt
        .block_on(
            h.pack
                .resume_flow_for_tool("plain.flow", snapshot, json!({})),
        )
        .expect_err("a snapshot resumes only the flow it was taken from");
    assert!(err.contains("form.flow"), "got {err}");
    Ok(())
}

#[cfg(feature = "agentic-worker")]
#[test]
fn the_flow_invoker_seam_suspends_and_resumes() -> Result<()> {
    use greentic_aw_runtime::{FlowInvokeOutcome, FlowInvoker};
    use greentic_runner_host::runner::flow_invoker::PackRuntimeFlowInvoker;

    let rt = *RUNTIME;
    let h = harness()?;
    let invoker = PackRuntimeFlowInvoker::new(vec![Arc::clone(&h.pack)], "demo".into());

    let first = rt
        .block_on(invoker.invoke_interactive("form.flow", "{}"))
        .map_err(anyhow::Error::msg)?;
    let FlowInvokeOutcome::Waiting {
        snapshot,
        presentation,
    } = first
    else {
        panic!("expected Waiting, got {first:?}");
    };
    assert!(mentions(&presentation, "pick a room"));

    let done = rt
        .block_on(invoker.resume("form.flow", snapshot, json!({ "text": "101" })))
        .map_err(anyhow::Error::msg)?;
    assert!(
        matches!(&done, FlowInvokeOutcome::Completed(v) if mentions(v, "booked")),
        "got {done:?}"
    );

    // The one-shot seam (graph tool nodes, deep workers) keeps refusing.
    let err = rt
        .block_on(invoker.invoke("form.flow", "{}"))
        .expect_err("one-shot invoke refuses a parking flow");
    assert!(err.contains("non-interactive"), "got {err}");
    Ok(())
}
