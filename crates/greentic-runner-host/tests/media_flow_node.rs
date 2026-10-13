//! End-to-end characterization of the `component == "media.create"` flow node
//! (LOCKED ENCODING v1: `format`/`name`/`text`/`rows`/`output` carried in the
//! node payload/config).
//!
//! A single-node `.gtpack` is built and run through the real [`FlowEngine`]
//! with a stub artifact port standing in for the admin `artifacts` door. The
//! harness mirrors `a2a_flow_node.rs`.

#![cfg(feature = "agentic-worker")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use greentic_ext_runtime::host_ports::{
    ArtifactPort, ArtifactPortError, ArtifactPutRequest, HostCallContext,
};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ExtensionInline, ExtensionRef, PackFlowEntry, PackKind, PackManifest, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "media.flow.node.test";
const FLOW_ID: &str = "media.flow";

static RUNTIME: Lazy<&'static tokio::runtime::Runtime> = Lazy::new(|| {
    Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test runtime"),
    ))
});

type Put = (String, String, Vec<u8>, Option<String>);

/// What the stub port saw and what it answers next.
#[derive(Default)]
struct StubPort {
    puts: Mutex<Vec<Put>>,
    refuse: Mutex<Option<ArtifactPortError>>,
}

impl ArtifactPort for StubPort {
    fn put(
        &self,
        _extension_id: &str,
        ctx: &HostCallContext,
        request: ArtifactPutRequest,
    ) -> Result<String, ArtifactPortError> {
        if let Some(error) = self.refuse.lock().unwrap().take() {
            return Err(error);
        }
        self.puts.lock().unwrap().push((
            request.name,
            request.mime_type,
            request.bytes,
            ctx.tenant.clone(),
        ));
        Ok(format!("artifact://{}", "b".repeat(64)))
    }
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

/// Build a `.gtpack` whose only flow is a `media.create` node `make`, routed
/// to `ok` / `err` end nodes when `error_route` is set.
fn build_pack(pack_path: &Path, node_input: Value, error_route: bool) -> Result<()> {
    let routing = if error_route {
        json!({ "custom": [
            { "condition": "event == \"on_success\"", "to": "ok" },
            { "condition": "event == \"on_error\"", "to": "err" }
        ] })
    } else {
        json!("end")
    };
    let mut nodes = serde_json::Map::new();
    nodes.insert(
        "make".to_string(),
        json!({ "component": "media.create", "input": node_input, "routing": routing }),
    );
    if error_route {
        for branch in ["ok", "err"] {
            nodes.insert(
                branch.to_string(),
                json!({
                    "component": "media.create",
                    "input": { "format": "txt", "text": branch },
                    "routing": "end",
                }),
            );
        }
    }
    let flows = vec![json!({
        "id": FLOW_ID,
        "flow_type": "messaging",
        "start": "make",
        "nodes": Value::Object(nodes),
    })];

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
        components: Vec::new(),
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
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

struct Harness {
    _temp: TempDir,
    config: Arc<HostConfig>,
    pack: Arc<PackRuntime>,
    engine: FlowEngine,
    port: Arc<StubPort>,
}

impl Harness {
    fn new(node_input: Value, error_route: bool, with_port: bool) -> Result<Self> {
        let temp = TempDir::new()?;
        let pack_path = temp.path().join("media.gtpack");
        let bindings_path = temp.path().join("bindings.yaml");
        std::fs::write(&bindings_path, b"tenant: demo")?;
        build_pack(&pack_path, node_input, error_route)?;
        let config = Arc::new(host_config(&bindings_path));
        let rt = *RUNTIME;
        let pack = Arc::new(rt.block_on(PackRuntime::load(
            &pack_path,
            Arc::clone(&config),
            None,
            None,
            None,
            Some(greentic_runner_host::storage::state::new_state_store()),
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
        let port = Arc::new(StubPort::default());
        if with_port {
            engine.set_ext_artifact_port(Some(Arc::clone(&port) as Arc<dyn ArtifactPort>));
        }
        Ok(Self {
            _temp: temp,
            config,
            pack,
            engine,
            port,
        })
    }

    /// Run the flow once and return the whole execution.
    fn run(&self) -> Result<greentic_runner_host::runner::engine::FlowExecution> {
        let pack_id = self.pack.metadata().pack_id.to_string();
        let ctx = flow_ctx(&self.config, &pack_id, FLOW_ID, Some("demo:p:c:u"));
        let execution = RUNTIME
            .block_on(self.engine.execute(ctx, json!({ "text": "hello" })))
            .context("media flow run")?;
        match execution.status {
            FlowStatus::Completed => Ok(execution),
            FlowStatus::Waiting(wait) => {
                anyhow::bail!("a media node must never park; got {:?}", wait.reason)
            }
        }
    }
}

/// Recursively find a value at `key` anywhere in the (envelope-wrapped) tree.
fn find_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => map
            .get(key)
            .or_else(|| map.values().find_map(|v| find_key(v, key))),
        Value::Array(items) => items.iter().find_map(|v| find_key(v, key)),
        _ => None,
    }
}

// ── tests ────────────────────────────────────────────────────────────────────

#[test]
fn each_format_stores_a_file_and_the_result_is_the_node_output() -> Result<()> {
    let cases = [
        (
            json!({"format":"txt","name":"Note","text":"hi {{ entry.text }}"}),
            "Note.txt",
            "text/plain",
        ),
        (
            json!({"format":"md","name":"Note","text":"# hi"}),
            "Note.md",
            "text/markdown",
        ),
        (
            json!({"format":"csv","name":"Note","rows":[["a","b"],[1,2]]}),
            "Note.csv",
            "text/csv",
        ),
        (
            json!({"format":"pdf","name":"Note","text":"hi"}),
            "Note.pdf",
            "application/pdf",
        ),
    ];
    for (input, name, mime) in cases {
        let harness = Harness::new(input, false, true)?;
        let execution = harness.run()?;
        // No `output:` key: the top-level `artifact` is the delivery contract.
        let out = execution.node_outputs.get("make").context("make output")?;
        assert_eq!(find_key(out, "ok"), Some(&json!(true)), "{out}");
        let artifact = find_key(out, "artifact").context("artifact")?;
        assert_eq!(artifact["name"], name);
        assert_eq!(artifact["mime_type"], mime);
        assert!(artifact["id"].as_str().unwrap().starts_with("artifact://"));
        let puts = harness.port.puts.lock().unwrap();
        assert_eq!(puts.len(), 1);
        assert_eq!(puts[0].0, name);
        assert_eq!(puts[0].3.as_deref(), Some("demo"));
        if mime == "application/pdf" {
            assert!(puts[0].2.starts_with(b"%PDF-"));
        }
        if name == "Note.txt" {
            assert_eq!(
                puts[0].2, b"hi hello",
                "the template is rendered before the node runs"
            );
        }
    }
    Ok(())
}

#[test]
fn an_output_key_binds_the_result_under_it() -> Result<()> {
    let harness = Harness::new(
        json!({"format":"txt","text":"x","output":"file"}),
        false,
        true,
    )?;
    let execution = harness.run()?;
    let out = execution.node_outputs.get("make").context("make output")?;
    let file = find_key(out, "file").context("bound output key")?;
    assert_eq!(file["ok"], true);
    assert!(file["artifact"]["id"].is_string());
    Ok(())
}

#[test]
fn blank_text_is_an_invalid_input_value_and_nothing_is_stored() -> Result<()> {
    let harness = Harness::new(
        json!({"format":"pdf","text":"{{ vars.missing }}","output":"file"}),
        false,
        true,
    )?;
    let execution = harness.run()?;
    let out = execution.node_outputs.get("make").context("make output")?;
    let file = find_key(out, "file").context("bound output key")?;
    assert_eq!(file["ok"], false);
    assert_eq!(file["error"]["code"], "invalid_input");
    assert!(harness.port.puts.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn without_a_port_the_node_answers_artifact_unavailable_as_a_value() -> Result<()> {
    let harness = Harness::new(json!({"format":"txt","text":"x"}), false, false)?;
    let execution = harness.run()?;
    let out = execution.node_outputs.get("make").context("make output")?;
    assert_eq!(find_key(out, "ok"), Some(&json!(false)));
    assert_eq!(
        find_key(out, "error").unwrap()["code"],
        "artifact_unavailable"
    );
    Ok(())
}

#[test]
fn door_refusals_map_onto_codes() -> Result<()> {
    let cases = [
        (
            ArtifactPortError::Unavailable("door answered 401".into()),
            "unauthorized",
        ),
        (
            ArtifactPortError::Unavailable("door answered 403".into()),
            "unauthorized",
        ),
        (
            ArtifactPortError::Unavailable("door answered 413".into()),
            "too_large",
        ),
        (ArtifactPortError::InvalidMediaType, "artifact_rejected"),
        (ArtifactPortError::QuotaExceeded, "artifact_rejected"),
    ];
    for (refusal, code) in cases {
        let harness = Harness::new(json!({"format":"txt","text":"x"}), false, true)?;
        *harness.port.refuse.lock().unwrap() = Some(refusal);
        let execution = harness.run()?;
        let out = execution.node_outputs.get("make").context("make output")?;
        assert_eq!(find_key(out, "error").unwrap()["code"], code, "{out}");
    }
    Ok(())
}

#[test]
fn an_error_route_is_taken_and_carries_the_message() -> Result<()> {
    let harness = Harness::new(json!({"format":"txt","text":"  "}), true, true)?;
    let execution = harness.run()?;
    assert!(
        execution.node_outputs.contains_key("err"),
        "{:?}",
        execution.node_outputs
    );
    assert!(!execution.node_outputs.contains_key("ok"));
    let out = execution.node_outputs.get("make").context("make output")?;
    let errors = out["errors"].as_array().context("errors envelope")?;
    assert_eq!(errors[0]["code"], "invalid_input");
    assert!(errors[0]["message"].as_str().unwrap().contains("text"));
    Ok(())
}

#[test]
fn a_success_with_an_error_route_takes_the_success_branch() -> Result<()> {
    let harness = Harness::new(json!({"format":"txt","text":"fine"}), true, true)?;
    let execution = harness.run()?;
    assert!(execution.node_outputs.contains_key("ok"));
    assert!(!execution.node_outputs.contains_key("err"));
    Ok(())
}

#[test]
fn without_an_error_route_a_failure_still_completes_the_flow() -> Result<()> {
    let harness = Harness::new(json!({"format":"docx","text":"x"}), false, true)?;
    let execution = harness.run()?;
    let out = execution.node_outputs.get("make").context("make output")?;
    assert_eq!(
        find_key(out, "error").unwrap()["code"],
        "unsupported_format"
    );
    Ok(())
}

fn flow_ctx<'a>(
    config: &'a HostConfig,
    pack_id: &'a str,
    flow_id: &'a str,
    session_id: Option<&'a str>,
) -> FlowContext<'a> {
    FlowContext {
        tenant: config.tenant.as_str(),
        pack_id,
        flow_id,
        node_id: None,
        tool: None,
        action: Some("messaging"),
        session_id,
        provider_id: Some("provider"),
        reply_scope: None,
        retry_config: config.retry.clone().into(),
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    }
}
