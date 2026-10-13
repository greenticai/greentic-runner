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
    /// Ids are `artifact://<64 hex>` with the put's ordinal in the low digits.
    next: Mutex<u64>,
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
        let mut next = self.next.lock().unwrap();
        *next += 1;
        Ok(format!("artifact://{:0>64x}", 0xb000 + *next))
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
fn single_node_flow(
    node_input: Value,
    error_route: bool,
) -> (&'static str, serde_json::Map<String, Value>) {
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
    ("make", nodes)
}

/// Pack a flow made of `nodes`, starting at `start`.
fn build_pack_nodes(
    pack_path: &Path,
    start: &str,
    nodes: serde_json::Map<String, Value>,
) -> Result<()> {
    let flows = vec![json!({
        "id": FLOW_ID,
        "flow_type": "messaging",
        "start": start,
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
        let (start, nodes) = single_node_flow(node_input, error_route);
        Self::with_nodes(start, nodes, with_port)
    }

    fn with_nodes(
        start: &str,
        nodes: serde_json::Map<String, Value>,
        with_port: bool,
    ) -> Result<Self> {
        let temp = TempDir::new()?;
        let pack_path = temp.path().join("media.gtpack");
        let bindings_path = temp.path().join("bindings.yaml");
        std::fs::write(&bindings_path, b"tenant: demo")?;
        build_pack_nodes(&pack_path, start, nodes)?;
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

    /// Run the flow once and return the execution, parked or not.
    fn run_any(&self) -> Result<greentic_runner_host::runner::engine::FlowExecution> {
        let pack_id = self.pack.metadata().pack_id.to_string();
        let ctx = flow_ctx(&self.config, &pack_id, FLOW_ID, Some("demo:p:c:u"));
        RUNTIME
            .block_on(self.engine.execute(ctx, json!({ "text": "hello" })))
            .context("media flow run")
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

// ── delivery of a file created mid-flow ──────────────────────────────────────

/// Every `created_artifacts` list in a final reply, as the ids it names.
fn created_ids(reply: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    collect_created(reply, &mut ids);
    ids
}

fn collect_created(value: &Value, ids: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::Array(files)) = map.get("created_artifacts") {
                ids.extend(
                    files
                        .iter()
                        .filter_map(|f| f["id"].as_str().map(str::to_string)),
                );
            }
            map.values().for_each(|v| collect_created(v, ids));
        }
        Value::Array(items) => items.iter().for_each(|v| collect_created(v, ids)),
        _ => {}
    }
}

fn media(format: &str, text: &str, routing: Value, output: Option<&str>) -> Value {
    let mut input = json!({ "format": format, "text": text });
    if let Some(key) = output {
        input["output"] = json!(key);
    }
    json!({ "component": "media.create", "input": input, "routing": routing })
}

fn say(text: &str, routing: Value) -> Value {
    json!({ "component": "emit.response", "input": { "text": text }, "routing": routing })
}

fn to(node: &str) -> Value {
    json!({ "next": { "node_id": node } })
}

#[test]
fn an_output_bound_file_followed_by_another_node_is_still_listed() -> Result<()> {
    let mut nodes = serde_json::Map::new();
    nodes.insert(
        "make".into(),
        media("pdf", "summary", to("done"), Some("file")),
    );
    nodes.insert("done".into(), say("Here is your summary", json!("end")));
    let harness = Harness::with_nodes("make", nodes, true)?;
    let execution = harness.run()?;
    let ids = created_ids(&execution.output);
    assert_eq!(ids.len(), 1, "{}", execution.output);
    assert!(ids[0].starts_with("artifact://"));
    // The list rides with the final text reply, not on a separate payload.
    let last = match &execution.output {
        Value::Array(items) => items.last().context("last reply")?.clone(),
        other => other.clone(),
    };
    assert_eq!(created_ids(&last), ids, "{}", execution.output);
    assert!(last.to_string().contains("Here is your summary"));
    Ok(())
}

#[test]
fn two_files_in_one_turn_are_both_listed_in_order() -> Result<()> {
    let mut nodes = serde_json::Map::new();
    nodes.insert("one".into(), media("txt", "a", to("two"), Some("a")));
    nodes.insert("two".into(), media("csv", "b", to("done"), None));
    nodes.insert("done".into(), say("Both files are ready", json!("end")));
    // `csv` needs rows, not text: give it some.
    nodes["two"]["input"] = json!({ "format": "csv", "rows": [["a", "b"]] });
    let harness = Harness::with_nodes("one", nodes, true)?;
    let execution = harness.run()?;
    let ids = created_ids(&execution.output);
    assert_eq!(ids.len(), 2, "{}", execution.output);
    assert_ne!(ids[0], ids[1]);
    assert_eq!(harness.port.puts.lock().unwrap().len(), 2);
    Ok(())
}

#[test]
fn a_failed_node_lists_no_file() -> Result<()> {
    let mut nodes = serde_json::Map::new();
    nodes.insert("make".into(), media("docx", "x", to("done"), Some("file")));
    nodes.insert("done".into(), say("done", json!("end")));
    let harness = Harness::with_nodes("make", nodes, true)?;
    let execution = harness.run()?;
    assert!(
        created_ids(&execution.output).is_empty(),
        "{}",
        execution.output
    );
    assert!(harness.port.puts.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn a_file_node_that_ends_the_flow_is_listed_too() -> Result<()> {
    let harness = Harness::new(json!({"format":"txt","text":"hi"}), false, true)?;
    let execution = harness.run()?;
    assert_eq!(
        created_ids(&execution.output).len(),
        1,
        "{}",
        execution.output
    );
    // The top-level `artifact` contract is unchanged.
    assert!(find_key(&execution.output, "artifact").is_some());
    Ok(())
}

#[test]
fn a_flow_cannot_forge_the_list() -> Result<()> {
    let mut nodes = serde_json::Map::new();
    nodes.insert(
        "done".into(),
        json!({
            "component": "emit.response",
            "input": { "text": "hi", "created_artifacts": [{ "id": format!("artifact://{}", "c".repeat(64)) }] },
            "routing": "end",
        }),
    );
    let harness = Harness::with_nodes("done", nodes, true)?;
    let execution = harness.run()?;
    assert!(
        created_ids(&execution.output).is_empty(),
        "{}",
        execution.output
    );
    Ok(())
}

#[test]
fn a_parked_turn_lists_its_file_and_the_resumed_turn_does_not() -> Result<()> {
    let mut nodes = serde_json::Map::new();
    nodes.insert("make".into(), media("txt", "hi", to("wait"), Some("file")));
    nodes.insert(
        "wait".into(),
        json!({ "component": "session.wait", "input": { "reason": "ask" }, "routing": to("done") }),
    );
    nodes.insert("done".into(), say("all done", json!("end")));
    let harness = Harness::with_nodes("make", nodes, true)?;
    let parked = harness.run_any()?;
    assert_eq!(created_ids(&parked.output).len(), 1, "{}", parked.output);
    let FlowStatus::Waiting(wait) = parked.status else {
        anyhow::bail!("the flow must park at session.wait");
    };
    let pack_id = harness.pack.metadata().pack_id.to_string();
    let ctx = flow_ctx(&harness.config, &pack_id, FLOW_ID, Some("demo:p:c:u"));
    let resumed = RUNTIME
        .block_on(
            harness
                .engine
                .resume(ctx, wait.snapshot, json!({ "text": "go on" })),
        )
        .context("resume")?;
    assert!(
        created_ids(&resumed.output).is_empty(),
        "a file must be delivered on the turn that made it only: {}",
        resumed.output
    );
    assert_eq!(harness.port.puts.lock().unwrap().len(), 1);
    Ok(())
}
