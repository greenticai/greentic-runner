//! End-to-end characterization of the `component == "mcp"` flow node
//! (LOCKED ENCODING v2: `server`/`tool`/`arguments`/`output` carried in the
//! node payload/config).
//!
//! This is the flow-execution MCP path (role `flow_editor`), separate from the
//! agent-loop MCP path (role `agentic_worker`). A single-node `.gtpack` flow is
//! built and run through the real [`FlowEngine`], with the per-tenant MCP
//! catalog backed by two wiremock servers: a fake admin
//! (`/api/v1/designer/tenant/me/mcp-servers`) returning one `flow_editor` MCP
//! server, and a fake MCP server speaking the JSON-RPC contract (initialize,
//! initialized, tools/list, tools/call).
//!
//! `FlowEngine::new` constructs the MCP source from the admin endpoint and
//! token env vars, so the success test points those at the admin wiremock. The
//! wiremock admin/MCP mount shapes mirror the in-module tests in the
//! `greentic-aw-runtime` mcp_source module (those helpers are test-only and
//! cannot be reused from here).
//!
//! Two paths are covered. The happy path calls the tool with templated
//! arguments and binds its structured result under the configured `output`
//! state key. The degraded path leaves the admin unconfigured (no env), so the
//! node FAILS GRACEFULLY with a structured error value while the flow still
//! COMPLETES (no panic, no aborted runtime).

#![cfg(feature = "agentic-worker")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use greentic_runner_host::runner::flow_adapter::{NodeIR, flow_doc_to_ir};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ExtensionInline, ExtensionRef, PackFlowEntry, PackKind, PackManifest, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "mcp.flow.node.test";
const FLOW_ID: &str = "mcp.flow";
const SESSION_HINT: &str = "demo:provider:chan:conv:user";

static RUNTIME: Lazy<&'static tokio::runtime::Runtime> = Lazy::new(|| {
    Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime"),
    ))
});

/// Serializes the env-dependent tests in this file: both read/write the shared
/// process-global `GREENTIC_AW_*` vars that `FlowEngine::new` consumes.
static ENV_GUARD: Mutex<()> = Mutex::new(());

// ── wiremock fakes (shapes mirror mcp_source.rs in-module tests) ──────────────

fn initialize_ok() -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("Mcp-Session-Id", "sess-1")
        .set_body_json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {
                "protocolVersion": "2025-06-18",
                "serverInfo": { "name": "fake", "version": "1.0.0" }
            }
        }))
}

/// Mount the 4-call MCP JSON-RPC contract on a fresh wiremock server, returning
/// `call_result_json` for `tools/call`.
async fn fake_mcp_server(call_result_json: Value) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(initialize_ok())
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({ "method": "notifications/initialized" }),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 2,
            "result": { "tools": [
                { "name": "get_issue", "description": "Get an issue",
                  "inputSchema": { "type": "object",
                    "properties": { "id": { "type": "string" } } } }
            ] }
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "tools/call" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 3,
            "result": call_result_json
        })))
        .mount(&server)
        .await;
    server
}

/// Mount the admin `mcp-servers` endpoint returning one `flow_editor` server
/// pointing at `mcp_url`.
async fn fake_admin(mcp_url: &str) -> MockServer {
    let admin = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .and(header("authorization", "Bearer gtc_live_test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "servers": [
                {
                    "id": "github", "name": "GitHub", "transport_url": mcp_url,
                    "auth_header_name": null, "auth_token": null,
                    "allowed_tools": null, "roles": ["flow_editor"]
                }
            ]
        })))
        .mount(&admin)
        .await;
    admin
}

// ── pack harness (mirrors runtime_call_nodes.rs build_dispatch_pack) ──────────

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

/// Build a `.gtpack` whose only flow is a single `component == "mcp"` node
/// (LOCKED ENCODING v2). `node_input` carries `{ server, tool, arguments,
/// output? }` directly in the node payload/config — `server`/`tool` are the
/// source of truth, NOT an `operation` string. No WASM component is needed:
/// the node is a native runtime dispatch.
///
/// `component = "mcp"` is a valid `greentic_types::ComponentId`, so the
/// runtime-flow pack round-trips it through `ComponentId::from_str` without the
/// earlier `:`/`/` rejection; the flow adapter sees exactly `"mcp"`.
fn build_mcp_pack(pack_path: &Path, node_input: Value) -> Result<()> {
    let mut nodes = serde_json::Map::new();
    nodes.insert(
        "call".to_string(),
        json!({
            "component": "mcp",
            "input": node_input,
            "routing": "end",
        }),
    );

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
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&manifest_bytes)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

fn build_engine(
    pack_path: &Path,
    config: Arc<HostConfig>,
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
    let engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;
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

/// Recursively find a value at `key` anywhere in the (possibly envelope-wrapped)
/// output tree.
fn find_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            if let Some(found) = map.get(key) {
                return Some(found);
            }
            map.values().find_map(|v| find_key(v, key))
        }
        Value::Array(items) => items.iter().find_map(|v| find_key(v, key)),
        _ => None,
    }
}

// ── YGTC op-key load path (flow_doc_to_ir) ───────────────────────────────────
//
// The designer's packc injection writes the MCP node as the YGTC op-key shape
//
//   nodes:
//     lookup:
//       mcp:
//         server: <id>
//         tool: <name>
//         arguments: { ... }   # values may be {{ }} templates
//         output: <state_key>
//       routing: [ ... ]
//
// (see greentic-designer `inject_mcp_nodes`). The runner deserialises that into
// `greentic_flow::model::FlowDoc` and lowers it with the REAL flow adapter
// `flow_doc_to_ir`. This is the load primitive the runner uses; the assertions
// below start from the op-key text (NOT a hand-built runtime node with
// `component` pre-set) and prove the op-key passes through verbatim as
// `component == "mcp"` with the payload intact.

/// Build the YGTC op-key node text for `lookup` and lower it through the real
/// runner adapter `flow_doc_to_ir`, returning the derived `(component, payload,
/// routes)` for that node.
///
/// `flow_doc_to_ir` performs STRUCTURAL op-key extraction: it reads the single
/// flattened `raw` key off `NodeDoc` (here `mcp`) and carries it through as the
/// component, independent of `greentic-flow`'s own `classify_node_type` /
/// `NodeKind` lowering. This is the evidence for the release-ordering answer:
/// the runner does not depend on greentic-flow's `NodeKind::Mcp` to recognise
/// the node at runtime.
fn lower_ygtc_mcp_node(node_payload: Value, routing: Value) -> Result<NodeIR> {
    let doc_json = json!({
        "id": FLOW_ID,
        "type": "messaging",
        "start": "lookup",
        "nodes": {
            "lookup": {
                "mcp": node_payload,
                "routing": routing,
            }
        }
    });
    let doc: greentic_flow::model::FlowDoc =
        serde_json::from_value(doc_json).context("deserialise YGTC FlowDoc")?;
    let ir = flow_doc_to_ir(doc).context("flow_doc_to_ir lowering")?;
    let node = ir
        .nodes
        .get("lookup")
        .cloned()
        .context("`lookup` node missing from lowered FlowIR")?;
    Ok(node)
}

/// Build a single-flow `.gtpack` whose `lookup` node is derived END-TO-END from
/// the YGTC op-key shape via [`lower_ygtc_mcp_node`]. The runtime-flow node's
/// `component`/`input`/`routing` are taken from the adapter output (NOT
/// hand-set), so a regression that drops the `mcp` op-key (or wraps it in
/// `component.exec`) would surface here as a non-`mcp` component.
fn build_ygtc_mcp_pack(pack_path: &Path, node_ir: &NodeIR) -> Result<()> {
    // Translate the adapter's `routes` back into the runtime-flow `routing`
    // shape the loader understands: a single terminal route -> `"end"`.
    let routing = if node_ir.routes.iter().all(|r| r.out || r.to.is_none()) {
        json!("end")
    } else {
        serde_json::to_value(&node_ir.routes)?
    };

    let mut nodes = serde_json::Map::new();
    nodes.insert(
        "lookup".to_string(),
        json!({
            "component": node_ir.component,
            "input": node_ir.payload_expr,
            "routing": routing,
        }),
    );

    let runtime_flow = json!({
        "id": FLOW_ID,
        "flow_type": "messaging",
        "start": "lookup",
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
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&manifest_bytes)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Release-ordering proof: the runner's CURRENT pinned `greentic-flow`
/// (1.1.x-dev, WITHOUT `NodeKind::Mcp`) already lowers the YGTC op-key
/// `{ mcp: { ... }, routing }` into a FlowIR node with `component == "mcp"` and
/// the payload intact — purely via `flow_doc_to_ir`'s structural op-key
/// extraction. No `greentic-flow` PR #235 is required at runtime.
#[test]
fn flow_doc_to_ir_preserves_mcp_op_key_verbatim() -> Result<()> {
    let node = lower_ygtc_mcp_node(
        json!({
            "server": "github",
            "tool": "get_issue",
            "arguments": { "id": "{{ entry.issue_id }}" },
            "output": "issue"
        }),
        json!([{ "out": true }]),
    )?;

    // The op-key survives verbatim as the component...
    assert_eq!(
        node.component, "mcp",
        "expected the `mcp` op-key to pass through `flow_doc_to_ir` verbatim, got `{}`",
        node.component
    );
    // ...and `mcp` is a valid ComponentId, so `flow_ir_to_flow` /
    // `ComponentId::from_str` accepts it at pack load.
    assert!(
        greentic_types::ComponentId::from_str(&node.component).is_ok(),
        "derived component `{}` must be a valid ComponentId",
        node.component
    );
    // ...and the payload (server/tool/arguments/output) is carried intact.
    assert_eq!(node.payload_expr["server"], json!("github"));
    assert_eq!(node.payload_expr["tool"], json!("get_issue"));
    assert_eq!(
        node.payload_expr["arguments"]["id"],
        json!("{{ entry.issue_id }}")
    );
    assert_eq!(node.payload_expr["output"], json!("issue"));
    Ok(())
}

/// End-to-end from the YGTC op-key shape through `flow_doc_to_ir` -> runtime
/// flow -> `FlowEngine`: the lowered MCP node calls the tool with templated
/// arguments rendered from flow state and binds the result under `output`.
#[test]
fn mcp_node_from_ygtc_op_key_calls_tool_and_binds_output() -> Result<()> {
    let _guard = ENV_GUARD.lock().unwrap();
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("mcp-ygtc-ok.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let mcp = rt.block_on(fake_mcp_server(
        json!({ "structuredContent": { "title": "Bug" } }),
    ));
    let admin = rt.block_on(fake_admin(&mcp.uri()));

    // Start from the YGTC op-key text and lower it with the REAL adapter.
    let node = lower_ygtc_mcp_node(
        json!({
            "server": "github",
            "tool": "get_issue",
            "arguments": { "id": "{{ entry.issue_id }}" },
            "output": "issue"
        }),
        json!([{ "out": true }]),
    )?;
    assert_eq!(
        node.component, "mcp",
        "YGTC op-key must lower to component `mcp` for the engine to dispatch it"
    );
    build_ygtc_mcp_pack(&pack_path, &node)?;

    let config = Arc::new(host_config(&bindings_path));

    // SAFETY: serialized by ENV_GUARD; cleared after the run below.
    unsafe {
        std::env::set_var("GREENTIC_AW_ADMIN_ENDPOINT", admin.uri());
        std::env::set_var("GREENTIC_AW_ADMIN_TOKEN", "gtc_live_test");
        std::env::remove_var("GREENTIC_AW_MCP");
    }

    let (pack, engine) = build_engine(&pack_path, Arc::clone(&config))?;

    unsafe {
        std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
        std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
    }

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, json!({ "issue_id": "42" })))
        .context("mcp ygtc flow run")?;

    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason),
    }

    let issue = find_key(&execution.output, "issue")
        .with_context(|| format!("output key `issue` missing, got {:?}", execution.output))?;
    assert_eq!(
        issue,
        &json!({ "title": "Bug" }),
        "bound output mismatch, got {:?}",
        execution.output
    );
    assert!(
        find_key(&execution.output, "error").is_none(),
        "unexpected error in output: {:?}",
        execution.output
    );
    Ok(())
}

/// Graceful-degrade twin of the YGTC-op-key path: with the admin unconfigured
/// the lowered MCP node fails with a structured error while the flow still
/// COMPLETES (no panic, no aborted runtime).
#[test]
fn mcp_node_from_ygtc_op_key_degrades_gracefully_when_unconfigured() -> Result<()> {
    let _guard = ENV_GUARD.lock().unwrap();
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("mcp-ygtc-degraded.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let node = lower_ygtc_mcp_node(
        json!({
            "server": "github",
            "tool": "get_issue",
            "arguments": { "id": "1" },
            "output": "issue"
        }),
        json!([{ "out": true }]),
    )?;
    assert_eq!(node.component, "mcp");
    build_ygtc_mcp_pack(&pack_path, &node)?;

    let config = Arc::new(host_config(&bindings_path));

    // No admin env configured -> MCP source is None -> graceful node error.
    unsafe {
        std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
        std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
        std::env::remove_var("GREENTIC_AW_MCP");
    }

    let (pack, engine) = build_engine(&pack_path, Arc::clone(&config))?;

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, json!({})))
        .context("mcp ygtc degraded flow run")?;

    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason),
    }

    let bound = find_key(&execution.output, "issue")
        .with_context(|| format!("output key `issue` missing, got {:?}", execution.output))?;
    let err = bound
        .get("error")
        .and_then(Value::as_str)
        .with_context(|| format!("expected an `error` string under `issue`, got {bound:?}"))?;
    assert!(
        err.contains("MCP is not configured"),
        "unexpected error message: {err}"
    );
    Ok(())
}

#[test]
fn mcp_node_calls_tool_and_binds_output() -> Result<()> {
    let _guard = ENV_GUARD.lock().unwrap();
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("mcp-ok.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    // Stand up the wiremock pair: MCP server returns a structured payload.
    let mcp = rt.block_on(fake_mcp_server(
        json!({ "structuredContent": { "title": "Bug" } }),
    ));
    let admin = rt.block_on(fake_admin(&mcp.uri()));

    // LOCKED ENCODING v2: the component ref the flow adapter parses is exactly
    // `"mcp"` — a valid `ComponentId` (no `:`/`/` to be rejected). This is what
    // `runtime_flow_to_flow` runs through `ComponentId::from_str` at pack load.
    assert!(
        greentic_types::ComponentId::from_str("mcp").is_ok(),
        "component ref `mcp` must be a valid ComponentId"
    );

    // `arguments` uses a `{{ }}` template against entry state; `output` names
    // the state key the result is bound under. `server`/`tool` live in the
    // payload (the source of truth), NOT in an `operation` string.
    build_mcp_pack(
        &pack_path,
        json!({
            "server": "github",
            "tool": "get_issue",
            "arguments": { "id": "{{ entry.issue_id }}" },
            "output": "issue"
        }),
    )?;

    let config = Arc::new(host_config(&bindings_path));

    // SAFETY: serialized by ENV_GUARD; cleared after the run below.
    unsafe {
        std::env::set_var("GREENTIC_AW_ADMIN_ENDPOINT", admin.uri());
        std::env::set_var("GREENTIC_AW_ADMIN_TOKEN", "gtc_live_test");
        std::env::remove_var("GREENTIC_AW_MCP");
    }

    let (pack, engine) = build_engine(&pack_path, Arc::clone(&config))?;

    unsafe {
        std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
        std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
    }

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, json!({ "issue_id": "42" })))
        .context("mcp flow run")?;

    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason),
    }

    // The structured tool result is bound under the `issue` output key.
    let issue = find_key(&execution.output, "issue")
        .with_context(|| format!("output key `issue` missing, got {:?}", execution.output))?;
    assert_eq!(
        issue,
        &json!({ "title": "Bug" }),
        "bound output mismatch, got {:?}",
        execution.output
    );
    // And no error surfaced.
    assert!(
        find_key(&execution.output, "error").is_none(),
        "unexpected error in output: {:?}",
        execution.output
    );
    Ok(())
}

#[test]
fn mcp_node_degrades_gracefully_when_unconfigured() -> Result<()> {
    let _guard = ENV_GUARD.lock().unwrap();
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("mcp-degraded.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    build_mcp_pack(
        &pack_path,
        json!({
            "server": "github",
            "tool": "get_issue",
            "arguments": { "id": "1" },
            "output": "issue"
        }),
    )?;

    let config = Arc::new(host_config(&bindings_path));

    // No admin env configured -> MCP source is None -> graceful node error.
    unsafe {
        std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
        std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
        std::env::remove_var("GREENTIC_AW_MCP");
    }

    let (pack, engine) = build_engine(&pack_path, Arc::clone(&config))?;

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, json!({})))
        .context("mcp degraded flow run")?;

    // The runtime must NOT abort: the flow completes...
    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason),
    }

    // ...and the node binds a structured error under the `issue` output key.
    let bound = find_key(&execution.output, "issue")
        .with_context(|| format!("output key `issue` missing, got {:?}", execution.output))?;
    let err = bound
        .get("error")
        .and_then(Value::as_str)
        .with_context(|| format!("expected an `error` string under `issue`, got {bound:?}"))?;
    assert!(
        err.contains("MCP is not configured"),
        "unexpected error message: {err}"
    );
    Ok(())
}

/// Flow-side local-wasm transport proof: an admin row with
/// `"transport": "local-wasm"`, `"component_ref": "router_echo"`, and
/// `"roles": ["flow_editor"]` causes the flow MCP node to dispatch through
/// `mcp_local::local_call_tool` in-process (no network MCP handshake) and
/// bind the result under the configured `output` state key.
///
/// Self-skips when `router_echo.wasm` is not built, mirroring the agent-loop
/// test in `mcp_local_loop.rs`. To run live:
///   ```
///   GREENTIC_MCP_ROUTER_ECHO_WASM=<path> cargo test -p greentic-runner-host mcp_node_local_wasm
///   ```
#[allow(unsafe_code)]
#[test]
fn mcp_node_local_wasm_calls_echo_tool_in_process() -> Result<()> {
    let _guard = ENV_GUARD.lock().unwrap();

    // Resolve the fixture wasm (self-skip when absent).
    let fixture_path = std::env::var("GREENTIC_MCP_ROUTER_ECHO_WASM")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../greentic-mcp/target/wasm32-wasip2/release/router_echo.wasm")
        });
    if !fixture_path.exists() {
        eprintln!(
            "SKIP: router_echo.wasm not found; set GREENTIC_MCP_ROUTER_ECHO_WASM to run live"
        );
        return Ok(());
    }

    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("mcp-local-wasm.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    let cache_dir = temp.path().join("mcp-local-cache");
    std::fs::create_dir_all(&cache_dir)?;
    std::fs::write(&bindings_path, b"tenant: demo")?;

    // Copy the fixture into the temp cache dir.
    std::fs::copy(&fixture_path, cache_dir.join("router_echo.wasm"))?;

    // Stand up a fake admin returning a `local-wasm` flow_editor server.
    let admin_server = rt.block_on(MockServer::start());
    rt.block_on(
        Mock::given(method("GET"))
            .and(path("/api/v1/designer/tenant/me/mcp-servers"))
            .and(header("authorization", "Bearer gtc_live_test"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "servers": [
                    {
                        "id": "local_echo",
                        "name": "LocalEcho",
                        "transport_url": "",
                        "auth_header_name": null,
                        "auth_token": null,
                        "allowed_tools": null,
                        "roles": ["flow_editor"],
                        "transport": "local-wasm",
                        "component_ref": "router_echo",
                        "component_version": "1.0.0"
                    }
                ]
            })))
            .mount(&admin_server),
    );

    // Build a pack with an MCP node calling the `echo` tool.
    build_mcp_pack(
        &pack_path,
        json!({
            "server": "local_echo",
            "tool": "echo",
            "arguments": { "message": "hi" },
            "output": "echo_result"
        }),
    )?;

    let config = Arc::new(host_config(&bindings_path));

    // SAFETY: serialised by ENV_GUARD.
    //
    // `GREENTIC_AW_ADMIN_ENDPOINT`/`GREENTIC_AW_ADMIN_TOKEN` are read by
    // `FlowEngine::new` to construct the `McpToolSource` and can be cleared
    // after `build_engine`. `GREENTIC_MCP_LOCAL_CACHE_DIR` is read lazily
    // inside `mcp_local::cache_dir()` at probe / call time (during `execute`),
    // so it must remain set until after the engine runs.
    unsafe {
        std::env::set_var("GREENTIC_AW_ADMIN_ENDPOINT", admin_server.uri());
        std::env::set_var("GREENTIC_AW_ADMIN_TOKEN", "gtc_live_test");
        std::env::remove_var("GREENTIC_AW_MCP");
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", &cache_dir);
    }

    let (pack, engine) = build_engine(&pack_path, Arc::clone(&config))?;

    // Clear only the admin endpoint/token; keep GREENTIC_MCP_LOCAL_CACHE_DIR
    // set until after execute (the cache_dir() call is lazy).
    unsafe {
        std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
        std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
    }

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, json!({})))
        .context("mcp local-wasm flow run")?;

    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
    }

    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason),
    }

    // The echo result is bound under `echo_result` — no error key.
    let echo_result = find_key(&execution.output, "echo_result").with_context(|| {
        format!(
            "output key `echo_result` missing, got {:?}",
            execution.output
        )
    })?;
    assert!(
        !echo_result.to_string().contains("\"error\""),
        "echo result must not contain an error; got: {echo_result}"
    );
    Ok(())
}

/// An embedding host can hand the engine an MCP source it built itself,
/// instead of the engine deriving one from process-global `GREENTIC_AW_*`
/// env vars.
///
/// This is what a host serving MANY tenants from ONE process needs: env can
/// only name a single tenant, so a per-tenant host has no way to say which
/// tenant its MCP catalog should come from. The designer's Run Demo is that
/// host.
///
/// The env is cleared BEFORE the engine is built, so the env-derived source
/// is `None`. If the injection did not take, the node would bind the
/// "MCP is not configured" error instead of the tool's result — which is
/// exactly the failure this test exists to catch.
#[test]
fn an_injected_mcp_source_is_used_when_the_env_is_unset() -> Result<()> {
    let _guard = ENV_GUARD.lock().unwrap();
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("mcp-injected.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let mcp = rt.block_on(fake_mcp_server(
        json!({ "structuredContent": { "title": "Bug" } }),
    ));
    let admin = rt.block_on(fake_admin(&mcp.uri()));

    build_mcp_pack(
        &pack_path,
        json!({
            "server": "github",
            "tool": "get_issue",
            "arguments": { "id": "{{ entry.issue_id }}" },
            "output": "issue"
        }),
    )?;

    let config = Arc::new(host_config(&bindings_path));

    // SAFETY: serialized by ENV_GUARD. Deliberately UNSET: the source under
    // test must come from the injection, not from the environment.
    unsafe {
        std::env::remove_var("GREENTIC_AW_ADMIN_ENDPOINT");
        std::env::remove_var("GREENTIC_AW_ADMIN_TOKEN");
        std::env::remove_var("GREENTIC_AW_MCP");
    }

    let injected = Arc::new(greentic_aw_runtime::McpToolSource::new(
        admin.uri(),
        "gtc_live_test",
    ));

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
    let engine = rt
        .block_on(FlowEngine::new(
            vec![Arc::clone(&pack)],
            Arc::clone(&config),
        ))?
        .with_mcp_source(Some(injected));

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, json!({ "issue_id": "42" })))
        .context("mcp flow run")?;

    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason),
    }

    let issue = find_key(&execution.output, "issue")
        .with_context(|| format!("output key `issue` missing, got {:?}", execution.output))?;
    assert_eq!(
        issue,
        &json!({ "title": "Bug" }),
        "an injected source must be used even with no MCP env set; got {:?}",
        execution.output
    );
    Ok(())
}

// ── pack-carried routes (no admin at all) ────────────────────────────────────
//
// The feature these cover: a deployed runner holds no admin credentials, so an
// opaque `server` id used to be unresolvable and every MCP node in a deployed
// bundle bound `{"error": "MCP is not configured on this runner"}`. The pack
// now carries the non-secret half of the route in `assets/mcp-routes.json`, and
// the credential is read from the secrets backend at dispatch time.
//
// These exercise `invoke_with_secrets` rather than a whole flow run because the
// secrets manager `invoke` uses is a process-global memoized `OnceLock` that a
// test cannot substitute; the seam takes it explicitly.

/// Build `.gtpack` bytes carrying exactly one `http` route sidecar.
fn pack_with_http_route(server_id: &str, transport_url: &str, auth_header_name: &str) -> Vec<u8> {
    pack_with_http_route_scoped(server_id, transport_url, auth_header_name, None)
}

/// [`pack_with_http_route`] plus the `auth_team` the authoring session
/// resolved the server's token under. `None` omits the field entirely, which is
/// what a pack predating it looks like.
fn pack_with_http_route_scoped(
    server_id: &str,
    transport_url: &str,
    auth_header_name: &str,
    auth_team: Option<&str>,
) -> Vec<u8> {
    let mut record = json!({
        "server_id": server_id,
        "name": server_id,
        "transport": "http",
        "transport_url": transport_url,
        "auth_header_name": auth_header_name,
    });
    if let Some(team) = auth_team {
        record["auth_team"] = json!(team);
    }
    let sidecar = json!([record]).to_string();

    let mut out = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut out));
        zip.start_file::<_, ()>(
            "assets/mcp-routes.json",
            zip::write::SimpleFileOptions::default(),
        )
        .expect("start entry");
        zip.write_all(sidecar.as_bytes()).expect("write sidecar");
        zip.finish().expect("finish zip");
    }
    out
}

/// A secrets backend holding exactly the `(uri, value)` pairs given. Anything
/// else is `NotFound`, which is what the missing-credential test relies on.
struct StubSecrets {
    entries: Vec<(String, String)>,
}

#[async_trait::async_trait]
impl greentic_secrets_lib::SecretsManager for StubSecrets {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        self.entries
            .iter()
            .find(|(uri, _)| uri == path)
            .map(|(_, value)| value.as_bytes().to_vec())
            .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
    }
    async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
}

fn stub_secrets_manager(
    entries: &[(String, String)],
) -> greentic_runner_host::secrets::DynSecretsManager {
    Arc::new(StubSecrets {
        entries: entries.to_vec(),
    })
}

/// Thin wrapper over the public `invoke_with_secrets` seam, so these tests read
/// the way the plan describes them.
#[allow(clippy::too_many_arguments)]
async fn invoke_with_pack_routes(
    source: Option<&Arc<greentic_aw_runtime::McpToolSource>>,
    pack_routes: Option<&greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes>,
    secrets: Option<greentic_runner_host::secrets::DynSecretsManager>,
    tenant: &str,
    env: &str,
    team: Option<&str>,
    server_id: &str,
    tool: &str,
    arguments: &Value,
) -> Value {
    invoke_with_pack_routes_for_unit(
        source,
        pack_routes,
        secrets,
        tenant,
        env,
        team,
        None,
        server_id,
        tool,
        arguments,
    )
    .await
}

/// [`invoke_with_pack_routes`] for a named deployed unit (the running
/// revision's `bundle_id`).
#[allow(clippy::too_many_arguments)]
async fn invoke_with_pack_routes_for_unit(
    source: Option<&Arc<greentic_aw_runtime::McpToolSource>>,
    pack_routes: Option<&greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes>,
    secrets: Option<greentic_runner_host::secrets::DynSecretsManager>,
    tenant: &str,
    env: &str,
    team: Option<&str>,
    unit: Option<&str>,
    server_id: &str,
    tool: &str,
    arguments: &Value,
) -> Value {
    greentic_runner_host::runner::mcp_node::aw::invoke_with_secrets(
        source,
        pack_routes,
        secrets.as_ref(),
        tenant,
        env,
        team,
        unit,
        server_id,
        tool,
        arguments,
    )
    .await
}

/// The whole point of the feature: a runner with NO admin credentials
/// dispatches an MCP node using only what the pack carries plus a secret from
/// its own secrets backend.
#[tokio::test]
async fn a_pack_route_dispatches_without_any_admin_source() {
    let server = fake_mcp_server(json!({ "structuredContent": { "premium": 42 } })).await;

    let routes = greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes::from_pack_bytes(
        &pack_with_http_route("srv-1", &server.uri(), "Authorization"),
    )
    .expect("routes");

    let secrets = stub_secrets_manager(&[(
        "secrets://default/acme/_/mcp/srv-1".to_string(),
        "bearer-token".to_string(),
    )]);

    let value = invoke_with_pack_routes(
        None, // no admin source at all
        Some(&routes),
        Some(secrets),
        "acme",
        "default",
        None,
        "srv-1",
        "create_quote",
        &json!({}),
    )
    .await;

    assert_eq!(
        value["premium"], 42,
        "expected a dispatched result, got {value}"
    );
}

/// A missing credential must say which URI was looked for. The old failure
/// mode -- an opaque NotFound -- is indistinguishable from a misconfigured
/// server, and cost a full investigation once already.
#[tokio::test]
async fn a_missing_secret_names_the_uri_it_looked_for() {
    let routes = greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes::from_pack_bytes(
        &pack_with_http_route("srv-1", "https://unused.example.com", "Authorization"),
    )
    .expect("routes");

    let value = invoke_with_pack_routes(
        None,
        Some(&routes),
        Some(stub_secrets_manager(&[])),
        "acme",
        "default",
        None,
        "srv-1",
        "create_quote",
        &json!({}),
    )
    .await;

    let error = value["error"].as_str().expect("an error string");
    assert!(
        error.contains("secrets://default/acme/_/mcp/srv-1"),
        "error must name the URI it looked for, got: {error}"
    );
}

/// The shipped flow path gains the team two-step. Admin seals an MCP token at
/// the server ROW's team scope, and the deployed runtime carries no team of its
/// own, so before this a team-scoped server's token was unreachable in EVERY
/// lane — the node reported a missing credential and the cause looked like a
/// misconfigured server.
#[tokio::test]
async fn a_team_scoped_secret_resolves_through_the_sidecars_auth_team() {
    let server = fake_mcp_server(json!({ "structuredContent": { "premium": 7 } })).await;

    let routes = greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes::from_pack_bytes(
        &pack_with_http_route_scoped("srv-1", &server.uri(), "Authorization", Some("sales")),
    )
    .expect("routes");
    let team = routes
        .get("srv-1")
        .and_then(|route| route.auth_team.as_deref());
    assert_eq!(team, Some("sales"), "the sidecar must round-trip auth_team");

    let secrets = stub_secrets_manager(&[(
        "secrets://default/acme/sales/mcp/srv-1".to_string(),
        "bearer-token".to_string(),
    )]);

    let value = invoke_with_pack_routes(
        None,
        Some(&routes),
        Some(secrets),
        "acme",
        "default",
        team,
        "srv-1",
        "create_quote",
        &json!({}),
    )
    .await;

    assert_eq!(value["premium"], 7, "expected a dispatch, got {value}");
}

/// The property the two-step must not break: a deployment whose token sits at
/// the tenant-default `_` scope keeps resolving even when the sidecar names a
/// team. There is no case where a previously-resolving token stops resolving.
#[tokio::test]
async fn a_tenant_default_secret_still_resolves_when_the_sidecar_names_a_team() {
    let server = fake_mcp_server(json!({ "structuredContent": { "premium": 9 } })).await;

    let routes = greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes::from_pack_bytes(
        &pack_with_http_route_scoped("srv-1", &server.uri(), "Authorization", Some("sales")),
    )
    .expect("routes");

    let secrets = stub_secrets_manager(&[(
        "secrets://default/acme/_/mcp/srv-1".to_string(),
        "bearer-token".to_string(),
    )]);

    let value = invoke_with_pack_routes(
        None,
        Some(&routes),
        Some(secrets),
        "acme",
        "default",
        Some("sales"),
        "srv-1",
        "create_quote",
        &json!({}),
    )
    .await;

    assert_eq!(value["premium"], 9, "expected a dispatch, got {value}");
}

/// With neither scope holding the token, the error must name BOTH URIs tried
/// and the `SECRETS_BACKEND=broker` requirement — an opaque NotFound here is
/// indistinguishable from an unregistered server.
#[tokio::test]
async fn a_missing_secret_names_every_scope_it_tried() {
    let routes = greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes::from_pack_bytes(
        &pack_with_http_route_scoped(
            "srv-1",
            "https://unused.example.com",
            "Authorization",
            Some("sales"),
        ),
    )
    .expect("routes");

    let value = invoke_with_pack_routes(
        None,
        Some(&routes),
        Some(stub_secrets_manager(&[])),
        "acme",
        "default",
        Some("sales"),
        "srv-1",
        "create_quote",
        &json!({}),
    )
    .await;

    let error = value["error"].as_str().expect("an error string");
    assert!(
        error.contains("secrets://default/acme/sales/mcp/srv-1")
            && error.contains("secrets://default/acme/_/mcp/srv-1"),
        "got: {error}"
    );
    assert!(error.contains("SECRETS_BACKEND=broker"), "got: {error}");
}

/// A sidecar predating `auth_team` omits the field, and the runner then behaves
/// exactly as it did before: `_`-only.
#[tokio::test]
async fn a_sidecar_without_auth_team_reads_only_the_tenant_default_scope() {
    let routes = greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes::from_pack_bytes(
        &pack_with_http_route("srv-1", "https://unused.example.com", "Authorization"),
    )
    .expect("routes");
    assert_eq!(
        routes.get("srv-1").and_then(|r| r.auth_team.as_deref()),
        None
    );

    let value = invoke_with_pack_routes(
        None,
        Some(&routes),
        Some(stub_secrets_manager(&[])),
        "acme",
        "default",
        None,
        "srv-1",
        "create_quote",
        &json!({}),
    )
    .await;

    let error = value["error"].as_str().expect("an error string");
    assert!(
        !error.contains("/sales/"),
        "no team scope may be tried when the sidecar names none; got: {error}"
    );
    assert!(
        error.contains("secrets://default/acme/_/mcp/srv-1"),
        "got: {error}"
    );
}

// ── per-unit credential scope ────────────────────────────────────────────────
//
// Two workers in one environment bind the same MCP server. Each deployed unit
// (revision `bundle_id`) resolves its OWN token first, at
// `…/mcp/<server_id>.unit-<segment>`, so neither can read the other's.

/// A unit resolves the credential staged for it, and a second unit with no
/// credential of its own does NOT borrow the first one's.
#[tokio::test]
async fn each_unit_resolves_only_its_own_unit_scoped_secret() {
    let server = fake_mcp_server(json!({ "structuredContent": { "premium": 11 } })).await;
    let routes = greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes::from_pack_bytes(
        &pack_with_http_route("srv-1", &server.uri(), "Authorization"),
    )
    .expect("routes");

    let unit_a_key =
        greentic_aw_runtime::mcp_secrets::mcp_unit_secret_key("srv-1", "worker-a").expect("key");
    let unit_b_key =
        greentic_aw_runtime::mcp_secrets::mcp_unit_secret_key("srv-1", "worker-b").expect("key");
    let secrets = stub_secrets_manager(&[(
        format!("secrets://default/acme/_/mcp/{unit_a_key}"),
        "token-a".to_string(),
    )]);

    let a = invoke_with_pack_routes_for_unit(
        None,
        Some(&routes),
        Some(secrets.clone()),
        "acme",
        "default",
        None,
        Some("worker-a"),
        "srv-1",
        "create_quote",
        &json!({}),
    )
    .await;
    assert_eq!(a["premium"], 11, "unit A must dispatch, got {a}");

    let b = invoke_with_pack_routes_for_unit(
        None,
        Some(&routes),
        Some(secrets),
        "acme",
        "default",
        None,
        Some("worker-b"),
        "srv-1",
        "create_quote",
        &json!({}),
    )
    .await;
    let error = b["error"].as_str().expect("unit B has no credential");
    assert!(
        error.contains(&format!("secrets://default/acme/_/mcp/{unit_b_key}"))
            && error.contains("secrets://default/acme/_/mcp/srv-1"),
        "the miss must name unit B's own scope and the shared fallback, got: {error}"
    );
    assert!(
        !error.contains(&unit_a_key),
        "unit B must never look at unit A's scope, got: {error}"
    );
}

/// The compatibility fallback: a deployment staged before the unit scope
/// existed keeps resolving its shared credential when the runner moves.
#[tokio::test]
async fn a_unit_with_no_scoped_secret_falls_back_to_the_shared_one() {
    let server = fake_mcp_server(json!({ "structuredContent": { "premium": 12 } })).await;
    let routes = greentic_runner_host::runner::mcp_pack_routes::PackMcpRoutes::from_pack_bytes(
        &pack_with_http_route_scoped("srv-1", &server.uri(), "Authorization", Some("sales")),
    )
    .expect("routes");
    let secrets = stub_secrets_manager(&[(
        "secrets://default/acme/_/mcp/srv-1".to_string(),
        "shared-token".to_string(),
    )]);

    let value = invoke_with_pack_routes_for_unit(
        None,
        Some(&routes),
        Some(secrets),
        "acme",
        "default",
        Some("sales"),
        Some("worker-a"),
        "srv-1",
        "create_quote",
        &json!({}),
    )
    .await;
    assert_eq!(value["premium"], 12, "expected a dispatch, got {value}");
}
