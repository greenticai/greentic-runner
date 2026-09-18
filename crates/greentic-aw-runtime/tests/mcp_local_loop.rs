//! End-to-end agent-loop path through the **local-wasm** MCP transport.
//!
//! Mirrors `tests/mcp_loop.rs` in structure and assertions, but replaces the
//! wiremock MCP server with an in-process `local-wasm` admin row: the admin
//! returns a server with `"transport": "local-wasm"` and `"component_ref":
//! "router_echo"`. A temp dir is set as `GREENTIC_MCP_LOCAL_CACHE_DIR` and the
//! `router_echo.wasm` fixture is copied into it before the step is run.
//!
//! The test self-skips when the fixture WASM is absent (not built), so CI that
//! has not compiled the `wasm32-wasip2` target does not fail.
//!
//! Resolution order for the fixture (mirrors `mcp_local.rs::fixture_wasm`):
//!   1. `GREENTIC_MCP_ROUTER_ECHO_WASM` env var (explicit override, used by CI
//!      and the `make verify` step in the task brief).
//!   2. `<CARGO_MANIFEST_DIR>/../../../greentic-mcp/target/wasm32-wasip2/release/router_echo.wasm`
//!      (relative path assuming the standard workspace layout).
//!
//! Run live:
//!   ```
//!   GREENTIC_MCP_ROUTER_ECHO_WASM=/path/to/router_echo.wasm \
//!     cargo test -p greentic-aw-runtime --test mcp_local_loop -- --nocapture
//!   ```

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::{LlmError, TerminationReason};
use greentic_aw_runtime::llm::{LlmBackend, LlmRequest, LlmResponse};
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::state::ToolCallRecord;
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, AgentStep, LlmProviderRef, McpToolSource,
    ToolRef,
};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ── fixture wasm resolution ───────────────────────────────────────────────────

/// Resolve the `router_echo.wasm` fixture path.
///
/// Returns `Some(path)` when the file exists, `None` otherwise (triggering a
/// self-skip so the test never blocks CI that hasn't built the WASM target).
fn fixture_wasm() -> Option<PathBuf> {
    let candidate = std::env::var("GREENTIC_MCP_ROUTER_ECHO_WASM")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../greentic-mcp/target/wasm32-wasip2/release/router_echo.wasm")
        });
    candidate.exists().then_some(candidate)
}

// ── recording LLM backend ─────────────────────────────────────────────────────

/// LLM backend that records the `(extension_id, tool_name)` pairs it was
/// offered on each turn and returns the next scripted response. This lets the
/// test assert exactly what the loop presented to the model.
struct RecordingLlmBackend {
    responses: Mutex<Vec<Result<LlmResponse, LlmError>>>,
    offered: Mutex<Vec<Vec<(String, String)>>>,
}

impl RecordingLlmBackend {
    fn new(responses: Vec<Result<LlmResponse, LlmError>>) -> Self {
        Self {
            responses: Mutex::new(responses),
            offered: Mutex::new(Vec::new()),
        }
    }
}

impl LlmBackend for RecordingLlmBackend {
    fn complete<'a>(
        &'a self,
        req: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        self.offered.lock().expect("offered mutex").push(
            req.tools
                .iter()
                .map(|tool| (tool.extension_id.clone(), tool.tool_name.clone()))
                .collect(),
        );
        let next = {
            let mut queue = self.responses.lock().expect("responses mutex");
            if queue.is_empty() {
                Err(LlmError::Transport("recording queue exhausted".into()))
            } else {
                queue.remove(0)
            }
        };
        Box::pin(async move { next })
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn build_agent_config(allowed_tools: Vec<ToolRef>) -> AgentConfig {
    AgentConfig {
        agent_id: "a".into(),
        system_prompt: "sys".into(),
        tools: allowed_tools,
        guardrails: vec![],
        llm: LlmProviderRef {
            provider: "mock".into(),
            model: "m".into(),
            credential_ref: None,
        },
        limits: AgentLimits {
            max_iter: 4,
            timeout: Duration::from_secs(60),
            ..AgentLimits::default()
        },
        memory: None,
        knowledge: None,
        conversational: false,
        opening_message: None,
    }
}

/// Script a `tools/call` to the `echo` tool on server `s1`, followed by a
/// final reply once the result is available.
fn call_echo_tool() -> LlmResponse {
    LlmResponse {
        content: None,
        tool_calls: vec![ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "mcp:s1".into(),
            tool_name: "echo".into(),
            args: json!({ "message": "hello" }),
        }],
        tokens_in: 5,
        tokens_out: 5,
    }
}

fn final_reply(text: &str) -> LlmResponse {
    LlmResponse {
        content: Some(text.into()),
        tool_calls: vec![],
        tokens_in: 5,
        tokens_out: 5,
    }
}

/// Mount the admin `mcp-servers` endpoint returning one `local-wasm` server
/// with `component_ref: "router_echo"` and `roles: ["agentic_worker"]`.
///
/// No `transport_url` is needed for the local-wasm transport; an empty string
/// is the conventional placeholder (mirrors `mcp_source.rs` unit tests).
async fn mount_admin_local_wasm(admin: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .and(header("authorization", "Bearer gtc_live_test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "servers": [
                {
                    "id": "s1",
                    "name": "RouterEcho",
                    "transport_url": "",
                    "auth_header_name": null,
                    "auth_token": null,
                    "allowed_tools": null,
                    "roles": ["agentic_worker"],
                    "transport": "local-wasm",
                    "component_ref": "router_echo",
                    "component_version": "1.0.0"
                }
            ]
        })))
        .mount(admin)
        .await;
}

fn build_runtime(
    llm: Arc<RecordingLlmBackend>,
    mcp_source: Option<Arc<McpToolSource>>,
    config: AgentConfig,
) -> (AgentRuntime, TenantContext) {
    let state_store = Arc::new(MockAgentStateStore::new());
    let telemetry = Arc::new(MockTelemetry::new());
    let config_provider = MockConfigProvider::new();
    let tenant_ctx = TenantContext::new("acme", "prod");
    config_provider.insert(&tenant_ctx, "a", config);
    let config_provider = Arc::new(config_provider);
    let token_meter = Arc::new(MockTokenMeter::new(0));
    let tool_ledger = Arc::new(NoopToolLedger);
    let ext_runtime = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let runtime = AgentRuntime::new(
        config_provider,
        state_store,
        ext_runtime,
        llm,
        telemetry,
        token_meter,
        tool_ledger,
        mcp_source,
    );
    (runtime, tenant_ctx)
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Happy path: the admin returns a `local-wasm` server with `component_ref:
/// "router_echo"`. The loop resolves its tool catalog in-process (no network
/// MCP handshake), offers `mcp:s1/echo` to the LLM, dispatches the call via
/// `local_call_tool`, and the echo result lands in the trail.
///
/// Marked `#[serial_test::serial]` because it writes
/// `GREENTIC_MCP_LOCAL_CACHE_DIR` to a process-global env var that is shared
/// with other tests in `mcp_local.rs`.
#[tokio::test]
#[serial_test::serial]
async fn local_wasm_mcp_tool_offered_called_and_result_in_trail() {
    let Some(fixture_path) = fixture_wasm() else {
        // Self-skip: router_echo.wasm not built; test does not block CI.
        eprintln!(
            "SKIP: router_echo.wasm not found; set GREENTIC_MCP_ROUTER_ECHO_WASM to run live"
        );
        return;
    };

    // Set up a temp cache dir and copy the fixture into it.
    let cache_dir = tempfile::tempdir().expect("create temp cache dir");
    // Safety: serial attribute ensures no concurrent env-var mutation between
    // tests that share GREENTIC_MCP_LOCAL_CACHE_DIR.
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache_dir.path());
    }
    std::fs::copy(&fixture_path, cache_dir.path().join("router_echo.wasm"))
        .expect("copy fixture wasm");

    // Stand up the fake admin returning the local-wasm server row.
    let admin = MockServer::start().await;
    mount_admin_local_wasm(&admin).await;

    // Script: first turn calls `echo`, second turn produces the final reply.
    let llm = Arc::new(RecordingLlmBackend::new(vec![
        Ok(call_echo_tool()),
        Ok(final_reply("done")),
    ]));

    let mcp_source = Arc::new(McpToolSource::new(admin.uri(), "gtc_live_test"));
    let allowed_tools = vec![ToolRef {
        extension_id: "mcp:s1".into(),
        tool_name: "echo".into(),
        description: None,
        input_schema: None,
        usage_note: None,
    }];
    let (runtime, tenant_ctx) = build_runtime(
        llm.clone(),
        Some(mcp_source),
        build_agent_config(allowed_tools),
    );

    let output = runtime
        .step(
            tenant_ctx,
            "s",
            "a",
            AgentInput {
                text: "go".into(),
                conversational: false,
            },
        )
        .await
        .expect("agent step must succeed");

    // (a) The local-wasm MCP tool was offered to the LLM on the first turn.
    let offered = llm.offered.lock().unwrap();
    assert!(
        offered[0].contains(&("mcp:s1".to_string(), "echo".to_string())),
        "first turn must offer the local-wasm mcp tool; got: {:?}",
        offered[0]
    );

    // (b) The echo tool result landed in the trail (no error key).
    let tool_result = output.trail.iter().find_map(|step| match step {
        AgentStep::ToolCall { name, result, .. } if name == "echo" => Some(result.clone()),
        _ => None,
    });
    assert!(
        tool_result.is_some(),
        "echo tool call must appear in the trail; trail: {:?}",
        output.trail
    );
    let result_value = tool_result.unwrap();
    assert!(
        !result_value.to_string().contains("\"error\""),
        "echo result must not contain an error; got: {result_value}"
    );

    // (c) The step completes with the final reply.
    assert_eq!(output.terminated_by, TerminationReason::FinalReply);
    assert_eq!(output.reply, "done");
}

/// Degrade case: the admin returns a `local-wasm` server but the cache dir
/// contains no matching component. The catalog probe fails → empty catalog →
/// the tool ref is dropped before the LLM sees it → the step still completes
/// without panic.
#[tokio::test]
#[serial_test::serial]
async fn local_wasm_unreachable_component_degrades_no_tool_offered() {
    // Point the cache dir at an EMPTY temp dir so the wasm lookup fails.
    let empty_cache_dir = tempfile::tempdir().expect("create empty cache dir");
    // Safety: serial attribute ensures no concurrent env-var mutation.
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", empty_cache_dir.path());
    }

    let admin = MockServer::start().await;
    mount_admin_local_wasm(&admin).await;

    let llm = Arc::new(RecordingLlmBackend::new(vec![Ok(final_reply("no tools"))]));
    let mcp_source = Arc::new(McpToolSource::new(admin.uri(), "gtc_live_test"));
    let allowed_tools = vec![ToolRef {
        extension_id: "mcp:s1".into(),
        tool_name: "echo".into(),
        description: None,
        input_schema: None,
        usage_note: None,
    }];
    let (runtime, tenant_ctx) = build_runtime(
        llm.clone(),
        Some(mcp_source),
        build_agent_config(allowed_tools),
    );

    let output = runtime
        .step(
            tenant_ctx,
            "s",
            "a",
            AgentInput {
                text: "go".into(),
                conversational: false,
            },
        )
        .await
        .expect("agent step must succeed even when local-wasm component is missing");

    // The LLM was offered NO tools (empty catalog dropped the mcp ref).
    let offered = llm.offered.lock().unwrap();
    assert!(
        offered[0].is_empty(),
        "missing local-wasm component → no tools offered; got: {:?}",
        offered[0]
    );

    // The step still completes cleanly.
    assert_eq!(output.terminated_by, TerminationReason::FinalReply);
    assert_eq!(output.reply, "no tools");
}
