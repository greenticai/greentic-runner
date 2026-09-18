//! End-to-end verified local-wasm path: store-pull → verify → run (agent loop).
//!
//! Proves that a registered+signed component pulls from the store, passes
//! integrity + signature verification, lands in the cache, and is subsequently
//! offered to the LLM and executed in an agent step — all with an empty cache
//! at the start (no pre-seeding).
//!
//! Two scenarios are covered:
//! 1. **Happy path** — correct digest + trusted key → tool offered + result in trail.
//! 2. **Tampered artifact** — wrong `component_digest` in the admin row →
//!    store pull fails → catalog empty → tool NOT offered → step completes
//!    without panic.
//!
//! The flow-node path is covered by the existing `mcp_source::dispatch_route`
//! contract test (transport-branching + degrade in `mcp_source.rs` unit tests)
//! and by the Task 3 `lazy_pull_on_catalog_miss_and_dispatch` test, which
//! exercises both the list and dispatch paths through the `LocalWasm` branch of
//! `call_route`. A standalone flow-node host integration test would require
//! standing up a full `greentic-runner-host` crate; that is out of scope for
//! this slice (see report for details).
//!
//! # Running live
//! ```text
//! GREENTIC_MCP_ROUTER_ECHO_WASM=/path/to/router_echo.wasm \
//!   cargo test -p greentic-aw-runtime --test mcp_store_pull_e2e -- --nocapture
//! ```
//!
//! Self-skips when the `GREENTIC_MCP_ROUTER_ECHO_WASM` env var is unset and the
//! default relative path does not resolve — CI that has not built the wasm
//! target does not fail.

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]

use std::future::Future;
use std::io::Write as _;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use ed25519_dalek::{Signer, SigningKey};
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
use sha2::{Digest, Sha256};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ── env var names (mirrors mcp_store_pull constants) ─────────────────────────

const STORE_URL_ENV: &str = "GREENTIC_STORE_URL";
const STORE_TOKEN_ENV: &str = "GREENTIC_STORE_TOKEN";
const TRUSTED_SIGNERS_ENV: &str = "GREENTIC_MCP_TRUSTED_SIGNERS";
const DESCRIBE_ENTRY: &str = "describe.json";
const WASM_ENTRY: &str = "extension.wasm";

// ── local fixture helpers ─────────────────────────────────────────────────────

/// Resolve the router_echo fixture wasm — same logic as `mcp_local_loop.rs`.
fn resolve_fixture_wasm() -> Option<PathBuf> {
    let candidate = std::env::var("GREENTIC_MCP_ROUTER_ECHO_WASM")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../greentic-mcp/target/wasm32-wasip2/release/router_echo.wasm")
        });
    candidate.exists().then_some(candidate)
}

/// Hex-lowercase SHA256 of `bytes`.
fn hex_sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as FmtWrite;
    let digest = Sha256::digest(bytes);
    digest.iter().fold(
        String::with_capacity(digest.len() * 2),
        |mut accumulator, byte| {
            let _ = write!(accumulator, "{byte:02x}");
            accumulator
        },
    )
}

/// Minimal unsigned describe document for `router_echo 1.0.0`.
fn sample_describe() -> serde_json::Value {
    json!({
        "apiVersion": "greentic.ai/v1",
        "kind": "ProviderExtension",
        "metadata": {
            "id": "router_echo",
            "version": "1.0.0",
            "summary": "echo router for tests"
        }
    })
}

/// Sign `describe` the same way the store does: serialize unsigned describe,
/// sign those bytes, then inject `signature {algorithm, publicKey, value}`.
fn sign_describe_like_store(describe: &serde_json::Value, signing: &SigningKey) -> Vec<u8> {
    let message = serde_json::to_vec(describe).unwrap();
    let signature = signing.sign(&message);
    let signature_b64 = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
    let public_b64 =
        base64::engine::general_purpose::STANDARD.encode(signing.verifying_key().to_bytes());
    let mut signed = describe.clone();
    signed.as_object_mut().unwrap().insert(
        "signature".to_string(),
        json!({
            "algorithm": "ed25519",
            "publicKey": public_b64,
            "value": signature_b64,
        }),
    );
    serde_json::to_vec(&signed).unwrap()
}

/// Build a `.gtxpack` ZIP from `describe.json` bytes + `extension.wasm` bytes.
fn build_gtxpack(describe_json: &[u8], wasm: &[u8]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options: zip::write::FileOptions<()> =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        writer.start_file(DESCRIBE_ENTRY, options).unwrap();
        writer.write_all(describe_json).unwrap();
        writer.start_file(WASM_ENTRY, options).unwrap();
        writer.write_all(wasm).unwrap();
        writer.finish().unwrap();
    }
    buf
}

/// Format a trusted-signer env value for `signing`.
fn pubkey_env_value(signing: &SigningKey) -> String {
    format!(
        "ed25519:{}",
        base64::engine::general_purpose::STANDARD.encode(signing.verifying_key().to_bytes())
    )
}

// ── mock helpers ──────────────────────────────────────────────────────────────

/// Stand up a mock store that serves `archive_bytes` at the artifact route.
async fn mount_store_artifact(store: &MockServer, archive_bytes: Vec<u8>) {
    Mock::given(method("GET"))
        .and(path("/api/v1/extensions/router_echo/1.0.0/artifact"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(archive_bytes),
        )
        .mount(store)
        .await;
}

/// Mount the admin `mcp-servers` endpoint returning a `local-wasm` row for
/// `router_echo 1.0.0` with the given `component_digest`. The bearer token is
/// `gtc_live_e2e`.
async fn mount_admin_local_wasm_with_digest(admin: &MockServer, component_digest: &str) {
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .and(header("authorization", "Bearer gtc_live_e2e"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "servers": [{
                "id": "s1",
                "name": "RouterEcho",
                "transport_url": "",
                "auth_header_name": null,
                "auth_token": null,
                "allowed_tools": null,
                "roles": ["agentic_worker"],
                "transport": "local-wasm",
                "component_ref": "router_echo",
                "component_version": "1.0.0",
                "component_digest": component_digest
            }]
        })))
        .mount(admin)
        .await;
}

// ── recording LLM backend (mirrors mcp_local_loop.rs) ────────────────────────

/// LLM backend that records the `(extension_id, tool_name)` pairs offered to it
/// per turn and returns the next scripted response.
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

// ── scripted LLM responses ────────────────────────────────────────────────────

fn call_echo_tool() -> LlmResponse {
    LlmResponse {
        content: None,
        tool_calls: vec![ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "mcp:s1".into(),
            tool_name: "echo".into(),
            args: json!({ "message": "e2e-hello" }),
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

// ── runtime builder ───────────────────────────────────────────────────────────

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

/// Happy path: the cache starts empty, the admin returns a `local-wasm` server
/// with a correct `component_digest`, and the trusted signer env is set. The
/// agent step should:
///   (a) pull and verify the `.gtxpack` from the mock store,
///   (b) write `router_echo.wasm` and its sidecar to the cache,
///   (c) offer `mcp:s1/echo` to the LLM,
///   (d) dispatch the tool call and get a non-error result in the trail,
///   (e) terminate with the final reply.
#[tokio::test]
#[serial_test::serial]
async fn verified_store_pull_tool_offered_and_result_in_trail() {
    let Some(fixture_path) = resolve_fixture_wasm() else {
        eprintln!(
            "SKIP: router_echo.wasm not found; set GREENTIC_MCP_ROUTER_ECHO_WASM to run live"
        );
        return;
    };

    // Build signed .gtxpack.
    let wasm_bytes = std::fs::read(&fixture_path).expect("read fixture wasm");
    let signing_key = SigningKey::from_bytes(&[42u8; 32]);
    let signed_describe = sign_describe_like_store(&sample_describe(), &signing_key);
    let archive = build_gtxpack(&signed_describe, &wasm_bytes);
    let gtxpack_digest = hex_sha256(&archive);

    // Stand up mock store + admin.
    let store_server = MockServer::start().await;
    mount_store_artifact(&store_server, archive).await;

    let admin_server = MockServer::start().await;
    mount_admin_local_wasm_with_digest(&admin_server, &gtxpack_digest).await;

    // Empty cache dir (proves lazy pull actually runs).
    let cache_dir = tempfile::tempdir().expect("create temp cache dir");

    // Safety: serial ensures exclusive env-var access with other serial tests.
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache_dir.path());
        std::env::set_var(STORE_URL_ENV, store_server.uri());
        std::env::set_var(TRUSTED_SIGNERS_ENV, pubkey_env_value(&signing_key));
        std::env::remove_var(STORE_TOKEN_ENV);
    }

    // Script: call echo, then reply.
    let llm = Arc::new(RecordingLlmBackend::new(vec![
        Ok(call_echo_tool()),
        Ok(final_reply("verified-done")),
    ]));

    let mcp_source = Arc::new(McpToolSource::new(admin_server.uri(), "gtc_live_e2e"));
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
            "sess",
            "a",
            AgentInput {
                text: "e2e-go".into(),
                conversational: false,
            },
        )
        .await
        .expect("agent step must succeed on verified pull");

    // (a+b) wasm + sidecar written to cache.
    let wasm_in_cache = cache_dir.path().join("router_echo.wasm");
    let sidecar_in_cache = cache_dir.path().join("router_echo.wasm.sha256");
    assert!(
        wasm_in_cache.exists(),
        "lazy pull must write router_echo.wasm to cache"
    );
    assert!(
        sidecar_in_cache.exists(),
        "lazy pull must write the wasm digest sidecar"
    );

    // (c) Tool was offered to the LLM on the first turn.
    let offered = llm.offered.lock().unwrap();
    assert!(
        offered[0].contains(&("mcp:s1".to_string(), "echo".to_string())),
        "first turn must offer mcp:s1/echo after verified pull; offered: {:?}",
        offered[0]
    );

    // (d) Echo result landed in the trail without an error.
    let tool_result = output.trail.iter().find_map(|step| match step {
        AgentStep::ToolCall { name, result, .. } if name == "echo" => Some(result.clone()),
        _ => None,
    });
    assert!(
        tool_result.is_some(),
        "echo call must appear in trail after verified pull; trail: {:?}",
        output.trail
    );
    let result_value = tool_result.unwrap();
    assert!(
        !result_value.to_string().contains("\"error\""),
        "echo result must not contain an error; got: {result_value}"
    );

    // (e) Step terminated cleanly with the final reply.
    assert_eq!(output.terminated_by, TerminationReason::FinalReply);
    assert_eq!(output.reply, "verified-done");

    // Cleanup env vars.
    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }
}

/// Tampered artifact: the admin row carries a `component_digest` that does NOT
/// match the archive served by the store. The integrity check must fail, the
/// catalog must be empty (server skipped), and the agent step must complete
/// without panicking (degrade: LLM is offered no tools and produces a final
/// reply on the first turn).
#[tokio::test]
#[serial_test::serial]
async fn tampered_digest_degrades_tool_not_offered() {
    let Some(fixture_path) = resolve_fixture_wasm() else {
        eprintln!(
            "SKIP: router_echo.wasm not found; set GREENTIC_MCP_ROUTER_ECHO_WASM to run live"
        );
        return;
    };

    // Build a genuine signed archive.
    let wasm_bytes = std::fs::read(&fixture_path).expect("read fixture wasm");
    let signing_key = SigningKey::from_bytes(&[43u8; 32]);
    let signed_describe = sign_describe_like_store(&sample_describe(), &signing_key);
    let archive = build_gtxpack(&signed_describe, &wasm_bytes);

    // Stand up the store (serves the genuine archive).
    let store_server = MockServer::start().await;
    mount_store_artifact(&store_server, archive).await;

    // Admin row carries a WRONG digest — the store-pull will fail at the
    // integrity check and the server will be skipped.
    let wrong_digest = "d".repeat(64);
    let admin_server = MockServer::start().await;
    mount_admin_local_wasm_with_digest(&admin_server, &wrong_digest).await;

    // Empty cache dir.
    let cache_dir = tempfile::tempdir().expect("create temp cache dir");

    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache_dir.path());
        std::env::set_var(STORE_URL_ENV, store_server.uri());
        std::env::set_var(TRUSTED_SIGNERS_ENV, pubkey_env_value(&signing_key));
        std::env::remove_var(STORE_TOKEN_ENV);
    }

    // LLM: single final reply (no tool call scripted — catalog is empty).
    let llm = Arc::new(RecordingLlmBackend::new(vec![Ok(final_reply(
        "no-tools-ok",
    ))]));

    let mcp_source = Arc::new(McpToolSource::new(admin_server.uri(), "gtc_live_e2e"));
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
            "sess",
            "a",
            AgentInput {
                text: "e2e-go".into(),
                conversational: false,
            },
        )
        .await
        .expect("step must not panic on a tampered artifact; degraded catalog is fine");

    // Nothing must be cached.
    assert!(
        !cache_dir.path().join("router_echo.wasm").exists(),
        "a digest mismatch must leave the cache empty"
    );

    // No tools were offered to the LLM (catalog degraded to empty).
    let offered = llm.offered.lock().unwrap();
    assert!(
        offered[0].is_empty(),
        "tampered artifact must result in no tools offered; offered: {:?}",
        offered[0]
    );

    // Step still completes cleanly.
    assert_eq!(output.terminated_by, TerminationReason::FinalReply);
    assert_eq!(output.reply, "no-tools-ok");

    // Cleanup.
    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }
}
