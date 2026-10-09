//! Dispatch-path, parse_rows unit tests, and local-wasm integration tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use secrecy::SecretString;
use serde_json::json;
use wiremock::MockServer;

use crate::mcp_source::source::{dispatch_route, parse_rows};
use crate::mcp_source::types::{McpRoute, Transport};

use super::catalog::{mount_admin, tenant};

#[tokio::test]
async fn route_debug_redacts_token() {
    let route = McpRoute {
        server_id: "s1".to_string(),
        transport_url: "https://mcp.example.com/".to_string(),
        auth_header_name: None,
        auth_token: Some(SecretString::from("tok-secret".to_string())),
        raw_tool_name: "get_issue".to_string(),
        transport: Transport::Http,
        component_ref: None,
        component_version: None,
        component_digest: None,
    };
    let dbg = format!("{route:?}");
    assert!(!dbg.contains("tok-secret"), "got: {dbg}");
    assert!(dbg.contains("[REDACTED]"), "got: {dbg}");
}

/// `from_parts` is a SECOND way to get a token into an `McpRoute` — a
/// pack-carried route resolves one from the secrets backend instead of an
/// admin row. A route built this way is logged on exactly the same paths, so
/// the redaction has to cover it too; the struct-literal test above cannot
/// catch a constructor that bypasses it.
#[test]
fn route_from_parts_redacts_its_token_in_debug() {
    let route = McpRoute::from_parts(
        "s1",
        "https://mcp.example.com/",
        Some("Authorization"),
        Some("tok-secret"),
        "http",
        None,
        None,
        None,
    )
    .with_tool("get_issue");

    let dbg = format!("{route:?}");
    assert!(!dbg.contains("tok-secret"), "got: {dbg}");
    assert!(dbg.contains("[REDACTED]"), "got: {dbg}");
    assert!(matches!(route.transport, Transport::Http));
}

/// The transport discriminator is a string on the wire; `local-wasm` is the
/// only value that is not HTTP, and getting it wrong would dial an empty URL.
#[test]
fn route_from_parts_maps_the_local_wasm_transport() {
    let route = McpRoute::from_parts(
        "s1",
        "",
        None,
        None,
        "local-wasm",
        Some("router_echo"),
        Some("1.0.0"),
        Some("sha256:abc"),
    );
    assert!(matches!(route.transport, Transport::LocalWasm));
    assert_eq!(route.component_digest.as_deref(), Some("sha256:abc"));
}

#[test]
fn parse_rows_defaults_transport_to_http_and_reads_local_wasm() {
    let body = json!({"servers": [
        { "id": "h", "name": "H", "transport_url": "https://x/", "auth_header_name": null,
          "auth_token": null, "allowed_tools": null, "roles": ["agentic_worker"] },
        { "id": "l", "name": "L", "transport_url": "", "auth_header_name": null,
          "auth_token": null, "allowed_tools": null, "roles": ["agentic_worker"],
          "transport": "local-wasm", "component_ref": "weather.component",
          "component_version": "1.0.0" }
    ]});
    let rows = parse_rows(body).expect("parse");
    assert!(matches!(rows[0].transport, Transport::Http));
    assert!(matches!(rows[1].transport, Transport::LocalWasm));
    assert_eq!(rows[1].component_ref.as_deref(), Some("weather.component"));
}

/// Regression: the admin serializes `transport_url: null` for local-wasm
/// servers. The whole `WireBody` must still deserialize (a bare `String`
/// `transport_url` rejected `null`, dropping ALL of a tenant's MCP tools the
/// moment one local-wasm server was registered). The http row alongside it must
/// survive too.
#[test]
fn parse_rows_accepts_null_transport_url_for_local_wasm() {
    let body = json!({"servers": [
        { "id": "h", "name": "H", "transport_url": "https://x/", "auth_header_name": null,
          "auth_token": null, "allowed_tools": null, "roles": ["agentic_worker"] },
        { "id": "l", "name": "L", "transport_url": null, "auth_header_name": null,
          "auth_token": null, "allowed_tools": null, "roles": ["agentic_worker"],
          "transport": "local-wasm", "component_ref": "weather.component",
          "component_version": "1.0.0" }
    ]});
    let rows = parse_rows(body).expect("null transport_url must not break the list");
    assert_eq!(
        rows.len(),
        2,
        "the http row must survive alongside the local-wasm row"
    );
    assert_eq!(rows[0].transport_url, "https://x/");
    assert!(matches!(rows[1].transport, Transport::LocalWasm));
    assert_eq!(rows[1].transport_url, "", "null maps to empty string");
}

#[test]
fn parse_rows_carries_component_version_and_digest() {
    let body = json!({"servers": [
        { "id": "h", "name": "H", "transport_url": "https://x/", "auth_header_name": null,
          "auth_token": null, "allowed_tools": null, "roles": ["agentic_worker"] },
        { "id": "l", "name": "L", "transport_url": "", "auth_header_name": null,
          "auth_token": null, "allowed_tools": null, "roles": ["agentic_worker"],
          "transport": "local-wasm", "component_ref": "weather.component",
          "component_version": "1.0.0",
          "component_digest": "abc123deadbeef" }
    ]});
    let rows = parse_rows(body).expect("parse");
    // HTTP row: both fields absent
    assert!(rows[0].component_version.is_none());
    assert!(rows[0].component_digest.is_none());
    // local-wasm row: both fields present
    assert_eq!(rows[1].component_version.as_deref(), Some("1.0.0"));
    assert_eq!(rows[1].component_digest.as_deref(), Some("abc123deadbeef"));
}

/// End-to-end local-wasm path: admin row with `transport: "local-wasm"` →
/// catalog probe via `mcp_local::local_list_tools` → dispatch via
/// `mcp_local::local_call_tool` — all in-process, no network.
///
/// This is the Phase-1 / operator-seeded path: the wasm is already in cache,
/// no store pull. Self-skips when the router_echo fixture is absent so the
/// test never blocks CI that hasn't built the WASM target. To run live:
///   `GREENTIC_MCP_ROUTER_ECHO_WASM=<path> cargo test -p greentic-aw-runtime mcp_source`
#[allow(unsafe_code)]
#[tokio::test]
#[serial_test::serial]
async fn local_wasm_server_lists_and_dispatches_in_process() {
    use crate::mcp_source::source::McpToolSource;

    // Resolve the fixture using the same strategy as mcp_local::fixture_wasm.
    let src = std::env::var("GREENTIC_MCP_ROUTER_ECHO_WASM")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../greentic-mcp/target/wasm32-wasip2/release/router_echo.wasm")
        });
    if !src.exists() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    // Safety: serial attribute ensures no concurrent env-var mutation with
    // other tests that share GREENTIC_MCP_LOCAL_CACHE_DIR.
    unsafe { std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", dir.path()) };
    // Pre-seed the cache (operator-seeded path, no store pull, no sidecar).
    std::fs::copy(&src, dir.path().join("router_echo.wasm")).unwrap();

    // Build a fake admin row that carries version + digest; since the wasm
    // is already in cache the store-pull no-ops (dest exists) so the store
    // URL is never called. The digest here is the gtxpack digest that
    // would be checked during a real pull — moot for a cache-hit path.
    let admin = MockServer::start().await;
    let body = json!({ "servers": [{
        "id": "local", "name": "Local", "transport_url": "",
        "auth_header_name": null, "auth_token": null, "allowed_tools": null,
        "roles": ["agentic_worker"], "transport": "local-wasm",
        "component_ref": "router_echo", "component_version": "1.0.0",
        "component_digest": "0000000000000000000000000000000000000000000000000000000000000000"
    }]});
    mount_admin(&admin, body).await;

    let source = McpToolSource::new(admin.uri(), "gtc_live_x");
    let catalog = source.catalog(&tenant()).await;
    assert!(
        catalog.tool_entry("local", "echo").is_some(),
        "echo tool must be listed"
    );

    let route = catalog
        .route("local", "echo")
        .expect("route for echo must exist");
    let scope =
        crate::mcp_scope::McpCallScope::new(crate::tenant::TenantContext::new("acme", "prod"));
    let out = dispatch_route(route, "{\"message\":\"hi\"}", &scope).await;
    assert!(
        !out.to_string().contains("\"error\""),
        "dispatch must succeed; got: {out}"
    );
}

/// Task 3 TDD test: admin row carries `component_version` + `component_digest`
/// but the cache starts empty. The lazy pull path must download the `.gtxpack`
/// from a wiremock store, verify it, extract the wasm, write the sidecar, and
/// then list + dispatch successfully.
///
/// Also proves the degrade contract: a wrong `component_digest` → empty
/// catalog (the server is skipped).
///
/// Self-skips when the `router_echo` fixture wasm is absent.
#[allow(unsafe_code)]
#[tokio::test]
#[serial_test::serial]
async fn lazy_pull_on_catalog_miss_and_dispatch() {
    use crate::mcp_source::source::McpToolSource;
    use crate::mcp_store_pull::{
        STORE_TOKEN_ENV, STORE_URL_ENV, TRUSTED_SIGNERS_ENV,
        fixtures::{
            build_gtxpack, fixture_wasm, hex_sha256, pubkey_env_value, sample_describe,
            sign_describe_like_store,
        },
    };
    use ed25519_dalek::SigningKey;
    use wiremock::matchers::{method, path as wm_path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let Some(wasm_src) = fixture_wasm() else {
        return; // self-skip: fixture not built
    };
    let wasm_bytes = std::fs::read(&wasm_src).unwrap();

    // Build a signed .gtxpack archive.
    let signing_key = SigningKey::from_bytes(&[20u8; 32]);
    let signed_describe = sign_describe_like_store(&sample_describe(), &signing_key);
    let archive = build_gtxpack(&signed_describe, &wasm_bytes);
    let gtxpack_digest = hex_sha256(&archive);

    // Stand up a mock store for the artifact endpoint.
    let store_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wm_path("/api/v1/extensions/router_echo/1.0.0/artifact"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(archive.clone()),
        )
        .mount(&store_server)
        .await;

    // Empty cache directory — no wasm pre-seeded.
    let cache_dir = tempfile::tempdir().unwrap();

    // Safety: serial ensures exclusive env-var ownership.
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache_dir.path());
        std::env::set_var(STORE_URL_ENV, store_server.uri());
        std::env::set_var(TRUSTED_SIGNERS_ENV, pubkey_env_value(&signing_key));
        std::env::remove_var(STORE_TOKEN_ENV);
    }

    // Admin row: transport=local-wasm, version+digest present, NO pre-seeded cache.
    let admin = MockServer::start().await;
    let body = json!({ "servers": [{
        "id": "local", "name": "Local", "transport_url": "",
        "auth_header_name": null, "auth_token": null, "allowed_tools": null,
        "roles": ["agentic_worker"], "transport": "local-wasm",
        "component_ref": "router_echo", "component_version": "1.0.0",
        "component_digest": gtxpack_digest
    }]});
    mount_admin(&admin, body).await;

    let source = McpToolSource::new(admin.uri(), "gtc_live_x");
    let catalog = source.catalog(&tenant()).await;

    // After lazy pull: wasm must be cached, sidecar written, echo tool listed.
    let wasm_in_cache = cache_dir.path().join("router_echo.wasm");
    let sidecar_in_cache = cache_dir.path().join("router_echo.wasm.sha256");
    assert!(
        wasm_in_cache.exists(),
        "wasm must be in cache after lazy pull"
    );
    assert!(
        sidecar_in_cache.exists(),
        "wasm digest sidecar must be written"
    );
    assert!(
        catalog.tool_entry("local", "echo").is_some(),
        "echo tool must be listed after lazy pull"
    );

    // Dispatch must succeed (lazy pull no-ops on cache hit, sidecar pins digest).
    let route = catalog.route("local", "echo").expect("route for echo");
    let scope =
        crate::mcp_scope::McpCallScope::new(crate::tenant::TenantContext::new("acme", "prod"));
    let out = dispatch_route(route, "{\"message\":\"world\"}", &scope).await;
    assert!(
        !out.to_string().contains("\"error\""),
        "dispatch must succeed after lazy pull; got: {out}"
    );

    // --- Wrong-digest degrade test ---
    // Remove the cache so a fresh pull is attempted, but serve the store
    // again (same server, fresh mount needed since MockServer is consumed).
    std::fs::remove_file(&wasm_in_cache).unwrap();
    std::fs::remove_file(&sidecar_in_cache).unwrap();

    let store_server2 = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wm_path("/api/v1/extensions/router_echo/1.0.0/artifact"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(archive),
        )
        .mount(&store_server2)
        .await;
    unsafe { std::env::set_var(STORE_URL_ENV, store_server2.uri()) };

    let admin2 = MockServer::start().await;
    let wrong_digest = "f".repeat(64); // definitely wrong
    let body2 = json!({ "servers": [{
        "id": "local", "name": "Local", "transport_url": "",
        "auth_header_name": null, "auth_token": null, "allowed_tools": null,
        "roles": ["agentic_worker"], "transport": "local-wasm",
        "component_ref": "router_echo", "component_version": "1.0.0",
        "component_digest": wrong_digest
    }]});
    mount_admin(&admin2, body2).await;

    let source2 = McpToolSource::new(admin2.uri(), "gtc_live_x");
    let catalog2 = source2.catalog(&tenant()).await;
    // Integrity check fails → server skipped → empty catalog.
    assert!(
        catalog2.is_empty(),
        "wrong component_digest must degrade to empty catalog"
    );

    // Cleanup env vars.
    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }
}

/// Dispatch-path degrade (Fix A from Task 4 review):
/// a `local-wasm` route whose store-pull fails at `call_route`/`dispatch_route`
/// time (wrong `component_digest`, empty cache) must return a JSON value
/// containing `"error"` and must NEVER panic. This complements the list-path
/// degrade already covered by `lazy_pull_on_catalog_miss_and_dispatch`.
///
/// Builds a route directly (bypassing catalog build) so we can simulate
/// a persisted/replayed route with a mismatched digest — the same scenario
/// that can arise when a session snapshot is replayed after an artifact is
/// re-published with a new digest.
///
/// Self-skips when the `router_echo` fixture wasm is absent.
#[allow(unsafe_code)]
#[tokio::test]
#[serial_test::serial]
async fn dispatch_route_local_wasm_wrong_digest_returns_error_not_panic() {
    use crate::mcp_store_pull::{
        STORE_TOKEN_ENV, STORE_URL_ENV, TRUSTED_SIGNERS_ENV,
        fixtures::{build_gtxpack, pubkey_env_value, sample_describe, sign_describe_like_store},
    };
    use ed25519_dalek::SigningKey;
    use wiremock::matchers::{method, path as wm_path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // The wrong-digest failure fires at the archive-integrity check (sha256
    // of the downloaded bytes != pinned digest) BEFORE the wasm is ever
    // unzipped/executed — so any non-empty bytes serve as the "wasm" entry
    // and this test needs no built wasm fixture (always runs in CI).
    let wasm_bytes = b"not-a-real-wasm-component".to_vec();

    // Build a valid signed archive but tell the route a WRONG digest.
    // The store-pull will download it, compute the real digest, compare
    // against the wrong pinned digest, and fail with Integrity error.
    let signing_key = SigningKey::from_bytes(&[30u8; 32]);
    let signed_describe = sign_describe_like_store(&sample_describe(), &signing_key);
    let archive = build_gtxpack(&signed_describe, &wasm_bytes);
    // deliberately wrong digest (not the real sha256 of archive)
    let wrong_digest = "e".repeat(64);

    // Stand up a mock store so the download itself succeeds (the integrity
    // check fires after the body is received).
    let store_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wm_path("/api/v1/extensions/router_echo/1.0.0/artifact"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(archive),
        )
        .mount(&store_server)
        .await;

    // Empty cache dir so ensure_cached must actually attempt the pull.
    let cache_dir = tempfile::tempdir().unwrap();

    // Safety: serial ensures exclusive env-var access.
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache_dir.path());
        std::env::set_var(STORE_URL_ENV, store_server.uri());
        std::env::set_var(TRUSTED_SIGNERS_ENV, pubkey_env_value(&signing_key));
        std::env::remove_var(STORE_TOKEN_ENV);
    }

    // Build a `local-wasm` route directly with the wrong digest.
    // This represents a persisted/replayed route whose digest pin has become
    // stale — dispatch_route must degrade, not panic.
    let route = McpRoute {
        server_id: "local".to_string(),
        transport_url: String::new(),
        auth_header_name: None,
        auth_token: None,
        raw_tool_name: "echo".to_string(),
        transport: Transport::LocalWasm,
        component_ref: Some("router_echo".to_string()),
        component_version: Some("1.0.0".to_string()),
        component_digest: Some(wrong_digest),
    };

    let scope =
        crate::mcp_scope::McpCallScope::new(crate::tenant::TenantContext::new("acme", "prod"));
    let result = dispatch_route(&route, r#"{"message":"degrade-test"}"#, &scope).await;

    // Clean up before asserting (to avoid leaking env state on failure).
    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }

    // Must return a JSON object with an "error" key — never panics.
    assert!(
        result.to_string().contains("\"error\""),
        "dispatch_route with wrong digest must degrade to {{\"error\": ...}}; got: {result}"
    );
    // Nothing leaked into the cache.
    assert!(
        !cache_dir.path().join("router_echo.wasm").exists(),
        "a digest mismatch during dispatch must leave the cache empty"
    );
}

/// A tool that takes longer than the catalog PROBE budget must still be allowed
/// to finish.
///
/// One 5s constant used to govern both "is this server alive?" and "has this
/// tool finished its work?". Those are different questions with different
/// budgets: a liveness probe should fail fast so a dead server is skipped
/// quickly, while real work — a desktop agent driving an application to produce
/// a quote, say — routinely takes tens of seconds. Sharing the constant made
/// every such tool permanently impossible to call, reported as
/// `tool call timed out after 5s`.
///
/// The delay here is deliberately longer than the probe budget and far shorter
/// than the call budget, so this fails if the two are ever collapsed back into
/// one constant.
#[tokio::test]
async fn a_slow_tool_call_is_not_cut_off_by_the_probe_budget() {
    use std::time::Duration;
    use wiremock::matchers::{body_partial_json, method};
    use wiremock::{Mock, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(super::catalog::initialize_ok())
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
        .and(body_partial_json(json!({ "method": "tools/call" })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(6))
                .set_body_json(json!({
                    "jsonrpc": "2.0", "id": 3,
                    "result": { "structuredContent": { "quote": "ok" } }
                })),
        )
        .mount(&server)
        .await;

    let route = crate::mcp_source::route_for_tests("srv", "create_quote", &server.uri());
    let scope =
        crate::mcp_scope::McpCallScope::new(crate::tenant::TenantContext::new("acme", "prod"));
    let out = dispatch_route(&route, "{}", &scope).await;

    assert!(
        !out.to_string().contains("timed out"),
        "a 6s tool call must not be cut off by the 5s probe budget; got: {out}"
    );
}

/// The call-budget override is parsed defensively: anything unusable falls back
/// to the default rather than being honoured.
///
/// A zero is the case worth stating. Honouring it literally would time out
/// every tool call instantly — reintroducing, via a typo in a knob, exactly the
/// "no MCP tool can ever run" failure this split exists to remove.
#[test]
fn an_unusable_call_timeout_override_falls_back_to_the_default() {
    use crate::mcp_source::types::{DEFAULT_CALL_TIMEOUT, call_timeout_from};
    use std::time::Duration;

    assert_eq!(call_timeout_from(Some("300")), Duration::from_secs(300));
    assert_eq!(call_timeout_from(Some("  300  ")), Duration::from_secs(300));

    for unusable in [
        None,
        Some(""),
        Some("abc"),
        Some("-5"),
        Some("0"),
        Some("1.5"),
    ] {
        assert_eq!(
            call_timeout_from(unusable),
            DEFAULT_CALL_TIMEOUT,
            "unusable override {unusable:?} must fall back to the default"
        );
    }
}
