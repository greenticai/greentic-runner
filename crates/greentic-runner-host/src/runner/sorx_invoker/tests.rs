//! Tests for [`super::http`] (env-configured `SorxHttpInvoker`) and
//! [`super::routed`] (per-SoR-routed `SorxRoutedInvoker`).

use greentic_aw_runtime::SorxInvoker;
use serde_json::json;
use wiremock::matchers::{body_partial_json, header, header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::http::fixtures::{
    caps_response_for_pack, caps_response_mixed, caps_response_one_business_action,
};
use super::http::{SorxHttpInvoker, parse_agent_endpoint_cap_uri};
use super::routed::SorxRoutedInvoker; // module is `pub(crate)`; see mod.rs
use crate::runner::sorla_route::test_secrets;

/// Mount a plain `200 GET /admin/v1/capabilities` response with `body` — the
/// shape every test below uses unless it also asserts on the request
/// (headers, `.expect()` counts), which stays written out in full.
async fn mount_caps_get(server: &MockServer, body: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path("/admin/v1/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

// ---- SorxHttpInvoker (GREENTIC_AW_SORX_URL env path) ----

#[tokio::test]
async fn fetch_surfaces_agent_endpoint_alongside_business_action() {
    let server = MockServer::start().await;
    mount_caps_get(&server, caps_response_mixed()).await;

    let invoker = SorxHttpInvoker::fetch(server.uri()).await;
    let mut ops = invoker.list_operations();
    ops.sort_by(|a, b| a.action.cmp(&b.action));

    assert_eq!(
        ops.len(),
        2,
        "business-action + agent-endpoint; event dropped"
    );
    // agent-endpoint op keyed by (pack, endpoint_id), cap stored verbatim.
    let ep = ops
        .iter()
        .find(|o| o.action == "tenants.create")
        .expect("agent-endpoint op");
    assert_eq!(ep.pack, "landlord");
    assert_eq!(
        ep.cap_uri,
        "cap://greentic/agent-endpoints/landlord/tenants.create/v0.1.0"
    );
    assert_eq!(ep.description, "Create a tenant record");
}

#[tokio::test]
async fn fetch_builds_one_op_from_business_action_offer() {
    let server = MockServer::start().await;
    mount_caps_get(&server, caps_response_one_business_action()).await;

    let invoker = SorxHttpInvoker::fetch(server.uri()).await;
    let ops = invoker.list_operations();

    assert_eq!(
        ops.len(),
        1,
        "the business-event offer must be filtered out"
    );
    let op = &ops[0];
    assert_eq!(op.pack, "landlord");
    assert_eq!(op.action, "record_rent_payment");
    assert_eq!(
        op.cap_uri,
        "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0"
    );
    assert_eq!(op.description, "Record a rent payment");
}

#[tokio::test]
async fn fetch_degrades_to_empty_ops_on_unreachable_server() {
    // A port nothing is listening on: `fetch` must never crash worker
    // startup, just log and return an empty-ops invoker.
    let invoker = SorxHttpInvoker::fetch("http://127.0.0.1:1".to_string()).await;
    assert!(invoker.list_operations().is_empty());
}

#[tokio::test]
async fn invoke_happy_path_returns_result_value() {
    let server = MockServer::start().await;
    mount_caps_get(&server, caps_response_one_business_action()).await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ok": true,
            "schema": "greentic.sorx.capability-invoke-result.v1",
            "capability": "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0",
            "status": "completed",
            "result": {"id": "pay-1"},
            "events": []
        })))
        .mount(&server)
        .await;

    let invoker = SorxHttpInvoker::fetch(server.uri()).await;
    let out = invoker
        .invoke("landlord", "record_rent_payment", "{}")
        .await
        .expect("invoke should succeed");
    assert_eq!(out, json!({"id": "pay-1"}));
}

#[tokio::test]
async fn invoke_202_approval_required_is_ok_not_err() {
    let server = MockServer::start().await;
    mount_caps_get(&server, caps_response_one_business_action()).await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "ok": false,
            "status": "approval_required",
            "capability": "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0",
            "approval": {"id": "appr-1", "state": "pending"}
        })))
        .mount(&server)
        .await;

    let invoker = SorxHttpInvoker::fetch(server.uri()).await;
    let out = invoker
        .invoke("landlord", "record_rent_payment", "{}")
        .await
        .expect("202 approval_required must be Ok, not Err");
    assert_eq!(out["status"], "approval_required");
    assert_eq!(out["approval"]["id"], "appr-1");
}

#[tokio::test]
async fn invoke_403_maps_to_denied_error_value() {
    let server = MockServer::start().await;
    mount_caps_get(&server, caps_response_one_business_action()).await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "ok": false,
            "error": {
                "code": "RUNTIME_CAPABILITY_DENIED",
                "message": "policy denied",
                "details": {}
            }
        })))
        .mount(&server)
        .await;

    let invoker = SorxHttpInvoker::fetch(server.uri()).await;
    let out = invoker
        .invoke("landlord", "record_rent_payment", "{}")
        .await
        .expect("403 must be a structured Ok value, not Err");
    assert_eq!(out["error"], "denied");
}

#[tokio::test]
async fn invoke_404_maps_to_capability_not_found_error_value() {
    let server = MockServer::start().await;
    mount_caps_get(&server, caps_response_one_business_action()).await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "ok": false,
            "error": {
                "code": "RUNTIME_CAPABILITY_NOT_FOUND",
                "message": "capability does not resolve to a business action",
                "details": {}
            }
        })))
        .mount(&server)
        .await;

    let invoker = SorxHttpInvoker::fetch(server.uri()).await;
    let out = invoker
        .invoke("landlord", "record_rent_payment", "{}")
        .await
        .expect("404 must be a structured Ok value, not Err");
    assert_eq!(out["error"], "capability_not_found");
}

#[tokio::test]
#[serial_test::serial]
async fn invoke_carries_tenant_caller_and_role_headers() {
    let server = MockServer::start().await;
    mount_caps_get(&server, caps_response_one_business_action()).await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .and(header("X-Greentic-Tenant-Id", "acme"))
        .and(header("X-Greentic-Caller-Id", "dw-agent"))
        .and(header("X-Greentic-Caller-Role", "agent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ok": true,
            "result": {}
        })))
        .expect(1)
        .mount(&server)
        .await;

    // SAFETY: single-threaded env mutation local to this test; no other
    // test reads GREENTIC_AW_SORX_TENANT concurrently (crate convention:
    // env-mutating tests run `#[serial]` or use process-unique vars).
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("GREENTIC_AW_SORX_TENANT", "acme");
    }
    let invoker = SorxHttpInvoker::fetch(server.uri()).await;
    let result = invoker
        .invoke("landlord", "record_rent_payment", "{}")
        .await;
    #[allow(unsafe_code)]
    unsafe {
        std::env::remove_var("GREENTIC_AW_SORX_TENANT");
    }
    result.expect("invoke should succeed; header expectation is checked by wiremock's .expect(1)");
}

#[tokio::test]
async fn invoke_unknown_pack_action_is_err() {
    let invoker = SorxHttpInvoker::fetch("http://127.0.0.1:1".to_string()).await;
    let err = invoker
        .invoke("nope", "nope", "{}")
        .await
        .expect_err("no capability registered => Err");
    assert!(err.contains("no capability"));
}

#[tokio::test]
async fn invoke_dispatches_agent_endpoint_cap_verbatim_and_parses_result() {
    let server = MockServer::start().await;
    mount_caps_get(&server, caps_response_mixed()).await;
    // The invoke MUST carry the agent-endpoint cap URI verbatim in the body.
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .and(body_partial_json(json!({
            "capability": "cap://greentic/agent-endpoints/landlord/tenants.create/v0.1.0"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ok": true,
            "schema": "greentic.sorx.agent-endpoint-invoke-result.v1",
            "result": {"tenant_id": "t-1"},
            "events": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    let invoker = SorxHttpInvoker::fetch(server.uri()).await;
    let out = invoker
        .invoke("landlord", "tenants.create", "{}")
        .await
        .expect("agent-endpoint invoke should succeed");
    assert_eq!(out, json!({"tenant_id": "t-1"}));
}

#[test]
fn parses_agent_endpoint_cap_uri_and_rejects_wrong_namespace() {
    assert_eq!(
        parse_agent_endpoint_cap_uri(
            "cap://greentic/agent-endpoints/landlord/tenants.create/v0.1.0"
        ),
        Some(("landlord".to_string(), "tenants.create".to_string()))
    );
    // Wrong namespace (business-functions) → None.
    assert_eq!(
        parse_agent_endpoint_cap_uri(
            "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0"
        ),
        None
    );
    // Missing trailing version segment → None.
    assert_eq!(
        parse_agent_endpoint_cap_uri("cap://greentic/agent-endpoints/landlord/tenants.create"),
        None
    );
    // Empty pack/id → None.
    assert_eq!(
        parse_agent_endpoint_cap_uri("cap://greentic/agent-endpoints//tenants.create/v0.1.0"),
        None
    );
    // Missing scheme prefix → None.
    assert_eq!(
        parse_agent_endpoint_cap_uri("greentic/agent-endpoints/a/b/v1"),
        None
    );
}

// ---- SorxRoutedInvoker (per-SoR routed) ----

/// A `secrets://default/acme/_/sorla/<sor>` document body for `url`, with an
/// optional bearer token.
fn route_doc(url: &str, token: Option<&str>) -> String {
    match token {
        Some(t) => format!(r#"{{"url":"{url}","token":"{t}","tenant":"acme"}}"#),
        None => format!(r#"{{"url":"{url}","tenant":"acme"}}"#),
    }
}

/// Build a [`SorxRoutedInvoker`] over `sors` (each `(sor, url, token)`),
/// under tenant `"acme"`. Returns the backing secrets fake too, so a test
/// can rewrite a route after construction (see
/// `invoke_rereads_the_route_on_every_call`).
async fn routed_invoker(
    sors: &[(&str, &str, Option<&str>)],
) -> (test_secrets::Handle, SorxRoutedInvoker) {
    let entries: Vec<(String, String)> = sors
        .iter()
        .map(|(sor, url, token)| {
            (
                format!("secrets://default/acme/_/sorla/{sor}"),
                route_doc(url, *token),
            )
        })
        .collect();
    let refs: Vec<(&str, &str)> = entries
        .iter()
        .map(|(u, b)| (u.as_str(), b.as_str()))
        .collect();
    let secrets = test_secrets::Handle::with(&refs);
    let names = sors.iter().map(|(sor, _, _)| sor.to_string()).collect();
    let inv =
        SorxRoutedInvoker::discover_all(names, secrets.manager(), "acme".to_string(), None).await;
    (secrets, inv)
}

#[tokio::test]
async fn no_token_sends_no_authorization_header() {
    let server = MockServer::start().await;
    mount_caps_get(&server, caps_response_one_business_action()).await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .and(header_exists("authorization"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .and(header("X-Greentic-Tenant-Id", "acme"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"ok": true, "result": {"id": "r1"}})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let (_secrets, inv) = routed_invoker(&[("landlord", &server.uri(), None)]).await;
    let out = inv
        .invoke("landlord", "record_rent_payment", "{}")
        .await
        .expect("ok");
    assert_eq!(out, json!({"id": "r1"}));
}

#[tokio::test]
async fn a_token_is_sent_as_bearer_on_discovery_and_invoke() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v1/capabilities"))
        .and(header("authorization", "Bearer t0k"))
        .respond_with(ResponseTemplate::new(200).set_body_json(caps_response_one_business_action()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .and(header("authorization", "Bearer t0k"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true, "result": {}})))
        .expect(1)
        .mount(&server)
        .await;

    let (_secrets, inv) = routed_invoker(&[("landlord", &server.uri(), Some("t0k"))]).await;
    inv.invoke("landlord", "record_rent_payment", "{}")
        .await
        .expect("ok");
}

#[tokio::test]
async fn two_sors_discover_and_invoke_independently() {
    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;

    mount_caps_get(&server_a, caps_response_one_business_action()).await;
    mount_caps_get(&server_b, caps_response_for_pack("billing")).await;

    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"ok": true, "result": {"from": "landlord"}})),
        )
        .expect(1)
        .mount(&server_a)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"ok": true, "result": {"from": "billing"}})),
        )
        .expect(1)
        .mount(&server_b)
        .await;

    let (_secrets, inv) = routed_invoker(&[
        ("landlord", &server_a.uri(), None),
        ("billing", &server_b.uri(), None),
    ])
    .await;

    let packs: std::collections::HashSet<_> = inv
        .list_operations()
        .into_iter()
        .map(|op| op.pack)
        .collect();
    assert!(packs.contains("landlord"));
    assert!(packs.contains("billing"));

    let from_landlord = inv
        .invoke("landlord", "record_rent_payment", "{}")
        .await
        .expect("landlord invoke should reach server_a");
    assert_eq!(from_landlord, json!({"from": "landlord"}));

    let from_billing = inv
        .invoke("billing", "record_rent_payment", "{}")
        .await
        .expect("billing invoke should reach server_b");
    assert_eq!(from_billing, json!({"from": "billing"}));
}

#[tokio::test]
async fn invoke_rereads_the_route_on_every_call() {
    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;

    mount_caps_get(&server_a, caps_response_one_business_action()).await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true, "result": {}})))
        .expect(0)
        .mount(&server_a)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true, "result": {}})))
        .expect(1)
        .mount(&server_b)
        .await;

    let (secrets, inv) = routed_invoker(&[("landlord", &server_a.uri(), None)]).await;

    // The sorx child restarted on a new port: rewrite the route document in
    // the shared secrets fake without rebuilding the invoker.
    secrets.set(
        "secrets://default/acme/_/sorla/landlord",
        &route_doc(&server_b.uri(), None),
    );

    inv.invoke("landlord", "record_rent_payment", "{}")
        .await
        .expect("invoke should reach the rewritten route, not the discovery-time one");
}

#[tokio::test]
async fn a_401_is_sorla_unauthorized_without_the_body() {
    let server = MockServer::start().await;
    mount_caps_get(&server, caps_response_one_business_action()).await;
    Mock::given(method("POST"))
        .and(path("/admin/v1/capabilities/invoke"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({"secret": "leak"})))
        .mount(&server)
        .await;

    let (_secrets, inv) = routed_invoker(&[("landlord", &server.uri(), None)]).await;
    let err = inv
        .invoke("landlord", "record_rent_payment", "{}")
        .await
        .expect_err("401 must be Err");
    assert!(err.contains("sorla_unauthorized"));
    assert!(!err.contains("leak"));
}
