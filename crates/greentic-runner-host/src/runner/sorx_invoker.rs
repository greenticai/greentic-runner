//! Runner-host implementation of `greentic_aw_runtime::SorxInvoker`.
//!
//! `SorxHttpInvoker` is the HTTP-bearing half of the `sorla:` tool seam: it
//! fetches a deployed SoR's BusinessAction capabilities once (`GET
//! {base}/admin/v1/capabilities`) and dispatches tool calls to the SoR's
//! capability-invoke endpoint (`POST {base}/admin/v1/capabilities/invoke`).
//! The aw-runtime half (`SorlaToolSource`/`SorlaToolCatalog`) depends only on
//! the `SorxInvoker` trait + JSON, so this is where `reqwest` enters.

#![cfg(feature = "agentic-worker")]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use greentic_aw_runtime::{SorxInvoker, SorxOperation};
use serde_json::{Value, json};

/// The `contracts` entry a SoRX capability offer carries when it is a
/// BusinessAction reachable via `POST /admin/v1/capabilities/invoke` (as
/// opposed to a business-event topic offer, which this invoker ignores).
const BUSINESS_ACTION_CONTRACT: &str = "greentic.sorx.business-action.invoke.v1";

/// The `contracts` entry a SoRX capability offer carries when it is a SoRLa
/// agent-endpoint reachable via `POST /admin/v1/capabilities/invoke` (added
/// to the capability surface in greentic-sorx #60). Distinct from
/// [`BUSINESS_ACTION_CONTRACT`]; both are dispatched through the same invoke
/// route with the cap URI sent verbatim.
const AGENT_ENDPOINT_CONTRACT: &str = "greentic.sorx.agent-endpoint.invoke.v1";

/// Caller identity stamped on every capability invocation. SP1 has no
/// per-agent caller identity to thread through `SorxInvoker::invoke` (the
/// trait carries only `(pack, action, args_json)`), so a fixed identity is
/// used; the deployed worker path can carry a real caller/tenant once the
/// trait grows a context parameter.
const SORLA_CALLER_ID: &str = "dw-agent";
const SORLA_CALLER_ROLE: &str = "agent";

/// Tenant stamped on every capability invocation. `SorxInvoker::invoke` has
/// no tenant parameter (SP1 scope), so this reads a fixed env var —
/// `GREENTIC_AW_SORX_TENANT` — falling back to `"default"` when unset.
fn sorx_tenant_from_env() -> String {
    std::env::var("GREENTIC_AW_SORX_TENANT")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

/// The `X-Greentic-*` headers every SoRX admin-capability request carries
/// (mirrors `operax_core::OperaxContext::sorx_headers`). Centralized here so
/// header names/values can't drift between the invoke request and any future
/// caller.
fn sorla_headers(tenant: &str) -> [(&'static str, String); 3] {
    [
        ("X-Greentic-Tenant-Id", tenant.to_string()),
        ("X-Greentic-Caller-Id", SORLA_CALLER_ID.to_string()),
        ("X-Greentic-Caller-Role", SORLA_CALLER_ROLE.to_string()),
    ]
}

/// Parse a SoRX capability URI `cap://greentic/<kind>/<pack>/<id>/v<version>`
/// into its `(pack, id)` identity, requiring the namespace `greentic` and the
/// given `kind` segment. `None` on any shape mismatch — never panics.
fn parse_cap_uri(cap_uri: &str, kind: &str) -> Option<(String, String)> {
    let rest = cap_uri.strip_prefix("cap://")?;
    let segments: Vec<&str> = rest.split('/').collect();
    let [namespace, uri_kind, pack, id, _version] = segments[..] else {
        return None;
    };
    if namespace != "greentic" || uri_kind != kind {
        return None;
    }
    if pack.is_empty() || id.is_empty() {
        return None;
    }
    Some((pack.to_string(), id.to_string()))
}

/// Parse a BusinessAction capability URI
/// (`cap://greentic/business-functions/<pack>/<action>/v<version>`).
fn parse_business_action_cap_uri(cap_uri: &str) -> Option<(String, String)> {
    parse_cap_uri(cap_uri, "business-functions")
}

/// Parse an agent-endpoint capability URI
/// (`cap://greentic/agent-endpoints/<pack>/<endpoint_id>/v<version>`).
fn parse_agent_endpoint_cap_uri(cap_uri: &str) -> Option<(String, String)> {
    parse_cap_uri(cap_uri, "agent-endpoints")
}

/// LLM-facing description for one SoR BusinessAction. Prefers the offer's
/// `metadata.action.label`; falls back to a generated description so a label
/// omission never blanks the tool out of the catalog.
fn describe_business_action(metadata: &Value, pack: &str, action: &str) -> String {
    metadata
        .pointer("/action/label")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("Invoke SoR business action '{action}' of pack '{pack}'."))
}

/// LLM-facing description for one SoR agent-endpoint. Prefers the offer's
/// `metadata.endpoint.intent`, then `title`; falls back to a generated
/// description so an omission never blanks the tool out of the catalog.
fn describe_agent_endpoint(metadata: &Value, pack: &str, id: &str) -> String {
    metadata
        .pointer("/endpoint/intent")
        .or_else(|| metadata.pointer("/endpoint/title"))
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("Invoke SoR agent endpoint '{id}' of pack '{pack}'."))
}

/// LLM-facing JSON-schema parameters for one SoR offer — BusinessAction or
/// agent-endpoint alike (namespace-neutral: nothing here depends on which
/// contract the offer carries). SP1's capability offers carry no explicit
/// input-schema field (see design doc); this checks the couple of plausible
/// slots first so a future SoRX release that adds one is picked up
/// automatically, then falls back to an unconstrained object schema.
fn business_action_parameters(offer: &Value, metadata: &Value) -> Value {
    metadata
        .pointer("/execution/input_schema")
        .or_else(|| metadata.get("input_schema"))
        .or_else(|| offer.get("input_schema"))
        .cloned()
        .unwrap_or_else(|| json!({ "type": "object" }))
}

/// `SorxInvoker` backed by a deployed SoR's SoRX capability admin API.
///
/// Capability discovery is a one-time fetch at construction ([`Self::fetch`])
/// — `list_operations` is sync per the [`SorxInvoker`] trait, so the captured
/// `ops`/`cap_by_key` are simply returned/looked-up, never re-fetched. A
/// down/unreachable SoR degrades to an empty operation set (logged) rather
/// than failing worker startup.
pub(crate) struct SorxHttpInvoker {
    base_url: String,
    ops: Vec<SorxOperation>,
    cap_by_key: HashMap<(String, String), String>,
}

impl SorxHttpInvoker {
    /// Fetch `GET {base_url}/admin/v1/capabilities` once, keep the
    /// BusinessAction offers (those whose `contracts` include
    /// [`BUSINESS_ACTION_CONTRACT`]) and the agent-endpoint offers (those
    /// whose `contracts` include [`AGENT_ENDPOINT_CONTRACT`]), and build the
    /// `ops`/`cap_by_key` this invoker serves. Any fetch/parse failure (down
    /// SoR, network error, malformed body) is logged and yields an invoker
    /// with empty ops rather than propagating — a down SoR must never crash
    /// worker startup.
    pub(crate) async fn fetch(base_url: String) -> Self {
        let url = format!("{}/admin/v1/capabilities", base_url.trim_end_matches('/'));
        let (ops, cap_by_key) = match reqwest::get(&url).await {
            Ok(resp) => match resp.json::<Value>().await {
                Ok(body) => Self::parse_capabilities(&body),
                Err(err) => {
                    tracing::warn!(
                        base_url = %base_url,
                        error = %err,
                        "sorla: failed to parse SoRX capabilities response; starting with an empty tool set"
                    );
                    (Vec::new(), HashMap::new())
                }
            },
            Err(err) => {
                tracing::warn!(
                    base_url = %base_url,
                    error = %err,
                    "sorla: failed to reach SoRX capabilities endpoint; starting with an empty tool set"
                );
                (Vec::new(), HashMap::new())
            }
        };
        tracing::info!(
            base_url = %base_url,
            ops = ops.len(),
            "sorla: SoRX tool source constructed"
        );
        Self {
            base_url,
            ops,
            cap_by_key,
        }
    }

    /// Extract `(ops, cap_by_key)` from a parsed `GET /admin/v1/capabilities`
    /// body, keeping both BusinessAction and agent-endpoint offers — each
    /// with a well-formed `cap://` capability URI of its own kind
    /// (`business-functions`/`agent-endpoints` respectively). Any other
    /// contract (business-event topics, unknown kinds) is dropped.
    /// Malformed/unparsable offers are skipped (logged), not fatal.
    fn parse_capabilities(body: &Value) -> (Vec<SorxOperation>, HashMap<(String, String), String>) {
        let mut ops = Vec::new();
        let mut cap_by_key = HashMap::new();
        let offers = body
            .get("offers")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for offer in offers {
            let contracts = offer.get("contracts").and_then(Value::as_array);
            let has =
                |c: &str| contracts.is_some_and(|cs| cs.iter().any(|v| v.as_str() == Some(c)));
            let Some(cap_uri) = offer.get("capability").and_then(Value::as_str) else {
                continue;
            };
            let metadata = offer.get("metadata").cloned().unwrap_or(Value::Null);

            let (pack, action, description) = if has(BUSINESS_ACTION_CONTRACT) {
                let Some((pack, action)) = parse_business_action_cap_uri(cap_uri) else {
                    tracing::warn!(
                        cap_uri,
                        "sorla: skipping business-action offer with an unparsable capability uri"
                    );
                    continue;
                };
                let description = describe_business_action(&metadata, &pack, &action);
                (pack, action, description)
            } else if has(AGENT_ENDPOINT_CONTRACT) {
                let Some((pack, id)) = parse_agent_endpoint_cap_uri(cap_uri) else {
                    tracing::warn!(
                        cap_uri,
                        "sorla: skipping agent-endpoint offer with an unparsable capability uri"
                    );
                    continue;
                };
                let description = describe_agent_endpoint(&metadata, &pack, &id);
                (pack, id, description)
            } else {
                continue; // business-event topics and any other kind are ignored
            };

            let parameters = business_action_parameters(&offer, &metadata);
            cap_by_key.insert((pack.clone(), action.clone()), cap_uri.to_string());
            ops.push(SorxOperation {
                pack,
                action,
                description,
                parameters,
                cap_uri: cap_uri.to_string(),
            });
        }
        (ops, cap_by_key)
    }
}

impl SorxInvoker for SorxHttpInvoker {
    fn list_operations(&self) -> Vec<SorxOperation> {
        self.ops.clone()
    }

    fn invoke<'a>(
        &'a self,
        pack: &'a str,
        action: &'a str,
        args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let cap = self
                .cap_by_key
                .get(&(pack.to_string(), action.to_string()))
                .ok_or_else(|| format!("no capability for sorla tool '{pack}/{action}'"))?;
            let input: Value = serde_json::from_str(args_json)
                .map_err(|e| format!("invalid sorla tool arguments: {e}"))?;
            let tenant = sorx_tenant_from_env();
            let body = json!({
                "capability": cap,
                "input": input,
                "context": {
                    "tenant_id": tenant,
                    "caller_id": SORLA_CALLER_ID,
                    "roles": [],
                },
                "dry_run": false,
            });

            let url = format!(
                "{}/admin/v1/capabilities/invoke",
                self.base_url.trim_end_matches('/')
            );
            let client = reqwest::Client::new();
            let mut req = client.post(&url).json(&body);
            for (name, value) in sorla_headers(&tenant) {
                req = req.header(name, value);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| format!("sorx invoke request failed: {e}"))?;
            let status = resp.status();
            let text = resp
                .text()
                .await
                .map_err(|e| format!("sorx invoke response read failed: {e}"))?;
            let parsed: Value = serde_json::from_str(&text).unwrap_or(Value::Null);

            match status.as_u16() {
                200 if parsed.get("ok").and_then(Value::as_bool) == Some(true) => {
                    Ok(parsed.get("result").cloned().unwrap_or(json!({})))
                }
                202 if parsed.get("status").and_then(Value::as_str)
                    == Some("approval_required") =>
                {
                    Ok(json!({
                        "status": "approval_required",
                        "approval": parsed.get("approval").cloned().unwrap_or(Value::Null),
                    }))
                }
                403 => Ok(json!({
                    "error": "denied",
                    "message": parsed
                        .pointer("/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("capability invocation denied"),
                })),
                404 => Ok(json!({
                    "error": "capability_not_found",
                    "message": parsed
                        .pointer("/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("capability not found"),
                })),
                other => Err(format!("sorx invoke failed: status={other} body={text}")),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SorxHttpInvoker;
    use greentic_aw_runtime::SorxInvoker;
    use serde_json::json;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// A `GET /admin/v1/capabilities` response with one business-action offer
    /// (mirrors `greentic-sorx-core`'s `CapabilityOffer` shape).
    fn caps_response_one_business_action() -> serde_json::Value {
        json!({
            "schema": "greentic.capabilities.v1",
            "offers": [
                {
                    "capability": "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0",
                    "contracts": ["greentic.sorx.business-action.invoke.v1"],
                    "metadata": {
                        "kind": "business_function",
                        "pack": { "name": "landlord", "version": "0.1.0" },
                        "action": {
                            "id": "record_rent_payment",
                            "version": "0.1.0",
                            "label": "Record a rent payment"
                        },
                        "execution": { "endpoint_id": "landlord.record_rent_payment" }
                    }
                },
                {
                    "capability": "cap://greentic/events/landlord/rent_paid",
                    "contracts": ["greentic.sorx.business-event.publish.v1"],
                    "metadata": {}
                }
            ],
            "requires": []
        })
    }

    /// A `GET /admin/v1/capabilities` response with one business-action offer,
    /// one agent-endpoint offer (greentic-sorx #60 shape), and one
    /// business-event offer that must still be dropped.
    fn caps_response_mixed() -> serde_json::Value {
        json!({
            "schema": "greentic.capabilities.v1",
            "offers": [
                {
                    "capability": "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0",
                    "contracts": ["greentic.sorx.business-action.invoke.v1"],
                    "metadata": { "action": { "id": "record_rent_payment", "label": "Record a rent payment" } }
                },
                {
                    "capability": "cap://greentic/agent-endpoints/landlord/tenants.create/v0.1.0",
                    "contracts": ["greentic.sorx.agent-endpoint.invoke.v1"],
                    "metadata": {
                        "kind": "agent_endpoint",
                        "pack": { "name": "landlord", "version": "0.1.0" },
                        "endpoint": { "id": "tenants.create", "intent": "Create a tenant record", "approval": "required" }
                    }
                },
                {
                    "capability": "cap://greentic/events/landlord/rent_paid",
                    "contracts": ["greentic.sorx.business-event.publish.v1"],
                    "metadata": {}
                }
            ],
            "requires": []
        })
    }

    #[tokio::test]
    async fn fetch_surfaces_agent_endpoint_alongside_business_action() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/admin/v1/capabilities"))
            .respond_with(ResponseTemplate::new(200).set_body_json(caps_response_mixed()))
            .mount(&server)
            .await;

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
        Mock::given(method("GET"))
            .and(path("/admin/v1/capabilities"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(caps_response_one_business_action()),
            )
            .mount(&server)
            .await;

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
        Mock::given(method("GET"))
            .and(path("/admin/v1/capabilities"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(caps_response_one_business_action()),
            )
            .mount(&server)
            .await;
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
        Mock::given(method("GET"))
            .and(path("/admin/v1/capabilities"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(caps_response_one_business_action()),
            )
            .mount(&server)
            .await;
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
        Mock::given(method("GET"))
            .and(path("/admin/v1/capabilities"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(caps_response_one_business_action()),
            )
            .mount(&server)
            .await;
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
        Mock::given(method("GET"))
            .and(path("/admin/v1/capabilities"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(caps_response_one_business_action()),
            )
            .mount(&server)
            .await;
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
        Mock::given(method("GET"))
            .and(path("/admin/v1/capabilities"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(caps_response_one_business_action()),
            )
            .mount(&server)
            .await;
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
        result.expect(
            "invoke should succeed; header expectation is checked by wiremock's .expect(1)",
        );
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
        Mock::given(method("GET"))
            .and(path("/admin/v1/capabilities"))
            .respond_with(ResponseTemplate::new(200).set_body_json(caps_response_mixed()))
            .mount(&server)
            .await;
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
        use super::parse_agent_endpoint_cap_uri;
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
}
