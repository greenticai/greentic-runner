//! HTTP transport for the SoRX capability admin API: discovery
//! (`GET {url}/admin/v1/capabilities`) and invocation
//! (`POST {url}/admin/v1/capabilities/invoke`), plus [`SorxHttpInvoker`], the
//! single-SoR `GREENTIC_AW_SORX_URL` development override that dispatches
//! through the same two functions with a fixed, token-less route document.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use greentic_aw_runtime::{SorxInvoker, SorxOperation};
use serde_json::{Value, json};

use super::{SORLA_CALLER_ID, shared_client};
use crate::runner::sorla_route::SorlaRouteDoc;

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

/// Caller role stamped on every capability invocation, alongside whichever
/// caller id the [`invoke_sor`] caller supplies.
const SORLA_CALLER_ROLE: &str = "agent";

/// Tenant stamped on every capability invocation made by [`SorxHttpInvoker`]
/// — the `GREENTIC_AW_SORX_URL` development override, which has no route
/// document of its own to carry a tenant. Reads `GREENTIC_AW_SORX_TENANT`,
/// falling back to `"default"` when unset.
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
fn sorla_headers(tenant: &str, caller_id: &str) -> [(&'static str, String); 3] {
    [
        ("X-Greentic-Tenant-Id", tenant.to_string()),
        ("X-Greentic-Caller-Id", caller_id.to_string()),
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
///
/// `pub(crate)` rather than private: exercised directly by
/// [`super::tests`].
pub(crate) fn parse_agent_endpoint_cap_uri(cap_uri: &str) -> Option<(String, String)> {
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

/// Extract `(ops, cap_by_key)` from a parsed `GET /admin/v1/capabilities`
/// body, keeping both BusinessAction and agent-endpoint offers — each with a
/// well-formed `cap://` capability URI of its own kind
/// (`business-functions`/`agent-endpoints` respectively). Any other contract
/// (business-event topics, unknown kinds) is dropped. Malformed/unparsable
/// offers are skipped (logged), not fatal.
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
        let has = |c: &str| contracts.is_some_and(|cs| cs.iter().any(|v| v.as_str() == Some(c)));
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

/// Discover a SoR's capability surface: `GET {route.url}/admin/v1/capabilities`,
/// carrying `Authorization: Bearer <token>` when `route.token` is `Some`. A
/// down/unreachable SoR, a non-2xx status, or an unparsable body all degrade
/// to an empty operation set (logged), never a propagated error — a down SoR
/// must never crash worker startup or discovery for its siblings.
///
/// The status is checked explicitly rather than left to `resp.json()`: a
/// rejected credential (401, or any other non-2xx) still carries a JSON body
/// on most SoRX implementations, which would otherwise parse as zero offers
/// and silently drop every tool with no signal beyond an empty list. Only
/// the status code is logged — never the body, which may echo the request
/// or carry the SoR's own diagnostic detail, and never the token.
pub(crate) async fn discover(
    client: &reqwest::Client,
    route: &SorlaRouteDoc,
) -> (Vec<SorxOperation>, HashMap<(String, String), String>) {
    let url = format!("{}/admin/v1/capabilities", route.url.trim_end_matches('/'));
    let mut req = client.get(&url);
    if let Some(token) = &route.token {
        req = req.bearer_auth(token);
    }
    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                if status.as_u16() == 401 {
                    tracing::warn!(
                        url = %route.url,
                        status = status.as_u16(),
                        "sorla_unauthorized: the SoR rejected this caller's credential during discovery; starting with an empty tool set"
                    );
                } else {
                    tracing::warn!(
                        url = %route.url,
                        status = status.as_u16(),
                        "sorla: SoRX capabilities endpoint returned a non-success status; starting with an empty tool set"
                    );
                }
                return (Vec::new(), HashMap::new());
            }
            match resp.json::<Value>().await {
                Ok(body) => parse_capabilities(&body),
                Err(err) => {
                    tracing::warn!(
                        url = %route.url,
                        error = %err,
                        "sorla: failed to parse SoRX capabilities response; starting with an empty tool set"
                    );
                    (Vec::new(), HashMap::new())
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                url = %route.url,
                error = %err,
                "sorla: failed to reach SoRX capabilities endpoint; starting with an empty tool set"
            );
            (Vec::new(), HashMap::new())
        }
    }
}

/// Invoke one SoR capability: `POST {route.url}/admin/v1/capabilities/invoke`,
/// carrying `Authorization: Bearer <token>` when `route.token` is `Some`.
/// `cap_uri` is sent verbatim (BusinessAction and agent-endpoint capabilities
/// share this one route). A `401` maps to a fixed, body-free error — the
/// SoR's response body and the credential must never be surfaced (spec §7).
pub(crate) async fn invoke_sor(
    client: &reqwest::Client,
    route: &SorlaRouteDoc,
    cap_uri: &str,
    input: Value,
    caller_id: &str,
) -> Result<Value, String> {
    let body = json!({
        "capability": cap_uri,
        "input": input,
        "context": {
            "tenant_id": route.tenant,
            "caller_id": caller_id,
            "roles": [],
        },
        "dry_run": false,
    });

    let url = format!(
        "{}/admin/v1/capabilities/invoke",
        route.url.trim_end_matches('/')
    );
    let mut req = client.post(&url).json(&body);
    for (name, value) in sorla_headers(&route.tenant, caller_id) {
        req = req.header(name, value);
    }
    if let Some(token) = &route.token {
        req = req.bearer_auth(token);
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
        202 if parsed.get("status").and_then(Value::as_str) == Some("approval_required") => {
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
        401 => Err("sorla_unauthorized: the SoR rejected this caller's credential".to_string()),
        other => {
            // The body is never in the error string: it can echo the request
            // (the capability, the record the caller sent) or carry the SoR's
            // own diagnostic detail, and this error reaches the flow's node
            // output and trace — the same reason `discover`'s non-2xx warn
            // above logs only the status. A truncated copy at `debug!` is for
            // a human tailing runner logs, never for the flow.
            tracing::debug!(
                url = %route.url,
                status = other,
                body = %text.chars().take(500).collect::<String>(),
                "sorla: SoRX invoke returned an unhandled status"
            );
            Err(format!("sorx invoke failed: status={other}"))
        }
    }
}

/// `SorxInvoker` backed by a single SoR reached via the
/// `GREENTIC_AW_SORX_URL` development override: no route document, no
/// per-call re-resolution — a fixed URL discovered once at construction.
///
/// Capability discovery is a one-time fetch at construction ([`Self::fetch`])
/// — `list_operations` is sync per the [`SorxInvoker`] trait, so the captured
/// `ops`/`cap_by_key` are simply returned/looked-up, never re-fetched.
pub(crate) struct SorxHttpInvoker {
    client: reqwest::Client,
    route: SorlaRouteDoc,
    ops: Vec<SorxOperation>,
    cap_by_key: HashMap<(String, String), String>,
}

impl SorxHttpInvoker {
    /// Discover `base_url`'s capability surface once, wrapping it in a
    /// token-less [`SorlaRouteDoc`] whose tenant comes from
    /// `GREENTIC_AW_SORX_TENANT` (falling back to `"default"`).
    pub(crate) async fn fetch(base_url: String) -> Self {
        let client = shared_client();
        let route = SorlaRouteDoc {
            url: base_url.clone(),
            token: None,
            tenant: sorx_tenant_from_env(),
        };
        let (ops, cap_by_key) = discover(&client, &route).await;
        tracing::info!(
            base_url = %base_url,
            ops = ops.len(),
            "sorla: SoRX tool source constructed"
        );
        Self {
            client,
            route,
            ops,
            cap_by_key,
        }
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
            invoke_sor(&self.client, &self.route, cap, input, SORLA_CALLER_ID).await
        })
    }
}

/// `GET /admin/v1/capabilities` response fixtures shared by
/// [`super::tests`] (both `SorxHttpInvoker` and `SorxRoutedInvoker` are
/// exercised against the same SoRX capability-offer shapes).
#[cfg(test)]
pub(crate) mod fixtures {
    use serde_json::json;

    /// One business-action offer (mirrors `greentic-sorx-core`'s
    /// `CapabilityOffer` shape) plus one business-event offer that must be
    /// filtered out.
    pub(crate) fn caps_response_one_business_action() -> serde_json::Value {
        json!({
            "schema": "greentic.capabilities.v1",
            "offers": [
                {
                    "capability": "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0",
                    "contracts": ["greentic.sorx.business-action.invoke.v1"],
                    "metadata": {
                        "kind": "business_function",
                        "pack": {"name": "landlord", "version": "0.1.0"},
                        "action": {"id": "record_rent_payment", "version": "0.1.0", "label": "Record a rent payment"},
                        "execution": {"endpoint_id": "landlord.record_rent_payment"}
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

    /// [`caps_response_one_business_action`] with every `"landlord"`
    /// occurrence replaced by `pack` — a second, independent SoR fixture
    /// (e.g. `"billing"`) without hand-duplicating the whole shape.
    pub(crate) fn caps_response_for_pack(pack: &str) -> serde_json::Value {
        let text = caps_response_one_business_action().to_string();
        serde_json::from_str(&text.replace("landlord", pack))
            .expect("substituting a pack name keeps the JSON well-formed")
    }

    /// One `landlord` business-action offer and one `billing` business-action
    /// offer from the SAME server — a misbehaving or shared-deployment SoR
    /// offering another pack's ops alongside its own.
    pub(crate) fn caps_response_landlord_plus_billing() -> serde_json::Value {
        json!({
            "schema": "greentic.capabilities.v1",
            "offers": [
                {
                    "capability": "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0",
                    "contracts": ["greentic.sorx.business-action.invoke.v1"],
                    "metadata": {"action": {"id": "record_rent_payment", "label": "Record a rent payment"}}
                },
                {
                    "capability": "cap://greentic/business-functions/billing/charge_card/v0.1.0",
                    "contracts": ["greentic.sorx.business-action.invoke.v1"],
                    "metadata": {"action": {"id": "charge_card", "label": "Charge a card"}}
                }
            ],
            "requires": []
        })
    }

    /// One business-action offer, one agent-endpoint offer (greentic-sorx
    /// #60 shape), and one business-event offer that must still be dropped.
    pub(crate) fn caps_response_mixed() -> serde_json::Value {
        json!({
            "schema": "greentic.capabilities.v1",
            "offers": [
                {
                    "capability": "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0",
                    "contracts": ["greentic.sorx.business-action.invoke.v1"],
                    "metadata": {"action": {"id": "record_rent_payment", "label": "Record a rent payment"}}
                },
                {
                    "capability": "cap://greentic/agent-endpoints/landlord/tenants.create/v0.1.0",
                    "contracts": ["greentic.sorx.agent-endpoint.invoke.v1"],
                    "metadata": {
                        "kind": "agent_endpoint",
                        "pack": {"name": "landlord", "version": "0.1.0"},
                        "endpoint": {"id": "tenants.create", "intent": "Create a tenant record", "approval": "required"}
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
}
