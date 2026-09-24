//! HTTP execution of a `sorla.call` flow node, for the one SoR named on the
//! node — the arm `engine::execute_sorla_call` tries before ever considering
//! NATS (amendment A1: a route document always wins over NATS).
//!
//! Shaped like [`crate::runner::sorx_invoker::routed::SorxRoutedInvoker`]'s
//! per-call body (resolve -> discover -> invoke), but for exactly one call
//! rather than a whole tool catalog: a flow node already names its action, so
//! there is no `list_operations()` step to cache, and nothing here is kept
//! alive between calls — a restarted local SoR child on a new port is
//! followed for free, because the route document is read fresh every time.

use greentic_secrets_lib::SecretsManager;
use serde_json::Value;

use super::sorla_route::{SorlaRouteError, resolve_route};
use super::sorx_invoker::{discover, invoke_sor, shared_client};

/// Run one `sorla.call` action over HTTP when a route document resolves for
/// `sor`.
///
/// `payload` is the node's whole rendered input mapping —
/// `{ await, operation: <action>, input: <record>, output? }` — never just
/// the inner `input`; this reads `operation`/`input` off it directly.
///
/// - `Ok(Some(result))` — the SoR answered. `result` is exactly what
///   [`invoke_sor`] returned: already status-mapped, so a `403`/`404` from
///   the SoR surfaces as a `{"error": ...}` VALUE here, never an `Err`.
/// - `Ok(None)` — no route document exists for `sor`
///   ([`SorlaRouteError::Missing`]). The caller decides between falling back
///   to NATS and failing with `sorla_route_missing`.
/// - `Err(_)` — a route document exists but is malformed
///   ([`SorlaRouteError::Invalid`]), the named action has no matching
///   capability on this SoR, or the HTTP call itself failed.
pub(crate) async fn execute_sorla_http(
    secrets: &dyn SecretsManager,
    tenant: &str,
    unit: Option<&str>,
    sor: &str,
    payload: &Value,
    caller_id: &str,
) -> Result<Option<Value>, String> {
    let route = match resolve_route(secrets, tenant, unit, sor).await {
        Ok(route) => route,
        // No route document exists for this SoR: the caller decides between
        // NATS and `sorla_route_missing`.
        Err(SorlaRouteError::Missing(_)) => return Ok(None),
        // A malformed document, or the secrets backend itself failing to
        // answer, is NOT "no route document" — surface it as a hard error so
        // it can never be silently read as license to fall back to NATS.
        Err(err @ (SorlaRouteError::Invalid(_) | SorlaRouteError::Unavailable(_))) => {
            return Err(err.to_string());
        }
    };

    let client = shared_client();
    // The SoR key IS the cap-URI pack segment (global constraint), so
    // looking a capability up by `(sor, action)` already scopes it to what
    // THIS SoR offers under its own name — an offer for another pack, from a
    // misbehaving or shared-deployment server, simply has no entry under
    // this key and is never reachable through it.
    let (_ops, cap_by_key) = discover(&client, &route).await;

    let action = payload
        .get("operation")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some(cap_uri) = cap_by_key.get(&(sor.to_string(), action.to_string())) else {
        return Err(format!(
            "no capability for sorla action `{action}` of SoR `{sor}`"
        ));
    };

    let input = payload.get("input").cloned().unwrap_or(Value::Null);
    invoke_sor(&client, &route, cap_uri, input, caller_id)
        .await
        .map(Some)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::runner::sorla_route::test_secrets::Handle;
    use crate::runner::sorx_invoker::fixtures::caps_response_one_business_action;

    async fn mount_caps_get(server: &MockServer, body: Value) {
        Mock::given(method("GET"))
            .and(path("/admin/v1/capabilities"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    async fn mount_invoke(server: &MockServer, status: u16, body: Value) {
        Mock::given(method("POST"))
            .and(path("/admin/v1/capabilities/invoke"))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .mount(server)
            .await;
    }

    /// Seed a `landlord` route document pointing at `server`, for tenant
    /// `acme`, no token.
    fn seed_landlord_route(server: &MockServer) -> Handle {
        Handle::with(&[(
            "secrets://default/acme/_/sorla/landlord",
            &format!(r#"{{"url":"{}","tenant":"acme"}}"#, server.uri()),
        )])
    }

    #[tokio::test]
    async fn resolves_the_action_through_discovery_and_invokes() {
        let server = MockServer::start().await;
        mount_caps_get(&server, caps_response_one_business_action()).await;
        mount_invoke(&server, 200, json!({"ok": true, "result": {"id": "pay-1"}})).await;
        let secrets = seed_landlord_route(&server).manager();

        let payload =
            json!({"await": true, "operation": "record_rent_payment", "input": {"amount": 5}});
        let result = execute_sorla_http(&*secrets, "acme", None, "landlord", &payload, "flow:f/n")
            .await
            .expect("a resolved route + a matching capability must succeed");

        assert_eq!(result, Some(json!({"id": "pay-1"})));
    }

    #[tokio::test]
    async fn no_route_document_is_none() {
        let secrets = Handle::with(&[]).manager();
        let payload = json!({"operation": "record_rent_payment", "input": {}});

        let result = execute_sorla_http(&*secrets, "acme", None, "landlord", &payload, "flow:f/n")
            .await
            .expect("a missing route document must be Ok(None), not Err");

        assert_eq!(result, None);
    }

    /// A secrets backend failure (as opposed to a genuine miss) must surface
    /// as `Err`, never `Ok(None)` — `Ok(None)` is the signal the engine reads
    /// as "fall back to NATS", and a backend that cannot answer says nothing
    /// about whether a route document exists.
    #[tokio::test]
    async fn a_backend_failure_is_an_error_not_ok_none() {
        let secrets = Handle::with(&[]);
        secrets.fail(
            "secrets://default/acme/_/sorla/landlord",
            "connection refused",
        );
        let payload = json!({"operation": "record_rent_payment", "input": {}});

        let err = execute_sorla_http(
            &*secrets.manager(),
            "acme",
            None,
            "landlord",
            &payload,
            "flow:f/n",
        )
        .await
        .expect_err("a backend failure must be Err, never Ok(None)");

        assert!(err.contains("could not be read"), "got: {err}");
    }

    #[tokio::test]
    async fn an_unknown_action_is_an_error_naming_it() {
        let server = MockServer::start().await;
        mount_caps_get(&server, caps_response_one_business_action()).await;
        let secrets = seed_landlord_route(&server).manager();

        let payload = json!({"operation": "does_not_exist", "input": {}});
        let err = execute_sorla_http(&*secrets, "acme", None, "landlord", &payload, "flow:f/n")
            .await
            .expect_err("an action absent from discovery must be an error");

        assert!(
            err.contains("does_not_exist"),
            "error must name the unknown action, got: {err}"
        );
        assert!(
            err.contains("landlord"),
            "error must name the SoR, got: {err}"
        );
    }
}
