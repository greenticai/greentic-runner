# SoRLa agent-endpoint invoke (runner PR-1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the runner's `sorla:` DW-agent tool catalog discover + dispatch SoRLa **agent-endpoint** capabilities (`cap://greentic/agent-endpoints/<pack>/<id>/v<ver>`), not just BusinessActions.

**Architecture:** The only change is discovery in `SorxHttpInvoker::parse_capabilities` (`crates/greentic-runner-host/src/runner/sorx_invoker.rs`). Two hard-coded gates reject agent-endpoint offers: a contract filter (`greentic.sorx.business-action.invoke.v1`) and a cap-URI namespace check (`business-functions`). Widen both to also accept agent-endpoints, keyed `(pack, endpoint_id)` with the cap stored verbatim. The invoke leg (`POST /admin/v1/capabilities/invoke`, cap sent verbatim) and result-parsing (200+`ok:true`+`result`, 202/403/404) are already namespace-agnostic and need NO change.

**Tech Stack:** Rust 1.95 edition 2024; `serde_json`; `reqwest`; `wiremock` (dev). Crate `greentic-runner-host`, feature `agentic-worker` (default-on; the file is `#![cfg(feature = "agentic-worker")]`).

## Global Constraints

- No `unwrap()`/`panic!()` in production code — parse guards return `None`; the two production `unwrap`/`expect` sites in this file are test-only and stay test-only.
- Purely ADDITIVE: existing business-action discovery, the `invoke` fn, and result-parsing are unchanged in behavior; a business-event offer is still dropped.
- Agent-endpoint tools share the `sorla:<pack>` namespace with business-actions, keyed `(pack, id)`; an id collision within a pack is a documented known limitation (last-write-wins), NOT handled here.
- English only; Conventional Commits; **NO Claude co-author attribution** on commits or PRs.
- Gates: `cargo fmt --all --check`; `cargo clippy -p greentic-runner-host --all-targets --features agentic-worker -- -D warnings`; `cargo test -p greentic-runner-host --features agentic-worker sorx_invoker`. (`ci/local_check.sh` may be pre-existing-red on base from wasmtime skew / heavy-WASM — reproduce on a pristine checkout before blaming this change.)

---

### Task 1: Cap-URI parsing for both namespaces

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/sorx_invoker.rs:22` (add contract const), `:54-70` (refactor parser)

**Interfaces:**
- Produces: `const AGENT_ENDPOINT_CONTRACT: &str`; `fn parse_agent_endpoint_cap_uri(&str) -> Option<(String, String)>` returning `(pack, endpoint_id)`. `parse_business_action_cap_uri` keeps its existing signature/behavior.

- [ ] **Step 1: Write the failing test**

Add to the existing `#[cfg(test)] mod tests` in `sorx_invoker.rs`:

```rust
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
    assert_eq!(parse_agent_endpoint_cap_uri("greentic/agent-endpoints/a/b/v1"), None);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p greentic-runner-host --features agentic-worker parses_agent_endpoint_cap_uri`
Expected: FAIL — `cannot find function parse_agent_endpoint_cap_uri`.

- [ ] **Step 3: Write minimal implementation**

Add the contract const next to `BUSINESS_ACTION_CONTRACT` (`:22`):

```rust
/// The `contracts` entry a SoRX capability offer carries when it is a SoRLa
/// agent-endpoint reachable via `POST /admin/v1/capabilities/invoke` (added
/// to the capability surface in greentic-sorx #60). Distinct from
/// [`BUSINESS_ACTION_CONTRACT`]; both are dispatched through the same invoke
/// route with the cap URI sent verbatim.
const AGENT_ENDPOINT_CONTRACT: &str = "greentic.sorx.agent-endpoint.invoke.v1";
```

Refactor the existing parser into a shared helper keyed on the expected `kind` segment, and add the agent-endpoint wrapper. Replace the current `parse_business_action_cap_uri` (`:57-70`) with:

```rust
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p greentic-runner-host --features agentic-worker parses_agent_endpoint_cap_uri`
Expected: PASS. Also run the existing `fetch_builds_one_op_from_business_action_offer` to confirm the refactor kept business-action parsing intact.

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-runner-host/src/runner/sorx_invoker.rs
git commit -m "feat(runner-host): parse agent-endpoint capability URIs in sorla invoker"
```

---

### Task 2: Describe helper + discovery for agent-endpoint offers

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/sorx_invoker.rs:75-82` (add describe helper), `:157-200` (kind-dispatch in `parse_capabilities`)

**Interfaces:**
- Consumes: `AGENT_ENDPOINT_CONTRACT`, `parse_agent_endpoint_cap_uri` (Task 1).
- Produces: `fn describe_agent_endpoint(metadata: &Value, pack: &str, id: &str) -> String`; `parse_capabilities` now emits `SorxOperation`s for agent-endpoint offers too (keyed `(pack, id)`, `cap_uri` verbatim).

- [ ] **Step 1: Write the failing test**

The existing `caps_response_one_business_action()` fixture returns a business-action offer plus a business-event offer. Add a fixture and a discovery test:

```rust
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

    assert_eq!(ops.len(), 2, "business-action + agent-endpoint; event dropped");
    // agent-endpoint op keyed by (pack, endpoint_id), cap stored verbatim.
    let ep = ops.iter().find(|o| o.action == "tenants.create").expect("agent-endpoint op");
    assert_eq!(ep.pack, "landlord");
    assert_eq!(ep.cap_uri, "cap://greentic/agent-endpoints/landlord/tenants.create/v0.1.0");
    assert_eq!(ep.description, "Create a tenant record");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p greentic-runner-host --features agentic-worker fetch_surfaces_agent_endpoint`
Expected: FAIL — `ops.len()` is 1 (agent-endpoint dropped at the contract gate).

- [ ] **Step 3: Write minimal implementation**

Add the describe helper next to `describe_business_action` (`:75`):

```rust
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
```

Rewrite the loop body of `parse_capabilities` (`:165-198`) to classify each offer by contract and dispatch to the matching parser/describer. Both kinds push into the SAME `ops`/`cap_by_key` keyed `(pack, id)`, cap stored verbatim:

```rust
for offer in offers {
    let contracts = offer.get("contracts").and_then(Value::as_array);
    let has = |c: &str| {
        contracts.is_some_and(|cs| cs.iter().any(|v| v.as_str() == Some(c)))
    };
    let Some(cap_uri) = offer.get("capability").and_then(Value::as_str) else {
        continue;
    };
    let metadata = offer.get("metadata").cloned().unwrap_or(Value::Null);

    let (pack, action, description) = if has(BUSINESS_ACTION_CONTRACT) {
        let Some((pack, action)) = parse_business_action_cap_uri(cap_uri) else {
            tracing::warn!(cap_uri, "sorla: skipping business-action offer with an unparsable capability uri");
            continue;
        };
        let description = describe_business_action(&metadata, &pack, &action);
        (pack, action, description)
    } else if has(AGENT_ENDPOINT_CONTRACT) {
        let Some((pack, id)) = parse_agent_endpoint_cap_uri(cap_uri) else {
            tracing::warn!(cap_uri, "sorla: skipping agent-endpoint offer with an unparsable capability uri");
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
```

(`business_action_parameters` is namespace-neutral — it probes `metadata.execution.input_schema` / `metadata.input_schema` / `offer.input_schema` then falls back to `{"type":"object"}` — so it is reused verbatim for agent-endpoints; the agent-endpoint offer's optional `input_schema` is picked up automatically.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p greentic-runner-host --features agentic-worker sorx_invoker`
Expected: PASS — the new discovery test plus all pre-existing tests (`fetch_builds_one_op_from_business_action_offer`, the invoke/403/404/202/header tests) stay green.

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-runner-host/src/runner/sorx_invoker.rs
git commit -m "feat(runner-host): discover agent-endpoint capabilities as sorla tools"
```

---

### Task 3: Dispatch parity for an agent-endpoint capability

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/sorx_invoker.rs` (test module only — invoke needs no code change)

**Interfaces:**
- Consumes: discovery from Task 2; the unchanged `invoke` fn.

- [ ] **Step 1: Write the failing test**

```rust
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
```

Add `body_partial_json` to the `wiremock::matchers` import line in the test module.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p greentic-runner-host --features agentic-worker invoke_dispatches_agent_endpoint`
Expected: FAIL if run against pre-Task-2 code (cap not in `cap_by_key` → `Err("no capability …")`). Against post-Task-2 code it should already PASS — this task PROVES end-to-end dispatch parity and locks in the result-shape tolerance (`greentic.sorx.agent-endpoint-invoke-result.v1`), so if it passes immediately that is the expected proof, not a skipped RED. (If it passes on first run, note that in the commit body; the discovery code from Task 2 is what makes it pass.)

- [ ] **Step 3: Write minimal implementation**

None — `invoke` sends the stored cap verbatim and parses `200 + ok:true → result`. This task is the regression proof for the agent-endpoint result envelope.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p greentic-runner-host --features agentic-worker sorx_invoker`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-runner-host/src/runner/sorx_invoker.rs
git commit -m "test(runner-host): prove agent-endpoint capability dispatch parity"
```
