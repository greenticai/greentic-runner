# Runner `GET /admin/capabilities` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the capabilities installed on a runner over `GET /admin/capabilities`, so `greentic-designer-admin` can preflight a mandatory guardrail policy instead of accepting one that will fail closed at runtime.

**Architecture:** `ServerState` gains an `ext_runtime` built once at server-build time; a new `AdminGuard`-protected handler serialises `CapabilityRegistry::offerings()` to JSON. The router is first extracted from `HostServer::with_sql` into a `router(state)` function so tests can drive the assembled router. All new surface is gated on the `agentic-worker` feature, mirroring `stream_observers`.

**Tech Stack:** Rust 1.94, edition 2024, axum 0.8, `tower::ServiceExt::oneshot` + `http_body_util` for router tests (no `axum-test` in this crate).

**Spec:** `greentic-designer-admin/docs/superpowers/specs/2026-07-15-guardrail-policy-preflight-design.md`

This is PR-1 of two. PR-2 (the admin-side preflight) is authored **after** this ships, against the shape this actually emits.

## Global Constraints

- Rust 1.94.0, edition 2024, pinned via `rust-toolchain.toml`. Do not edit.
- English only in source, tests, comments, and tracing logs.
- Conventional Commits (`feat:`, `fix:`, `refactor:`, `docs:`).
- **Do NOT add Claude co-author attribution to commits or PRs** (repo rule, `CLAUDE.md`).
- No `unwrap()` / `panic!()` in production paths — `anyhow`/`thiserror`. Tests may use `.unwrap()`; that is the in-crate precedent (`src/sql/routes.rs`).
- `agentic-worker` is in `default = ["verify", "agentic-worker", "state-disk"]`, so plain `cargo test` exercises it. `--no-default-features` exercises the lean path and must still compile.
- Verify with `cargo clippy --all-targets --all-features -- -D warnings` and `cargo test -p greentic-runner-host`.
- Work in the worktree `.worktrees/guardrail-caps-endpoint` (branch `feat/admin-capabilities-endpoint`, based on `research`). The main checkout is on another branch with uncommitted work — do not touch it.

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/greentic-runner-host/src/runner/mod.rs` | Router assembly, `ServerState` | Extract `router()`; add `ext_runtime` field; register route; router tests |
| `crates/greentic-runner-host/src/http/admin.rs` | Admin handlers | Add `offerings_to_json()` + `capabilities()` handler + their tests |
| `crates/greentic-runner-host/src/http/auth.rs` | `AdminGuard` test helper | Add `ext_runtime: None` to inline `server_state()` |
| `crates/greentic-runner-host/src/http/health.rs` | Health test helper | Add `ext_runtime: None` to inline `state()` |
| `crates/greentic-runner-host/src/runner/agent_node.rs` | Extension boot scan | Log registered cap ids instead of a bare count |
| `crates/greentic-runner-host/Cargo.toml` | Deps | Add `greentic-extension-sdk-contract` dev-dep |

## Context an implementer needs

**Why the router extraction (Task 1) comes first.** `HostServer`'s fields are private and there is no `router()` accessor, so nothing today can drive the assembled router. Deleting a `.route(...)` line would fail no test. The registration test in Task 3 is the whole point of this PR's safety net — it encodes the greentic-designer #796 failure, where a router was defined but never merged: 404 in embedded mode, and in dev mode the unmatched request fell through to a proxy that pointed back at the app, producing an infinite loop and a hang. The in-crate precedent for the extraction is `sql::routes::router` (`src/sql/routes.rs:193-198`).

**`AdminGuard` gotcha.** The guard reads `ConnectInfo<SocketAddr>` from request extensions and returns **500 "connect info unavailable"** when it is absent. Production supplies it via `.into_make_service_with_connect_info::<SocketAddr>()`; `oneshot` does **not**. Every router test below therefore injects it with `.extension(...)`. With `AdminAuth::default()` (token `None`), a loopback address passes.

**Boot cost, accepted deliberately.** `build_ext_runtime` needs a `secrets_backend` and `llm_port` that differ per tenant, and is called per-tenant at `agent_node.rs:1141`; `ServerState` is process-level. The registry itself derives purely from `describe.capabilities.offered`, but `register_loaded_from_dir` compiles the WASM. So the process-level instance costs one extra set of WASM loads at boot. The cheaper alternative (a describe-only scan) needs a new function in `greentic-ext-runtime` and would wake the five-repo release train. The spec chose the boot cost.

---

### Task 1: Extract `router(state)` from `HostServer::with_sql`

Pure refactor — no behaviour change. Makes the assembled router testable.

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/mod.rs:102-125` (extract), `:73-101` (call it)

**Interfaces:**
- Produces: `pub(crate) fn router(state: ServerState) -> Router` — Task 3 registers a route inside it and tests through it.

- [ ] **Step 1: Write the failing test**

Append to `crates/greentic-runner-host/src/runner/mod.rs` (end of file):

```rust
#[cfg(all(test, feature = "agentic-worker"))]
mod router_tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt as _;

    /// `AdminGuard` 500s without `ConnectInfo`; `oneshot` does not supply it.
    fn loopback() -> ConnectInfo<SocketAddr> {
        ConnectInfo("127.0.0.1:8080".parse::<SocketAddr>().unwrap())
    }

    #[tokio::test]
    async fn assembled_router_serves_admin_packs_status() {
        let app = router(ServerState::for_test());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/packs/status")
                    .extension(loopback())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p greentic-runner-host --lib router_tests`
Expected: FAIL — compile error, `cannot find function 'router' in this scope`.

- [ ] **Step 3: Extract the function**

In `crates/greentic-runner-host/src/runner/mod.rs`, replace lines 102-124 (the `let router = Router::new()…` chain through `Ok(Self { … })`) with:

```rust
        let router = router(state.clone());
        Ok(Self {
            addr,
            router,
            _state: state,
        })
    }
```

Then add this free function immediately after the `impl HostServer { … }` block closes (before `pub struct ServerState`):

```rust
/// Assemble the full host router for `state`.
///
/// Extracted from [`HostServer::with_sql`] so tests can drive the *assembled*
/// router — route registration included — without binding a socket. Mirrors the
/// `sql::routes::router` precedent.
pub(crate) fn router(state: ServerState) -> Router {
    let router = Router::new()
        .route("/operator/op/invoke", post(operator::invoke))
        .route("/healthz", get(http::health::handler))
        .route("/admin/packs/status", get(admin::status))
        .route("/admin/packs/reload", post(admin::reload))
        .route("/agent/chat", post(crate::http::agent_chat::agent_chat));
    #[cfg(feature = "agentic-worker")]
    let router = router.route(
        "/agent/chat/stream",
        post(crate::http::agent_chat::agent_chat_stream),
    );
    router
        .route(
            "/sql/{conn}/schema",
            get(crate::sql::routes::schema_handler),
        )
        .route("/sql/{conn}/query", post(crate::sql::routes::query_handler))
        .with_state(state)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p greentic-runner-host --lib router_tests`
Expected: PASS (1 test).

Then confirm the lean build still compiles:
Run: `cargo check -p greentic-runner-host --no-default-features --features verify`
Expected: success, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-runner-host/src/runner/mod.rs
git commit -m "refactor: extract host router assembly into router(state)

HostServer's fields are private with no accessor, so no test could drive
the assembled router — deleting a .route() line failed nothing. Extract
the chain into router(state), mirroring the sql::routes::router
precedent, and cover an existing route through it."
```

---

### Task 2: `offerings_to_json` — the cross-repo contract

The JSON shape lives here. `greentic-designer-admin` parses it; if a key drifts, its preflight degrades to "no caps offered" **silently**. This task pins the shape with a literal assertion.

**Files:**
- Modify: `crates/greentic-runner-host/src/http/admin.rs`
- Modify: `crates/greentic-runner-host/Cargo.toml` (`[dev-dependencies]`)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `fn offerings_to_json(registry: &greentic_ext_runtime::CapabilityRegistry) -> serde_json::Value` — Task 3's handler calls it.

- [ ] **Step 1: Add the dev-dependency**

`greentic-extension-sdk-contract` is already in `[workspace.dependencies]` (root `Cargo.toml:249`, tag `v1.3.0-research.1`) and `greentic-aw-runtime` pins `=1.3.0-research.1`, so `workspace = true` resolves to the same version — no new pin, no cascade.

In `crates/greentic-runner-host/Cargo.toml`, add to `[dev-dependencies]`:

```toml
greentic-extension-sdk-contract.workspace = true
```

- [ ] **Step 2: Write the failing test**

Append inside the existing `#[cfg(test)] mod tests` block in `crates/greentic-runner-host/src/http/admin.rs`:

```rust
    #[cfg(feature = "agentic-worker")]
    #[test]
    fn offerings_to_json_matches_the_admin_contract() {
        use greentic_extension_sdk_contract::ExtensionKind;
        use greentic_ext_runtime::{CapabilityRegistry, OfferedBinding};

        let mut registry = CapabilityRegistry::new();
        registry.add_offering(OfferedBinding {
            extension_id: "greentic.guardrail-pii".to_string(),
            cap_id: "greentic:guardrail/pii".parse().unwrap(),
            version: "0.1.0".parse().unwrap(),
            kind: ExtensionKind::Design,
            export_path: String::new(),
        });

        assert_eq!(
            offerings_to_json(&registry),
            serde_json::json!({
                "capabilities": [{
                    "extension_id": "greentic.guardrail-pii",
                    "cap_id": "greentic:guardrail/pii",
                    "version": "0.1.0",
                    "kind": "DesignExtension"
                }]
            })
        );
    }

    #[cfg(feature = "agentic-worker")]
    #[test]
    fn offerings_to_json_is_ordered_and_handles_empty() {
        use greentic_extension_sdk_contract::ExtensionKind;
        use greentic_ext_runtime::{CapabilityRegistry, OfferedBinding};

        assert_eq!(
            offerings_to_json(&CapabilityRegistry::new()),
            serde_json::json!({ "capabilities": [] })
        );

        // `offerings()` walks a HashMap, so ordering is not inherent. Two caps
        // inserted in reverse order must still come out sorted, or the admin's
        // response and these assertions become flaky.
        let mut registry = CapabilityRegistry::new();
        for cap in ["greentic:guardrail/secrets", "greentic:guardrail/injection"] {
            registry.add_offering(OfferedBinding {
                extension_id: "ext".to_string(),
                cap_id: cap.parse().unwrap(),
                version: "1.0.0".parse().unwrap(),
                kind: ExtensionKind::Design,
                export_path: String::new(),
            });
        }
        let body = offerings_to_json(&registry);
        let caps = body["capabilities"].as_array().unwrap();
        assert_eq!(caps[0]["cap_id"], "greentic:guardrail/injection");
        assert_eq!(caps[1]["cap_id"], "greentic:guardrail/secrets");
    }
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p greentic-runner-host --lib offerings_to_json`
Expected: FAIL — compile error, `cannot find function 'offerings_to_json' in this scope`.

- [ ] **Step 4: Implement**

Add to `crates/greentic-runner-host/src/http/admin.rs`, after the `status` handler:

```rust
/// Serialise a capability registry's offerings into the `/admin/capabilities`
/// response body.
///
/// CROSS-REPO CONTRACT. `greentic-designer-admin` parses this exact shape to
/// preflight guardrail policies (see that repo's
/// `docs/superpowers/specs/2026-07-15-guardrail-policy-preflight-design.md`).
/// Renaming a key does not fail its build — the preflight silently degrades to
/// "no capabilities offered". Change this shape only alongside that consumer.
///
/// Sorted by `(cap_id, extension_id)`: `offerings()` walks a `HashMap`, so the
/// order is otherwise arbitrary between calls.
#[cfg(feature = "agentic-worker")]
fn offerings_to_json(registry: &greentic_ext_runtime::CapabilityRegistry) -> serde_json::Value {
    let mut offerings: Vec<&greentic_ext_runtime::OfferedBinding> = registry.offerings().collect();
    offerings.sort_by(|a, b| {
        a.cap_id
            .to_string()
            .cmp(&b.cap_id.to_string())
            .then_with(|| a.extension_id.cmp(&b.extension_id))
    });
    let caps: Vec<serde_json::Value> = offerings
        .into_iter()
        .map(|offering| {
            json!({
                "extension_id": offering.extension_id,
                "cap_id": offering.cap_id.to_string(),
                "version": offering.version.to_string(),
                "kind": offering.kind,
            })
        })
        .collect();
    json!({ "capabilities": caps })
}
```

Note: `offering.cap_id` is a `CapabilityId` (not a `String`) and `offering.version` is a `semver::Version` — both need `.to_string()`. `offering.kind` is an `ExtensionKind`, which derives `Serialize` with renames (`Design` → `"DesignExtension"`), so it serialises directly.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p greentic-runner-host --lib offerings_to_json`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add crates/greentic-runner-host/src/http/admin.rs crates/greentic-runner-host/Cargo.toml
git commit -m "feat: add offerings_to_json for the admin capabilities contract

Serialises CapabilityRegistry offerings into the shape
greentic-designer-admin will parse to preflight guardrail policies.
Sorted, because offerings() walks a HashMap. The literal assertion is
the contract: a key rename degrades the consumer silently, so it must
fail here."
```

---

### Task 3: Wire `ext_runtime` + `GET /admin/capabilities`

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/mod.rs` (field, construction, route, test)
- Modify: `crates/greentic-runner-host/src/http/admin.rs` (handler)
- Modify: `crates/greentic-runner-host/src/http/auth.rs:137-138`, `src/http/health.rs:115-116` (test helpers)

**Interfaces:**
- Consumes: `router(state)` from Task 1; `offerings_to_json(&registry)` from Task 2.
- Produces: `GET /admin/capabilities` → `200 {"capabilities":[…]}`.

**Known coverage gap, stated deliberately.** These tests exercise the handler's
`None` branch only. Covering the `Some(runtime)` branch would mean constructing a
real `ExtensionRuntime`, which compiles WASM — `ExtensionRuntime` offers no way to
inject a pre-built registry. What that leaves untested is only the one-line glue
`offerings_to_json(&runtime.capability_registry())`; the mapping itself is covered
by Task 2 against a hand-built registry, and a wrong deref here fails to compile
rather than failing at runtime. The `Some` branch is genuinely exercised by the
live `curl` in Task 5 — which is why that step is mandatory and not a nicety.

- [ ] **Step 1: Write the failing test**

Append inside the `router_tests` module created in Task 1 (`src/runner/mod.rs`):

```rust
    /// Anti-regression for the greentic-designer #796 class of bug: a handler
    /// that exists but is never registered. Without the `.route(...)` line this
    /// returns 404.
    #[tokio::test]
    async fn assembled_router_serves_admin_capabilities() {
        let app = router(ServerState::for_test());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/capabilities")
                    .extension(loopback())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // `ServerState::for_test()` has no ext_runtime, so the list is empty —
        // but the envelope key must still be present and an array.
        assert_eq!(body, serde_json::json!({ "capabilities": [] }));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p greentic-runner-host --lib router_tests::assembled_router_serves_admin_capabilities`
Expected: FAIL — `assertion left == right failed: left: 404, right: 200`.

- [ ] **Step 3: Add the `ext_runtime` field to `ServerState`**

In `crates/greentic-runner-host/src/runner/mod.rs`, add as the **last** field of `pub struct ServerState` (after `stream_observers`, matching the fully-qualified-path style that avoids a cfg-gated import):

```rust
    /// Extension runtime backing `GET /admin/capabilities`, so an operator
    /// console can see which capabilities this runner actually has installed.
    ///
    /// Built once at server-build time. This is a *separate* instance from the
    /// per-tenant runtimes in `agent_node::build_ext_runtime`, which need
    /// per-tenant secrets backends — so it costs one extra set of WASM loads at
    /// boot. All tenants scan the same `GREENTIC_EXTENSIONS_DIR/design/`, so the
    /// registries are identical and a process-level answer is correct.
    ///
    /// `None` when the runtime could not be built (e.g. no extension directory);
    /// the handler then reports an empty list rather than failing.
    #[cfg(feature = "agentic-worker")]
    pub ext_runtime: Option<std::sync::Arc<greentic_ext_runtime::ExtensionRuntime>>,
```

- [ ] **Step 4: Populate it at the construction site**

In `HostServer::with_sql`, inside the `let state = ServerState { … };` literal (`src/runner/mod.rs:86-101`), add after the `stream_observers` line and before `host,`:

```rust
            // Built here rather than reused from `host`: the per-tenant runtimes
            // are constructed later, inside `TenantRuntime`, with per-tenant
            // secrets. Blocking during boot is intentional and bounded — it runs
            // once, before the listener binds.
            #[cfg(feature = "agentic-worker")]
            ext_runtime: crate::runner::agent_node::build_ext_runtime(
                std::sync::Arc::new(crate::runner::agent_node::EnvSecretsBackend),
                None,
            ),
```

- [ ] **Step 5: Add `ext_runtime: None` to all four test helpers**

The field is gated, so every helper that literal-constructs `ServerState` must set it. Add this line next to each existing `stream_observers` line:

```rust
            #[cfg(feature = "agentic-worker")]
            ext_runtime: None,
```

in each of:
- `crates/greentic-runner-host/src/runner/mod.rs` (`ServerState::for_test`, ~line 182)
- `crates/greentic-runner-host/src/http/admin.rs` (`state()`, ~line 101)
- `crates/greentic-runner-host/src/http/auth.rs` (`server_state()`, ~line 137)
- `crates/greentic-runner-host/src/http/health.rs` (`state()`, ~line 115)

- [ ] **Step 6: Add the handler**

In `crates/greentic-runner-host/src/http/admin.rs`, after `offerings_to_json`:

```rust
/// `GET /admin/capabilities` — report the capabilities installed on this runner.
///
/// Consumed by `greentic-designer-admin` to preflight a mandatory guardrail
/// policy before saving it: a policy naming a cap absent from this list will
/// fail closed at runtime (`greentic-aw-runtime` `loop.rs`), blocking every
/// agent turn in scope.
#[cfg(feature = "agentic-worker")]
pub async fn capabilities(
    _: AdminGuard,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    let body = match &state.ext_runtime {
        Some(runtime) => offerings_to_json(&runtime.capability_registry()),
        None => json!({ "capabilities": [] }),
    };
    (StatusCode::OK, Json(body))
}
```

- [ ] **Step 7: Register the route**

In the `router()` function from Task 1, extend the existing `agentic-worker` gated block so it reads:

```rust
    #[cfg(feature = "agentic-worker")]
    let router = router
        .route(
            "/agent/chat/stream",
            post(crate::http::agent_chat::agent_chat_stream),
        )
        .route("/admin/capabilities", get(admin::capabilities));
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cargo test -p greentic-runner-host --lib router_tests`
Expected: PASS (2 tests).

Run: `cargo check -p greentic-runner-host --no-default-features --features verify`
Expected: success — the lean build has no `ext_runtime` field, no handler, no route.

- [ ] **Step 9: Commit**

```bash
git add crates/greentic-runner-host/src/runner/mod.rs \
        crates/greentic-runner-host/src/http/admin.rs \
        crates/greentic-runner-host/src/http/auth.rs \
        crates/greentic-runner-host/src/http/health.rs
git commit -m "feat: add GET /admin/capabilities to the runner admin API

Reports the capabilities installed on this runner so an operator console
can preflight a mandatory guardrail policy, instead of accepting one that
fails closed at runtime and blocks every agent turn in scope.

ServerState carries a process-level ExtensionRuntime built at
server-build time; the per-tenant runtimes need per-tenant secrets and
are built later, so this costs one extra set of WASM loads at boot.

The registration test asserts 200 rather than 404 through the assembled
router — the failure mode this guards is a handler that exists but is
never wired."
```

---

### Task 4: Log registered cap ids at boot

Independent of the endpoint and useful on its own: today the boot log reports a bare count, so "which caps are loaded" is invisible until a runtime failure surfaces it.

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/agent_node.rs:920`

**Interfaces:**
- Consumes: nothing. Produces: nothing consumed by other tasks.

- [ ] **Step 1: Make the change**

In `crates/greentic-runner-host/src/runner/agent_node.rs`, replace line 920:

```rust
                tracing::info!(loaded, dir = %design_dir.display(), "loaded design extensions");
```

with:

```rust
                // Log the cap ids, not just a count: an unresolved mandatory
                // guardrail cap blocks every agent turn, and until now the only
                // way to see which caps a runner has was to trigger that failure.
                let mut cap_ids: Vec<String> = runtime
                    .capability_registry()
                    .offerings()
                    .map(|offering| offering.cap_id.to_string())
                    .collect();
                cap_ids.sort();
                tracing::info!(
                    loaded,
                    dir = %design_dir.display(),
                    caps = %cap_ids.join(","),
                    "loaded design extensions"
                );
```

`runtime` is in scope at this point (it is used at line 912 and moved into `Arc::new(runtime)` only at line 927).

- [ ] **Step 2: Verify it compiles clean**

Run: `cargo clippy -p greentic-runner-host --all-targets -- -D warnings`
Expected: success, zero warnings.

- [ ] **Step 3: Commit**

```bash
git add crates/greentic-runner-host/src/runner/agent_node.rs
git commit -m "feat: log registered capability ids when design extensions load

The boot log reported a bare count, so which caps a runner actually has
was invisible until an unresolved mandatory guardrail blocked a turn."
```

---

### Task 5: Full gate + live verification

The contract this PR publishes is consumed by another repo. A green test suite here does not prove the shape is right — the fixtures are ours.

**Files:** none modified.

- [ ] **Step 1: Run the full local gate**

Run: `cargo fmt --all --check`
Expected: no diff.

Run: `cargo clippy --all-targets --all-features -- -D warnings`
Expected: zero warnings.

Run: `cargo test -p greentic-runner-host`
Expected: all pass, including the 2 `router_tests` and 2 `offerings_to_json` tests.

> If `--all-features` fails on `surrealdb-librocksdb-sys` (`stdbool.h` not found), that is a known environment issue behind default-off `*-chronicle` features, not this change. Fall back to `cargo clippy --all-targets -- -D warnings` and note it in the PR.

- [ ] **Step 2: Verify against a real runner**

Start a runner with extensions installed and query the endpoint:

```bash
GREENTIC_EXTENSIONS_DIR=~/.greentic/extensions \
GREENTIC_CHANNEL=research \
cargo run -p greentic-runner -- --port 8787 &
curl -s http://127.0.0.1:8787/admin/capabilities | jq .
```

Expected: `{"capabilities":[{"extension_id":"…","cap_id":"greentic:guardrail/pii","version":"…","kind":"DesignExtension"}, …]}` — with the guardrail caps present.

Two things to confirm, both of which would otherwise surface as a silent failure in PR-2:

1. The response body's keys match the literal asserted in Task 2 **exactly**. Copy the real body into the PR description; PR-2's stub must reuse it verbatim.
2. If the list is empty or short, check the channel. Packs versioned `-research` are skipped on the default `Main` channel — an empty list may mean a channel mismatch, not a missing endpoint. `GREENTIC_CHANNEL=research` is set above for that reason.

Loopback needs no token (`AdminAuth` with no token allows loopback); a non-loopback caller needs `Authorization: Bearer $ADMIN_TOKEN`.

- [ ] **Step 3: Open the PR**

```bash
git push -u origin feat/admin-capabilities-endpoint
gh pr create --base research \
  --title "feat: add GET /admin/capabilities to the runner admin API" \
  --body "$(cat <<'EOF'
Reports capabilities installed on a runner so greentic-designer-admin can
preflight a mandatory guardrail policy instead of accepting one that fails
closed at runtime and blocks every agent turn in scope.

PR-1 of two. Spec:
greentic-designer-admin/docs/superpowers/specs/2026-07-15-guardrail-policy-preflight-design.md

## Contract

Consumed cross-repo. The exact body from a live runner is below; PR-2's test
stub must reuse it verbatim. A key rename here degrades that preflight
silently rather than failing its build.

```json
PASTE THE VERBATIM `curl` OUTPUT FROM TASK 5 STEP 2 HERE.
Do not hand-write this block from the spec or from Task 2's test literal —
the point is to publish what the runner actually emitted. If it differs from
Task 2's assertion, that is a real defect: fix the code, not the paste.
```

## Notes

- All new surface is gated on `agentic-worker`; `--no-default-features` still builds.
- `ServerState` now carries a process-level `ExtensionRuntime`, costing one extra
  set of WASM loads at boot. Per-tenant runtimes need per-tenant secrets and are
  built later, so reuse was not available. Trade accepted in the spec to avoid a
  new function in `greentic-ext-runtime` and the release-train cascade it implies.
- The router was extracted into `router(state)` so the assembled router is
  testable; the registration test asserts 200 not 404.
- No designer bump: the designer pins this repo by rev, so its pin does not move.
EOF
)"
```

---

## What this plan does NOT do

- **No admin-side preflight.** That is PR-2, authored after this ships, against the shape verified in Task 5.
- **No change to the runtime's fail-closed behaviour.** An unresolved mandatory guardrail still denies. That is the governance guarantee, out of scope per the spec.
- **No per-environment capability view.** The admin will probe a single reference runner. Multi-env fan-out was considered and rejected in the spec.
