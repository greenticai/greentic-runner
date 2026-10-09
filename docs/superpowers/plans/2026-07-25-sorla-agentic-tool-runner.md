# SoRLa agentic-worker tool — PR-1 (greentic-runner) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement
> this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `sorla:<pack>` tool family to `greentic-aw-runtime` + `greentic-runner-host` so a
deployed/sidecar `dw.agent` can invoke a SoR's SoRX **BusinessAction** as an LLM tool, dispatched to
the shipped `POST /admin/v1/capabilities/invoke`.

**Architecture:** Mirror the existing `component:` tool family end-to-end. New `sorla_source.rs`
(trait + catalog + source) in aw-runtime; a fourth `sorla:` prefix arm in the three tool seams; an
`AgentRuntime::with_sorla_source` builder; and a runner-host `SorxHttpInvoker` (the only HTTP-bearing
piece) wired via `sorla_source_from_env()`. aw-runtime stays HTTP-client-free.

**Tech Stack:** Rust (edition per `rust-toolchain.toml`), `tokio`, `serde_json`, `dashmap`,
`reqwest`/existing runner-host HTTP client, `async-trait`-free (hand-rolled `Pin<Box<dyn Future>>`
like `ComponentInvoker`).

Design doc: `docs/superpowers/specs/2026-07-25-sorla-agentic-worker-tool-design.md`.

## Global Constraints

- **No `unwrap()`/`panic!()` in production paths.** The dispatch/invoke path must NEVER return `Err`
  from `dispatch_tool_call` nor panic — an unknown route or invoker failure becomes
  `Ok(json!({"error": ...}))`, exactly like the `component:`/`mcp:`/`flow:` arms.
- **No Claude co-author attribution** on commits or PRs (`greentic-runner` CLAUDE.md:382).
- **aw-runtime must not gain an HTTP/SoRX-client dependency** — the HTTP client lives in
  `greentic-runner-host` behind the `SorxInvoker` trait (the `component_source.rs` module doc states
  this contract for `component:`; hold it for `sorla:`).
- English only; Conventional Commits (`feat:`).
- Default behavior unchanged when `GREENTIC_AW_SORLA_TOOLS=0` or `GREENTIC_AW_SORX_URL` unset (source
  is `None` → no `sorla:` tools; identical to today).
- `bash ci/local_check.sh` green before done (fmt + clippy `-D warnings` + tests).
- Mirror the sibling `component:`/`flow:` code precisely (naming, doc-comment density, resilience
  contract). Reviewers will compare against `component_source.rs`.

---

### Task 1: `sorla_source.rs` — trait, catalog, source (aw-runtime)

New self-contained module mirroring `crates/greentic-aw-runtime/src/component_source.rs`. Compiles
and unit-tests in isolation; no wiring yet.

**Files:**
- Create: `crates/greentic-aw-runtime/src/sorla_source.rs`
- Modify: `crates/greentic-aw-runtime/src/lib.rs` (add `pub mod sorla_source;` near the other
  `pub mod *_source;` declarations, ~line 46-47 area; and the `pub use` in Task 2)
- Test: inline `#[cfg(test)]` in `sorla_source.rs` (mirror component_source's `test_support`)

**Interfaces:**
- Produces (consumed by Tasks 2-3):
  ```rust
  pub struct SorxOperation { pub pack: String, pub action: String, pub description: String, pub parameters: serde_json::Value, pub cap_uri: String }
  pub struct SorlaToolEntry { pub description: String, pub parameters: serde_json::Value }
  pub trait SorxInvoker: Send + Sync {
      fn list_operations(&self) -> Vec<SorxOperation>;
      fn invoke<'a>(&'a self, pack: &'a str, action: &'a str, args_json: &'a str)
          -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;
  }
  pub struct SorlaToolCatalog { /* (pack, action) -> SorlaToolEntry + invoker + fetched_at */ }
  pub struct SorlaToolSource { /* invoker + per-tenant DashMap cache, CATALOG_TTL 5min */ }
  ```
  Key by `(pack, action)` (the suffix after `sorla:` is `pack`, `tool_name` is `action`).

- [ ] **Step 1: Write the module by mirroring `component_source.rs`.**
  Copy `component_source.rs` verbatim, then apply these exact renames + the one field addition:
  - `ComponentOperation` → `SorxOperation`; its `component_ref` field → `pack`, `operation` → `action`;
    **add** `pub cap_uri: String` (the SoRX capability string; carried so the invoker impl in
    runner-host can resolve it — the catalog itself ignores `cap_uri` for list/dispatch keying).
  - `ComponentToolEntry` → `SorlaToolEntry` (unchanged fields).
  - `ComponentInvoker` → `SorxInvoker`; method params `component_ref`/`operation` → `pack`/`action`.
  - `ComponentToolCatalog` → `SorlaToolCatalog`; `ComponentToolSource` → `SorlaToolSource`.
  - In `from_invoker`, key insertion uses `(op.pack, op.action)`.
  - `dispatch(component_ref, operation, args_json)` → `dispatch(pack, action, args_json)`; the
    `"unknown component tool '{...}/{...}'"` message → `"unknown sorla tool '{pack}/{action}'"`.
  - Module doc: describe `sorla:<pack>` resolving to a SoR BusinessAction invoked over the host SoRX
    interact client behind `SorxInvoker`; keep the two resilience-contract bullets verbatim
    (infallible `catalog`, `dispatch` never panics/`Err`).
  - `test_support::FakeInvoker` → keep, renamed to `SorxInvoker`; its `ComponentOperation` list →
    `SorxOperation` (populate `cap_uri` with any test string).

- [ ] **Step 2: Add `pub mod sorla_source;` to `lib.rs`** beside `pub mod flow_source;` /
  `pub mod component_source;` (search `pub mod component_source` — it may be implicit via the
  `pub use` block at ~line 65; if there is no `pub mod component_source;` line, components are
  declared elsewhere — add `mod sorla_source;` + the `pub use` in Task 2 to match component's
  visibility exactly).

- [ ] **Step 3: Write failing tests** (mirror component_source's tests, adapted):
  ```rust
  #[tokio::test]
  async fn catalog_dispatch_routes_to_invoker() {
      let inv = Arc::new(test_support::FakeInvoker::new(
          vec![SorxOperation { pack: "landlord".into(), action: "record_rent_payment".into(),
                description: "Record a rent payment".into(), parameters: serde_json::json!({"type":"object"}),
                cap_uri: "cap://greentic/business-functions/landlord/record_rent_payment/v0.1.0".into() }],
          Ok(serde_json::json!({"id":"pay-1"})),
      ));
      let src = SorlaToolSource::new(inv);
      let cat = src.catalog(&test_tenant()).await;
      let out = cat.dispatch("landlord", "record_rent_payment", "{}").await;
      assert_eq!(out, serde_json::json!({"id":"pay-1"}));
  }

  #[tokio::test]
  async fn catalog_dispatch_unknown_is_error_value_not_panic() {
      let src = SorlaToolSource::new(Arc::new(test_support::FakeInvoker::new(vec![], Ok(serde_json::json!({})))));
      let cat = src.catalog(&test_tenant()).await;
      let out = cat.dispatch("nope", "nope", "{}").await;
      assert!(out.get("error").is_some());
  }

  #[tokio::test]
  async fn catalog_is_ttl_cached_per_tenant() {
      let inv = Arc::new(test_support::FakeInvoker::new(vec![], Ok(serde_json::json!({}))));
      let src = SorlaToolSource::new(inv.clone());
      let _ = src.catalog(&test_tenant()).await;
      let _ = src.catalog(&test_tenant()).await; // within TTL -> no re-list
      assert_eq!(inv.list_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
  }
  ```
  Provide `test_tenant()` mirroring how `component_source`/`tools.rs` tests build a `TenantContext`
  (reuse the same constructor — grep `TenantContext` in existing tests).

- [ ] **Step 4: Run tests → verify RED, then GREEN.**
  Run: `cargo test -p greentic-aw-runtime sorla_source -- --nocapture`
  Expected: fails before the module exists / compiles, passes after.

- [ ] **Step 5: `cargo fmt -p greentic-aw-runtime` + `cargo clippy -p greentic-aw-runtime --all-targets -- -D warnings`.**

- [ ] **Step 6: Commit** — `feat(aw-runtime): add sorla_source (SorxInvoker trait + catalog + source)`

---

### Task 2: thread `sorla:` through the three tool seams + builder (aw-runtime)

Atomic signature change: add a `sorla` param + arm to `list_tools_for_llm`, `missing_tools`,
`dispatch_tool_call`; add the `AgentRuntime.sorla` field + `with_sorla_source`; resolve + thread the
catalog in `loop.rs`. Everything must compile together.

**Files:**
- Modify: `crates/greentic-aw-runtime/src/tools.rs` (3 functions + their tests' call sites)
- Modify: `crates/greentic-aw-runtime/src/lib.rs` (field ~191, constructor default ~241,
  `with_sorla_source` beside `with_flow_source` ~291, `pub use sorla_source::{...}` beside the
  flow_source `pub use` ~76)
- Modify: `crates/greentic-aw-runtime/src/loop.rs` (resolve `sorla_catalog` ~293; thread into
  `missing_tools` ~307, `list_tools_for_llm` ~348, `dispatch_tool_call` ~599)

**Interfaces:**
- Consumes: `SorlaToolCatalog`, `SorlaToolSource` (Task 1).
- Produces: `AgentRuntime::with_sorla_source(self, Option<Arc<SorlaToolSource>>) -> Self`.

- [ ] **Step 1: `lib.rs` — field + builder + export.**
  - Field beside `flows` (~197): `pub(crate) sorla: Option<Arc<crate::sorla_source::SorlaToolSource>>,`
    with a doc comment mirroring the `flows`/`components` ones.
  - Constructor default beside `flows: None` (~242): `sorla: None,`.
  - Builder beside `with_flow_source` (~291):
    ```rust
    /// Attach a per-tenant SoRLa SoR tool source: `sorla:<pack>` tool refs
    /// resolve to a SoR BusinessAction invoked over the host SoRX interact
    /// client. Coexists with the mcp/component/flow sources.
    #[must_use]
    pub fn with_sorla_source(mut self, sorla: Option<Arc<crate::sorla_source::SorlaToolSource>>) -> Self {
        self.sorla = sorla;
        self
    }
    ```
  - `pub use sorla_source::{SorxInvoker, SorxOperation, SorlaToolCatalog, SorlaToolEntry, SorlaToolSource};`
    beside the `flow_source` `pub use` (~76).

- [ ] **Step 2: `tools.rs` — add the `sorla:` arm to all three functions.**
  In `list_tools_for_llm` add param `sorla: Option<&SorlaToolCatalog>` (after `flows`), and an arm
  **identical to the `component:` arm** but `strip_prefix("sorla:")`, keyed via
  `sorla.and_then(|c| c.tool_entry(pack, &t.tool_name))`, warn text "sorla tool not found in catalog;
  dropping from LLM tool list". Same for `missing_tools` (param + arm, reason "sorla tool not found
  in the catalog"). **`sorla` is the LAST catalog param everywhere** — new signatures:
  `list_tools_for_llm(ext_runtime, mcp, components, flows, sorla, allowed)`,
  `missing_tools(ext_runtime, mcp, components, flows, sorla, allowed)`,
  `dispatch_tool_call(ext_runtime, mcp, components, flows, sorla, call, tenant)` with
  `sorla: Option<Arc<SorlaToolCatalog>>`. The dispatch arm mirrors the `component:` dispatch arm:
  ```rust
  if let Some(pack) = call.extension_id.strip_prefix("sorla:") {
      let value = match sorla.as_deref() {
          Some(cat) => cat.dispatch(pack, &call.tool_name, &call.args.to_string()).await,
          None => {
              tracing::warn!(pack = %pack, tool = %call.tool_name,
                  "sorla call has no catalog wired; returning error value");
              serde_json::json!({ "error": format!("unknown sorla tool '{}/{}'", pack, call.tool_name) })
          }
      };
      return Ok(value);
  }
  ```
  Place each `sorla:` arm immediately after the `component:` arm (before `flow:`), keeping order
  stable. Add `use crate::sorla_source::SorlaToolCatalog;` at the top (beside the
  `ComponentToolCatalog` import ~line 23).

- [ ] **Step 3: `tools.rs` — update the existing test call sites.**
  Every `list_tools_for_llm(...)` / `missing_tools(...)` / `dispatch_tool_call(...)` call in the test
  module now needs the extra `None` arg in the new position (after the `components`/`flows` arg as
  applicable). Update them so the crate compiles (they are at the lines listed in the design doc's
  probe; let the compiler point them out).

- [ ] **Step 4: `loop.rs` — resolve + thread the catalog.**
  After the `flow_catalog` resolution (~298) add:
  ```rust
  // Resolve the per-tenant SoRLa SoR tool catalog once per step (mirrors the
  // component/flow catalogs above). Infallible + TTL-cached; `None` source →
  // no `sorla:` tools at all.
  let sorla_catalog = match runtime.sorla.as_ref() {
      Some(src) => Some(src.catalog(&tenant).await),
      None => None,
  };
  ```
  Then add `sorla_catalog.as_deref(),` to the `missing_tools(...)` call (~307, after
  `flow_catalog.as_deref()`), `sorla_catalog.as_deref(),` to `list_tools_for_llm(...)` (~348), and
  `sorla_catalog.clone(),` to `dispatch_tool_call(...)` (~599, after `flow_catalog.clone()`).

- [ ] **Step 5: Write a failing test** in `tools.rs` proving the `sorla:` dispatch routes (mirror
  `dispatch_routes_component_ref` at ~1127):
  ```rust
  #[tokio::test]
  async fn dispatch_routes_sorla_ref() {
      let catalog = /* SorlaToolCatalog::for_tests with one ("landlord","record_rent_payment") entry + FakeInvoker returning {"ok":true} */;
      let call = ToolCallRecord { extension_id: "sorla:landlord".into(), tool_name: "record_rent_payment".into(), /* args {} */ .. };
      // args order: (ext, mcp, components, flows, sorla, call, tenant)
      let out = dispatch_tool_call(rt.clone(), None, None, None, Some(catalog), call, &tc).await.unwrap();
      assert_eq!(out, serde_json::json!({"ok":true}));
  }
  ```
  (Add a `#[cfg(test)] pub(crate) fn for_tests(...)` to `SorlaToolCatalog` mirroring
  `ComponentToolCatalog::for_tests` if not already added in Task 1 — add it in Task 1 Step 1 to be safe.)
  Also add `list_includes_sorla_ref` mirroring `component_ref_listed_from_catalog` (~1049).

- [ ] **Step 6: Run tests → RED then GREEN.**
  Run: `cargo test -p greentic-aw-runtime -- --nocapture`  (whole crate, since signatures changed)
  Expected: all green including the new `sorla` tests.

- [ ] **Step 7: fmt + clippy `-D warnings` (whole crate).**

- [ ] **Step 8: Commit** — `feat(aw-runtime): route sorla: tool refs through list/missing/dispatch + with_sorla_source`

---

### Task 3: `SorxHttpInvoker` + `sorla_source_from_env` + runner-host wiring

The only HTTP-bearing piece. Implements `SorxInvoker` against `GET/POST /admin/v1/capabilities`.

**Files:**
- Create: `crates/greentic-runner-host/src/runner/sorx_invoker.rs` (`SorxHttpInvoker`)
- Modify: `crates/greentic-runner-host/src/runner/agent_node.rs` (`sorla_source_from_env` beside
  `component_source_from_packs` ~508; wire `.with_sorla_source(...)` at ~1267-1268; module
  re-export ~2977)
- Modify: `crates/greentic-runner-host/src/runner/mod.rs` (or wherever `component_invoker` is
  declared) to add `mod sorx_invoker;`
- Test: inline `#[cfg(test)]` in `sorx_invoker.rs` (mock HTTP server) + an env-gate test in
  `agent_node.rs` tests (mirror `mcp_source_from_env_default_on_with_opt_out` ~2366 /
  `flow_source_from_packs` returns-None tests ~2696).

**Interfaces:**
- Consumes: `greentic_aw_runtime::{SorxInvoker, SorxOperation, SorlaToolSource}` (Tasks 1-2).
- Produces: `sorla_source_from_env() -> Option<Arc<greentic_aw_runtime::SorlaToolSource>>` (async).

- [ ] **Step 1: `SorxHttpInvoker` — capability fetch + invoke.**
  ```rust
  pub(crate) struct SorxHttpInvoker { base_url: String, ops: Vec<SorxOperation>, cap_by_key: HashMap<(String,String), String> }

  impl SorxHttpInvoker {
      /// Fetch GET {base}/admin/v1/capabilities once, keep business-action
      /// offers, build ops + cap map. Returns None-worthy empty on any fetch/
      /// parse failure (logged) so a down SoR never breaks worker startup.
      pub(crate) async fn fetch(base_url: String) -> Self { /* reqwest GET; for each offer whose
          contracts contain "greentic.sorx.business-action.invoke.v1": parse cap:// -> (pack, action),
          description from metadata.action or a default, parameters from the offer's input schema
          (metadata.execution / a generic {"type":"object"} fallback); push SorxOperation + cap_by_key */ }
  }

  impl SorxInvoker for SorxHttpInvoker {
      fn list_operations(&self) -> Vec<SorxOperation> { self.ops.clone() }
      fn invoke<'a>(&'a self, pack: &'a str, action: &'a str, args_json: &'a str)
          -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
          Box::pin(async move {
              let cap = self.cap_by_key.get(&(pack.to_string(), action.to_string()))
                  .ok_or_else(|| format!("no capability for sorla tool '{pack}/{action}'"))?;
              let input: serde_json::Value = serde_json::from_str(args_json).map_err(|e| e.to_string())?;
              let body = serde_json::json!({ "capability": cap, "input": input,
                  "context": { "tenant_id": /* tenant */, "caller_id": "dw-agent", "roles": [] }, "dry_run": false });
              // POST {base}/admin/v1/capabilities/invoke with X-Greentic-Tenant-Id/-Caller-Id/-Caller-Role headers.
              // Map: 200 ok:true -> result; 202 status==approval_required -> {"status":"approval_required","approval":...};
              //      403 -> {"error":"denied",...}; 404 -> {"error":"capability_not_found",...}; other -> Err(status+body).
          })
      }
  }
  ```
  Parse the cap URI `cap://greentic/business-functions/<pack>/<action>/v<version>` to `(pack, action)`
  with a small helper (split on `/`, guard segment count — no `unwrap`). Tenant: SP1 uses a
  process/tenant-agnostic default or the `GREENTIC_AW_SORX_TENANT` env (document); the deployed
  worker path can pass its tenant later — keep the header build in one helper.

- [ ] **Step 2: `sorla_source_from_env()` (agent_node.rs, mirror `component_source_from_packs` ~508).**
  ```rust
  pub(crate) async fn sorla_source_from_env() -> Option<Arc<greentic_aw_runtime::SorlaToolSource>> {
      if std::env::var("GREENTIC_AW_SORLA_TOOLS").ok().as_deref() == Some("0") { return None; }
      let base = std::env::var("GREENTIC_AW_SORX_URL").ok().filter(|s| !s.is_empty())?;
      let invoker = Arc::new(crate::runner::sorx_invoker::SorxHttpInvoker::fetch(base).await);
      Some(Arc::new(greentic_aw_runtime::SorlaToolSource::new(invoker)))
  }
  ```

- [ ] **Step 3: Wire into `build_runtime_with_stores` (~1267).**
  After `.with_flow_source(flow_source_from_packs(&packs, &tenant))` (~1268) chain
  `.with_sorla_source(sorla_source_from_env().await)`. (`build_runtime_with_stores` is already
  `async`.) Add `sorla_source_from_env` to the module re-export list at ~2977 if `component_source_from_packs`
  is re-exported there.

- [ ] **Step 4: Write failing tests.**
  - `sorx_invoker.rs`: spin a mock HTTP server (use the crate's existing test HTTP helper — grep for
    how `mcp_source`/existing tests mock HTTP; else a tiny `tokio` TcpListener). Assert:
    (a) `fetch` on a caps response with one business-action offer builds one op + cap_by_key entry;
    (b) `invoke` happy `{"ok":true,"result":{...}}` → returns the `result`;
    (c) `202 {"status":"approval_required","approval":{...}}` → returns
        `{"status":"approval_required", ...}` (NOT `Err`);
    (d) `403` → `{"error":"denied",...}`; `404` → `{"error":"capability_not_found",...}`;
    (e) request carries `X-Greentic-Tenant-Id`/`-Caller-Id`/`-Caller-Role` headers.
  - `agent_node.rs` tests: `sorla_source_from_env().await` is `None` when `GREENTIC_AW_SORLA_TOOLS=0`;
    `None` when `GREENTIC_AW_SORX_URL` unset; `Some` when a URL is set (point at the mock or a
    non-connecting URL — `fetch` degrades to empty ops, still `Some`). Use the same env-var
    serialization guard the existing `mcp_source_from_env` tests use (they set/remove env vars).

- [ ] **Step 5: Run tests → RED then GREEN.**
  Run: `cargo test -p greentic-runner-host sorx_invoker` and
  `cargo test -p greentic-runner-host sorla_source_from_env`
  Expected: green.

- [ ] **Step 6: fmt + clippy `-D warnings` for `greentic-runner-host`.**

- [ ] **Step 7: Commit** — `feat(runner-host): SorxHttpInvoker + sorla_source_from_env wired into agent runtime`

---

## Final verification (before PR)

- [ ] `bash ci/local_check.sh` from the repo root — green (fmt + clippy `-D warnings` + tests). If it
  fails outside this change's scope (pre-existing), document it in the PR summary rather than hiding it.
- [ ] Confirm the default path is byte-unchanged: with no `GREENTIC_AW_SORX_URL`, `AgentRuntime.sorla`
  is `None` and no `sorla:` tool is listed or dispatched (the `None`-arm warn only fires if a
  `sorla:` ref is declared without a source).
- [ ] PR → `greentic-runner` `research`. Title `feat(runner): SoRLa SoR BusinessActions as dw.agent tools (sorla: family)`.
  Body: link the design doc; note the release-train follow-up (bump the aw-runtime `rev` in
  `greentic-designer/Cargo.toml` after merge, then PR-2 surfacing). NO Claude co-author trailer.

## Out of scope (this PR)

- Designer surfacing (PR-2, separate plan, after the rev-bump).
- Multi-SoR per worker / per-binding addressing (`ToolRef` widening) / deployment-registry resolution.
- In-process designer test-chat wiring (execution is via sidecar/deployed runner-host).
- Any `greentic-sorx` change.
