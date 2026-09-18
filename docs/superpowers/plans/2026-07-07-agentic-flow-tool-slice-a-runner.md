# Agentic Flow-Tool — Slice A (Runner) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Teach `greentic-runner` to expose a `flow:`-prefixed agent tool source — an agent's LLM can call a non-interactive same-pack flow as a tool — mirroring the existing `component:` source, with the LLM-facing contract derived from the flow itself.

**Architecture:** In `greentic-aw-runtime`, add a `FlowInvoker` trait + `FlowToolCatalog` + `FlowToolSource` (mirror of `ComponentInvoker`/`ComponentToolCatalog`/`ComponentToolSource`), an `AgentRuntime.flows` field + `with_flow_source` builder, and a `flow:` branch in `list_tools_for_llm`/`dispatch_tool_call` + per-turn catalog resolution in `loop.rs`. In `greentic-runner-host`, add a `PackRuntimeFlowInvoker` that resolves a `flow_ref` to a same-pack flow and runs it synchronously via a new `PackRuntime::run_flow_for_tool` (reuses the existing self-contained-engine `run_flow`, but errors on `Waiting`), env-gated and wired into `build_runtime_handler_with_stores`.

**Tech Stack:** Rust (edition per workspace), `async_trait`, `tokio`, `serde_json`, `DashMap`.

**Repo:** greentic-runner. Worktree `runner-flowinvoker`, branch `feat/agentic-flow-invoker` (from `origin/research`, HEAD e5c6b54f). PR to `research`. Crates under `crates/`.

## Global Constraints

- English only in source/tests/comments/logs. Conventional Commits. No `unwrap()`/`panic!()` in production paths — return the error as a value.
- The `flow:` tool source MIRRORS the `component:` source structurally. Tool dispatch NEVER returns `Err` for a routing/execution failure — it returns a `{"error": "..."}` JSON value so the LLM observes it (same contract as `dispatch_tool_call`'s component branch).
- LLM-facing contract is DERIVED FROM THE FLOW: `FlowToolCatalog` supplies `description` (from the flow's name/description) and `parameters` (the flow's own input schema, falling back to `{"type":"object"}`). The author-defined `tool_name` from the designer binding is used as the LLM tool name (it is `ToolRef.tool_name`); the catalog is keyed by `flow_ref` alone.
- Called flows MUST be non-interactive: a flow that returns `FlowStatus::Waiting` yields an error value, never a pause. Same-pack only (`flow_ref` resolves to a flow in a loaded pack).
- Env gate: `GREENTIC_AW_FLOW_TOOLS=0` disables the source (mirrors `GREENTIC_AW_COMPONENT_TOOLS`).
- `CARGO_BUILD_JOBS=2`. Heavy/silent cargo builds run in the FOREGROUND (a background idle-output watchdog kills long silent jobs). `greentic-aw-runtime` builds in ~3min cold (now warm); `greentic-runner-host` is heavier — budget accordingly.
- `greentic-aw-runtime` and `greentic-runner-host` are internal (`publish = false`); the designer consumes them by git rev — this branch does not publish, but its merged rev is what the designer will later pin.

## File map

- `crates/greentic-aw-runtime/src/flow_source.rs` — CREATE: `FlowInvoker` trait, `FlowOperation`, `FlowToolEntry`, `FlowToolCatalog`, `FlowToolSource`.
- `crates/greentic-aw-runtime/src/lib.rs` — MODIFY: register `mod flow_source` + re-export; add `flows` field to `AgentRuntime`; add `with_flow_source` builder.
- `crates/greentic-aw-runtime/src/tools.rs` — MODIFY: add `flows: Option<&FlowToolCatalog>` param + `flow:` branch to `list_tools_for_llm`; add `flows: Option<Arc<FlowToolCatalog>>` param + `flow:` branch to `dispatch_tool_call`.
- `crates/greentic-aw-runtime/src/loop.rs` — MODIFY: resolve the flow catalog per-turn; pass to `list_tools_for_llm`/`missing_tools`/`dispatch_tool_call`.
- `crates/greentic-runner-host/src/pack.rs` — MODIFY: add `run_flow_for_tool(flow_id, input) -> Result<Value, String>` (errors on `Waiting`).
- `crates/greentic-runner-host/src/runner/flow_invoker.rs` — CREATE: `PackRuntimeFlowInvoker` impl `FlowInvoker`.
- `crates/greentic-runner-host/src/runner/mod.rs` — MODIFY: `mod flow_invoker;`.
- `crates/greentic-runner-host/src/runner/agent_node.rs` — MODIFY: `flow_source_from_packs(&packs, &tenant)` + `.with_flow_source(...)` in `build_runtime_handler_with_stores`.

---

### Task 1: `FlowInvoker` trait + `FlowToolCatalog` + `FlowToolSource` (aw-runtime)

**Files:**
- Create: `crates/greentic-aw-runtime/src/flow_source.rs`
- Modify: `crates/greentic-aw-runtime/src/lib.rs` (register module + re-export)
- Test: unit tests inside `flow_source.rs`

**Interfaces:**
- Produces:
  ```rust
  pub trait FlowInvoker: Send + Sync {
      fn list_flows(&self) -> Vec<FlowOperation>;
      fn invoke<'a>(&'a self, flow_ref: &'a str, args_json: &'a str)
          -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;
  }
  pub struct FlowOperation { pub flow_ref: String, pub description: String, pub parameters: serde_json::Value }
  pub struct FlowToolEntry { pub description: String, pub parameters: serde_json::Value }
  pub struct FlowToolCatalog { /* keyed by flow_ref */ }
  impl FlowToolCatalog {
      pub fn tool_entry(&self, flow_ref: &str) -> Option<&FlowToolEntry>;
      pub async fn dispatch(&self, flow_ref: &str, args_json: &str) -> serde_json::Value;
      pub fn is_empty(&self) -> bool; pub fn len(&self) -> usize;
  }
  pub struct FlowToolSource { /* invoker + DashMap cache */ }
  impl FlowToolSource { pub fn new(invoker: Arc<dyn FlowInvoker>) -> Self; pub async fn catalog(&self, tenant: &TenantContext) -> Arc<FlowToolCatalog>; }
  ```

**Context:** This is a structural mirror of `crates/greentic-aw-runtime/src/component_source.rs` (`ComponentInvoker` :69-82, `ComponentToolCatalog` :87-172, `ComponentToolSource` :181-217). The key SIMPLIFICATION vs components: a flow's catalog is keyed by a SINGLE `flow_ref` string (components key by `(component_ref, operation)`), because a flow is the whole unit. `dispatch` mirrors `ComponentToolCatalog::dispatch` (:137-156) — it awaits `invoker.invoke`, and on `Err(msg)` returns `{"error": msg}` (never propagates `Err`). Read `component_source.rs` fully before writing; match its cache/TTL approach in `FlowToolSource::catalog`.

- [ ] **Step 1: Write the failing test**

Add at the bottom of `crates/greentic-aw-runtime/src/flow_source.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct FakeInvoker;
    impl FlowInvoker for FakeInvoker {
        fn list_flows(&self) -> Vec<FlowOperation> {
            vec![FlowOperation {
                flow_ref: "lookup".into(),
                description: "Look things up".into(),
                parameters: serde_json::json!({ "type": "object" }),
            }]
        }
        fn invoke<'a>(&'a self, flow_ref: &'a str, args_json: &'a str)
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
            Box::pin(async move {
                if flow_ref == "lookup" {
                    Ok(serde_json::json!({ "echoed": args_json }))
                } else {
                    Err(format!("flow '{flow_ref}' not found"))
                }
            })
        }
    }

    #[tokio::test]
    async fn catalog_lists_and_dispatches_flows() {
        let cat = FlowToolCatalog::from_invoker(Arc::new(FakeInvoker));
        assert_eq!(cat.len(), 1);
        let entry = cat.tool_entry("lookup").expect("entry");
        assert_eq!(entry.description, "Look things up");
        let out = cat.dispatch("lookup", "{\"q\":1}").await;
        assert_eq!(out["echoed"], "{\"q\":1}");
    }

    #[tokio::test]
    async fn dispatch_missing_flow_returns_error_value_not_err() {
        let cat = FlowToolCatalog::from_invoker(Arc::new(FakeInvoker));
        let out = cat.dispatch("nope", "{}").await;
        assert!(out.get("error").is_some(), "missing flow must yield an error value");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime flow_source 2>&1 | tail`
Expected: FAIL — module/types not found.

- [ ] **Step 3: Implement `flow_source.rs`**

Write the module mirroring `component_source.rs`. Full implementation:

```rust
//! Flow-as-agent-tool source. Mirrors `component_source` but a flow is the
//! whole tool unit, so the catalog is keyed by a single `flow_ref`. The
//! LLM-facing description + parameters are DERIVED FROM THE FLOW (supplied by
//! the host-side `FlowInvoker`), not from the agent config.
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::tenant::TenantContext;

const CATALOG_TTL: Duration = Duration::from_secs(60);

/// One flow offered as an agent tool, with its LLM-facing contract.
pub struct FlowOperation {
    pub flow_ref: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// A resolved catalog entry (description + JSON-schema parameters).
pub struct FlowToolEntry {
    pub description: String,
    pub parameters: serde_json::Value,
}

/// Host boundary: enumerate + invoke flows. The concrete impl lives in
/// runner-host (`PackRuntimeFlowInvoker`) and is injected at the edge.
pub trait FlowInvoker: Send + Sync {
    fn list_flows(&self) -> Vec<FlowOperation>;
    fn invoke<'a>(
        &'a self,
        flow_ref: &'a str,
        args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;
}

/// Per-tenant snapshot of flow tools, keyed by `flow_ref`.
pub struct FlowToolCatalog {
    tools: HashMap<String, FlowToolEntry>,
    invoker: Arc<dyn FlowInvoker>,
    fetched_at: Instant,
}

impl FlowToolCatalog {
    pub fn from_invoker(invoker: Arc<dyn FlowInvoker>) -> Self {
        let mut tools = HashMap::new();
        for op in invoker.list_flows() {
            tools.insert(
                op.flow_ref,
                FlowToolEntry { description: op.description, parameters: op.parameters },
            );
        }
        Self { tools, invoker, fetched_at: Instant::now() }
    }

    pub fn len(&self) -> usize { self.tools.len() }
    pub fn is_empty(&self) -> bool { self.tools.is_empty() }
    pub fn tool_entry(&self, flow_ref: &str) -> Option<&FlowToolEntry> { self.tools.get(flow_ref) }

    /// Run one flow. Always returns a JSON value: the flow output on success,
    /// or `{"error": ...}` on any failure — never an `Err`.
    pub async fn dispatch(&self, flow_ref: &str, args_json: &str) -> serde_json::Value {
        match self.invoker.invoke(flow_ref, args_json).await {
            Ok(value) => value,
            Err(msg) => serde_json::json!({ "error": msg }),
        }
    }

    fn is_fresh(&self) -> bool { self.fetched_at.elapsed() < CATALOG_TTL }
}

/// TTL-cached, per-tenant `FlowToolCatalog` factory.
pub struct FlowToolSource {
    invoker: Arc<dyn FlowInvoker>,
    cache: DashMap<String, Arc<FlowToolCatalog>>,
}

impl FlowToolSource {
    pub fn new(invoker: Arc<dyn FlowInvoker>) -> Self {
        Self { invoker, cache: DashMap::new() }
    }

    pub async fn catalog(&self, tenant: &TenantContext) -> Arc<FlowToolCatalog> {
        let key = tenant.tenant_id.clone();
        if let Some(existing) = self.cache.get(&key) {
            if existing.is_fresh() {
                return Arc::clone(existing.value());
            }
        }
        let fresh = Arc::new(FlowToolCatalog::from_invoker(Arc::clone(&self.invoker)));
        self.cache.insert(key, Arc::clone(&fresh));
        fresh
    }
}
```

(VERIFY: the `TenantContext` field used as the cache key — read how `ComponentToolSource::catalog` keys the `DashMap` (`component_source.rs:181-217`) and mirror it exactly, including the freshness/TTL handling. If `ComponentToolSource` keys by a composite tenant/env string, do the same here.)

- [ ] **Step 4: Register the module + re-export**

In `crates/greentic-aw-runtime/src/lib.rs`, next to the existing `mod component_source;` + its `pub use`, add:

```rust
mod flow_source;
pub use flow_source::{FlowInvoker, FlowOperation, FlowToolCatalog, FlowToolEntry, FlowToolSource};
```

(Match the exact `pub use` style used for `ComponentToolSource`/`ComponentInvoker` — grep `component_source` in lib.rs.)

- [ ] **Step 5: Run test to verify it passes**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime flow_source 2>&1 | tail`
Expected: PASS. Then `CARGO_BUILD_JOBS=2 cargo build -p greentic-aw-runtime` clean.

- [ ] **Step 6: Commit**

```bash
git add crates/greentic-aw-runtime/src/flow_source.rs crates/greentic-aw-runtime/src/lib.rs
git commit -m "feat(aw-runtime): FlowInvoker trait + FlowToolSource (flow-as-agent-tool catalog)"
```

---

### Task 2: `flow:` tool resolution + `AgentRuntime.with_flow_source` (aw-runtime)

**Files:**
- Modify: `crates/greentic-aw-runtime/src/lib.rs` (`AgentRuntime.flows` field + `with_flow_source`)
- Modify: `crates/greentic-aw-runtime/src/tools.rs` (`flows` param + `flow:` branch in both fns)
- Modify: `crates/greentic-aw-runtime/src/loop.rs` (per-turn flow catalog resolution)
- Test: unit tests in `tools.rs`

**Interfaces:**
- Consumes: `FlowToolCatalog` (Task 1), `ToolRef { extension_id, tool_name }` (config.rs:15), `LlmToolSchema` (llm.rs:27), `ToolCallRecord` (state.rs:105).
- Produces: `AgentRuntime::with_flow_source(Option<Arc<FlowToolSource>>) -> Self`; new signatures
  `list_tools_for_llm(ext_runtime, mcp, components, flows: Option<&FlowToolCatalog>, allowed)` and
  `dispatch_tool_call(ext_runtime, mcp, components, flows: Option<Arc<FlowToolCatalog>>, call, tenant)`.

**Context:** Mirror the `component:` branches exactly. `list_tools_for_llm` component branch is `tools.rs:80-94`; `dispatch_tool_call` component branch is `tools.rs:262-283`. The flow catalog is keyed by `flow_ref` ALONE (no operation) — `tool_entry(flow_ref)`. The LLM tool name stays `t.tool_name` (author's name); description+parameters come from the catalog entry. Adding a param to both fns means updating every caller: `loop.rs` (the call sites near :239-242) and any existing `tools.rs`/`loop.rs` tests. `with_flow_source` mirrors `with_component_source` (lib.rs:248-254); the `flows` field mirrors `components` (lib.rs:167).

- [ ] **Step 1: Write the failing test**

Add to the `tools.rs` test module (mirror the existing component dispatch/list tests — grep `component:` in `tools.rs` tests for the fixture style; reuse the `FakeInvoker`/catalog pattern or build a `FlowToolCatalog::from_invoker`):

```rust
#[tokio::test]
async fn flow_prefixed_tool_is_listed_and_dispatched() {
    // Build a FlowToolCatalog with one flow "lookup" via a fake invoker
    // (reuse flow_source::tests::FakeInvoker shape).
    let flows = std::sync::Arc::new(
        crate::flow_source::FlowToolCatalog::from_invoker(std::sync::Arc::new(test_flow_invoker())),
    );
    let allowed = vec![ToolRef { extension_id: "flow:lookup".into(), tool_name: "look_up".into() }];
    let schemas = list_tools_for_llm(&ext_runtime_stub(), None, None, Some(&flows), &allowed);
    assert!(schemas.iter().any(|s| s.extension_id == "flow:lookup" && s.tool_name == "look_up"));

    let call = ToolCallRecord {
        call_id: "c1".into(),
        extension_id: "flow:lookup".into(),
        tool_name: "look_up".into(),
        args: serde_json::json!({ "q": 1 }),
    };
    let out = dispatch_tool_call(ext_runtime_stub_arc(), None, None, Some(flows), call, &tenant_stub()).await.unwrap();
    assert!(out.get("error").is_none(), "known flow must dispatch, got {out}");
}
```

(Adapt `ext_runtime_stub`/`tenant_stub` to the existing helpers the component tests use — reuse them verbatim; if the component tests construct these inline, copy that construction.)

- [ ] **Step 2: Run test to verify it fails**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime flow_prefixed_tool 2>&1 | tail`
Expected: FAIL — `list_tools_for_llm`/`dispatch_tool_call` don't take a `flows` param yet.

- [ ] **Step 3: Add the `flows` field + builder**

In `lib.rs`, add to `AgentRuntime` (next to `components`, :167):
```rust
    pub(crate) flows: Option<Arc<crate::flow_source::FlowToolSource>>,
```
Initialize it to `None` wherever `AgentRuntime` is constructed (the `new` body sets `components: None`; set `flows: None` alongside). Add the builder next to `with_component_source` (:248-254):
```rust
    #[must_use]
    pub fn with_flow_source(mut self, flows: Option<Arc<crate::flow_source::FlowToolSource>>) -> Self {
        self.flows = flows;
        self
    }
```

- [ ] **Step 4: Add the `flow:` branch to `tools.rs`**

Add `flows: Option<&FlowToolCatalog>` param to `list_tools_for_llm` (after `components`) and, before the extension fallthrough, mirror the component branch (:80-94):
```rust
        if let Some(flow_ref) = t.extension_id.strip_prefix("flow:") {
            match flows.and_then(|c| c.tool_entry(flow_ref)) {
                Some(entry) => out.push(LlmToolSchema {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    description: entry.description.clone(),
                    parameters: entry.parameters.clone(),
                }),
                None => tracing::warn!(
                    extension = %t.extension_id, tool = %t.tool_name,
                    "flow tool not found in catalog; dropping from LLM tool list"
                ),
            }
            continue;
        }
```
Add `flows: Option<Arc<FlowToolCatalog>>` param to `dispatch_tool_call` (after `components`) and mirror the component dispatch branch (:262-283):
```rust
        if let Some(flow_ref) = call.extension_id.strip_prefix("flow:") {
            let value = match flows.as_deref() {
                Some(cat) => cat.dispatch(flow_ref, &call.args.to_string()).await,
                None => {
                    tracing::warn!(flow = %flow_ref, "flow call has no catalog wired; returning error value");
                    serde_json::json!({ "error": format!("unknown flow tool '{flow_ref}'") })
                }
            };
            return Ok(value);
        }
```
Also update `missing_tools` if it takes the same catalogs (grep it) to accept + consider `flows` (a `flow:` ref present in the catalog is not "missing").

- [ ] **Step 5: Thread `flows` through `loop.rs`**

In `loop.rs`, next to the component catalog resolution (:228-231), add:
```rust
    let flow_catalog = match runtime.flows.as_ref() {
        Some(src) => Some(src.catalog(&tenant).await),
        None => None,
    };
```
Pass `flow_catalog.as_deref()` to `list_tools_for_llm`/`missing_tools` and `flow_catalog.clone()` to `dispatch_tool_call`, matching how `component_catalog` is passed at those call sites.

- [ ] **Step 6: Run tests to verify they pass**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime 2>&1 | tail -15`
Expected: PASS (the new test + all existing aw-runtime tests, including component/mcp tests whose call sites you updated). Then `CARGO_BUILD_JOBS=2 cargo build -p greentic-aw-runtime` clean.

- [ ] **Step 7: Commit**

```bash
git add crates/greentic-aw-runtime/src/lib.rs crates/greentic-aw-runtime/src/tools.rs crates/greentic-aw-runtime/src/loop.rs
git commit -m "feat(aw-runtime): resolve and dispatch flow: agent tools (with_flow_source)"
```

---

### Task 3: `PackRuntimeFlowInvoker` + `run_flow_for_tool` (runner-host)

**Files:**
- Modify: `crates/greentic-runner-host/src/pack.rs` (`run_flow_for_tool`)
- Create: `crates/greentic-runner-host/src/runner/flow_invoker.rs`
- Modify: `crates/greentic-runner-host/src/runner/mod.rs` (`mod flow_invoker;`)
- Test: unit test in `flow_invoker.rs` (+ a `pack.rs` test if a fixture pack is available)

**Interfaces:**
- Consumes: `greentic_aw_runtime::{FlowInvoker, FlowOperation}` (Task 1); `PackRuntime` (`pack.rs`) — `list_flows()` (:1882), `run_flow` (:1905), `metadata().pack_id` (:2237), `load_flow` (:2218).
- Produces: `PackRuntime::run_flow_for_tool(&self, flow_id: &str, input: serde_json::Value) -> Result<serde_json::Value, String>`; `PackRuntimeFlowInvoker { packs: Vec<Arc<PackRuntime>>, tenant: String }` impl `FlowInvoker`; `pub(crate) fn flow_source_from_packs(packs: &[Arc<PackRuntime>], tenant: &str) -> Option<Arc<FlowToolSource>>` (added in Task 4's file, declared here as the invoker's consumer).

**Context:** `PackRuntime::run_flow` (:1905) already loads a fresh `FlowEngine` and executes the flow, but WRAPS `FlowStatus::Waiting` as a `{"status":"pending",...}` value. For an agent tool we need the non-interactive guarantee (mirror `execute_flow_call`'s bail at `engine.rs:1512`). `run_flow_for_tool` reuses the same load+execute but returns `Err("flow '<id>' tried to pause; agent tools must be non-interactive")` on `Waiting`, `Ok(output)` on `Completed`. `PackRuntimeFlowInvoker` mirrors `PackRuntimeComponentInvoker` (`component_invoker.rs:31-153`): `list_flows()` iterates packs → `pack.list_flows()` → `FlowOperation { flow_ref: descriptor.id, description: descriptor.description.unwrap_or(id), parameters: <flow input schema or {"type":"object"}> }`; `invoke(flow_ref, args)` finds the pack whose `list_flows()` contains `flow_ref`, parses `args_json` to `Value`, calls `pack.run_flow_for_tool(flow_ref, input)`.

- [ ] **Step 1: Write the failing test (invoker resolution)**

Create `crates/greentic-runner-host/src/runner/flow_invoker.rs` with a test that a `flow_ref` not present in any pack returns the not-found error (this is testable without a full pack fixture):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invoke_unknown_flow_returns_not_found_error() {
        let invoker = PackRuntimeFlowInvoker::new(Vec::new(), "acme".into());
        let out = invoker.invoke("nope", "{}").await;
        assert!(out.is_err(), "unknown flow must Err (folded to error value by the catalog)");
        assert!(out.unwrap_err().contains("nope"));
    }
}
```

- [ ] **Step 2: Run it — verify it fails**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host flow_invoker 2>&1 | tail`
Expected: FAIL — module/type not found.

- [ ] **Step 3: Add `run_flow_for_tool` to `PackRuntime`**

In `pack.rs`, next to `run_flow` (:1905), add a non-interactive variant. Reuse the SAME load + `FlowEngine::new` + `engine.execute(ctx, input)` body as `run_flow`, but the terminal match becomes:
```rust
    match execution.status {
        FlowStatus::Completed => Ok(execution.output),
        FlowStatus::Waiting(wait) => Err(format!(
            "flow '{flow_id}' tried to pause ({:?}); agent tools must be non-interactive",
            wait.reason
        )),
    }
```
Signature: `pub async fn run_flow_for_tool(&self, flow_id: &str, input: serde_json::Value) -> Result<serde_json::Value, String>`. Map any internal `anyhow` error to `Err(e.to_string())` (no `?`-propagation of a non-`String` error out of this signature). If `run_flow`'s body can be factored into a shared private helper returning `FlowExecution`, do that (DRY); otherwise duplicating the ~20-line load+execute is acceptable — note the choice in the report.

- [ ] **Step 4: Implement `PackRuntimeFlowInvoker`**

In `flow_invoker.rs`:
```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use greentic_aw_runtime::{FlowInvoker, FlowOperation};

use crate::pack::PackRuntime;

/// Host impl of `FlowInvoker` over the loaded packs. Resolves a `flow_ref` to a
/// same-pack flow and runs it synchronously (non-interactive) via
/// `PackRuntime::run_flow_for_tool`.
pub struct PackRuntimeFlowInvoker {
    packs: Vec<Arc<PackRuntime>>,
    tenant: String,
}

impl PackRuntimeFlowInvoker {
    pub fn new(packs: Vec<Arc<PackRuntime>>, tenant: String) -> Self {
        Self { packs, tenant }
    }
}

impl FlowInvoker for PackRuntimeFlowInvoker {
    fn list_flows(&self) -> Vec<FlowOperation> {
        let mut out = Vec::new();
        for pack in &self.packs {
            // list_flows is async on PackRuntime; block briefly on a current-thread
            // runtime OR use the manifest-backed sync path. Prefer the manifest
            // descriptors (pack.list_flows()) — see NOTE below.
            if let Ok(descriptors) = futures::executor::block_on(pack.list_flows()) {
                for d in descriptors {
                    out.push(FlowOperation {
                        flow_ref: d.id.clone(),
                        description: d.description.clone().unwrap_or_else(|| d.id.clone()),
                        parameters: serde_json::json!({ "type": "object" }),
                    });
                }
            }
        }
        out
    }

    fn invoke<'a>(&'a self, flow_ref: &'a str, args_json: &'a str)
        -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let input: serde_json::Value = serde_json::from_str(args_json)
                .map_err(|e| format!("invalid JSON args for flow '{flow_ref}': {e}"))?;
            for pack in &self.packs {
                let has = pack.list_flows().await.map(|ds| ds.iter().any(|d| d.id == flow_ref)).unwrap_or(false);
                if has {
                    return pack.run_flow_for_tool(flow_ref, input).await;
                }
            }
            Err(format!("flow '{flow_ref}' not found in any loaded pack"))
        })
    }
}
```

NOTE (resolve during impl): `list_flows()` on `PackRuntime` is `async` (:1882). `FlowInvoker::list_flows` is sync. Prefer a SYNC accessor for the manifest descriptors if one exists (grep `PackRuntime` for a sync flows/manifest getter — e.g. `metadata().entry_flows` or a manifest field); use it instead of `block_on`. If only the async `list_flows` exists, `block_on` inside a sync fn called from an async context will PANIC — in that case, add a small sync `flow_descriptors(&self) -> Vec<FlowDescriptor>` accessor to `PackRuntime` that reads the cached manifest without awaiting, and use it here. Do NOT ship a `block_on` on the async runner thread. Report which path you took. Also: derive `parameters` from the flow's real input schema if a cheap sync accessor exists (`get_flow_schema`/manifest); otherwise the `{"type":"object"}` fallback is acceptable for this slice — note it.

- [ ] **Step 5: Register the module**

In `crates/greentic-runner-host/src/runner/mod.rs`, add `mod flow_invoker;` (and `pub(crate) use` if the wiring in Task 4 needs the type path — match how `component_invoker` is declared).

- [ ] **Step 6: Run tests + build**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host flow_invoker 2>&1 | tail` → PASS.
Then `CARGO_BUILD_JOBS=2 cargo build -p greentic-runner-host` (heavy — FOREGROUND) clean.

- [ ] **Step 7: Commit**

```bash
git add crates/greentic-runner-host/src/pack.rs crates/greentic-runner-host/src/runner/flow_invoker.rs crates/greentic-runner-host/src/runner/mod.rs
git commit -m "feat(runner-host): PackRuntimeFlowInvoker + PackRuntime::run_flow_for_tool (non-interactive)"
```

---

### Task 4: Wire the flow source into the agent runtime (runner-host)

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/agent_node.rs` (`flow_source_from_packs` + `.with_flow_source(...)`)
- Test: a unit test for `flow_source_from_packs` env-gating

**Interfaces:**
- Consumes: `PackRuntimeFlowInvoker` (Task 3), `greentic_aw_runtime::FlowToolSource` (Task 1), `AgentRuntime::with_flow_source` (Task 2).
- Produces: `pub(crate) fn flow_source_from_packs(packs: &[Arc<PackRuntime>], tenant: &str) -> Option<Arc<greentic_aw_runtime::FlowToolSource>>`.

**Context:** Mirror `component_source_from_packs` (`agent_node.rs:432-453`) verbatim, swapping the invoker/source types and the env var (`GREENTIC_AW_FLOW_TOOLS`). Wire it at the `AgentRuntime` builder chain in `build_runtime_handler_with_stores` (:951-952) right after `.with_component_source(...)`.

- [ ] **Step 1: Write the failing test**

Add to the `agent_node.rs` test module (mirror any existing `component_source_from_packs` test; if none, assert the env gate):
```rust
#[test]
fn flow_source_disabled_by_env_and_empty_packs() {
    // SAFETY: single-threaded test; restore after.
    unsafe { std::env::set_var("GREENTIC_AW_FLOW_TOOLS", "0"); }
    assert!(flow_source_from_packs(&[], "acme").is_none());
    unsafe { std::env::remove_var("GREENTIC_AW_FLOW_TOOLS"); }
    assert!(flow_source_from_packs(&[], "acme").is_none(), "empty packs => None");
}
```
(Match the exact `set_var`/`unsafe` idiom the repo uses in existing env tests — grep `set_var` in the crate.)

- [ ] **Step 2: Run it — verify it fails**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host flow_source_disabled 2>&1 | tail`
Expected: FAIL — `flow_source_from_packs` not defined.

- [ ] **Step 3: Implement `flow_source_from_packs` + wire it**

Add next to `component_source_from_packs` (:432):
```rust
pub(crate) fn flow_source_from_packs(
    packs: &[Arc<crate::pack::PackRuntime>],
    tenant: &str,
) -> Option<Arc<greentic_aw_runtime::FlowToolSource>> {
    if std::env::var("GREENTIC_AW_FLOW_TOOLS").ok().as_deref() == Some("0") {
        tracing::info!("GREENTIC_AW_FLOW_TOOLS=0; flow tool source disabled");
        return None;
    }
    if packs.is_empty() {
        return None;
    }
    let invoker = Arc::new(crate::runner::flow_invoker::PackRuntimeFlowInvoker::new(
        packs.to_vec(),
        tenant.to_string(),
    ));
    tracing::info!(tenant = %tenant, packs = packs.len(), "flow tool source constructed");
    Some(Arc::new(greentic_aw_runtime::FlowToolSource::new(invoker)))
}
```
In `build_runtime_handler_with_stores` (:951-952), extend the builder chain:
```rust
    .with_component_source(component_source_from_packs(&packs, &tenant))
    .with_flow_source(flow_source_from_packs(&packs, &tenant)),
```

- [ ] **Step 4: Run tests + build**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host flow_source_disabled 2>&1 | tail` → PASS.
Then `CARGO_BUILD_JOBS=2 cargo build -p greentic-runner-host` (FOREGROUND) clean.

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-runner-host/src/runner/agent_node.rs
git commit -m "feat(runner-host): wire flow tool source into the agent runtime (GREENTIC_AW_FLOW_TOOLS)"
```

---

### Task 5: Full local CI + PR

- [ ] **Step 1: Run the workspace checks (foreground)**

Run (each FOREGROUND):
```
CARGO_BUILD_JOBS=2 cargo fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo clippy -p greentic-aw-runtime -p greentic-runner-host --all-targets -- -D warnings
CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime -p greentic-runner-host 2>&1 | tail -20
```
If the repo has `ci/local_check.sh`, run it; document any pre-existing, out-of-scope failure in the PR rather than "fixing" it.

- [ ] **Step 2: PR to research**

```bash
git push -u origin feat/agentic-flow-invoker
gh pr create --base research --title "feat(runner): agentic flow-tool source (Spec 2, Slice A)" --body "Slice A of the agentic-worker Call-Flow tool. Adds a flow:-prefixed agent tool source mirroring component: — FlowInvoker trait + FlowToolSource (aw-runtime), flow: branches in list_tools_for_llm/dispatch_tool_call + with_flow_source, and a PackRuntimeFlowInvoker running a same-pack flow synchronously via PackRuntime::run_flow_for_tool (errors on Waiting — non-interactive). The LLM-facing contract is derived from the flow (description + schema); the author's tool_name is the LLM tool name. Env-gated GREENTIC_AW_FLOW_TOOLS. Completes the runtime half of the feature (designer authoring shipped as Slice B). Follow-up: bump the runner rev the designer pins. Spec: docs/superpowers/specs/2026-07-07-agentic-flow-tool-design.md."
```

---

## Self-Review

**Spec coverage (Slice A):** FlowInvoker trait + FlowToolSource → Task 1; flow: prefix in list/dispatch + with_flow_source → Task 2; PackRuntimeFlowInvoker + non-interactive backstop (run_flow_for_tool errors on Waiting) → Task 3; wiring + env gate → Task 4; CI+PR → Task 5. Derive-from-flow contract decision → Tasks 1/3 (catalog keyed by flow_ref, description/params from the flow). Session isolation → inherent (run_flow_for_tool stands up its own engine, fresh session). Designer rev-bump = out-of-scope follow-up. ✓

**Placeholder scan:** No TBD. The two genuine adaptation points name concrete resolutions: (a) `FlowToolSource::catalog` cache-key — mirror `ComponentToolSource::catalog` exactly; (b) `PackRuntimeFlowInvoker::list_flows` sync-vs-async — add a sync `flow_descriptors` accessor rather than `block_on` on the runner thread; parameters fall back to `{"type":"object"}`. ✓

**Type consistency:** `FlowInvoker`/`FlowOperation`/`FlowToolCatalog`/`FlowToolEntry`/`FlowToolSource` names consistent Tasks 1→4; catalog keyed by `flow_ref` (single String) everywhere; `with_flow_source(Option<Arc<FlowToolSource>>)` matches `with_component_source`; `run_flow_for_tool -> Result<Value, String>` consumed by `PackRuntimeFlowInvoker::invoke`; `flow_source_from_packs` signature matches `component_source_from_packs`. ✓
