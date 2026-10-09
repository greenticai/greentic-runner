# Required flow-variable fail-fast (greentic-runner) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the runner fail-fast at flow start when a Start-node parameter marked `required` has no seeded value (no default and no provided value), instead of silently rendering `{{vars.name}}` as an empty string.

**Architecture:** The designer already writes Start parameters into the flow YGTC under the untyped `metadata.extra.vars_init.<name>` blob (currently `{ type, default? }`; the paired designer plan adds `required: true`). The runner parses that blob in `impl From<Flow> for HostFlow` and seeds vars in `FlowEngine::execute_once`. We (1) carry the `required` names into a new `HostFlow.required_vars`, (2) add a pure helper that seeds defaults and reports missing required vars, and (3) call it from `execute_once`, bailing with a clear, non-retryable, i18n-keyed error before any node runs.

**Tech Stack:** Rust 1.94, edition 2024. Crate `greentic-runner-host`. `serde_json` (`Map as JsonMap`, `Value`), `anyhow`, `tokio` tests via `Runtime::new()`.

## Global Constraints

- Rust 1.94.0, edition 2024 (pinned via `rust-toolchain.toml`) — do not edit the pin.
- `#![forbid(unsafe_code)]` norm; no `unwrap()`/`panic!()` in production paths — use `anyhow`.
- English only in source, tests, comments, tracing logs.
- User-facing message strings use an i18n key via `crate::runner::i18n` (vendored `english_message` match in `crates/greentic-runner-host/src/runner/i18n.rs`), never a hardcoded `.ftl`/`.json` bundle.
- `should_retry` (`engine.rs`) retries only when the lowercased error string contains `transient`/`unavailable`/`internal`/`timeout`. The new error message MUST avoid all four substrings so it is classified non-retryable.
- No `greentic-types` change: `vars_init` lives in the untyped `metadata.extra` blob; `required` is read from untyped JSON.
- No Claude co-author attribution on commits.
- Anchor edits on the verbatim code snippets below, not absolute line numbers (line numbers drift between branches; the Edit tool matches on content).
- Base branch: `origin/research`. Worktree: `~/projects/Works/greentic-worktrees/required-flow-var-runner`, branch `feat/required-flow-var-fail-fast`.
- Local CI: `cargo fmt --all -- --check`, `cargo clippy -p greentic-runner-host --all-targets -- -D warnings`, `cargo test -p greentic-runner-host`.

---

## File structure

- Modify `crates/greentic-runner-host/src/runner/engine.rs`:
  - `struct HostFlow` — add `required_vars: Vec<String>` field.
  - `impl From<Flow> for HostFlow` — parse `required` names.
  - new free fn `seed_vars_and_collect_missing_required(...)` — pure seed + missing-report helper.
  - `FlowEngine::execute_once` — call the helper, bail on missing.
  - tests module — unit + e2e tests.
- Modify `crates/greentic-runner-host/src/runner/i18n.rs`:
  - `english_message` match — add `runner.flow.required_var_missing` key.

---

### Task 1: Carry `required` names into `HostFlow.required_vars`

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (`struct HostFlow`, `impl From<Flow> for HostFlow`, all `HostFlow`/`Self` struct literals)
- Test: same file, `mod tests`

**Interfaces:**
- Produces: `HostFlow.required_vars: Vec<String>` — names of vars whose `vars_init` decl has `"required": true`. Order follows the `metadata.extra.vars_init` map iteration.

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` block (next to `from_flow_extracts_vars_init`, which uses the existing `flow_with_extra` helper):

```rust
    #[test]
    fn from_flow_collects_required_vars() {
        let flow = flow_with_extra(serde_json::json!({
            "vars_init": {
                "name":   { "type": "string", "required": true },
                "region": { "type": "string", "default": "us-east-1" },
                "note":   { "type": "string", "required": false }
            }
        }));
        let host: HostFlow = HostFlow::from(flow);
        assert_eq!(host.required_vars, vec!["name".to_string()]);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p greentic-runner-host from_flow_collects_required_vars`
Expected: FAIL to compile — `HostFlow` has no field `required_vars`.

- [ ] **Step 3: Add the field to `HostFlow`**

In `struct HostFlow` (verbatim current form):

```rust
#[derive(Clone, Debug)]
struct HostFlow {
    id: String,
    start: Option<NodeId>,
    nodes: IndexMap<NodeId, HostNode>,
    vars_init: JsonMap<String, Value>,
}
```

Add the field after `vars_init`:

```rust
#[derive(Clone, Debug)]
struct HostFlow {
    id: String,
    start: Option<NodeId>,
    nodes: IndexMap<NodeId, HostNode>,
    vars_init: JsonMap<String, Value>,
    required_vars: Vec<String>,
}
```

- [ ] **Step 4: Parse `required` in `impl From<Flow> for HostFlow`**

The current impl ends with the `vars_init` let-binding and a `Self { ... }` literal. Immediately AFTER the existing `let vars_init = ...;` block (which reads `value.metadata.extra`), add a sibling binding. Because `value.metadata.extra` was borrowed (not moved) by the `vars_init` block, it is still available:

```rust
        let required_vars = value
            .metadata
            .extra
            .get("vars_init")
            .and_then(|v| v.as_object())
            .map(|decls| {
                decls
                    .iter()
                    .filter(|(_, decl)| decl.get("required") == Some(&Value::Bool(true)))
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
```

Then add `required_vars` to the `Self { ... }` literal:

```rust
        Self {
            id: value.id.as_str().to_string(),
            start,
            nodes,
            vars_init,
            required_vars,
        }
```

- [ ] **Step 5: Build and fix every other `HostFlow` struct literal**

Run: `cargo build -p greentic-runner-host --all-targets`
Expected: `E0063: missing field required_vars` at each test-side `HostFlow { ... }` literal.

Find them: `grep -n "HostFlow {" crates/greentic-runner-host/src/runner/engine.rs`
For each such literal (they currently set `vars_init: JsonMap::new()`), add `required_vars: Vec::new(),` after the `vars_init` line. Re-run the build until it is clean.

- [ ] **Step 6: Run tests**

Run: `cargo test -p greentic-runner-host from_flow_collects_required_vars from_flow_extracts_vars_init from_flow_vars_init_absent`
Expected: PASS (new test + the two existing vars_init tests as regression).

- [ ] **Step 7: Commit**

```bash
cd ~/projects/Works/greentic-worktrees/required-flow-var-runner
git add crates/greentic-runner-host/src/runner/engine.rs
git commit -m "feat(runner): carry required flow-var names into HostFlow.required_vars"
```

---

### Task 2: Pure helper — seed defaults and collect missing required vars

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (new free fn near `HostFlow`)
- Test: same file, `mod tests`

**Interfaces:**
- Produces: `fn seed_vars_and_collect_missing_required(vars_init: &JsonMap<String, Value>, required: &[String], target: &mut JsonMap<String, Value>) -> Vec<String>` — seeds each `vars_init` entry into `target` via entry-or-insert (never overwrites an already-present key), then returns the subset of `required` names absent from `target` (order-preserving).

- [ ] **Step 1: Write the failing test**

Add to `mod tests`:

```rust
    #[test]
    fn seed_vars_seeds_defaults_and_reports_missing_required() {
        use serde_json::json;
        let mut vars_init = JsonMap::new();
        vars_init.insert("region".into(), json!("us-east-1"));

        // "name" is required but has no default; "region" required WITH a default.
        let required = vec!["name".to_string(), "region".to_string()];
        let mut target = JsonMap::new();

        let missing = seed_vars_and_collect_missing_required(&vars_init, &required, &mut target);

        assert_eq!(target.get("region"), Some(&json!("us-east-1")), "default seeded");
        assert_eq!(missing, vec!["name".to_string()], "only the defaultless required var is missing");
    }

    #[test]
    fn seed_vars_respects_preexisting_value_for_required() {
        use serde_json::json;
        let vars_init = JsonMap::new(); // no defaults declared
        let required = vec!["name".to_string()];
        let mut target = JsonMap::new();
        target.insert("name".into(), json!("Budi")); // operator-provided value already present

        let missing = seed_vars_and_collect_missing_required(&vars_init, &required, &mut target);

        assert!(missing.is_empty(), "a required var with a provided value is not missing");
        assert_eq!(target.get("name"), Some(&json!("Budi")), "provided value not overwritten");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p greentic-runner-host seed_vars`
Expected: FAIL to compile — `seed_vars_and_collect_missing_required` not found.

- [ ] **Step 3: Write minimal implementation**

Add as a free fn in `engine.rs` (place it just above `impl From<Flow> for HostFlow`):

```rust
/// Seed declared flow variables into `target` (entry-or-insert; never
/// overwrites an already-present key), then return the `required` names that
/// remain absent from `target`. A required var is satisfied by either a
/// declared default (seeded here) or a value already placed in `target`
/// (e.g. an operator-provided demo value).
fn seed_vars_and_collect_missing_required(
    vars_init: &JsonMap<String, Value>,
    required: &[String],
    target: &mut JsonMap<String, Value>,
) -> Vec<String> {
    for (name, default) in vars_init.iter() {
        target
            .entry(name.clone())
            .or_insert_with(|| default.clone());
    }
    required
        .iter()
        .filter(|name| !target.contains_key(name.as_str()))
        .cloned()
        .collect()
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p greentic-runner-host seed_vars`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-runner-host/src/runner/engine.rs
git commit -m "feat(runner): add seed_vars_and_collect_missing_required helper"
```

---

### Task 3: Enforce in `execute_once` + i18n key + non-retry

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/i18n.rs` (`english_message` match)
- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (`execute_once`)
- Test: `engine.rs` `mod tests` (e2e + should_retry unit)

**Interfaces:**
- Consumes: `HostFlow.required_vars` (Task 1), `seed_vars_and_collect_missing_required` (Task 2), `crate::runner::i18n::resolve_message`.
- Produces: `execute_once` returns `Err` (non-retryable) when any required var is unseeded, before any node runs.

- [ ] **Step 1: Add the i18n key**

In `crates/greentic-runner-host/src/runner/i18n.rs`, inside the `english_message` match (alongside the existing `"runner.operator.*"` / `"runner.schema.*"` arms), add:

```rust
        "runner.flow.required_var_missing" => Some("required flow variable not provided"),
```

- [ ] **Step 2: Write the failing e2e test**

Add to `mod tests` (this mirrors `execute_once_seeds_declared_vars` — a single `emit.log` node — but declares `region` as required WITHOUT a default and expects an error before the node runs):

```rust
    #[test]
    fn execute_once_fails_on_missing_required_var() {
        let node_id = NodeId::from_str("n1").unwrap();
        let node = Node {
            id: node_id.clone(),
            component: FlowComponentRef {
                id: "emit.log".parse().unwrap(),
                pack_alias: None,
                operation: None,
            },
            input: InputMapping {
                mapping: json!({ "message": "{{vars.region}}" }),
            },
            output: OutputMapping { mapping: Value::Null },
            err_map: None,
            routing: Routing::End,
            telemetry: TelemetryHints::default(),
            conversational: false,
        };
        let mut nodes = indexmap::IndexMap::default();
        nodes.insert(node_id.clone(), node);
        let flow = Flow {
            schema_version: "1.0".into(),
            id: FlowId::from_str("vars.flow").unwrap(),
            kind: FlowKind::Messaging,
            entrypoints: BTreeMap::from([(
                "default".to_string(),
                Value::String(node_id.to_string()),
            )]),
            nodes,
            metadata: FlowMetadata {
                title: None,
                description: None,
                tags: Default::default(),
                extra: json!({
                    "vars_init": {
                        "region": { "type": "string", "required": true }
                    }
                }),
            },
        };
        let host_flow = HostFlow::from(flow);

        let engine = FlowEngine {
            packs: Vec::new(),
            flows: Vec::new(),
            flow_sources: HashMap::new(),
            flow_cache: RwLock::new(HashMap::from([(
                FlowKey {
                    pack_id: "test-pack".to_string(),
                    flow_id: "vars.flow".to_string(),
                },
                host_flow,
            )])),
            default_env: "local".to_string(),
            validation: ValidationConfig { mode: ValidationMode::Off },
            cross_pack_resolver: None,
            remote_dispatch_handler: None,
            #[cfg(feature = "agentic-worker")]
            dw_agent_dispatch: crate::runner::agent_node::DwAgentDispatch::InProcess,
            #[cfg(feature = "agentic-worker")]
            agent_node_handler: None,
            #[cfg(feature = "agentic-worker")]
            graph_node_handler: None,
            #[cfg(feature = "agentic-worker")]
            mcp_tool_source: None,
        };

        let observer = CountingObserver::new();
        let ctx = FlowContext {
            tenant: "demo",
            pack_id: "test-pack",
            flow_id: "vars.flow",
            node_id: None,
            tool: None,
            action: None,
            session_id: None,
            provider_id: None,
            reply_scope: None,
            retry_config: RetryConfig { max_attempts: 1, base_delay_ms: 1 },
            attempt: 1,
            observer: Some(&observer),
            mocks: None,
        };

        let rt = Runtime::new().unwrap();
        let err = rt
            .block_on(engine.execute(ctx, Value::Null))
            .expect_err("a required var with no default and no value must fail the run");
        let msg = err.to_string();
        assert!(msg.contains("region"), "error names the missing var: {msg}");
        assert!(
            !should_retry(&err),
            "missing-required-var is deterministic and must not be retried"
        );
        // The node must never run — nothing emitted.
        assert!(observer.ends.lock().unwrap().is_empty(), "flow aborted before the node ran");
    }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p greentic-runner-host execute_once_fails_on_missing_required_var`
Expected: FAIL — currently the run seeds nothing for `region`, the node runs, `{{vars.region}}` renders empty, and `execute` returns `Ok` (so `expect_err` panics).

- [ ] **Step 4: Wire the helper into `execute_once`**

Current `execute_once` (verbatim):

```rust
    async fn execute_once(&self, ctx: &FlowContext<'_>, input: Value) -> Result<FlowExecution> {
        let flow_ir = self.get_or_load_flow(ctx.pack_id, ctx.flow_id).await?;
        let mut state = ExecutionState::new(input);
        for (name, default) in flow_ir.vars_init.iter() {
            state
                .vars
                .entry(name.clone())
                .or_insert_with(|| default.clone());
        }
        self.drive_flow(ctx, flow_ir, state, None, ctx.flow_id.to_string())
            .await
    }
```

Replace the manual seed loop with the helper + a fail-fast bail:

```rust
    async fn execute_once(&self, ctx: &FlowContext<'_>, input: Value) -> Result<FlowExecution> {
        let flow_ir = self.get_or_load_flow(ctx.pack_id, ctx.flow_id).await?;
        let mut state = ExecutionState::new(input);
        let missing = seed_vars_and_collect_missing_required(
            &flow_ir.vars_init,
            &flow_ir.required_vars,
            &mut state.vars,
        );
        if !missing.is_empty() {
            // Non-retryable by design: the message avoids should_retry's trigger
            // words (transient/unavailable/internal/timeout).
            let label = crate::runner::i18n::resolve_message(
                "runner.flow.required_var_missing",
                "required flow variable not provided",
                "en",
            );
            anyhow::bail!("{label}: {}", missing.join(", "));
        }
        self.drive_flow(ctx, flow_ir, state, None, ctx.flow_id.to_string())
            .await
    }
```

Note: `&flow_ir.*` borrows end before `flow_ir` is moved into `drive_flow`, so this compiles.

- [ ] **Step 5: Run the enforcement test + the seed regression test**

Run: `cargo test -p greentic-runner-host execute_once_fails_on_missing_required_var execute_once_seeds_declared_vars`
Expected: PASS both — the new test errors on the missing var; the existing seed test still seeds `region` from its default and renders `us-east-1` (behavior preserved through the helper).

- [ ] **Step 6: Full crate test + lint**

Run: `cargo test -p greentic-runner-host`
Expected: PASS (no regression across the crate).

Run: `cargo fmt --all -- --check && cargo clippy -p greentic-runner-host --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add crates/greentic-runner-host/src/runner/engine.rs crates/greentic-runner-host/src/runner/i18n.rs
git commit -m "feat(runner): fail-fast when a required flow variable is unseeded"
```

---

## Scope notes (call out in the PR, no code)

- **Resume path not covered.** `FlowEngine::resume` seeds from a snapshot, not `execute_once`, so the fail-fast covers fresh executions only. A required var that was satisfied on the first run persists in the snapshot; a resume that somehow lacks it is not enforced. This is intentional for this slice.
- **Engine-layer errors are English-only today.** `execute_once` has no request locale in scope (existing engine errors are plain `anyhow`). We register the i18n key and resolve it via `resolve_message(.., "en")` so the string is centralized and localizable later, consistent with the vendored i18n layer.

## Self-review checklist (run before opening the PR)

- [ ] `cargo test -p greentic-runner-host` green.
- [ ] `cargo fmt --all -- --check` and `cargo clippy -p greentic-runner-host --all-targets -- -D warnings` clean.
- [ ] Error message contains none of `transient`/`unavailable`/`internal`/`timeout`.
- [ ] Existing `execute_once_seeds_declared_vars` still passes (no behavior regression).
