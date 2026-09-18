# Flow-Tool Author Contract — Slice A (Runner / aw-runtime) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `greentic_aw_runtime::ToolRef` two optional fields (`description`, `input_schema`) and make `list_tools_for_llm`'s `flow:` branch prefer them over the catalog — so a flow tool can carry its author-defined LLM contract.

**Architecture:** Extend `ToolRef` with serde-default optional fields (backward-compatible), fix the in-crate literal construction sites, then read the override in the `flow:` branch only (component/MCP branches unchanged). This defines the wire format the downstream slices (dw-authoring, designer) will populate.

**Tech Stack:** Rust, `serde`, `serde_json`.

**Repo:** greentic-runner. Worktree `runner-flowschema`, branch `feat/flow-tool-schema` (from `research`, HEAD 7f7517cb). PR to `research`. Crate: `crates/greentic-aw-runtime`.

## Global Constraints

- English only in source/tests/comments. Conventional Commits. **No Claude co-authorship trailer.** No `unwrap()`/`panic!()` in production paths (tests may).
- The new `ToolRef` fields MUST be serde-default + `skip_serializing_if = "Option::is_none"` so existing serialized payloads (designer registry JSON, pack `dw-agents.json`, manifests) round-trip unchanged.
- The override is consumed by the **`flow:` branch ONLY**. The `component:` and `mcp:` branches of `list_tools_for_llm` MUST remain catalog-only (no behavior change).
- A flow tool with neither an override nor a catalog entry is dropped from the LLM list (with a warn) — same drop policy as today, widened to "neither source".
- `CARGO_BUILD_JOBS=2`. `greentic-aw-runtime` builds warm (~seconds incremental). Heavy silent builds run FOREGROUND.

## File map

- `crates/greentic-aw-runtime/src/config.rs` — MODIFY: extend `ToolRef` (`:14`); fix in-crate `ToolRef { .. }` literals in this file's tests.
- `crates/greentic-aw-runtime/src/tools.rs` — MODIFY: `flow:` branch preference logic (`:103`); fix any `ToolRef { .. }` literals in this file's tests.
- Any other in-crate `ToolRef { .. }` literal sites — MODIFY: add `description: None, input_schema: None` (grep to find them; e.g. `src/dw.rs`, `tests/`).

---

### Task 1: Extend `ToolRef` with the author-contract fields

**Files:**
- Modify: `crates/greentic-aw-runtime/src/config.rs` (`ToolRef` at `:14`)
- Modify: every in-crate `ToolRef { .. }` construction site (grep)
- Test: `config.rs` test module (round-trip backward-compat)

**Interfaces:**
- Produces: `ToolRef { extension_id: String, tool_name: String, description: Option<String>, input_schema: Option<serde_json::Value> }` — the last two `#[serde(default, skip_serializing_if = "Option::is_none")]`.

**Context:** `ToolRef` (`config.rs:14`) currently has only `extension_id` + `tool_name`, no serde container attrs, no `deny_unknown_fields` (verified). `AgentConfig.tools: Vec<ToolRef>` (`config.rs:96`) also has no `deny_unknown_fields`. Adding the two optional fields is backward-compatible on deserialize. But `ToolRef` does NOT derive `Default`, so every `ToolRef { extension_id, tool_name }` struct literal in the crate must gain `description: None, input_schema: None`. Grep `ToolRef {` across `crates/greentic-aw-runtime/` (src + tests) and update each.

- [ ] **Step 1: Write the failing test**

Add to the `config.rs` test module:

```rust
#[test]
fn tool_ref_deserializes_without_author_contract_fields() {
    // Old payload: no description / input_schema keys.
    let json = r#"{"extension_id":"flow:lookup","tool_name":"look_up"}"#;
    let t: ToolRef = serde_json::from_str(json).expect("old ToolRef must still deserialize");
    assert_eq!(t.extension_id, "flow:lookup");
    assert_eq!(t.tool_name, "look_up");
    assert!(t.description.is_none());
    assert!(t.input_schema.is_none());
}

#[test]
fn tool_ref_omits_none_author_contract_fields_on_serialize() {
    let t = ToolRef {
        extension_id: "flow:lookup".into(),
        tool_name: "look_up".into(),
        description: None,
        input_schema: None,
    };
    let json = serde_json::to_string(&t).unwrap();
    assert!(!json.contains("description"), "None description must be omitted: {json}");
    assert!(!json.contains("input_schema"), "None input_schema must be omitted: {json}");
}

#[test]
fn tool_ref_round_trips_with_author_contract() {
    let t = ToolRef {
        extension_id: "flow:refund".into(),
        tool_name: "refund_lookup".into(),
        description: Some("Look up a refund".into()),
        input_schema: Some(serde_json::json!({"type":"object","properties":{"order_id":{"type":"string"}}})),
    };
    let json = serde_json::to_string(&t).unwrap();
    let back: ToolRef = serde_json::from_str(&json).unwrap();
    assert_eq!(back, t);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime tool_ref 2>&1 | tail`
Expected: FAIL — `ToolRef` has no `description`/`input_schema` fields (compile error).

- [ ] **Step 3: Extend `ToolRef` + fix literals**

In `config.rs`, change `ToolRef` to:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRef {
    pub extension_id: String,
    pub tool_name: String,
    /// Author-defined LLM description (flow tools only). `None` falls back to
    /// the runtime catalog. Ignored by the component/MCP tool branches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Author-defined JSON-schema for the tool's input (flow tools only).
    /// `None` falls back to the runtime catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<serde_json::Value>,
}
```

NOTE: `ToolRef` derives `Eq`, but `serde_json::Value` is NOT `Eq` (only `PartialEq`). Adding an `Option<serde_json::Value>` field will BREAK the `Eq` derive. Resolve by REMOVING `Eq` from the derive (keep `PartialEq`) — and check whether anything requires `ToolRef: Eq` (grep for `HashSet<ToolRef>`/`BTreeSet<ToolRef>`/`Hash`; the grounding shows dedup uses `Vec::contains` which needs only `PartialEq`, and `AgentConfig` derives no `Eq`). If a `HashSet<ToolRef>` exists, that is a real blocker — report it. Then grep `ToolRef {` across the crate and add `description: None, input_schema: None` to each literal (config.rs tests, tools.rs tests, src/dw.rs if it builds literals, any `tests/`).

- [ ] **Step 4: Run test to verify it passes**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime tool_ref 2>&1 | tail`
Expected: PASS. Then `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime 2>&1 | tail` — all existing tests still pass (the literal fixes compile). `cargo build -p greentic-aw-runtime` clean.

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-aw-runtime/src/config.rs crates/greentic-aw-runtime/src/tools.rs crates/greentic-aw-runtime/src/dw.rs
git commit -m "feat(aw-runtime): ToolRef carries optional author description + input_schema"
```
(Add any other files whose `ToolRef` literals you fixed.)

---

### Task 2: `flow:` branch prefers the author contract

**Files:**
- Modify: `crates/greentic-aw-runtime/src/tools.rs` (`list_tools_for_llm` `flow:` branch, `:103`)
- Test: `tools.rs` test module

**Interfaces:**
- Consumes: `ToolRef.description` / `ToolRef.input_schema` (Task 1); `FlowToolCatalog::tool_entry` (existing); `LlmToolSchema { extension_id, tool_name, description, parameters }` (`llm.rs:26`).

**Context:** The current `flow:` branch (`tools.rs:103`) reads `flows.and_then(|c| c.tool_entry(flow_ref))` and pushes the catalog `description`/`parameters`. `t` (the `ToolRef`) is in scope. Change it to prefer `t.description`/`t.input_schema`, else the catalog entry. If BOTH the override and the catalog are absent for a field, drop the tool with a warn (widen today's "not found in catalog" drop). Component/MCP branches (`:73-101`) are NOT touched.

- [ ] **Step 1: Write the failing test**

Add to the `tools.rs` test module (reuse the existing flow-test fixtures — the `FakeFlowInvoker`/`FlowToolCatalog::from_invoker` helper from the existing `flow_prefixed_tool_is_listed_and_dispatched` test):

```rust
#[test]
fn flow_tool_prefers_author_contract_over_catalog() {
    // Catalog has flow "lookup" with a catalog description + open schema.
    let flows = std::sync::Arc::new(
        crate::flow_source::FlowToolCatalog::from_invoker(std::sync::Arc::new(test_flow_invoker())),
    );
    // ToolRef carries an author override.
    let allowed = vec![ToolRef {
        extension_id: "flow:lookup".into(),
        tool_name: "look_up".into(),
        description: Some("Author description".into()),
        input_schema: Some(serde_json::json!({"type":"object","properties":{"q":{"type":"string"}}})),
    }];
    let schemas = list_tools_for_llm(&ext_runtime_stub(), None, None, Some(&flows), &allowed);
    let s = schemas.iter().find(|s| s.extension_id == "flow:lookup").expect("flow tool listed");
    assert_eq!(s.description, "Author description", "override description must win");
    assert_eq!(s.parameters["properties"]["q"]["type"], "string", "override schema must win");
}

#[test]
fn flow_tool_falls_back_to_catalog_when_no_override() {
    let flows = std::sync::Arc::new(
        crate::flow_source::FlowToolCatalog::from_invoker(std::sync::Arc::new(test_flow_invoker())),
    );
    let allowed = vec![ToolRef {
        extension_id: "flow:lookup".into(),
        tool_name: "look_up".into(),
        description: None,
        input_schema: None,
    }];
    let schemas = list_tools_for_llm(&ext_runtime_stub(), None, None, Some(&flows), &allowed);
    assert!(schemas.iter().any(|s| s.extension_id == "flow:lookup"),
        "with no override, the catalog entry is used");
}
```

(Adapt `test_flow_invoker`/`ext_runtime_stub` to the exact helpers the existing flow test uses — reuse them verbatim.)

- [ ] **Step 2: Run test to verify it fails**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime flow_tool_prefers 2>&1 | tail`
Expected: FAIL — the branch currently ignores `t.description`/`t.input_schema` (uses catalog).

- [ ] **Step 3: Implement the preference**

Replace the `flow:` branch body (`tools.rs:103`) with:

```rust
        if let Some(flow_ref) = t.extension_id.strip_prefix("flow:") {
            let entry = flows.and_then(|c| c.tool_entry(flow_ref));
            let description = t
                .description
                .clone()
                .or_else(|| entry.map(|e| e.description.clone()));
            let parameters = t
                .input_schema
                .clone()
                .or_else(|| entry.map(|e| e.parameters.clone()));
            match (description, parameters) {
                (Some(description), Some(parameters)) => out.push(LlmToolSchema {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    description,
                    parameters,
                }),
                _ => tracing::warn!(
                    extension = %t.extension_id, tool = %t.tool_name,
                    "flow tool has neither an author contract nor a catalog entry; dropping from LLM tool list"
                ),
            }
            continue;
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime 2>&1 | tail -15`
Expected: PASS — the two new tests + all existing (esp. `flow_prefixed_tool_is_listed_and_dispatched`, which has no override and must still list via the catalog). Then `cargo build -p greentic-aw-runtime` clean.

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-aw-runtime/src/tools.rs
git commit -m "feat(aw-runtime): flow: tool list prefers the author contract over the catalog"
```

---

### Task 3: Gate + PR

- [ ] **Step 1: fmt + clippy + tests (foreground)**

Run (each FOREGROUND):
```
CARGO_BUILD_JOBS=2 cargo fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo clippy -p greentic-aw-runtime --all-targets -- -D warnings
CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime 2>&1 | tail -15
```
(Use default features — `--all-features` pulls surrealdb's rocksdb backend, which fails to build in this environment on a missing C header; that is an environment limitation, not this change.) If any subagent skipped `cargo fmt`, run `cargo fmt --all` and commit the result as a `style(...)` fixup.

- [ ] **Step 2: PR to research**

```bash
git push -u origin feat/flow-tool-schema
gh pr create --base research --title "feat(aw-runtime): ToolRef author contract for flow tools (Slice A)" --body "Slice A of the flow-tool author-contract change. ToolRef gains optional serde-default description + input_schema; list_tools_for_llm's flow: branch prefers them over the catalog (component/MCP unchanged). Backward-compatible wire format (no deny_unknown_fields; None fields omitted). Downstream slices (greentic-dw-authoring, greentic-designer) will populate these fields from the author's ExtensionToolBinding. Spec: docs/superpowers/specs/2026-07-07-flow-tool-author-contract-design.md."
```

---

## Self-Review

**Spec coverage:** wire format (ToolRef + serde-default + backward-compat) → Task 1; flow-branch preference (override else catalog, drop if neither) → Task 2; component/MCP unchanged → Task 2 (branch untouched); gate + PR → Task 3. Downstream population (dw-authoring, designer) = Slices B/C, out of scope. ✓

**Placeholder scan:** No TBD. The `Eq`-derive break (Value isn't Eq) is called out explicitly with the resolution (drop `Eq`, verify no `HashSet<ToolRef>`); the literal-fix step names grep + the concrete files. ✓

**Type consistency:** `description: Option<String>` + `input_schema: Option<serde_json::Value>` consistent across Tasks 1-2; `LlmToolSchema.description: String` / `.parameters: Value` targets match; `tool_entry(flow_ref)` single-key catalog lookup unchanged. ✓
