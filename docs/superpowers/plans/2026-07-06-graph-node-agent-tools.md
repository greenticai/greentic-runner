# Graph-Node Agent Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thread an agent-graph node's declared `tools` (`Vec<String>`) through `AgentTurnRequest` into the `AgentConfig` that `run_one_agent_turn` builds, mapping `"<ext>/<tool>"` → `ToolRef`, so graph agents can call tools and the B-3b audit path fires.

**Architecture:** Two crates in greentic-runner, one branch. `greentic-aw-runtime` (`graph/executor.rs`): add `tools: Vec<String>` to `AgentTurnRequest`, stop dropping it in the two `NodeKind::Agent` arms, populate it in the two request builds. `greentic-runner-host` (`runner/graph_node.rs`): a `parse_tool_ref` helper + map `req.tools` → `Vec<ToolRef>` in `run_one_agent_turn` (replacing `tools: vec![]`). Mirrors precedent commit `4d2b9e1` (threading `provider`).

**Tech Stack:** Rust (edition 2024), the existing agent-graph types (`NodeKind::Agent`, `AgentTurnRequest`, `AgentConfig`, `ToolRef`).

## Global Constraints

- **Repo:** greentic-runner only, branch `feat/graph-node-tools` → `research`. Crates `greentic-aw-runtime` + `greentic-runner-host`.
- **Format convention:** node tool string = `"<extension_id>/<tool_name>"`, split on the **LAST** `/`; no `/` → warn + skip (never panic). Document it.
- **Do NOT change:** `run_one_supervisor_turn`'s `tools: vec![]` (`graph_node.rs:1199` — supervisor schema has no tools); `mcp: None` (`graph_node.rs:1065` — MCP/component graph tools are a documented follow-up); any audit code (`AgentAuditObserver` is already injected and unchanged — it lights up automatically once tools resolve).
- **Precedent to mirror:** commit `4d2b9e1` threaded `provider: Option<String>` into `AgentTurnRequest` the exact same way — read it for the pattern.
- **Conventional commits, NO Claude co-author.**
- **Build discipline (shared machine; disk now ample ~113GB):** cargo with `-j2` + `CARGO_BUILD_JOBS=2`, FOREGROUND; never pkill/kill or delete another worktree's `target/`.

---

### Task 1: thread `tools` through `AgentTurnRequest` (greentic-aw-runtime)

**Files:**
- Modify: `crates/greentic-aw-runtime/src/graph/executor.rs` (`AgentTurnRequest` @ ~:61; the two `NodeKind::Agent` destructures @ ~:544, ~:1350; the two `AgentTurnRequest {}` builds @ ~:557, ~:1364)
- Test: inline `#[cfg(test)]` in `executor.rs`

**Interfaces:**
- Produces: `AgentTurnRequest` gains `pub tools: Vec<String>` (populated from the agent node's `tools`).
- Consumes: `NodeKind::Agent { tools, .. }` (the field already exists in `graph/model.rs`).

- [ ] **Step 1: Read the precedent** — `git show 4d2b9e1 -- crates/greentic-aw-runtime/src/graph/executor.rs` (how `provider` was added to `AgentTurnRequest` + threaded through the two `NodeKind::Agent` arms). Read `AgentTurnRequest` (:61), the two destructures (:544, :1350), the two builds (:557, :1364).
- [ ] **Step 2: Write the failing test** — build a graph with one agent node declaring `tools: ["myext/dothing"]`, drive the path that constructs `AgentTurnRequest` (mirror the existing `provider` round-trip test), assert the resulting `AgentTurnRequest.tools == vec!["myext/dothing"]`.
- [ ] **Step 3: Run — expect FAIL** (`CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime -j2 graph::executor`).
- [ ] **Step 4: Implement** — add `pub tools: Vec<String>` to `AgentTurnRequest`; in the two `NodeKind::Agent { system_prompt, model, provider, tools, .. }` destructures capture `tools`; in the two `AgentTurnRequest { .. }` builds set `tools: tools.clone()` (or move). Keep every other field unchanged.
- [ ] **Step 5: PASS + commit** (`feat(graph): carry agent-node tools through AgentTurnRequest`).

---

### Task 2: map `req.tools` → `ToolRef` in `run_one_agent_turn` (greentic-runner-host)

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/graph_node.rs` (`run_one_agent_turn` `AgentConfig` build @ ~:1039-1052, the `tools: vec![]` @ ~:1042; add a `parse_tool_ref` helper)
- Test: inline `#[cfg(test)]` in `graph_node.rs`

**Interfaces:**
- Consumes: `AgentTurnRequest.tools: Vec<String>` (from Task 1); `ToolRef { extension_id: String, tool_name: String }` (`greentic-aw-runtime/src/config.rs:15`).
- Produces: `fn parse_tool_ref(s: &str) -> Option<ToolRef>` (split on last `/`; `None` if no `/`).

- [ ] **Step 1: Write failing tests.**
```rust
#[test]
fn parse_tool_ref_splits_on_last_slash() {
    assert_eq!(parse_tool_ref("myext/dothing"), Some(ToolRef{extension_id:"myext".into(), tool_name:"dothing".into()}));
    assert_eq!(parse_tool_ref("component:owner/repo/list"), Some(ToolRef{extension_id:"component:owner/repo".into(), tool_name:"list".into()}));
    assert_eq!(parse_tool_ref("noslash"), None);
    assert_eq!(parse_tool_ref("trailing/"), None); // empty tool_name → None
}
```
- [ ] **Step 2: Run — expect FAIL** (`CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host -j2 graph_node`).
- [ ] **Step 3: Implement** `parse_tool_ref` (`s.rsplit_once('/')` → both parts non-empty → `Some(ToolRef{..})`, else `None` + `tracing::warn!` naming the skipped string). In `run_one_agent_turn`, replace `tools: vec![]` (:1042) with `tools: req.tools.iter().filter_map(|t| parse_tool_ref(t)).collect()`. Leave `memory: None`, `mcp: None`, and the supervisor turn unchanged.
- [ ] **Step 4: PASS.** Then a wiring assertion: extend/add a test that `run_one_agent_turn` (or its `AgentConfig` build) with a request carrying `tools: ["myext/dothing"]` yields an `AgentConfig` whose `tools` contains `ToolRef{"myext","dothing"}` (non-empty). If `run_one_agent_turn` is hard to call directly, factor the `Vec<String>`→`Vec<ToolRef>` mapping into a tested helper and assert on it.
- [ ] **Step 5: Gate + commit.** `cargo fmt --all`; `CARGO_BUILD_JOBS=2 cargo clippy -p greentic-aw-runtime -p greentic-runner-host -j2 --all-targets -- -D warnings`; `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime -p greentic-runner-host -j2`. Commit (`feat(graph): resolve graph-agent tools from declared refs (lights up B-3b audit)`). Then finishing-a-development-branch → PR to `research` noting the B-3b audit now fires for graph agents with declared design-extension tools; MCP/component graph tools + designer emit are documented follow-ups.

---

## Self-Review
- **Spec coverage:** §1 drop points 1-2 → Task 1 (executor); drop point 3 → Task 2 (graph_node mapping); §3 format convention → Task 2 (`parse_tool_ref`); §5 tests → both tasks; §4 out-of-scope (supervisor/mcp/designer) → Global Constraints "do NOT change" + PR note.
- **Placeholder scan:** "read precedent 4d2b9e1" + "mirror the provider round-trip test" are deliberate (the threading pattern + test shape must be read from the repo). Format convention is explicit. No TBD.
- **Type consistency:** `AgentTurnRequest.tools: Vec<String>` (Task 1) consumed by Task 2's mapping → `Vec<ToolRef>`; `parse_tool_ref` returns `Option<ToolRef>` matching `config.rs:15`.
- **Scope:** 2 files, 2 crates, one branch; additive; audit unchanged (lights up automatically).
