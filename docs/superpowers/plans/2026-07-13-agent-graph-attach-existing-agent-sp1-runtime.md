# SP1 — Graph agent-node runs a referenced agent with full fidelity — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a graph `agent.llm` node carry `agent_ref: Option<String>` (an agent id). When set, the graph executor runs that agent turn using the agent's **full** `AgentConfig` (system_prompt/model/tools **plus** memory/knowledge/guardrails) resolved from a per-tenant merged-agents map, instead of the stripped ephemeral config it fabricates today. `agent_ref = None` is byte-unchanged.

**Architecture:** Thread `agent_ref` from the graph model → `AgentTurnRequest` → the host's `run_one_agent_turn`. Give the graph handler the same `merged_agents: HashMap<String, AgentConfig>` the `dw.agent` runtime is built from (currently it has no access). In `run_one_agent_turn`, when `agent_ref` is `Some(id)`, resolve the full config from that map and build a full-fidelity `AgentRuntime` (mirroring `agent_node::build_agent_runtime`'s `.with_guardrails` / `.with_short_term_memory` / long-term + knowledge attach), then `.step(..., <referenced id>, ...)`.

**Tech Stack:** Rust (edition 2024, pinned per `rust-toolchain.toml`), `serde`, the existing agent-graph types + the aw-runtime `AgentRuntime`/`ConfigProvider` seam.

## Global Constraints

- **Repo:** greentic-runner only. Crates: `greentic-aw-runtime` (graph model + executor) + `greentic-runner-host` (graph_node handler + runtime wiring). Branch: `feat/agent-ref-graph-sp1` → `research`.
- **Additive / backward compatible:** `NodeKind::Agent.agent_ref` and `AgentTurnRequest.agent_ref` are `#[serde(default, skip_serializing_if = "Option::is_none")] Option<String>`. `None` ⇒ every existing agent-graph is byte-unchanged (the stripped path). `agent_ref` valid in schemaVersion 1 (Agent already is; `requires_v2()` does not gate it — `model.rs:115-123`).
- **Full fidelity means:** a resolved `agent_ref` turn runs with the agent's `guardrails` + short-term memory + (feature-gated) long-term memory + knowledge attach — the exact attachments `agent_node::build_agent_runtime` (`agent_node.rs:1366-1473`) applies. Do NOT hard-code `guardrails: vec![], memory: None, knowledge: None` for the `agent_ref` branch.
- **Missing reference is an ERROR, not silent:** `agent_ref = Some(id)` with `id` absent from the merged map ⇒ `GraphExecError::AgentTurn("referenced agent '<id>' not found")`. Never fall back to an empty agent.
- **`merged_agents` is MOVED into `build_agent_node_handler` at `runtime.rs:324`/`:348` BEFORE the graph handler is built (`runtime.rs:395-399`)** — clone it for the graph handler; do not reorder in a way that breaks the dw.agent handler.
- **Feature gate:** all `aw` graph code is behind `agentic-worker` (default-on). Run graph tests with `--features agentic-worker` (aw-runtime unit) / the host crate's default features.
- **Build discipline (shared machine):** `CARGO_BUILD_JOBS=2 cargo ... -j2`, FOREGROUND, scoped to `-p greentic-aw-runtime` / `-p greentic-runner-host`; never delete another worktree's `target/`. Avoid `--all-features`.
- **Conventional commits, NO Claude co-author** (per `greentic-runner/CLAUDE.md`).

## File Structure

- **Modify** `crates/greentic-aw-runtime/src/graph/model.rs` — add `agent_ref` to `NodeKind::Agent`; `Graph::validate` Agent arm.
- **Modify** `crates/greentic-aw-runtime/src/graph/executor.rs` — add `agent_ref` to `AgentTurnRequest` + the two destructure/construct sites.
- **Modify** `crates/greentic-runner-host/src/runner/graph_node.rs` — thread `merged_agents` through `RuntimeGraphNodeHandler`/`RuntimeTurnSource`/`from_parts`/`build_graph_node_handler`/`build_agent_turn`; consume `agent_ref` in `run_one_agent_turn`.
- **Modify** `crates/greentic-runner-host/src/runner/runtime.rs` — clone `merged_agents` into `build_graph_node_handler`.
- **Tests:** inline `#[cfg(test)]` in `model.rs`, `executor.rs`; integration in `crates/greentic-runner-host/tests/` (a graph with an `agent_ref` node + a merged config carrying memory/knowledge).

---

### Task 1: Carry `agent_ref` through the graph model + `AgentTurnRequest` (aw-runtime)

**Files:**
- Modify: `crates/greentic-aw-runtime/src/graph/model.rs` (`NodeKind::Agent` @ :28-40)
- Modify: `crates/greentic-aw-runtime/src/graph/executor.rs` (`AgentTurnRequest` @ :60-79; destructure/construct sites @ :549-571 and :1358-1381)
- Test: inline `#[cfg(test)]` in `model.rs`

**Interfaces:**
- Produces: `NodeKind::Agent` gains `pub agent_ref: Option<String>` (serde camelCase → `"agentRef"`, default None, skipped when None). `AgentTurnRequest` gains `pub agent_ref: Option<String>`. Consumed by Task 3.

- [ ] **Step 1: Write the failing test** (append to `model.rs` tests): a graph JSON whose agent node has `"agentRef": "support"` parses and the node's `agent_ref == Some("support")`; a node without it ⇒ `None`.

```rust
#[test]
fn agent_node_carries_optional_agent_ref() {
    let j = serde_json::json!({
        "schemaVersion": 1,
        "entry": "a",
        "nodes": [
            {"id":"a","kind":"agent","systemPrompt":"","model":"gpt-4","agentRef":"support"},
            {"id":"r","kind":"respond"}
        ],
        "edges": [{"from":"a","to":"r"}]
    });
    let cfg = GraphConfig::from_json(&j.to_string()).unwrap();
    let a = cfg.graph.nodes.iter().find(|n| n.id == "a").unwrap();
    match &a.kind {
        NodeKind::Agent { agent_ref, .. } => assert_eq!(agent_ref.as_deref(), Some("support")),
        other => panic!("expected Agent, got {other:?}"),
    }
    // absent -> None: re-serialize without agentRef must round-trip to None (skip_serializing_if)
    let back = serde_json::to_value(&a.kind).unwrap();
    assert_eq!(back["agentRef"], serde_json::json!("support"));
}
```

- [ ] **Step 2: Run — expect FAIL** (`no field agent_ref`):

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime -j2 --features agentic-worker agent_node_carries_optional_agent_ref`
Expected: FAIL — compile error (unknown field in destructure / missing field).

- [ ] **Step 3: Implement — model.rs.** In `NodeKind::Agent` (`:28-40`) add the field after `provider`:

```rust
    Agent {
        system_prompt: String,
        model: String,
        #[serde(default)]
        tools: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        /// When set, run the referenced published agent's FULL config
        /// (memory/knowledge/guardrails) resolved from the pack's merged
        /// agents, instead of the inline system_prompt/model/tools.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_ref: Option<String>,
    },
```

- [ ] **Step 4: Implement — executor.rs `AgentTurnRequest`** (`:60-79`) add field after `tools`:

```rust
    pub tools: Vec<String>,
    /// Referenced published-agent id (from the node's `agent_ref`); when Some,
    /// the host runs that agent's full config instead of the inline fields.
    pub agent_ref: Option<String>,
```

- [ ] **Step 5: Implement — both destructure + construct sites in executor.rs.** At site A (`:549-555` destructure, `:564-571` construct) and site B (`:1358-1364`, `:1374-1381`): capture `agent_ref` in the `NodeKind::Agent { system_prompt, model, provider, tools, agent_ref, .. }` destructure and set `agent_ref: agent_ref.clone()` in the `AgentTurnRequest { .. }` build. Keep every other field unchanged.

- [ ] **Step 6: Fix any other `NodeKind::Agent {` / `AgentTurnRequest {` literal** that the new field breaks (grep `rg "NodeKind::Agent \{|AgentTurnRequest \{" crates/greentic-aw-runtime/src`). Existing destructures using `..` are unaffected; explicit constructions in tests need `agent_ref: None`.

- [ ] **Step 7: Run — expect PASS.**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime -j2 --features agentic-worker agent_node_carries_optional_agent_ref`
Expected: PASS.

- [ ] **Step 8: Commit.**

```bash
git add crates/greentic-aw-runtime/src/graph/model.rs crates/greentic-aw-runtime/src/graph/executor.rs
git commit -m "feat(graph): carry optional agent_ref through Agent node + AgentTurnRequest"
```

---

### Task 2: Thread the merged-agents map into the graph handler (runner-host)

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/graph_node.rs` (`RuntimeTurnSource` @ :448-465; `RuntimeGraphNodeHandler` @ :538-551; `from_parts` @ :566-611; `build_graph_node_handler` @ :337-402; `build_agent_turn` @ :930-970; `RuntimeTurnSource::agent_turn` @ :467-485)
- Modify: `crates/greentic-runner-host/src/runner/runtime.rs` (the `build_graph_node_handler(...)` call @ :395-399; `merged_agents` move sites @ :324/:348)
- Test: inline `#[cfg(test)]` assertion that a handler built with a map exposes it to the turn (or a helper unit — see Step 4)

**Interfaces:**
- Consumes: `merged_agents: std::collections::HashMap<String, greentic_aw_runtime::AgentConfig>` (built in `runtime.rs`).
- Produces: `RuntimeTurnSource` + `RuntimeGraphNodeHandler` gain a `merged_agents: Arc<HashMap<String, AgentConfig>>` field; `build_graph_node_handler` gains a `merged_agents` parameter; `run_one_agent_turn` (Task 3) receives `merged_agents: Arc<HashMap<String, AgentConfig>>`.

- [ ] **Step 1: Read the precedent** — how `merged_agents` flows today: `runtime.rs:324`/`:348` (`build_agent_node_handler(merged_agents, …)` — the MOVE) and `runtime.rs:395-399` (`build_graph_node_handler(graphs, agent_audit_sink, packs)` — no map today). Confirm the map type at its construction site.

- [ ] **Step 2: Add the field + param (no behavior change yet).**
  - `RuntimeTurnSource` (`:448-465`): add `merged_agents: Arc<HashMap<String, AgentConfig>>`.
  - `RuntimeGraphNodeHandler` (`:538-551`): add `merged_agents: Arc<HashMap<String, AgentConfig>>`.
  - `from_parts` (`:566-611`): accept `merged_agents: Arc<HashMap<String, AgentConfig>>` and store it on both structs.
  - `build_graph_node_handler` (`:337-402`): add a `merged_agents: HashMap<String, AgentConfig>` param; wrap `Arc::new(merged_agents)` and pass into `from_parts`.
  - `build_agent_turn` (`:930-970`) + `RuntimeTurnSource::agent_turn` (`:467-485`): clone the `Arc` into the closure and pass it to `run_one_agent_turn` (add the param to the `run_one_agent_turn` signature now, unused until Task 3 — pass it, prefix `_merged_agents` to avoid the unused warning until Task 3 consumes it).

- [ ] **Step 3: Update `runtime.rs` call site** (`:395-399`). Because `merged_agents` is MOVED into `build_agent_node_handler` at `:324`/`:348`, clone it for the graph handler BEFORE that move:

```rust
// before the build_agent_node_handler(merged_agents, …) move:
let graph_agents = merged_agents.clone();
// … existing build_agent_node_handler(merged_agents, …) …
// at the graph handler build:
build_graph_node_handler(graphs, agent_audit_sink, packs, graph_agents)
```
(Match the exact existing arg order at `:395-399`; append `graph_agents` as the new trailing arg matching Step 2's param.)

- [ ] **Step 4: Compile + a wiring test.** There is no behavior yet; assert the plumbing compiles and the map reaches the handler. Add a `#[cfg(test)]` unit constructing a `RuntimeGraphNodeHandler` via `from_parts` with a one-entry map and asserting `handler.merged_agents.contains_key("x")` (add a `#[cfg(test)]`-only accessor if the field is private).

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host -j2 graph_node`
Expected: compiles; wiring test PASS.

- [ ] **Step 5: Commit.**

```bash
git add crates/greentic-runner-host/src/runner/graph_node.rs crates/greentic-runner-host/src/runner/runtime.rs
git commit -m "feat(graph): thread merged-agents map into the graph node handler"
```

---

### Task 3: Resolve `agent_ref` → run the full-fidelity agent turn (runner-host)

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/graph_node.rs` (`run_one_agent_turn` @ :1056-1155)
- Test: `crates/greentic-runner-host/tests/` new integration test (a graph with an `agent_ref` node + a merged config carrying `memory`/`knowledge` populated)

**Interfaces:**
- Consumes: `AgentTurnRequest.agent_ref` (Task 1); `merged_agents: Arc<HashMap<String, AgentConfig>>` (Task 2); the attachment calls in `agent_node::build_agent_runtime` (`agent_node.rs:1366-1473`): `.with_guardrails(...)`, `.with_short_term_memory(Arc::new(InMemoryMemoryProvider::new()))`, `crate::runner::long_term_memory::attach(base).await` (`#[cfg(feature = "long-term-chronicle")]`), `crate::runner::knowledge_mount::attach(base).await` (`#[cfg(feature = "knowledge-chronicle")]`).
- Produces: `run_one_agent_turn` runs the referenced agent's full config when `agent_ref` is `Some`.

- [ ] **Step 1: Read the precedent** — `agent_node::build_agent_runtime` (`agent_node.rs:1366-1473`): the exact `.with_guardrails` / `.with_short_term_memory` / long-term + knowledge `attach` sequence, and `ExtRuntimeGuardrailEvaluator`. This is what the `agent_ref` branch mirrors.

- [ ] **Step 2: Write the failing integration test.** A graph `{ supervisor "c" → agent "s" (agent_ref="faq") → respond }`, with a `merged_agents` map whose `"faq"` config has a distinctive `system_prompt` (e.g. contains `"FAQ-BOT"`) and non-empty `memory`/`knowledge`. Drive the graph with a stub LLM (record the system prompt seen) and assert: (a) the turn used the `"faq"` config's system_prompt (contains `"FAQ-BOT"`), NOT the node's inline `system_prompt`; (b) a graph whose node has `agent_ref="missing"` (not in the map) ⇒ the run errors with `"referenced agent 'missing' not found"`; (c) regression: a node with `agent_ref=None` still uses its inline `system_prompt`. Mirror the existing graph_node integration-test harness (search `crates/greentic-runner-host/tests` for the current graph executor test that injects a stub turn / LLM). If no LLM-injecting graph harness exists, assert at the `run_one_agent_turn` seam instead: factor the config-resolution into a tested helper `resolve_turn_config(req: &AgentTurnRequest, merged: &HashMap<String,AgentConfig>) -> Result<AgentConfig, GraphExecError>` and unit-test that (agent_ref present → full config from map incl. memory/knowledge; None → the stripped inline config; missing id → Err).

- [ ] **Step 3: Run — expect FAIL** (`resolve_turn_config` undefined / inline prompt used).

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host -j2 agent_ref`
Expected: FAIL.

- [ ] **Step 4: Implement `resolve_turn_config`** in `graph_node.rs` (near `run_one_agent_turn`):

```rust
/// The AgentConfig a graph agent-turn runs. `agent_ref` present → the full
/// published config resolved from the merged map (memory/knowledge/guardrails
/// intact); absent → the inline config fabricated from the node fields.
fn resolve_turn_config(
    req: &AgentTurnRequest,
    merged: &std::collections::HashMap<String, AgentConfig>,
    agent_id: &str,
) -> Result<AgentConfig, GraphExecError> {
    if let Some(id) = req.agent_ref.as_deref() {
        return merged.get(id).cloned().ok_or_else(|| {
            GraphExecError::AgentTurn(format!("referenced agent '{id}' not found"))
        });
    }
    Ok(AgentConfig {
        agent_id: agent_id.to_string(),
        system_prompt: req.system_prompt.clone(),
        tools: map_tool_refs(&req.tools),
        guardrails: vec![],
        llm: LlmProviderRef {
            provider: req.provider.clone().unwrap_or_else(|| "openai".into()),
            model: req.model.clone(),
            credential_ref: None,
        },
        limits: AgentLimits::default(),
        memory: None,
        knowledge: None,
        conversational: false,
    })
}
```

- [ ] **Step 5: Rewire `run_one_agent_turn`** (`:1056-1155`) to use `resolve_turn_config` + build the full runtime when `agent_ref` is set. Add the `merged_agents: Arc<HashMap<String, AgentConfig>>` param (threaded in Task 2). Replace the hard-coded `cfg` build with:

```rust
    let cfg = resolve_turn_config(&req, &merged_agents, &agent_id)?;
    // key the provider by the id we will .step() with:
    let step_agent_id = req.agent_ref.clone().unwrap_or_else(|| agent_id.clone());
    let mut provider = InMemoryConfigProvider::new();
    provider.insert(&tenant, &step_agent_id, cfg.clone());

    let component_source =
        super::super::agent_node::component_source_from_packs(&packs, real_tenant.tenant_id.as_str());

    let mut runtime = AgentRuntime::new(
        Arc::new(provider), state_store, ext_runtime.clone(), llm,
        telemetry, token_meter, ledger, mcp_source,
    )
    .with_component_source(component_source);

    // Full fidelity ONLY for a referenced agent (mirrors build_agent_runtime).
    if req.agent_ref.is_some() {
        runtime = runtime
            .with_guardrails(
                std::sync::Arc::new(greentic_aw_runtime::guardrail::StaticGuardrailPolicy::new(cfg.guardrails.clone())),
                std::sync::Arc::new(super::super::agent_node::ExtRuntimeGuardrailEvaluator { ext_runtime: ext_runtime.clone() }),
            )
            .with_short_term_memory(std::sync::Arc::new(
                greentic_aw_runtime::memory::InMemoryMemoryProvider::new(),
            ));
        #[cfg(feature = "long-term-chronicle")]
        { runtime = crate::runner::long_term_memory::attach(runtime).await; }
        #[cfg(feature = "knowledge-chronicle")]
        { runtime = crate::runner::knowledge_mount::attach(runtime).await; }
    }

    let out = run_agent_step(
        &runtime, tenant.clone(), &session_id, &step_agent_id, AgentInput { text: String::new() },
        audit_sink, real_tenant,
    ).await.map_err(|e| GraphExecError::AgentTurn(format!("agent step failed: {e}")))?;
```
Verify the exact `with_guardrails` policy/evaluator types + `StaticGuardrailPolicy` constructor against `build_agent_runtime` (Step 1) and adjust the two `Arc::new(...)` lines to match its real signatures. Keep the state-seed block and the `AgentTurnResult { resolved, reply }` return unchanged.

- [ ] **Step 6: Run — expect PASS.**

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host -j2 agent_ref`
Expected: PASS (agent_ref uses full config; missing id errors; None unchanged).

- [ ] **Step 7: Commit.**

```bash
git add crates/greentic-runner-host/src/runner/graph_node.rs
git commit -m "feat(graph): run a referenced agent turn with its full config (memory/knowledge/guardrails)"
```

---

### Task 4: Validate `agent_ref` + gate + PR

**Files:**
- Modify: `crates/greentic-aw-runtime/src/graph/model.rs` (`Graph::validate` Agent arm @ :249-259)
- Test: inline `#[cfg(test)]` in `model.rs`

- [ ] **Step 1: Write the failing test** — a graph whose agent node has NEITHER inline `system_prompt`/`model` meaningfully NOR `agent_ref` should still validate (both are strings today, so this is advisory); the concrete rule we enforce: an agent node with `agent_ref` set is valid with exactly one outgoing edge (unchanged edge rule). Assert an `agent_ref` node with one outgoing edge validates; with zero edges fails the existing "exactly 1 outgoing edge" rule.

```rust
#[test]
fn agent_ref_node_validates_with_one_edge() {
    let ok = /* graph JSON: agent "a" with agentRef + 1 edge to respond */;
    assert!(GraphConfig::from_json(&ok).is_ok());
    let bad = /* same but agent "a" has 0 outgoing edges */;
    assert!(GraphConfig::from_json(&bad).unwrap_err().contains("exactly 1 outgoing edge"));
}
```

- [ ] **Step 2: Run — expect PASS already** (the existing edge rule covers this; the `Agent { .. }` arm ignores fields). If it passes, no `validate` change is needed — DELETE any speculative validation. Only add code if a test genuinely fails. (YAGNI: do not add "inline XOR agent_ref" enforcement unless a real requirement needs it — the runtime already errors on a missing `agent_ref`, and inline fields are harmlessly ignored when `agent_ref` is set.)

Run: `CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime -j2 --features agentic-worker agent_ref_node_validates_with_one_edge`
Expected: PASS with no `validate` change.

- [ ] **Step 3: Gate.**

```bash
cargo fmt --all
CARGO_BUILD_JOBS=2 cargo clippy -p greentic-aw-runtime -p greentic-runner-host -j2 --all-targets -- -D warnings
CARGO_BUILD_JOBS=2 cargo test -p greentic-aw-runtime -p greentic-runner-host -j2
```
Expected: clean; all green.

- [ ] **Step 4: PR.** Use superpowers:finishing-a-development-branch → PR `feat/agent-ref-graph-sp1` → `research`. Body: SP1 of the reusable-agent-specialist epic (spec `docs/superpowers/specs/2026-07-13-agent-graph-attach-existing-agent-design.md`). Additive `NodeKind::Agent.agent_ref` + full-fidelity execution of a referenced agent. Next: SP2 (dw-authoring resolves `Specialist.from_published` → embeds config + sets `agent_ref`). NO Claude co-author.

---

## Self-Review

- **Spec coverage:** design §SP1 "add `agent_ref` to `NodeKind::Agent`, gated schemaVersion 2" → Task 1 (note: Agent is valid in v1 already, so the field is additive without a v2 bump; validate unchanged per Task 4). "executor resolves full AgentConfig from the merged agents + builds full runtime (memory/knowledge/guardrails) via `build_agent_runtime`" → Tasks 2 (plumb map) + 3 (resolve + full runtime). "missing ref → clear error" → Task 3 (`resolve_turn_config` Err). "None ⇒ byte-unchanged" → Task 3 regression + the `if req.agent_ref.is_some()` gate. §Testing SP1 (full config used / missing errors / None unchanged) → Task 3 Step 2.
- **Out of scope (correctly deferred):** who SETS `agent_ref` (SP2 authoring); the designer UI (SP3); per-turn runtime caching (design §Risks — a hardening detail, not v1); the `dw-agents.json` embed of a cross-pack agent (SP2). SP1 only makes the runtime *able* to run a referenced agent.
- **Placeholder scan:** the two "read the precedent" steps (Task 2 Step 1, Task 3 Step 1) point at exact code (`runtime.rs:324/348/395`, `agent_node.rs:1366-1473`) — the threading + attachment wiring must be matched to the real signatures there (same device as prior epics). Task 3 Step 5 flags verifying the `with_guardrails`/`StaticGuardrailPolicy` types against the precedent. Test bodies for the graph integration test reference "the existing graph harness" with a concrete fallback (the `resolve_turn_config` unit) so the task is testable regardless.
- **Type consistency:** `agent_ref: Option<String>` identical across model.rs (Task 1), executor.rs `AgentTurnRequest` (Task 1), consumed in `run_one_agent_turn` (Task 3). `merged_agents: Arc<HashMap<String, AgentConfig>>` defined in Task 2, consumed in Task 3. `resolve_turn_config` signature stable between Task 3 Step 4 (def) and Step 5 (call).
- **Scope:** two crates, one branch; additive; each task ends with a passing, independently-reviewable test cycle.
