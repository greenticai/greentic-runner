# Graph-Node Component Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Complete the graph-agent tool trilogy: after design-extension (`#523`) and MCP (`#524`) tools, wire the **component** tool source so a graph agent declaring a `component:<ref>/<tool>` ref resolves it — mirroring the `dw.agent` path's `.with_component_source(component_source_from_packs(&packs, &tenant))`.

**Architecture:** `component_source_from_packs(packs, tenant)` needs the tenant's `PackRuntime`s AND is tenant-pinned, so (unlike the MCP source, which is built once) it is built **per-turn** with the turn's tenant — cheap, because packs are already in memory (no network). Thread the pack list (built once) from `runtime.rs` → `build_graph_node_handler` → `from_parts` → `RuntimeTurnSource` → `build_agent_turn` → `run_one_agent_turn`, and there call `.with_component_source(component_source_from_packs(&packs, tenant))` on the `AgentRuntime`.

**Tech Stack:** Rust (edition 2024); `greentic_aw_runtime::{AgentRuntime, ComponentToolSource}`; `crate::pack::PackRuntime`.

## Global Constraints
- **Crate:** greentic-runner-host. Files: `src/runner/agent_node.rs` (expose `component_source_from_packs`), `src/runner/graph_node.rs` (thread packs + per-turn build), `src/runtime.rs` (pass packs at the call site).
- **`component_source_from_packs`** (`agent_node.rs:408`, in `mod aw`) — expose it `pub(crate)` by adding it to the existing `pub(crate) use aw::{...}` re-export (`agent_node.rs:2216`, same as `mcp_source_from_env`/`build_ext_runtime`). Off via `GREENTIC_AW_COMPONENT_TOOLS=0`; `None` semantics preserved.
- **Per-turn build (NOT once):** the component source is tenant-pinned + `RuntimeTurnSource` is shared across tenants, so build it inside `run_one_agent_turn` with the turn's tenant. This is cheap (in-memory packs, no admin fetch) — the MCP `built-once` concern (network per turn) does NOT apply here.
- **`AgentRuntime` builder:** `AgentRuntime::new(...)` then `.with_component_source(component_source_from_packs(&packs, tenant_str))` (`greentic-aw-runtime/src/lib.rs:248`). The graph turn currently does `let runtime = AgentRuntime::new(...)` (with the MCP source as the 8th arg from `#524`). Chain `.with_component_source(...)` on it. `mcp: None` behavior unchanged; supervisor unchanged.
- **Pack list source:** `runtime.rs` has `packs: Vec<Arc<PackRuntime>>` / `pack_runtimes` in `from_packs` (~:162-174) in scope at the `build_graph_node_handler` call (`runtime.rs:388`). Store on `RuntimeTurnSource` as `Arc<Vec<Arc<crate::pack::PackRuntime>>>` (cheap clone per turn).
- **Conventional commits, NO Claude co-author.** Target `research`.
- **Build discipline (shared machine, disk ~79GB ample):** cargo `-j2` + `CARGO_BUILD_JOBS=2`, FOREGROUND; if a build dies with `signal:15`/SIGKILL (memory pressure), retry once then `-j1`; never pkill/kill or delete another worktree's target/. **For the call-site cascade, build first and let the compiler (E0061) enumerate the exact broken sites, then fix each precisely — do NOT blind-replace (supervisor calls share arg shape and must NOT get the new param).**

---

### Task 1: thread pack list + per-turn component source into graph-agent turns

**Files:**
- Modify: `src/runner/agent_node.rs` (add `component_source_from_packs` to the `pub(crate) use aw::{...}` re-export at ~:2216)
- Modify: `src/runner/graph_node.rs` (`RuntimeTurnSource` struct + `from_parts` + `build_graph_node_handler` signatures + `agent_turn` + `build_agent_turn` + `run_one_agent_turn` + the `.with_component_source` call + fix test call sites)
- Modify: `src/runtime.rs` (pass the pack list at the `build_graph_node_handler` call ~:388)
- Test: the existing graph_node tests must still pass; add a `component_source_from_packs`-reachability compile assertion if cheap

**Interfaces:**
- Consumes: `agent_node::component_source_from_packs(packs: &[Arc<PackRuntime>], tenant: &str) -> Option<Arc<ComponentToolSource>>`; `AgentRuntime::with_component_source`.
- Produces: `RuntimeTurnSource` gains `packs: Arc<Vec<Arc<crate::pack::PackRuntime>>>`; `build_graph_node_handler`/`from_parts`/`build_agent_turn`/`run_one_agent_turn` gain a `packs` parameter.

- [ ] **Step 1: Read** `agent_node.rs:408` (`component_source_from_packs` body + the `GREENTIC_AW_COMPONENT_TOOLS=0` opt-out), `:927` (how `dw.agent` chains `.with_component_source`), `:2216` (the `pub(crate) use aw::{...}` re-export); `graph_node.rs` `RuntimeTurnSource` (~:462), `from_parts` (~:568), `build_graph_node_handler` (~:337 + the `from_parts` call ~:402), `agent_turn` (~:477), `build_agent_turn` (~:935), `run_one_agent_turn` (~:1057 + the `AgentRuntime::new` call ~:1090); `runtime.rs:388` (the `build_graph_node_handler` call + the in-scope pack list).
- [ ] **Step 2: Expose** `component_source_from_packs` by adding it to `pub(crate) use aw::{EnvSecretsBackend, build_ext_runtime, build_llm_backend, mcp_source_from_env};` → append `, component_source_from_packs`.
- [ ] **Step 3: Thread the pack list.** Add `packs: Arc<Vec<Arc<crate::pack::PackRuntime>>>` to `RuntimeTurnSource`; add a `packs` param to `from_parts` + `build_graph_node_handler` (thread it in); at `runtime.rs:388` pass the in-scope pack list (clone into an `Arc<Vec<...>>`). Add a `packs` param to `build_agent_turn` + `run_one_agent_turn` (thread from `agent_turn` → `build_agent_turn` → `run_one_agent_turn`, mirroring how `#524` threaded `mcp_source`). The supervisor path does NOT need packs (leave `build_supervisor` unchanged).
- [ ] **Step 4: Build the source per-turn.** In `run_one_agent_turn`, after `let runtime = AgentRuntime::new(...)`, chain `.with_component_source(super::super::agent_node::component_source_from_packs(&packs, real_tenant.tenant_id.as_str()))` (confirm the `TenantCtx`→`&str` accessor). Keep the MCP source arg + supervisor unchanged.
- [ ] **Step 5: Build + fix the call-site cascade.** `CARGO_BUILD_JOBS=2 cargo build -p greentic-runner-host --tests -j2`; the compiler (E0061) lists each `run_one_agent_turn`/`build_agent_turn`/`from_parts`/`build_graph_node_handler` call missing the new arg — fix each precisely (test helpers pass an empty `Arc::new(vec![])`; do NOT touch supervisor calls).
- [ ] **Step 6: Gate + commit.** `cargo fmt --all`; `CARGO_BUILD_JOBS=2 cargo clippy -p greentic-runner-host -j2 --all-targets -- -D warnings`; `CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host -j2 --lib -- graph_node component_source`. Commit (`feat(graph): wire component tool source into graph-agent turns (per-turn, tenant-scoped)`). Then finishing-a-development-branch → PR to `research` noting: completes the graph-tool trilogy (design-ext + MCP + component); production activation still needs the designer to emit graph-node tool refs (frontend follow-up).

## Self-Review
- **Coverage:** expose fn (Step 2) + thread packs (Step 3) + per-turn build (Step 4) + cascade (Step 5).
- **Placeholder scan:** "read agent_node:408/927/2216 + confirm TenantCtx→&str" are deliberate (exact accessor + chain shape from the repo). No TBD.
- **Type consistency:** `packs: Arc<Vec<Arc<PackRuntime>>>` on the struct + params; `component_source_from_packs(&packs, tenant_str)` per turn; `.with_component_source(...)` chained on `AgentRuntime`.
- **Scope:** 3 files; additive; per-turn build (cheap); supervisor + MCP + off-by-default (`GREENTIC_AW_COMPONENT_TOOLS=0`) unchanged.
