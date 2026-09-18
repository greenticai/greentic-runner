# Graph-Node Agent Tools — Design Spec

**Status:** Draft — 2026-07-06
**Initiative:** Agentic platform coverage — EPIC-B follow-up (activates the latent B-3b agent-graph audit) + a real capability gap (graph agents cannot use tools).

## 1. Problem

An agent node inside an **agent graph** (`NodeKind::Agent`) already has a declarable `tools: Vec<String>` field in its schema (`greentic-aw-runtime/src/graph/model.rs`), and the graph handler already builds a real `ext_runtime` (design-extension tool catalog) and threads it into the turn source. **But the declared tool list is silently dropped at three layers**, so a graph agent can never call a tool — and the B-3b audit path (which emits `greentic.runner.agent.tool_call`/`tool_result` when a graph agent calls a tool) is latent: it can never fire in production because there is never any tool traffic.

The three drop points (verified on `research` @ 4340b3d):
1. `graph/executor.rs:544` + `:1350` — the two `NodeKind::Agent { .. }` destructures discard `tools` via `..`.
2. `graph/executor.rs:61` — `AgentTurnRequest` has no `tools` field, so the two constructions (`:557`, `:1364`) can't carry it.
3. `runner-host/src/runner/graph_node.rs:1042` — `run_one_agent_turn` hardcodes `tools: vec![]` in the `AgentConfig` it builds.

## 2. Goal

Thread the agent node's declared `tools` from the graph schema through `AgentTurnRequest` into the `AgentConfig` that `run_one_agent_turn` builds, mapping each flat tool string to a `ToolRef{extension_id, tool_name}` so the runtime's `list_tools_for_llm` resolves it against the already-wired design-extension catalog. Once tools resolve, the already-injected `AgentAuditObserver` (B-3b) emits tool-call/result audit events with **zero audit-code changes**.

This mirrors the existing precedent commit `4d2b9e1` ("thread per-node LLM provider through agent-graph turns"), which added the `provider` field to `AgentTurnRequest` and threaded it the same way.

## 3. The one design decision: flat string → `ToolRef`

The graph schema declares tools as flat `Vec<String>`, but `ToolRef` is `{extension_id, tool_name}`. No parser exists. **Convention (v1): `"<extension_id>/<tool_name>"`, split on the LAST `/`** — `extension_id` = everything before the last `/` (may itself contain `/` or `:`, e.g. `component:owner/repo`), `tool_name` = after the last `/`. A string with no `/` cannot name an extension + tool → **warn + skip** (dropped from the allowlist, never panics). This matches how `list_tools_for_llm` already treats unresolvable refs (warn + skip).

## 4. Scope

**In:** thread `tools` through the two `NodeKind::Agent` turn builds in `executor.rs` + map to `Vec<ToolRef>` in `run_one_agent_turn`; the `<ext>/<tool>` parse convention; unit tests at both layers.

**Out (v1, documented follow-ups):**
- `run_one_supervisor_turn` (`graph_node.rs:1199`) keeps `tools: vec![]` — supervisor nodes have no `tools` in their schema (correct, not a gap).
- No `McpToolSource` is attached to the graph runtime (the graph handler passes `None` for the MCP source) — **design-extension** tools resolve (real `ext_runtime` present), but `mcp:`/`component:`-prefixed tools would need an `McpToolSource` wired like the `dw.agent` path. Deferred; such a ref parses fine but warn+drops at resolution inside the runtime (documented). (`AgentConfig` itself has no `mcp` field — the tool source is a separate `AgentRuntime` argument.)
- **Designer emitting `tools` on graph agent nodes** — the designer currently emits `"tools": []` for graph nodes (`graph_to_flow.rs:102`). This slice makes the runner *honor* declared tools; a hand-authored `.ygtc` or a future designer change can populate them. Designer UX is a separate follow-up.

## 5. Testing (offline)
- **executor (`greentic-aw-runtime`):** a graph JSON with an agent node declaring `"tools": ["myext/dothing"]` → the built `AgentTurnRequest.tools == ["myext/dothing"]` (no longer dropped). Round-trip mirrors the existing `provider`-threading test.
- **graph_node (`greentic-runner-host`):** `parse_tool_ref("myext/dothing")` → `ToolRef{extension_id:"myext", tool_name:"dothing"}`; `parse_tool_ref("component:owner/repo/list")` → `{extension_id:"component:owner/repo", tool_name:"list"}`; `parse_tool_ref("noslash")` → `None`; `run_one_agent_turn` builds an `AgentConfig` whose `tools` reflects the mapped refs (non-empty when the request carries tools).
- The end-to-end audit emit is already covered by B-3b's tests; no new audit test needed (the observer is unchanged).

## 6. Rollout
Cross-crate change in one repo (greentic-runner): `greentic-aw-runtime` (request field) + `greentic-runner-host` (mapping) land together on one branch → `research`. Additive; supervisor + mcp paths unchanged. Follow-ups: MCP/component tools for graph agents; designer emit of graph-node tools.
