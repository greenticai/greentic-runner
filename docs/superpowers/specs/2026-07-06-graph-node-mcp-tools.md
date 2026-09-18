# Graph-Node MCP Tools — Design Note

**Status:** Implemented — 2026-07-06
**Initiative:** Agentic platform coverage — completes the runner-side tool support for agent-graph nodes (follow-up to `#523`, which wired *design-extension* tools).

## Problem
`#523` made a graph agent node (`NodeKind::Agent`) resolve its declared `tools` (`<ext>/<tool>` → `ToolRef`) against the design-extension catalog. But the graph turn's `AgentRuntime` was built with **no MCP tool source** (`graph_node.rs`'s `run_one_agent_turn` passed `None` as the 8th `AgentRuntime::new` arg). So a graph agent declaring an `mcp:<server>/<tool>` ref parsed fine but warn-dropped at resolution — graph agents could use design-extension tools but not MCP tools, unlike the `dw.agent` path.

**Scope note:** this wires only the **MCP** source (`mcp:<server>/<tool>` refs). The `dw.agent` path also attaches a component source (`.with_component_source(...)`); that is *not* added here, so `component:` refs on graph nodes still resolve to nothing — a separate follow-up.

## Change
Mirror the `dw.agent` path: pass `agent_node::mcp_source_from_env()` (already used at `agent_node.rs:925`/`:1149` for `dw.agent`) as the graph turn's MCP source instead of `None`. `mcp_source_from_env` builds an `McpToolSource` from the admin endpoint/token (`GREENTIC_AW_ADMIN_ENDPOINT` + `GREENTIC_AW_ADMIN_TOKEN`), off via `GREENTIC_AW_MCP=0`, `None` when creds are absent — so the default (no creds) is unchanged. Exposed it `pub(crate)` via the existing `pub(crate) use aw::{...}` re-export.

Authorization is unchanged and double-gated exactly as for `dw.agent`: the tenant registers the MCP server (with the `agentic_worker` role) in admin, AND the graph node's tool allowlist must explicitly reference `mcp:<server_id>`. Tenant identity is carried by the single admin bearer token baked into `mcp_source_from_env()` (the `TenantContext` passed to the catalog is only a cache key) — the same single-`GREENTIC_AW_ADMIN_TOKEN`-per-process model `dw.agent` already uses, so there is no cross-tenant lookup.

**The source is built once** (in `RuntimeGraphNodeHandler::from_parts`, alongside the other Arcs) and stored on `RuntimeTurnSource`, so its 5-min TTL catalog cache + warmed HTTP client are reused across every graph-agent visit — NOT reconstructed per turn.

`run_one_supervisor_turn` keeps `None` (supervisor nodes have no `tools` in their schema).

## Scope
One-line wiring at the graph agent turn + a `pub(crate)` re-export. No new MCP logic (reuses `mcp_source_from_env` + `McpToolSource`). Covered by the existing `mcp_source_from_env_default_on_with_opt_out` test; the graph turn now constructs the runtime with the source (compile-verified).

## Follow-up (production activation)
As with design-extension graph tools, the **designer** must emit `tools` (including `mcp:`/`component:` refs) on graph agent nodes for this to activate in visually-authored flows (it currently emits `[]`). Hand-authored `.ygtc` graphs can declare them today. Designer emit is a separate frontend follow-up.
