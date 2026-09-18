# Epic: Attach an Existing Published Agent as an Agent-Graph Specialist — Design

**Status:** approved (brainstorm), pending spec review
**Scope:** multi-repo epic — decomposed into 3 sub-projects (SP1 runtime → SP2 authoring → SP3 designer), each with its own spec + plan + PR.
**Repos:** greentic-runner (`greentic-aw-runtime`), greentic-dw-authoring, greentic-designer.

## Goal

Let a flow/agent author build a multi-agent graph by **reusing agents they already built** instead of re-authoring each specialist inline. In the Agentic Worker Composer's **Graph** tab, a specialist can be an **existing published agent** attached by reference — its full behaviour (instructions, model, tools, memory, knowledge, guardrails) carries into the graph. The coordinator only sets that specialist's routing (`Handles`).

Target UX: `Coordinator → [Attach: "Support-FAQ Agent"] + [Attach: "Refund Agent"] + [+ New agent]` — the specialists are the user's saved Agentic Workers, single-source-of-truth, not duplicated definitions.

## Background — why this needs new primitives

Established during recon (all file:line current on `research`):

- **Agent-graph specialists are inline today.** The designer form (`web/src/features/dw-composer/agentGraphForm/types.ts:1-12` — `SpecialistForm { id, name, handles, instructions, tools }`) and the dw-authoring spec (`greentic-dw-authoring/src/model.rs:203-210` — `Specialist { name, instructions, tools }`) both carry only inline instructions + tools. `formToGraph.ts:52-70` projects a specialist → an `agent.llm` graph node copying only `systemPrompt`(=instructions), `tools`, and the **graph-wide default** model/provider — dropping memory/knowledge/guardrails/per-agent model.
- **The runtime graph model is purely inline.** `greentic-aw-runtime/src/graph/model.rs:29-40` — `NodeKind::Agent { system_prompt, model, tools, provider }`. No `agent_id`/reference field on any node kind. The graph executor's `run_one_agent_turn` (`greentic-runner-host/src/runner/graph_node.rs`) builds an ephemeral `AgentConfig` from the node's inline fields with `memory: None` — so **graph agents run without memory/knowledge/guardrails today**.
- **But full agent configs already exist and are addressable.** A published agent's full `AgentConfig` (`greentic-aw-runtime/src/config.rs:105-125` — `system_prompt, tools, guardrails, llm, limits, memory, knowledge, conversational`) lives in the shared index `~/.greentic/dw-agents/index.json` (`DwAgentIndexEntry.agent_config`, `greentic-designer/src/orchestrate/dw_agent_index.rs:33-34`), is retrievable by id (endpoint `POST /api/dw/composer/open/{agent_id}`, `dw_composer_import.rs:90-133`), and is **already embedded per-pack** as the `dw-agents.json` sidecar (`inject.rs:25-42`; consumed by top-level `dw.agent` via `pick_agent_config`, `dw_worker_store.rs:365`).

So the epic is: (1) let a graph agent-node **reference** an agent by id and have the runtime run it with its **full** config from the pack's own sidecar; (2) let the build **resolve + embed** a referenced published agent; (3) surface **attach existing / + new agent** in the composer.

## Architecture

```
Composer (Graph tab) — specialist "Attach existing agent":
    pick from /api/dw/agents  →  store agent_id (+ Handles for routing)
    ( instructions / model / tools = the agent's own, read-only )
                    │
                    ▼  Build .gtpack (dw-authoring assemble)
    resolve agent_id → full AgentConfig
      → embed into THIS pack's dw-agents.json sidecar (already the mechanism)
      → graph agent-node carries agent_ref = agent_id   (not the copied fields)
                    │
                    ▼  Runtime (graph executor)
    node has agent_ref → load full AgentConfig from the in-pack sidecar
      → run with FULL runtime: memory + knowledge + guardrails + tools
      → self-contained (no external fetch), same behaviour as standalone
```

**Central decision:** the graph node stores an **`agent_ref` (agent_id)**, not a copy of the fields. The full config lives once, in the pack's `dw-agents.json` sidecar (DRY + self-contained + portable). This is what lets "resolve at build" + "full fidelity" + "portable pack" coexist. "Resolve at build" = the build re-pulls the referenced agent's latest published config into the sidecar each time, so republishing the source and rebuilding picks up changes; the runtime never fetches externally.

## The three sub-projects

### SP1 — Runtime: graph agent-node runs a referenced agent with full fidelity (greentic-runner / aw-runtime)

- `NodeKind::Agent` (`graph/model.rs:29-40`) gains `#[serde(default)] agent_ref: Option<String>`. `None` ⇒ today's inline behaviour (byte-unchanged, backward compatible). `Some(id)` ⇒ inline fields ignored; config resolved from the sidecar. Gated at graph schemaVersion 2 (v2 kinds already exist); `Graph::validate` accepts the field.
- The graph executor's agent turn (`run_one_agent_turn`, `runner-host/src/runner/graph_node.rs`): when the node has `agent_ref`, resolve the full `AgentConfig` from the **same** per-tenant/pack merged-agents source top-level `dw.agent` uses (`pick_agent_config` / the merged `dw-agents.json`), and build a **full `AgentRuntime`** (memory + knowledge + guardrails + tools) via the shared `build_agent_runtime` that `agent_node.rs` uses — instead of today's stripped ephemeral runtime. **This is the largest lift.**
- Routing is unchanged (supervisor → this node); only *where the config comes from* and *that the runtime is full* change.
- Error, not silent: `agent_ref` id absent from the sidecar ⇒ a clear turn error.
- Per-turn full-runtime build cost may be cached (plan detail).
- **Out of scope for SP1:** the designer UI (SP3) and the authoring resolve/embed (SP2). SP1 only makes the runtime *able* to run a referenced agent with full fidelity; who *sets* `agent_ref` is SP2.

### SP2 — Authoring: resolve + embed a referenced published agent (greentic-dw-authoring)

- `Specialist` (`model.rs:203-210`) gains `#[serde(default)] from_published: Option<String>` (agent_id).
- `assemble` (`agent_configs` / `build_agent_config`, `assemble.rs:345-422`): `from_published = None` ⇒ unchanged (build config from inline instructions+tools). `from_published = Some(id)` ⇒ resolve the published agent's full `AgentConfig` (from the shared index `~/.greentic/dw-agents/index.json`'s `agent_config`, falling back to pack-bytes reconstruction for legacy entries whose `agent_config` is `None`), **embed it into this pack's `dw-agents.json` sidecar** (`embed_dw_agents`, `inject.rs:25-42`), and set `agent_ref = id` on the generated graph agent-node.
- The specialist's `Handles` still becomes the supervisor `routes[].description` (routing unchanged).

### SP3 — Designer: attach existing / + new agent (greentic-designer)

- `SpecialistCard.tsx` gains a **mode selector**: **Define inline** (current, retained) / **Attach existing agent** / **+ New agent**.
  - *Attach existing agent*: picker from `/api/dw/agents` (reuse `DynamicAgentSelect` / `useDwAgents`); store `agent_id`. Card collapses to: agent name + **Handles** (routing) only — instructions/tools/model are the agent's own and not shown for editing. Optional read-only preview via the existing `POST /api/dw/composer/open/{id}`.
  - *+ New agent*: opens the Composer for a new agent in a **new browser tab**; after the user publishes it there and returns, refreshing the picker surfaces it.
- `SpecialistForm` (`types.ts:1-7`) gains `fromPublished?: string`.
- `formToGraph.ts:52-70`: when `fromPublished` is set, project the specialist → an `agent.llm` node carrying `agent_ref = id` (not inline systemPrompt/tools). `graphToForm.ts` decodes `agent_ref` back to attach mode.
- `/api/dw/agents` needs no new fields for v1 — id + display_name is enough for the picker; the full config is resolved at Build, not in the browser.

## Decomposition & order

Strict dependency order (each is its own spec → plan → PR):

1. **SP1** (runtime) — reference + full-fidelity execution. Independently testable (a graph with an `agent_ref` node + a sidecar config runs with memory/knowledge). Foundational + highest risk.
2. **SP2** (dw-authoring) — resolve + embed the referenced config; set `agent_ref`. Depends on SP1's node field.
3. **SP3** (designer) — the attach/new-agent UI + projection. Depends on SP2 consuming `from_published` and SP1 honouring `agent_ref`. Needs a runner + dw-authoring rev bump (mirrors prior epics: runtime PR → publish → designer rev bump).

Each sub-project ships working, independently-reviewable software. SP1–SP2 are runtime/authoring; SP3 is the user-facing payoff.

## Backward compatibility

- `NodeKind::Agent.agent_ref` and `Specialist.from_published` are additive `#[serde(default)] Option<String>`; absent ⇒ every existing agent-graph is byte-unchanged inline behaviour.
- Existing inline authoring (Define inline) stays. Attach is an added mode, not a replacement.

## Risks / open questions (resolve in sub-project specs)

- **SP1 full-runtime build per specialist:** the graph executor must build memory/knowledge/guardrail providers per referenced specialist (mirrors the `dw.agent` path, absent from the graph path today). Decide caching of the per-agent runtime within a graph run.
- **SP2 config source:** resolve from the shared `dw-agents.json` index file directly, or have the designer pass the resolved config into the build. The index path is the DRY source; the designer-passed path avoids dw-authoring reading a user-home file. Decide in SP2 spec.
- **Missing reference:** a `from_published`/`agent_ref` id not found (unpublished/deleted) — fail the **Build** with a clear error (SP2), and a clear **turn** error at runtime (SP1); never silently fall back to an empty agent.
- **Legacy `agent_config: None`:** older spec-packed agents lack an inline config in the index (`dw_agent_index.rs:81`); SP2 must fall back to reconstructing from the published pack bytes (always available).
- **Self/cycle reference:** attaching an agent-graph agent, or an agent that references back — guard against cycles at Build (SP2) the way flow-as-tool guards self-recursion.

## Testing strategy (per sub-project)

- **SP1:** a graph with one agent-node carrying `agent_ref` + a sidecar `AgentConfig` that has knowledge/memory ⇒ the turn runs with the sidecar's system_prompt/model/**memory/knowledge**, not the node's inline fields. Regression: a node without `agent_ref` behaves exactly as today. Missing `agent_ref` id ⇒ explicit error.
- **SP2:** authoring test — a `Specialist { from_published: Some(id) }` ⇒ the built pack's `dw-agents.json` sidecar contains the resolved full config and the graph node carries `agent_ref = id`; inline specialists unchanged; missing id ⇒ build error; legacy `agent_config: None` ⇒ pack-bytes fallback resolves.
- **SP3:** designer — attach mode round-trips through `formToGraph`/`graphToForm` (a `fromPublished` specialist ⇒ `agent_ref` node ⇒ back to attach mode); picker lists published agents; "+ New agent" opens a composer tab; inline mode unchanged.
