# Flow-Tool Author Contract — Design

**Status:** Approved design. Spans three repos; a rev-bump chain (A → B → C).

## Goal

Give an agent's LLM the **author-defined description + input schema** for a `flow:` tool (the values typed in the designer's "Add flow as tool" picker), instead of the current `{"type":"object"}` / `flow_ref` fallback. A follow-up to the agentic-worker Call-Flow tool (Spec 2), motivated by evidence that deriving the contract from the flow itself yields almost nothing (the runtime `flow_adapter` hardcodes `description: None`, and declared flow parameters are usually empty).

## Why this shape

The author's `description` + `input_schema_json` already travel most of the way: the designer's answer-doc path emits them, and `WorkerSpec.extension_tools` carries them into the pack builder. They are **dropped at two `ToolRef`-construction sites** solely because `greentic_aw_runtime::ToolRef` — reused verbatim by `greentic-dw-authoring` for the pack's `dw-agents.json` agent config — holds only `{extension_id, tool_name}`. The fix is: give `ToolRef` two optional fields, populate them where the binding is already in scope, and read them in one place.

Scope decision: the override applies to **flow tools only**. Component/MCP tools keep their catalog/manifest contract (authoritative and rich). The new `ToolRef` fields are populated for flow tools and ignored by the component/MCP branches of `list_tools_for_llm`.

## Architecture

Extend one type, populate at two sites, read at one branch.

### Wire format (aw-runtime)

`greentic_aw_runtime::ToolRef` (`crates/greentic-aw-runtime/src/config.rs:14`) gains two serde-default optional fields:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRef {
    pub extension_id: String,
    pub tool_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<serde_json::Value>,
}
```

Backward-compatible: no `deny_unknown_fields` on `ToolRef`, `AgentConfig`, or any parent; existing serialized payloads (designer registry JSON, pack `dw-agents.json`, manifests) deserialize with the fields defaulting to `None`; existing struct literals compile once the fields default (or are named). `greentic-aw-runtime` is `publish = false` (internal), so the wire change is not semver-constrained — consumers update in lockstep via the rev-bump chain.

### Read (aw-runtime)

`list_tools_for_llm` (`crates/greentic-aw-runtime/src/tools.rs`), **`flow:` branch only** (~:103): prefer the override when present, else the catalog.

```rust
if let Some(flow_ref) = t.extension_id.strip_prefix("flow:") {
    let entry = flows.and_then(|c| c.tool_entry(flow_ref));
    let description = t.description.clone()
        .or_else(|| entry.map(|e| e.description.clone()));
    let parameters = t.input_schema.clone()
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
            "flow tool has neither an author contract nor a catalog entry; dropping"
        ),
    }
    continue;
}
```

The `t` (`ToolRef`) is in scope in this branch. The component and MCP branches are **unchanged** (catalog-only). If the override supplies only one of the two, the other still falls back to the catalog.

### Populate (two sites)

Both sites already iterate the binding (which carries `description: String` + `input_schema_json: String`); they just need to set the new `ToolRef` fields, parsing the JSON string into a `Value`. A tiny shared helper `parse_input_schema(s: &str) -> Option<serde_json::Value>` (returns `None` on empty/invalid) keeps it DRY within each repo.

1. **greentic-dw-authoring** — `assemble::tool_refs_from_extension_tools` (`src/assemble.rs:444`): the pack path. Populate for every agentic binding; only flow tools will be read at runtime.
2. **greentic-designer** — `dw_form_to_agent_config::collect_agentic_tool_refs` (`src/orchestrate/dw_form_to_agent_config.rs:134`): the test-chat path.

The answer-doc path (`dw_form_to_answer_doc::extension_tool_to_doc`) already emits `description` + `input_schema_json`; **no change**.

## Decomposition — 3 slices (rev-bump chain)

- **Slice A — greentic-runner (aw-runtime):** extend `ToolRef`; `flow:` branch prefers the override. Defines the wire format. Merge → runner rev `R1`.
- **Slice B — greentic-dw-authoring:** populate the fields in `tool_refs_from_extension_tools`; pin aw-runtime `@R1`. Merge → dw-authoring rev `D1`.
- **Slice C — greentic-designer:** populate the fields in `collect_agentic_tool_refs`; pin aw-runtime `@R1` + dw-authoring `@D1`. Answer-doc unchanged.

Order: A first. B and C both consume A; C also pins B. Each downstream slice is a small change plus a `Cargo.toml` rev bump.

## Data flow (after)

```
designer picker  ExtensionToolBinding { description, input_schema_json }
   ├─ test-chat:   collect_agentic_tool_refs  → ToolRef{ …, description, input_schema }   (Slice C)
   └─ pack build:  answer-doc (already emits)  → WorkerSpec.extension_tools
                     dw-authoring tool_refs_from_extension_tools → ToolRef{…, description, input_schema} (Slice B)
                     → dw-agents.json AgentConfig.tools
runtime  list_tools_for_llm flow: branch → LlmToolSchema{ description, parameters } from the override  (Slice A)
```

## Error handling

- **Empty / invalid `input_schema_json`** → `parse_input_schema` returns `None`; the `flow:` branch falls back to the catalog `parameters` (today's `{"type":"object"}`). Never panics, never drops the tool for a bad schema alone.
- **Override present but flow removed from pack** → catalog entry absent; if the override supplies description + input_schema, the tool is still listed (the override is self-sufficient). Dispatch still resolves by `flow_ref` against the catalog; a truly-absent flow yields the existing `{"error": ...}` at call time.
- **Component/MCP tools with author descriptions** → ignored at runtime (branches unchanged); no behavior change.

## Testing

- **Slice A (aw-runtime):** `ToolRef` round-trips with and without the new fields (backward-compat). `list_tools_for_llm` flow branch: override present → LLM sees the override; override absent → catalog fallback; partial override (description only) → parameters from catalog.
- **Slice B (dw-authoring):** `tool_refs_from_extension_tools` populates `description` + parses `input_schema_json` into `input_schema`; empty/invalid schema → `None`; non-agentic bindings still filtered.
- **Slice C (designer):** `collect_agentic_tool_refs` populates the fields from the binding; a round-trip that a `flow:` binding's author contract reaches the built `ToolRef`.

## Non-goals (YAGNI)

- Overriding component/MCP tool contracts (flow-only).
- Deriving a schema from the flow's own nodes/params (the author contract supersedes it).
- Output-schema propagation (only description + input schema).
- Changing the answer-doc format (already carries the data).

## Rev-bump / activation

Same multi-repo pattern as the parent epic: A merges → B pins A + merges → C pins A/B + merges. The feature is exercised wherever `dw.agent` runs with flow tools (the production runner build). No separate deploy step beyond the normal rev-bump chain landing.
