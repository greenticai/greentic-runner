# SP1 — Exit signal: `end_conversation` built-in tool (greentic-aw-runtime) — Design

**Status:** approved (brainstorm), pending spec review
**Epic:** [In-flow Conversational Chat Segment](2026-07-07-conversational-agent-chat-segment-epic-design.md) — sub-project 1 of 4
**Scope:** single crate — `greentic-aw-runtime`. No cross-repo changes.
**Base:** `origin/research` (epic doc lives here). Branch `feat/conversational-agent-sp1` → PR to `research`.

## Goal

Give the agentic-worker Plan-Act-Observe loop a way to signal *"this conversation is
complete"*. Today every normal reply terminates the loop with
`TerminationReason::FinalReply`; there is no "conversation is over" value for the
downstream park-loop node (SP2) to key on.

SP1 adds:

1. A new `TerminationReason::ConversationEnded` value.
2. A host built-in `end_conversation` tool, offered **only** to conversational agents.
3. Loop handling: when the model calls `end_conversation`, the turn terminates with
   `ConversationEnded` and the agent's closing message becomes the final `reply`.
4. A short system-prompt note (conversational agents only) telling the model the tool exists.

The `AgentOutput` contract (`{reply, trail, terminated_by}`) is unchanged in shape —
only a new `terminated_by` value can now appear. SP2 (runner engine) consumes it.

## Non-goals (deferred to later sub-projects)

- **Park-and-loop node behaviour** keyed on `ConversationEnded` → SP2 (runner engine).
- **`conversational` flag in the flow doc / IR** and its threading into `NodeKind::DwAgent` → SP3 (greentic-flow). SP1 only consumes an already-resolved `AgentConfig.conversational`; how that field gets populated from a flow node is SP2/SP3's job.
- **Runaway-segment safety cap (`max_turns`)** — a *segment-level* concept spanning multiple parked turns, so it belongs to SP2's engine loop, not SP1's single-turn loop. The epic lists this as "decide in SP1/SP2"; this spec decides: **defer to SP2**. SP1's existing `MaxIterations`/`Timeout` still bound a single turn's reasoning, unchanged.
- Designer authoring toggle + demo → SP4.

## Design

### 1. Config surface — `AgentConfig.conversational` (`config.rs`)

Add an opt-in flag to `AgentConfig`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentConfig {
    // ... existing fields ...
    #[serde(default)]
    pub conversational: bool,   // NEW — default false
}
```

- `#[serde(default)]` ⇒ every existing manifest / stored `AgentConfig` deserializes as
  `conversational: false` (today's one-shot behaviour), so no manifest migration is
  needed. (A `skip_serializing_if` to also drop the field from serialized output was
  considered but omitted: the correct predicate for a `bool` needs a tiny `is_false`
  helper, and an emitted `"conversational": false` is harmless. Add the helper only if a
  serialization snapshot test demands byte-stability.)
- `AgentConfig` has **no** `Default` derive and its literal constructors do not use
  `..Default::default()`, so this new field forces a one-line addition
  (`conversational: false,`) at every in-crate literal construction site. This is
  mechanical churn across the crate's test fixtures and the few production/mock
  constructors; enumerated in the implementation plan.
- **Rejected alternative:** placing the flag inside `AgentLimits` (which *does* have
  `#[serde(default)]` + `Default`, avoiding the churn). Rejected because it is a
  capability/behaviour flag, not a limit — the wrong semantic home. The churn is
  accepted as the honest cost of the correct location.

### 2. Termination reason — `TerminationReason::ConversationEnded` (`error.rs`)

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationReason {
    FinalReply,
    MaxIterations,
    Timeout,
    Error,
    TokenBudgetExceeded,
    ConversationEnded,   // NEW — serde: "conversation_ended"
}
```

- Additive. A repo-wide check confirms `TerminationReason` is **never** matched
  exhaustively (only `assert_eq!` against specific variants in tests), so the new
  variant breaks no `match` arms.
- The OTel `terminated_by` span attribute (via the existing serde snake_case) becomes
  `conversation_ended` for these turns, at no extra code.

### 3. Tool definition — new module `src/end_conversation.rs`

Mirrors the existing host built-in tool pattern (`short_term.rs` /
`long_term.rs`): a reserved `"host"` extension id and an LLM-facing schema.

```rust
/// Shared with short-/long-term built-ins.
pub(crate) const HOST_EXTENSION_ID: &str = "host";
pub(crate) const END_CONVERSATION_TOOL: &str = "end_conversation";

/// The tool is offered only when the agent is conversational.
pub(crate) fn conversational_active(config: &AgentConfig) -> bool {
    config.conversational
}

pub(crate) fn end_conversation_tool_schema() -> LlmToolSchema {
    LlmToolSchema {
        extension_id: HOST_EXTENSION_ID.to_string(),
        tool_name: END_CONVERSATION_TOOL.to_string(),
        description: "End the current conversation when it has reached a natural \
            conclusion — the user's goal is met, they say goodbye, or there is \
            nothing left to do. Optionally include a brief closing message."
            .to_string(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "final_message": {
                    "type": "string",
                    "description": "Optional short closing message shown to the user."
                }
            }
        }),
    }
}
```

- **Param name `final_message`.** The epic doc describes the tool loosely as taking
  "an optional `reason`/closing note" (name not pinned). Realized here as
  `final_message` — clearer that it is user-facing closing text, not why-ended metadata.
- Reuses the `"host"` extension id already used by the memory built-ins, so the
  interception + advertising pattern is identical.

### 4. Loop integration (`loop.rs`)

Two edits, both minimal and following the existing built-in-tool shape.

**(a) Advertise the schema** — alongside the memory tools where `tools_schema` is
assembled each iteration:

```rust
if conv_active {
    tools_schema.push(crate::end_conversation::end_conversation_tool_schema());
}
```

where `conv_active = crate::end_conversation::conversational_active(&config)` is
computed once per step (next to `lt_active` / `st_active`).

**(b) Short-circuit on the call** — inside the `if !response.tool_calls.is_empty()`
block, **before** the assistant-tool_calls message push and the per-call loop:

```rust
if conv_active
    && let Some(end_call) = response
        .tool_calls
        .iter()
        .find(|c| c.tool_name == crate::end_conversation::END_CONVERSATION_TOOL)
{
    observer.on_tool_call(&end_call.tool_name, &end_call.call_id);
    let closing = end_call
        .args
        .get("final_message")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .or_else(|| response.content.clone())
        .unwrap_or_default();
    let ok = serde_json::json!({ "ok": true });
    observer.on_tool_result(&end_call.tool_name, &end_call.call_id, &ok);
    trail.push(AgentStep::ToolCall {
        name: end_call.tool_name.clone(),
        call_id: end_call.call_id.clone(),
        result: ok,
    });
    reply = closing;
    terminated_by = TerminationReason::ConversationEnded;
    break; // exit the Plan-Act-Observe loop
}
```

Design points:

- **Final reply resolution:** `final_message` (if present and non-empty) →
  else the assistant content accompanying the tool call (`response.content`) →
  else empty string. Matches the approved decision.
- **Clean history:** the short-circuit does **not** push the multi-tool
  `Assistant { tool_calls }` message. Any *other* tool calls in the same batch are
  ignored (the agent chose to end). Consequence: no dangling `tool_call` without a
  matching `tool` result is written to saved history. **Known limitation:** if the
  model emits `end_conversation` alongside other tool calls, those others are dropped;
  the segment ends. Documented, acceptable for v1 (rare model behaviour).
- **Reuse the guarded-return path.** After `break`, the existing post-loop block that
  runs the **outbound guardrail**, pushes the `Assistant` reply message, truncates +
  saves state, records telemetry, and returns is currently gated
  `terminated_by == FinalReply`. Change that gate to:

  ```rust
  if matches!(
      terminated_by,
      TerminationReason::FinalReply | TerminationReason::ConversationEnded
  ) {
  ```

  So the closing message is guardrailed, persisted, and returned exactly like a normal
  final reply — symmetric, no duplicated logic. The returned `terminated_by` is the
  variable's value (`ConversationEnded`), so the caller sees the correct signal.
- The guardrail runs on the closing message just like `FinalReply` (including the
  pre-existing empty-string behaviour); no new guardrail edge case is introduced.

### 5. System-prompt note (conversational agents only)

The model must know the tool exists (epic risk: "tool discoverability"). When
`conv_active`, append a short note to the system prompt, following the same
augmentation pattern used by long-term memory / knowledge injection
(`augment_system_prompt`). Injected after any LT/knowledge blocks, before the request:

> When the conversation has reached a natural end — the user's goal is met, they say
> goodbye, or there is nothing left to do — call the `end_conversation` tool with a
> brief `final_message`. Do not call it while the user still needs help.

Only injected for conversational agents, so non-conversational system prompts are
byte-unchanged.

## Data flow (one conversational turn)

```
inbound user text
  → inbound guardrail → push User message
  → assemble system prompt (+ LT / knowledge / end_conversation note if conv_active)
  → loop:
       advertise tools (+ end_conversation if conv_active)
       LLM turn
       ├─ tool_calls include end_conversation (conv_active)
       │     → reply = final_message ?? accompanying content ?? ""
       │     → terminated_by = ConversationEnded ; break
       ├─ other tool_calls → dispatch, observe, continue (unchanged)
       └─ no tool_calls → reply = content ; terminated_by = FinalReply ; break
  → guarded return (FinalReply | ConversationEnded):
       outbound guardrail → push Assistant reply → save → telemetry → return
```

## Backward compatibility

- `conversational` defaults `false` ⇒ every existing agent is byte-identical: tool not
  advertised, no system-prompt note, loop behaviour unchanged.
- `end_conversation` offered only when `conversational` ⇒ non-conversational tool sets
  unchanged. If a non-conversational agent's model somehow names `end_conversation`, it
  is not intercepted, falls through to the allow-list, and is blocked as
  "not allowed" (it is never in `config.tools`) — it does **not** terminate.
- New `TerminationReason` variant is additive; no exhaustive matches to update.

## Testing (`--features test-mock`, scripted-loop mocks)

Unit tests in `loop.rs` / `end_conversation.rs`, driven by `MockLlmBackend` scripted
responses (as the existing `loop_scripted` / guardrail tests do):

1. **Happy path:** conversational agent, model emits `end_conversation { final_message: "Bye!" }`
   ⇒ `AgentOutput.reply == "Bye!"`, `terminated_by == ConversationEnded`, telemetry recorded once.
2. **Fallback to accompanying content:** `end_conversation` with no `final_message`, but
   the response carries content ⇒ `reply == content`.
3. **Fallback to empty:** neither present ⇒ `reply == ""`, `terminated_by == ConversationEnded`.
4. **Gating — not advertised:** `conversational: false` ⇒ `end_conversation` schema absent
   from the tools the LLM sees (assert on `MockLlmBackend`'s seen request), and a model that
   still names it is blocked (not terminated) — normal `FinalReply`/tool behaviour holds.
5. **Outbound guardrail applies:** a deny-guardrail over the closing message ⇒
   `AgentError::GuardrailDenied` (the farewell is guarded like any reply).
6. **Schema shape:** `end_conversation_tool_schema()` has `extension_id == "host"`,
   `tool_name == "end_conversation"`, `final_message` optional (not in `required`).
7. **Regression:** a normal no-tool reply on a conversational agent still yields `FinalReply`.

## Risks

- **greentic-dw-providers branch pin (epic risk):** aw-runtime pins
  greentic-dw-providers by `branch=research`. SP1 lands on a research-based branch, so
  the locked greentic-dw-providers is already the research/types-compatible lineage;
  no re-pin needed. Verify `cargo test -p greentic-aw-runtime --features test-mock`
  builds clean on the worktree before PR.
- **Literal-constructor churn:** adding the `AgentConfig` field touches many test
  fixtures; a missed site is a compile error (caught immediately by the build), not a
  silent bug. Low risk, high visibility.
```
