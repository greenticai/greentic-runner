# SP2 — Park-loop conversational `dw.agent` node (greentic-runner engine) — Design

**Status:** approved (brainstorm), pending spec review
**Epic:** [In-flow Conversational Chat Segment](2026-07-07-conversational-agent-chat-segment-epic-design.md) — sub-project 2 of 4
**Depends on:** SP1 (`TerminationReason::ConversationEnded`, merged to research — runner `0757855`, PR #537).
**Scope:** single crate — `greentic-runner-host`, file `runner/engine.rs`. No aw-runtime, no greentic-flow changes.
**Base:** `origin/research`. Branch `feat/conversational-agent-sp2` → PR to `research`.

## Goal

Make a `dw.agent` flow node behave as a **multi-turn conversation segment** when marked
conversational: after the agent runs, the flow **parks at the node and re-enters it** on the
next inbound user message (agent keeps memory across turns) until the agent itself signals the
conversation is over (SP1's `end_conversation` → `terminated_by == "conversation_ended"`), at
which point the flow **advances to the node's successor**. A non-conversational `dw.agent` keeps
today's one-shot behaviour.

## Background — the engine mechanics this reuses

Established by reading `runner/engine.rs`:

- `NodeKind::DwAgent { agent_id }` (engine.rs) is dispatched by `dispatch_node`. With the
  `agentic-worker` feature, the **in-process** path runs
  `execute_dw_agent(...).map(DispatchOutcome::complete)` — it always `complete`s (routes on the
  node's emit); it never parks.
- `execute_dw_agent` returns a `NodeOutput` whose `payload` is the agent result JSON
  `{ reply, trail, terminated_by }` (confirmed: `agent_node::guardrail_denied_json` and the
  success path both carry `terminated_by`). So the exit signal is readable as
  `payload["terminated_by"] == "conversation_ended"` — a **string compare**, no enum import.
- Parking is driven by `NodeControl`. The drive loop matches `NodeControl::{Continue, Wait, Jump,
  Respond}`. Two distinct park behaviours already exist:
  - **`NodeControl::Wait { reason }`** (from `DispatchOutcome::wait`) → snapshots
    `next_node = <routing successor>` (requires a non-empty route) and `finalize_with(None)`
    (does **not** render the node output). Resumes **past** the node.
  - **`NextDecision::Wait`** (conditional-routing fall-through) → snapshots
    `next_node = <current node id>` and `finalize_with(Some(output.payload))` (renders the
    reply). Resumes **at the same node**.
- The conversational loop needs the *second* shape (re-enter self + render reply) but triggered
  by the DwAgent node's own decision, not by conditional routing. Neither existing `NodeControl`
  arm does this, so SP2 adds one.

## Design

### 1. `NodeKind::DwAgent` gains a `conversational` flag

```rust
DwAgent {
    agent_id: String,
    conversational: bool,   // NEW
},
```

- Loader (`engine.rs`, the `"dw.agent" => NodeKind::DwAgent { .. }` arm): set
  `conversational: false` for now, with a comment that **SP3** will populate it from the flow
  doc / IR. SP2 does not read a flow-doc field (that IR field does not exist until SP3); SP2's
  behaviour is exercised by unit tests that construct `NodeKind::DwAgent { conversational: true }`
  directly (the epic's SP2 test strategy).
- The `NodeKind::DwAgent { .. } => "dw.agent"` reverse-mapping arm is unaffected (`..` ignores the
  new field).

### 2. New `NodeControl::LoopHere { reason }`

```rust
enum NodeControl {
    Continue,
    Wait { reason: Option<String> },
    LoopHere { reason: Option<String> },   // NEW — park & re-enter THIS node, rendering the output
    Jump(JumpControl),
    Respond { .. },
}
```

Drive-loop handling (a new arm next to `NodeControl::Wait`), mirroring the proven
`NextDecision::Wait` snapshot block:

```rust
NodeControl::LoopHere { reason } => {
    let mut snapshot_state = state.clone();
    snapshot_state.clear_egress();
    let snapshot = FlowSnapshot {
        pack_id: step_ctx.pack_id.to_string(),
        flow_id: step_ctx.flow_id.to_string(),
        next_flow: (current_flow_id != step_ctx.flow_id).then_some(current_flow_id.clone()),
        next_node: node_id.as_str().to_string(),          // <-- re-enter THIS node
        state: snapshot_state,
    };
    let output_value = state.finalize_with(Some(output.payload.clone())); // <-- render the reply
    return Ok(FlowExecution::waiting(
        output_value,
        FlowWait { reason, snapshot },
    ));
}
```

Difference from `NodeControl::Wait`: `next_node` is the **current** node (loop-back, no
successor required) and the reply **is** rendered (`finalize_with(Some(..))`). On the next inbound
activity, `resume(...)` drives from `snapshot.next_node` (this node) → re-runs the agent turn.

### 3. Conversational branch in the in-process DwAgent dispatch arm

In `dispatch_node`, the `NodeKind::DwAgent { agent_id, conversational }` arm, **in-process path
only** (`DwAgentDispatch::InProcess` under the `agentic-worker` feature). The
`#[cfg(not(feature = "agentic-worker"))]` path is left unchanged: its `execute_dw_agent`
unconditionally `bail!`s ("compiled without the agentic-worker feature"), so it errors before any
conversational branch would run — adding the branch there would be dead code.

```rust
let output = self.execute_dw_agent(ctx, agent_id, payload).await?;
if *conversational {
    let ended = output
        .payload
        .get("terminated_by")
        .and_then(Value::as_str)
        == Some("conversation_ended");
    if ended {
        Ok(DispatchOutcome::complete(output))            // route to successor
    } else {
        Ok(DispatchOutcome::with_control(
            output,
            NodeControl::LoopHere {
                reason: Some(format!("conversational dw.agent `{agent_id}` awaiting next user message")),
            },
        ))
    }
} else {
    Ok(DispatchOutcome::complete(output))                // unchanged one-shot behaviour
}
```

### 4. Out-of-process (Nats) path — deferred

The `DwAgentDispatch::Nats` arm reroutes to `execute_remote_dispatch(ctx, "agentic", ...)` with
`await: true` — a different park/resume machinery (dispatch listener, routes on the response's
emit, does not loop back to the same node). Making it loop is a separate, harder change.
**SP2 scopes it out**: the conversational branch applies only to the in-process path; the Nats
path is unchanged (a conversational agent under Nats dispatch behaves as today until a follow-up
wires it). Documented as a known limitation.

### 5. `max_turns` runaway cap — deferred

A conversational node whose agent never calls `end_conversation` (and whose user keeps replying)
parks indefinitely. Per the epic ("agent-decides is the exit model; a safety cap is a hardening
detail"), SP2 ships pure agent-decides and **defers** `max_turns` to a follow-up. `MaxIterations`
/ `Timeout` from SP1 still bound a single turn's reasoning — they do not end the segment.
Documented as a known limitation.

## Data flow (conversational segment)

```
inbound → enter DwAgent node → execute_dw_agent (agent turn, persisted memory)
   payload.terminated_by == "conversation_ended" ?
     yes → DispatchOutcome::complete → route to successor (e.g. thanks card)
     no  → NodeControl::LoopHere → render reply + snapshot(next_node = this node) + park
              next inbound activity → resume(next_node = this node) → re-enter, run next turn
```

## Backward compatibility

- `conversational` defaults `false` at the loader ⇒ every existing `dw.agent` flow is
  byte-identical (`DispatchOutcome::complete`, one-shot).
- `NodeControl::LoopHere` is additive; only the conversational-in-process path emits it. All
  existing `NodeControl` matches gain one arm (a `match` on `NodeControl` is exhaustive — the new
  arm must be added, which the compiler enforces).
- No change to aw-runtime (SP1) or greentic-flow (SP3). Reading `terminated_by` as a string keeps
  the engine decoupled from the aw-runtime enum.

## Testing (`--features test-mock,agentic-worker`)

Engine tests in `engine.rs` (or the runner-host test module) driving a **recording stub**
`AgentNodeHandler` that returns a scripted `{ reply, terminated_by }` payload, plus a small flow
(`dw.agent` node → successor):

1. **Segment continues on a normal reply:** conversational node, stub returns
   `terminated_by = "final_reply"` ⇒ `FlowExecution::Waiting`, `snapshot.next_node == <the
   dw.agent node id>`, and the reply is present in the rendered output.
2. **Segment ends on ConversationEnded:** conversational node, stub returns
   `terminated_by = "conversation_ended"` ⇒ `FlowExecution::Completed` (or `Waiting` at the
   successor if the successor itself parks) with routing advanced to the successor node — i.e. NOT
   a loop-back (`next_node != <dw.agent node id>` / flow completed).
3. **Non-conversational regression:** non-conversational node ⇒ `complete` regardless of
   `terminated_by` (both `"final_reply"` and `"conversation_ended"` route onward identically to
   today).

If constructing the engine + a stub handler in-module proves heavy, mirror the existing
`agentic-worker` engine tests (there is already engine test scaffolding around the DwAgent path
and `AgentConfig` fixtures) — reuse their handler/stub construction rather than inventing new
infrastructure.

## Risks

- **Resume-scope correctness (epic risk):** `LoopHere` reuses the exact snapshot shape of the
  proven `NextDecision::Wait` path (`next_node = current node`, `clear_egress`, `finalize_with
  (Some(payload))`). The session-scope hash round-trip is already covered by existing resume tests
  for that path. Implementation must set `next_node` to the node's own id (verified by test 1).
- **`terminated_by` contract:** SP2 depends on the DwAgent output payload carrying
  `terminated_by` as a snake_case string. This is produced by SP1's `AgentOutput` serialization
  (merged). If a future change drops/renames it, test 1/2 fail fast (string mismatch → wrong
  branch), not silently.
- **Feature-gating:** the conversational branch lives only on the `agentic-worker` in-process
  path. The non-feature build's `execute_dw_agent` `bail!`s, so a conversational `dw.agent` there
  errors (as any `dw.agent` does today) rather than looping — no behavioural divergence to guard.
  The new `NodeControl::LoopHere` arm in the drive loop is not feature-gated (the enum + drive
  loop are always compiled), so it must compile in both builds even though only the
  `agentic-worker` build can emit it.
