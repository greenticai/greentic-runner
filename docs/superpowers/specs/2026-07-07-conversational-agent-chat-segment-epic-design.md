# Epic: In-flow Conversational Chat Segment (agent-decides-exit) — Design

**Status:** approved (brainstorm), pending spec review
**Scope:** multi-repo epic — decomposed into 4 sub-projects, each with its own spec + plan + PR.
**Relationship:** builds on Slice 1 (`greentic-designer#965`, MERGED), which made a one-shot `dw.agent` reply render + chat via repeated whole-flow turns. This epic adds a true **in-flow chat segment**: the flow parks AT the agent and loops until the agent itself ends the conversation, then advances to the next node.

## Goal

Let a flow author drop a `dw.agent` node into the middle of a card flow and have it behave as a **multi-turn conversation segment**: when the flow reaches the node it runs the agent, shows the reply, and **parks awaiting the user's next message** — looping (agent keeps conversation memory) until the agent decides the conversation is complete, at which point the flow **advances to the node's successor** (e.g. a thanks card). A non-conversational `dw.agent` keeps today's one-shot behaviour.

Target authoring shape: `greeting card → dw.agent (conversational) → thanks card`, where the middle node is a sustained chat, not a single turn.

## Background — why this needs new primitives

Established during Slice 1 investigation:

- A `dw.agent` node runs the agentic-worker Plan-Act-Observe loop once and returns `{reply, trail, terminated_by}`; the engine maps it to `DispatchOutcome::complete` → the flow routes on the node's emit. It is **one-shot**; it never parks for more input.
- `TerminationReason` (aw-runtime `error.rs`) = `FinalReply | MaxIterations | Timeout | Error | TokenBudgetExceeded`. Every normal reply is `FinalReply`. There is **no "conversation is over" signal** — so "the agent decides to exit" (the chosen exit model) has nothing to key on today.
- The engine already has the machinery this epic reuses: `NodeKind::Wait` returns `DispatchOutcome::wait(output, FlowWait)`, which snapshots the flow; on the next inbound activity `drive_flow(..., Some(snapshot.next_node), ...)` resumes and re-evaluates that node. Setting a parked node's resume target to **itself** produces the loop.

So the epic is: (1) give the agent a way to signal "done", (2) make the `dw.agent` node park-and-loop until that signal, (3) let the flow model opt a node into this, (4) surface it in the designer.

## Architecture

Conversational `dw.agent` node, per turn:

```
enter node ──> run agent (with persisted conversation memory)
                 │
      terminated_by == ConversationEnded ?
        │ yes                      │ no  (FinalReply / normal reply)
        ▼                          ▼
  DispatchOutcome::complete   DispatchOutcome::wait(reply, FlowWait{ resume_node = THIS node })
   → route to successor        → park; render reply; next user message re-enters THIS node
```

The exit signal is an **`end_conversation` built-in tool**: the agent calls it when it judges the conversation complete; the AW loop terminates that turn with `TerminationReason::ConversationEnded`. The reply the agent emits alongside (its closing message) still renders before the flow advances.

Reused as-is: the FlowWait snapshot/resume path, session-scope keying, the demo warm-host multi-turn loop, and Slice 1's `activity_to_outbox` reply rendering (the parked reply is the same `{reply}`/`response.reply` shape it already renders).

## The four primitives

### SP1 — Exit signal (greentic-aw-runtime)
- Add `TerminationReason::ConversationEnded` (serde `conversation_ended`).
- Register a built-in `end_conversation` tool in the agent's tool set (available when the node is conversational — see SP3 wiring; the tool takes an optional `reason`/closing note and returns immediately).
- In the Plan-Act-Observe loop, when the model calls `end_conversation`, stop the loop with `terminated_by = ConversationEnded` and use the agent's accompanying message (or the tool's closing note) as the final `reply`.
- Output contract unchanged (`{reply, trail, terminated_by}`); only a new `terminated_by` value.

### SP2 — Park-loop node behaviour (greentic-runner engine)
- `NodeKind::DwAgent` gains a conversational flag (threaded from the flow IR — see SP3).
- After `execute_dw_agent` returns, a conversational node inspects `terminated_by`:
  - `ConversationEnded` → `DispatchOutcome::complete(output)` (route to successor as today).
  - anything else (normal `FinalReply`) → `DispatchOutcome::wait(output, FlowWait { resume_node = <this node id> })` so the reply renders and the next inbound message re-enters this node.
- Non-conversational `DwAgent` is unchanged (`complete`).
- Guard the loop: reuse the existing per-turn budgeting; a conversational segment relies on the user (or `end_conversation`) to exit — document that `MaxIterations`/`Timeout` still terminate a single turn's reasoning, not the segment.

### SP3 — Flow model (greentic-flow)
- Add an opt-in `conversational: bool` (default `false`) to the `dw.agent` node in the flow doc → compiled `Flow`/IR, threaded to `NodeKind::DwAgent`.
- Backward compatible: absent/false ⇒ today's one-shot node. Validation: a conversational node SHOULD have a successor (the post-conversation route); warn if terminal.

### SP4 — Designer authoring + demo (greentic-designer)
- Editor: a "Conversational" toggle on the `dw.agent` node inspector → sets the flow-doc flag; pack export carries it.
- Demo: **no new mechanism** — the warm-host multi-turn path already parks on `pending` and resumes the parked flow on the next composer send (Run Demo docs). Once SP2 parks the node, resume re-enters the agent automatically; Slice 1 already renders the reply and keeps the composer enabled for `dw.agent` flows. Verify + test the loop end-to-end; surface the `end_conversation`→advance transition.

## Decomposition & order

Strict dependency order (each is its own spec → plan → PR):

1. **SP1** (aw-runtime) — exit signal + tool. Independently testable (loop returns `ConversationEnded` when the tool is called).
2. **SP2** (runner engine) — park-loop keyed on `terminated_by`. Depends on SP1's new variant. Testable with a stub handler returning `ConversationEnded` vs `FinalReply`.
3. **SP3** (greentic-flow) — `conversational` flag through the compiler to the IR. Depends on SP2 consuming the flag.
4. **SP4** (designer) — authoring toggle + demo verification. Depends on SP1–SP3 shipped + a runner rev bump (mirrors the Slice-1 productionization: runner PR → publish → designer rev bump).

Each sub-project ships working, independently-reviewable software. SP1–SP3 are runtime; SP4 is the user-facing payoff.

## Backward compatibility

- New `TerminationReason` variant is additive; existing consumers match on the old values (add an arm).
- `conversational` defaults false ⇒ every existing `dw.agent` flow is byte-unchanged in behaviour.
- `end_conversation` tool is only offered to conversational nodes, so non-conversational agents' tool sets are unchanged.

## Risks / open questions (resolve in sub-project specs)

- **Runaway segments:** a conversational node with an agent that never calls `end_conversation` parks forever. Mitigation options (decide in SP1/SP2): an optional per-node `max_turns` that forces `ConversationEnded`, and/or a user-side exit affordance. The chosen exit model is agent-decides; a safety cap is a hardening detail, not a second exit UX.
- **Tool discoverability:** the agent must know it *may* end the conversation. SP1 should inject a short system-prompt note for conversational nodes ("call `end_conversation` when the user's goal is met").
- **greentic-dw-providers branch pin:** aw-runtime pins greentic-dw-providers by branch=research; SP1 changes must land on a runner rev whose locked greentic-dw-providers is greentic-types-compatible (see the Slice-1 dep-skew note in the demo-agentic-worker memory) — pin by the same 7f7517c-lineage.
- **Resume scope correctness:** re-entering the same node relies on FlowWait's snapshot keying; SP2 must set `resume_node` to the node's own id and confirm the session-scope hash round-trips (existing resume tests cover the mechanism).

## Testing strategy (per sub-project)

- **SP1:** unit — model calls `end_conversation` ⇒ loop returns `terminated_by = ConversationEnded` + the closing reply; normal reply ⇒ `FinalReply` (unchanged).
- **SP2:** engine test with a recording stub handler — conversational node + `FinalReply` ⇒ `wait` with `resume_node == node id`; + `ConversationEnded` ⇒ `complete` routing to the successor; non-conversational node ⇒ `complete` regardless (regression).
- **SP3:** flow-compiler test — `conversational: true` in the doc ⇒ IR node carries the flag; absent ⇒ false; conversational-terminal-node validation warning.
- **SP4:** designer — toggle round-trips through pack export; demo e2e: greeting → agent (≥2 user messages, memory persists) → `end_conversation` → thanks card renders.
