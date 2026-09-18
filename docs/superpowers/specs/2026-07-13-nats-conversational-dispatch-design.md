# Out-of-process (NATS) conversational `dw.agent` dispatch — Design

**Status:** approved (brainstorm)
**Date:** 2026-07-13
**Repo:** `greentic-runner` (`greentic-runner-host`)
**Related:** conversational-agent epic — SP2 park-loop (`docs/superpowers/specs/2026-07-07-conversational-agent-sp2-park-loop-design.md` §4 deferred the NATS path); the in-process park-loop cap (PR #554, `park_turns`).

## Problem

A `conversational` `dw.agent` is a multi-turn segment: the flow parks and re-enters the node on each user message until the agent emits `terminated_by == "conversation_ended"`, then advances to the successor. This is implemented **only for in-process dispatch** (`DwAgentDispatch::InProcess`).

The out-of-process arm (`DwAgentDispatch::Nats`, used by distributed/HA deploys where the agentic-worker runs as a separate `aw-serve` service) ignores `conversational` entirely: it does a single `execute_remote_dispatch(ctx, "agentic", …)` with `await: true`, which pauses the flow, and on the agent's NATS response **resumes at the routing successor** — i.e. one-shot. A conversational agent under NATS dispatch never gets its multi-turn park-loop.

Goal: make the NATS-dispatched conversational `dw.agent` behave identically to the in-process one — park-and-loop until `conversation_ended`, then advance — without changing non-conversational or in-process behavior.

## Root cause (mechanism)

Two different park/resume mechanisms meet at the same node:

- **In-process conversational** uses `NodeControl::LoopHere` → snapshot `next_node = self`, **session-keyed**; the next **user message** (ingress) resumes it. The conversational decision runs *synchronously* on `execute_dw_agent`'s output.
- **NATS await** uses `NodeControl::Wait` → snapshot `next_node = routing successor`, **correlation-keyed** (`await-runtime:{correlation_id}`); the agent's **NATS response** (dispatch listener → `RuntimeSessionResumer`) resumes it. The agent output arrives *asynchronously*, and the flow advances past the node.

So the conversational decision (`ended → advance` / `else → re-park`) cannot run at dispatch time for NATS — the output isn't there yet — and the current await resumes past the node, leaving no place to evaluate `terminated_by`.

## Non-goals

- No change to non-conversational `dw.agent` (either dispatch mode) or to in-process conversational behavior — all byte-identical.
- No change to the `agentic.call` node, `sorla.call`, or other `execute_remote_dispatch` consumers — only the `dw.agent` NATS arm gains conversational looping.
- Not a new resume-store or transport; reuse the existing correlation-keyed (dispatch listener) and session-keyed (ingress) resume paths.

## Chosen approach (A — unify the conversational decision at resume time)

The conversational decision runs on the agent **output regardless of how it was obtained** (sync in-process, or async via NATS resume). For NATS the decision runs at **resume time**, when the response arrives. The node becomes a small phase machine keyed by a per-node marker distinguishing "resuming with an agent response" from "resuming from a user message".

### Control flow (per node, NATS conversational)

```
[user message] ─▶ dispatch to NATS (await) ; set pending_agent_await[node] ;
                  AwaitHere(resume-at-self, correlation-keyed)
                              │
   [NATS response] ──────────┘
                              ▼
  pending_agent_await[node] set?  ── YES ▶ clear marker ; read terminated_by:
        • "conversation_ended" → complete → advance to routing successor ; reset park_turns
        • capped (park_turns ≥ MAX_PARK_TURNS) → complete → advance ; warn ; reset park_turns
        • else → surface reply ; bump park_turns ; LoopHere(resume-at-self, session-keyed)
                              │
   [next user message] ──────┘  (marker clear → re-dispatch to NATS)
```

The conversational branch logic is identical to in-process; only the output source (sync vs async-resume) differs.

## Components (all additive)

### 1. `NodeControl::AwaitHere { reason, correlation_id }`

A cousin of `LoopHere`: park **at the current node** awaiting an async runtime response, resumed by **correlation id** (not session key). Its drive-loop handler mirrors the existing remote-await `Wait` snapshot construction (`clear_egress`, `finalize_with`, the `await-runtime:{correlation_id}` reason and correlation-keyed save) **except** `next_node = current node` instead of the routing successor. This is the one piece that makes the NATS response re-enter the same node.

### 2. `ExecutionState.pending_agent_await: HashMap<String, ()>`

Per-node phase marker, `#[serde(default)]`, persisted in the park/resume snapshot exactly like `park_turns` (serde-derived round-trip; legacy snapshots decode to empty). Presence of `node_id` means "the next resume at this node is the awaited agent response" (evaluate `terminated_by`); absence means "resume is a fresh user message" (dispatch to NATS). Methods mirror `park_turns`: `mark_agent_await(node_id)`, `take_agent_await(node_id) -> bool` (check-and-clear), on the `ExecutionState` impl.

### 3. Conversational branch in the `DwAgentDispatch::Nats` arm of `dispatch_node`

```
if *conversational {
    if state.take_agent_await(node_id) {
        // Resuming with the agent's response (payload = the NATS response).
        let ended = payload…terminated_by == "conversation_ended";
        if ended { state.reset_park_turns(node_id); complete(output) }
        else {
            let turns = state.bump_park_turns(node_id);
            if turns >= MAX_PARK_TURNS { warn; state.reset_park_turns(node_id); complete(output) }
            else { LoopHere(resume-at-self, session-keyed) with the reply surfaced }
        }
    } else {
        // Fresh user turn: dispatch to NATS, park awaiting the response.
        state.mark_agent_await(node_id);
        // execute_remote_dispatch produces the AwaitHere outcome (resume-at-self)
        // instead of the current Wait (resume-at-successor).
        self.execute_remote_dispatch_conversational(ctx, "agentic", agent_id, remote_payload).await
    }
} else {
    // Non-conversational NATS: unchanged single await → advance.
    self.execute_remote_dispatch(ctx, "agentic", agent_id, remote_payload).await
}
```

`execute_remote_dispatch_conversational` is a thin variant (or a `resume_at_self: bool` parameter on the existing fn) that emits `AwaitHere` instead of `Wait`; everything else (correlation id, dispatch send, `DispatchMode::Await`) is shared with the existing path so the two can never diverge on the wire.

### 4. `park_turns` cap reuse

The `MAX_PARK_TURNS` backstop (PR #554) applies unchanged: the re-park-per-user-turn concern is identical. Both `complete` exit paths (`conversation_ended` and cap) reset the counter, per #554's fix.

## Data flow / resume keying (the crux)

**Correction (spike §Q3 + final review):** an earlier draft of this section claimed AwaitHere and LoopHere use **different keys** so "they never collide." That is **wrong**. The spike established that `FlowResumeStore::save`/`build_store_ctx` store **every** wait kind under the SAME `(session_hint, ReplyScope.scope_hash)` store key — the correlation id is *stripped from the key*. So the two phases occupy the **same single slot**; they never coexist because each park overwrites it. The correlation id only drives how the NATS response reconstructs the hint/scope (`RuntimeSessionResumer`) so it recomputes that same key.

- **AwaitHere** (fresh-turn → dispatched) is resumed by the NATS response; the resume envelope `{ok, output, events, error}` lands in `state.entry`, and the branch reads `state.entry.output.terminated_by`.
- **LoopHere** (not-ended → re-park) is resumed by the next user message via ingress; the branch re-dispatches to NATS.

On every resume `next_node = self`, so `dispatch_node` re-runs at the node; `pending_agent_await` (check-and-clear) selects the phase. **Safety rests on sequential single-slot resume, not key separation** — turns are serialized per session, so the happy path is correct. A known interleaving limitation (a user message arriving *before* the awaited NATS response resolves to the same slot and is misread) is tracked as a follow-up, not fixed in this slice. The non-conversational NATS arm keeps using `Wait` (successor) — unchanged.

### Known limitations (follow-ups, not in this slice)

- **Interleave desync:** a user message arriving mid-await hits the single AwaitHere slot with the phase marker still set → misread as the (absent) agent response (null turn), then the real response is misread as a fresh turn. Pre-existing single-slot property, amplified by interactivity; mitigate with an envelope-shape guard on the marker (only honor it when `state.entry.ok` is present).
- **No await deadline:** the dw.agent remote payload sets no `deadline_ms`; a dropped `aw-serve` response wedges the conversational segment forever (marker set). Pre-existing for non-conversational NATS dw.agent too; add a deadline + timeout→error-re-park.
- **Error-envelope silent re-park:** an `{ok:false, error, output:null}` response yields a null reply + a `park_turns` bump; a run of errors silently force-advances past the segment. Follow-up: surface the error and do NOT bump `park_turns` on transport/agent errors.

## Integration risk + spike-first

The load-bearing assumption: a correlation-keyed await resume can land back at the **same node** (`next_node = self`) and deliver the **agent response payload** to that node's `dispatch_node` (today the remote-await resume targets the successor and injects the response there). **The plan's first task is a spike** that confirms, against `runner/dispatch_listener.rs`, `runner/runtime_session_resumer.rs`, and the `drive_flow` resume path, that (a) a self-`next_node` await snapshot resumes into `dispatch_node` for that node and (b) the response payload is the node's incoming payload. If confirmed, proceed with the full design; if the resumer hard-wires successor delivery in a way that can't carry the payload to a self-resuming node, re-evaluate (narrow fallback: a dedicated one-hop internal "conversational-check" successor node that reads the response and routes back — a constrained slice of Approach B — documented before building).

## Testing

- **Unit (ExecutionState):** `pending_agent_await` mark / take-and-clear per node; snapshot serde round-trip + legacy-snapshot decode (empty map) — mirrors the `park_turns` tests.
- **Behavioral (test-mock NATS via `aw-serve`, features `serve,test-mock`):** a conversational NATS `dw.agent` — turn 1 dispatches and awaits; a "not-ended" response re-parks (session-keyed); a user reply re-dispatches; a `conversation_ended` response advances to the successor. A separate case drives past `MAX_PARK_TURNS` and asserts force-advance. Assert the **replies are identical** to the in-process conversational path for the same script (the two modes must be observationally equivalent).
- **Regression:** non-conversational NATS `dw.agent` and in-process conversational are byte-identical (existing tests stay green).

## Rollout

Single `greentic-runner` PR to `research`. No env flag, no config knob (conversational behavior is already driven by the existing `conversational` config, honored identically across dispatch modes). Distributed deploys pick it up when the runner rev is bumped; single-node in-process is unaffected.
