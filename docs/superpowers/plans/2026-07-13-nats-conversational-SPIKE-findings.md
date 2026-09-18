# Spike findings: NATS conversational `dw.agent` dispatch — resume-at-self + payload

**Task:** Task 1 (spike, no production code) of the NATS conversational dispatch plan.
**Repo:** `greentic-runner`, worktree `nats-conv`, branch `feat/nats-conversational-dispatch`.
**Design under test:** `docs/superpowers/specs/2026-07-13-nats-conversational-dispatch-design.md`.

## Verdict

**GO** — with one required adjustment to the design's pseudocode, documented below (§Q2 / §Loci-d). Resume-at-self re-enters `dispatch_node` for the same node (confirmed), and the raw agent response *is* reachable at resume time — but not via the `payload: Value` argument `dispatch_node` receives. It must be read from `ExecutionState.entry` (a private field, readable from any code in `runner/engine.rs` since it's the same module), and it is nested one level under an envelope wrapper (`entry.output.terminated_by`, not `entry.terminated_by`). This is a small, mechanical fix, not a rewrite — proceed with Tasks 2-6 as written, calling out this detail to whoever implements Task 4/5.

---

## Q1 — Does a self-`next_node` await snapshot resume into `dispatch_node` for that same node?

**Yes, confirmed generically.** `drive_flow`'s resume entry has no special-casing for "self" vs. "successor" — it is driven purely by whatever node id sits in `FlowSnapshot.next_node`.

- `FlowEngine::resume` (`crates/greentic-runner-host/src/runner/engine.rs:572-604`) does:
  ```rust
  state.replace_input(input.clone());
  state.entry = input;
  self.drive_flow(&ctx, flow_ir, state, Some(snapshot.next_node), resume_flow).await
  ```
  (`engine.rs:600-602`)
- `drive_flow` (`engine.rs:619-635`) turns `resume_from` into `current` via `NodeId::from_str(&node)` (`engine.rs:627-629`), then loops, calling `dispatch_node(&step_ctx, node_id.as_str(), node, &mut state, payload, &event)` for whatever `current` is (`engine.rs:690-699`).
- Nothing rejects or special-cases `next_node == <the node that produced the wait>`. This is not hypothetical: the **existing in-process conversational `LoopHere` handler already does exactly this** — its snapshot sets `next_node: node_id.as_str().to_string()` (self) at `engine.rs:850`, and the conversational behavioral tests (`conversational_dw_agent_parks_and_loops_on_normal_reply`, `engine.rs:6593+`) prove the resume re-enters the same `dw.agent` node. The only difference for the NATS/correlation-keyed case is *how the snapshot is found* (see Q3), not how `drive_flow`/`resume` dispatches once found.

**Contrast — today's remote-await `Wait` handler does NOT self-target:** `NodeControl::Wait { reason }` (`engine.rs:797-837`) computes `resume_target` from `node.routing` (`engine.rs:798-819`, the routing successor) and sets `next_node: resume_target.as_str().to_string()` (`engine.rs:827`) — i.e. today's NATS await snapshot targets the successor, not self. Task 3's `AwaitHere` needs to mirror this block but with `next_node = node_id.as_str().to_string()` instead (see Loci §a below).

## Q2 — Is the agent response available as the resumed node's incoming `payload`?

**Not via the `payload` function-argument to `dispatch_node` — but yes via `state.entry`, with a caveat on shape.** Traced end to end:

1. **What the NATS response listener hands to the resumer.** `decode_response` (`runner/dispatch_listener.rs:39-72`) builds an *envelope*, not the bare agent output:
   ```rust
   let output = serde_json::json!({
       "ok": response.ok, "output": response.output,
       "events": response.events, "error": response.error,
   });
   ```
   (`dispatch_listener.rs:60-65`). For the agentic-worker serve path, `response.output` is the agent's `{reply, trail, terminated_by}` (per repo `CLAUDE.md` / `RuntimeAgentDispatchInvoker`), so the terminal shape is `{ok, output: {reply, trail, terminated_by}, events, error}` — **`terminated_by` is nested one level under `.output`.**

2. **What becomes the resume envelope's `payload`.** `RuntimeSessionResumer::build_resume_envelope` (`runner/runtime_session_resumer.rs:125-183`) sets `payload: output` verbatim (`runtime_session_resumer.rs:169`) — the same `{ok, output, events, error}` value, unmodified.

3. **What becomes `resume()`'s `input`.** `PackFlowAdapter::call_traced` (`engine/runtime.rs:589-722`) does `let payload = envelope.payload.clone();` (`engine/runtime.rs:607`) then, on the resume branch, `self.engine.resume(resume_ctx, snapshot, payload).await` (`engine/runtime.rs:719`) — so `FlowEngine::resume`'s `input` parameter is exactly that envelope.

4. **What `resume()` does with it.** `state.entry = input;` (`engine.rs:601`) — `ExecutionState.entry` (private field, `engine.rs:2120-2122`) now holds the raw `{ok, output, events, error}` envelope. `template_context` aliases it as both `"entry"` and `"in"` (`engine.rs:2567-2581`, specifically `ctx.insert("in".into(), entry)` at `engine.rs:2575`) for template rendering.

5. **What `dispatch_node` actually receives as `payload`.** Critically, this is **not** `state.entry` directly. `drive_flow`'s loop re-renders the node's *own declared request template* every visit, regardless of whether this is a fresh dispatch or a resume:
   ```rust
   let payload_template = node.payload_expr.clone();               // engine.rs:658
   let ctx_value = template_context(&state, prev);                  // engine.rs:664
   let payload = render_template_value(&payload_template, &ctx_value, ...); // engine.rs:676-678
   ```
   `dispatch_node(..., payload, ...)` is then called with *that* rendered value (`engine.rs:690-699`), not with `state.entry` itself. For a `dw.agent` node, `payload_expr` is the flow-authored request mapping (e.g. `{"user_text": "{{in.text}}"}` — see the `in_map` pattern noted in `engine.rs:6511`/`6528` test fixtures and the repo's flow-demo card→agent fix memory). On a **fresh user turn** resume (in-process `LoopHere`), `in` = the next inbound activity, so this mapping correctly re-renders a fresh agent request. On the **NATS agent-response** resume (`AwaitHere`, Task 3/4), `in` = the `{ok, output, events, error}` envelope — rendering the *same* request-mapping template against it does **not** reproduce the response; it produces whatever garbage/nulls result from applying a "build an agent request" template to a response-shaped value.

   **Consequence for Task 4/5:** the conversational-Nats branch in `dispatch_node`'s `NodeKind::DwAgent` arm must **not** read the `payload` function argument to get `terminated_by` when `state.take_agent_await(node_id)` is true. It must read `state.entry` directly. This is legal today with zero new plumbing: `ExecutionState`'s fields are private (no `pub`) but `dispatch_node` is defined in the very same module (`runner/engine.rs`), so `state.entry` is already visible there (Rust module-privacy, not struct-privacy) — same trick `dispatch_node` already uses one line up: `inject_card_locale(&mut payload, &state.entry);` at `engine.rs:897`. The path to read is `state.entry.get("output").and_then(|o| o.get("terminated_by"))`, not `payload.get("terminated_by")`.

   This is the one "documented, small way to make it so" the GO verdict allows for — no resumer rewrite, no new plumbing, just: read the right field.

## Q3 — How is the await snapshot correlation-keyed, and what must `AwaitHere` reuse?

**The `reason` string (`await-runtime:{correlation_id}`) is cosmetic only — it is never parsed back into a key.** The real keying mechanism is shared, unconditionally, by every wait/resume in the engine (`Wait`, `LoopHere`, and the future `AwaitHere` alike): a `(session hint, ReplyScope.scope_hash)` pair computed by `build_store_ctx`.

- **Save:** `FlowResumeStore::save` (`engine/runtime.rs:94-112`) calls `build_store_ctx(envelope)` (`engine/runtime.rs:138-160`), which derives `hint = envelope.session_hint [+ "::pack=<id>"]` and `scope = envelope.reply_scope` (correlation stripped for the primary store key), then stores under `StoreSessionKey::new(format!("{hint}::{}", store_scope.scope_hash()))` (`engine/runtime.rs:107`). The `FlowWait.reason` is stashed only as `cursor.with_wait_reason(reason)` (`engine/runtime.rs:171-173`) — audit/debug metadata, not a lookup key.
- **What differs between session-keyed (`LoopHere`) and "correlation-keyed" (`Wait`/`AwaitHere`) is not the storage mechanism — it's how the *next* `IngressEnvelope` is reconstructed to reproduce the same `(hint, scope_hash)`:**
  - Session-keyed: the next real inbound provider activity naturally carries the same conversation's `session_hint`/`ReplyScope`, so no reconstruction is needed.
  - Correlation-keyed (NATS): the response wire only carries `(tenant, env, correlation_id)` headers (`dispatch_listener.rs:39-72`). `execute_remote_dispatch` (`engine.rs:1287-1381`) builds that `correlation_id` at dispatch time by embedding the bare hint + pack + flow + optional thread/reply markers:
    ```rust
    let mut correlation_id = format!("{}::pack={}::flow={}", bare_hint, ctx.pack_id, ctx.flow_id);
    // + optional ::thread=/::reply= markers
    ```
    (`engine.rs:1330-1341`), then on `AwaitingResponse`, sets `reason = format!("await-runtime:{correlation_id}")` and returns `DispatchOutcome::wait(output, Some(reason))` (`engine.rs:1362-1372` — this is **the exact return site Task 4 branches** to instead return an `AwaitHere` control for conversational agents).
    `RuntimeSessionResumer::build_resume_envelope` (`runner/runtime_session_resumer.rs:125-183`) parses those markers back out of the echoed `correlation_id` and reconstructs `session_hint = bare_hint` + `reply_scope = {conversation, thread, reply_to}` (`runtime_session_resumer.rs:148-182`) so `build_store_ctx` recomputes the identical `(hint, scope_hash)` used at save time.

**What `AwaitHere` must reuse, unchanged:** the entire `correlation_id` construction and NATS dispatch/response wiring inside `execute_remote_dispatch` (`engine.rs:1287-1381`) — none of that needs to change. Only the **branch at the `AwaitingResponse` return site** (`engine.rs:1362-1372`) needs a conversational variant that returns a new `NodeControl::AwaitHere{reason, correlation_id}` instead of `NodeControl::Wait{reason}`, and only the **new `drive_flow` match arm** for that control (modeled on `Wait`'s snapshot block, `engine.rs:797-837`) needs to set `next_node = node_id.as_str().to_string()` (self) instead of `resume_target` (successor, `engine.rs:827`). The save/fetch keying machinery (`engine/runtime.rs:94-160`) is untouched and does not care whether `next_node` is self or successor.

---

## Exact loci for Tasks 3-4

| # | What | File:line |
|---|------|-----------|
| a | `Wait`-handler snapshot-construction block to model `AwaitHere` on (copy this, change one line: `next_node`) | `crates/greentic-runner-host/src/runner/engine.rs:797-837` |
| a′ | For contrast/confidence: the *existing* self-resume precedent (`LoopHere`), proving self-`next_node` already works end-to-end | `crates/greentic-runner-host/src/runner/engine.rs:838-860` |
| b | Correlation-keyed save mechanism (`FlowResumeStore::save` + `build_store_ctx`) — shared, unmodified by Task 3/4 | `crates/greentic-runner-host/src/engine/runtime.rs:94-112` (save), `:138-160` (`build_store_ctx`) |
| b′ | Correlation-id construction that must stay byte-identical between conversational and non-conversational NATS dispatch | `crates/greentic-runner-host/src/runner/engine.rs:1330-1341` |
| b″ | Decode side that reconstructs the resume envelope from the echoed correlation id | `crates/greentic-runner-host/src/runner/runtime_session_resumer.rs:125-183` |
| c | `execute_remote_dispatch` `AwaitingResponse` return site Task 4 branches (conversational → `AwaitHere`, else unchanged `Wait`) | `crates/greentic-runner-host/src/runner/engine.rs:1362-1372` |
| d | How the resumed node's `payload` argument is actually set (rendered request-template, NOT the raw response) — Task 4/5 must read `state.entry` instead | render: `crates/greentic-runner-host/src/runner/engine.rs:658,664,676-678`; `dispatch_node` signature/call: `:888-899,690-699`; raw envelope landing in `state.entry`: `:601`; existing same-module read-of-`state.entry` precedent: `:897` |
| d′ | Envelope shape agent response is nested under (`.output.terminated_by`, not top-level) | `crates/greentic-runner-host/src/runner/dispatch_listener.rs:60-65` |
| d″ | `DwAgentDispatch::Nats` arm in `dispatch_node` where the Task 3/4 conversational branch is inserted | `crates/greentic-runner-host/src/runner/engine.rs:972-987` (current single-shot arm), contrast with the in-process conversational branch immediately below it at `:987-1027` |

## Summary for the controller

- Q1 (self-resume re-enters `dispatch_node`): **YES**, confirmed by tracing `resume`/`drive_flow` generically plus the existing `LoopHere` precedent (`engine.rs:838-860`).
- Q2 (response is the node's payload): **Not via the `payload` argument** (which is a freshly re-rendered request-mapping template, `engine.rs:658-678`) — **but yes via `state.entry`** (`engine.rs:601`, readable in-module), nested as `state.entry.output.terminated_by` (`dispatch_listener.rs:60-65`). Task 4/5 must read `state.entry`, not `payload`.
- Q3 (correlation-keyed save): keying is `(session_hint, ReplyScope.scope_hash)` via `build_store_ctx`/`FlowResumeStore::save` (`engine/runtime.rs:94-160`), shared unmodified by every wait kind; the `reason` string is not parsed as a key — the actual correlation id embedded/decoded around `engine.rs:1330-1341` and `runtime_session_resumer.rs:125-183` is what must round-trip, and `AwaitHere` reuses it verbatim.

**GO** — proceed with Tasks 2-6, flagging the `state.entry` (not `payload`) + nested `.output.terminated_by` detail to whoever writes Task 4/5's conversational branch.
