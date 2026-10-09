# NATS conversational `dw.agent` dispatch — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an out-of-process (NATS-dispatched) conversational `dw.agent` park-and-loop until `conversation_ended` — identical to the in-process path — instead of running one-shot.

**Architecture:** Add a `NodeControl::AwaitHere` control (park at the current node awaiting a correlation-keyed async response, resume-at-self) and a per-node `ExecutionState.pending_agent_await` phase marker. The `DwAgentDispatch::Nats` arm, when the node is `conversational`, dispatches → `AwaitHere`; on the response resume it evaluates `terminated_by` (advance) or re-parks with `LoopHere` (session-keyed) for the next user message, reusing the `park_turns` cap. Non-conversational and in-process paths are byte-identical.

**Tech Stack:** Rust 1.94, edition 2024; `greentic-runner-host` `FlowEngine`/`ExecutionState`/`NodeControl`; NATS dispatch (`execute_remote_dispatch`, `RuntimeSessionResumer`, dispatch listener); serde_json.

**Reference:** `docs/superpowers/specs/2026-07-13-nats-conversational-dispatch-design.md`

## Global Constraints

- English-only source/tests/comments/commits. Conventional Commits. **No Claude co-author trailer.**
- Rust 1.94, edition 2024; `cargo fmt --all --check` clean; `cargo clippy --all-targets --all-features -- -D warnings` clean.
- **No env var, no config knob** — behavior is driven by the existing `conversational` config, honored identically across dispatch modes.
- **Additive only:** non-conversational `dw.agent` (both dispatch modes) and in-process conversational behavior stay byte-identical. The `agentic.call`/`sorla.call`/other `execute_remote_dispatch` consumers are untouched.
- `ExecutionState` new fields are `#[serde(default)]` so park/resume snapshots round-trip and legacy snapshots decode.
- All files under `crates/greentic-runner-host/src/runner/`. Primary file: `engine.rs`.

---

## Task 1: Spike — confirm resume-at-self-with-payload (no production code)

**Files:**
- Read: `crates/greentic-runner-host/src/runner/engine.rs` (`drive_flow` resume path ~:602-660, the `NodeControl::Wait` handler ~:838, `resume` ~:572), `runner/dispatch_listener.rs`, `runner/runtime_session_resumer.rs`, `runner/remote_dispatch.rs`.
- Create: `docs/superpowers/plans/2026-07-13-nats-conversational-SPIKE-findings.md`

**Goal:** Confirm the load-bearing assumption before building: a correlation-keyed await snapshot with `next_node = <the dw.agent node itself>` resumes into `dispatch_node` **for that node** with the **agent response** as the node's incoming `payload`.

- [ ] **Step 1: Trace the await→resume path.** For the existing NATS `dw.agent` await (`execute_remote_dispatch` → `DispatchOutcome::wait` → the `Wait` handler snapshot), record: (a) exactly where `next_node` is set for the remote-await Wait (the `Wait` handler at ~:838 sets `next_node = resume_target` from routing — confirm), (b) how the dispatch listener + `RuntimeSessionResumer` build the resume envelope from the NATS response and which resume-store key they use (`await-runtime:{correlation_id}`), (c) how `drive_flow`/`resume` (~:572, :602) re-enters: does it call `dispatch_node(node = snapshot.next_node, payload = <response>)`, and what becomes the node's `payload` on resume?

- [ ] **Step 2: Answer the two gating questions** in the findings doc:
  1. If a snapshot is saved with `next_node = <self node id>` and correlation-keyed, does the response resume re-run `dispatch_node` for that same node? (Expected yes — `drive_flow` resumes from `snapshot.next_node` generically.)
  2. Is the **agent response** payload available as that node's `payload` on resume (so the conversational branch can read `terminated_by`)? Record exactly which value is threaded as the resumed node's payload.

- [ ] **Step 3: Record the exact loci** Tasks 3-4 need: the `Wait`-handler snapshot-construction block to model `AwaitHere` on (file:line range), the correlation-keyed save call, and the `execute_remote_dispatch` return site (`DispatchOutcome::wait(...)`) that Task 4 will branch.

- [ ] **Step 4: Go/No-Go.** Write a one-line verdict:
  - **GO:** resume-at-self re-enters `dispatch_node` and the response is the node payload → proceed with Tasks 2-6 as written.
  - **NO-GO:** if the resumer hard-wires successor delivery or cannot carry the payload to a self node → STOP and escalate. Document the narrow fallback from the spec (a dedicated one-hop internal "conversational-check" successor node) so the controller can re-scope. Do not hack the resumer.

- [ ] **Step 5: Commit** `docs: NATS conversational dispatch spike findings + go/no-go`

---

## Task 2: `ExecutionState.pending_agent_await` phase marker

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (`ExecutionState` struct ~:2140, its constructor ~:2153, `to_json`/`context` builder ~:2184, methods near `bump_park_turns` ~:2216)
- Test: same-file `#[cfg(test)] mod tests`

**Interfaces:**
- Produces on `ExecutionState`: `fn mark_agent_await(&mut self, node_id: &str)`; `fn take_agent_await(&mut self, node_id: &str) -> bool` (check-and-clear, returns whether the marker was present). Field `pending_agent_await: HashMap<String, ()>`.

- [ ] **Step 1: Write the failing test** (in the `mod tests` where `park_turns_bump_and_reset` lives):

```rust
#[test]
fn pending_agent_await_mark_and_take() {
    let mut st = ExecutionState::new(serde_json::json!({}));
    assert!(!st.take_agent_await("agent"), "unmarked node takes false");
    st.mark_agent_await("agent");
    assert!(st.take_agent_await("agent"), "marked node takes true");
    assert!(!st.take_agent_await("agent"), "take clears the marker");
    // Independent per node.
    st.mark_agent_await("a");
    st.mark_agent_await("b");
    assert!(st.take_agent_await("a"));
    assert!(st.take_agent_await("b"));
}

#[test]
fn pending_agent_await_survives_snapshot_roundtrip() {
    let mut st = ExecutionState::new(serde_json::json!({}));
    st.mark_agent_await("agent");
    let json = serde_json::to_string(&st).expect("serialize");
    let back: ExecutionState = serde_json::from_str(&json).expect("deserialize");
    let mut back = back;
    assert!(back.take_agent_await("agent"), "marker survives serde round-trip");
    // Legacy snapshot without the key decodes to empty (serde default).
    let legacy = r#"{"entry":{},"input":{},"nodes":{},"egress":[],"redirect_count":0,"vars":{},"park_turns":{}}"#;
    let mut legacy: ExecutionState = serde_json::from_str(legacy).expect("legacy decode");
    assert!(!legacy.take_agent_await("agent"), "absent key → empty");
}
```

- [ ] **Step 2: Run — expect FAIL** (`mark_agent_await`/`take_agent_await` undefined): `cargo test -p greentic-runner-host --lib pending_agent_await`

- [ ] **Step 3: Implement** — mirror `park_turns` exactly:
  - Add the field to the `ExecutionState` struct (next to `park_turns`):
    ```rust
    #[serde(default)]
    pending_agent_await: HashMap<String, ()>,
    ```
  - Constructor (`ExecutionState::new`, next to `park_turns: HashMap::new(),`):
    ```rust
    pending_agent_await: HashMap::new(),
    ```
  - In the `to_json`/`context` builder (next to `"park_turns": self.park_turns.clone(),`), for parity with `park_turns`:
    ```rust
    "pending_agent_await": self.pending_agent_await.keys().cloned().collect::<Vec<_>>(),
    ```
  - Methods (next to `bump_park_turns`/`reset_park_turns`):
    ```rust
    fn mark_agent_await(&mut self, node_id: &str) {
        self.pending_agent_await.insert(node_id.to_string(), ());
    }
    /// Check-and-clear: returns whether `node_id` was awaiting an agent response.
    fn take_agent_await(&mut self, node_id: &str) -> bool {
        self.pending_agent_await.remove(node_id).is_some()
    }
    ```

- [ ] **Step 4: Run — expect PASS**, then the full `ExecutionState` legacy-decode test still passes: `cargo test -p greentic-runner-host --lib runner::engine::tests`

- [ ] **Step 5: Commit** `feat(engine): add pending_agent_await phase marker to ExecutionState`

---

## Task 3: `NodeControl::AwaitHere` variant + drive-loop handler

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (`NodeControl` enum ~:2239; the drive-loop control `match` where `Wait`/`LoopHere` are handled ~:820-865)
- Test: same-file `#[cfg(test)]` (a resume-at-self assertion if expressible without a live NATS runtime; else covered by Task 6 behavioral)

**Interfaces:**
- Consumes: the spike's confirmed loci (Task 1) — the `Wait`-handler snapshot block + correlation-keyed save.
- Produces: `NodeControl::AwaitHere { reason: Option<String>, correlation_id: String }`; a `DispatchOutcome` constructor `DispatchOutcome::await_here(output, reason, correlation_id)` (mirror `DispatchOutcome::wait`). Consumed by Task 4.

- [ ] **Step 1: Add the enum variant** to `NodeControl` (after `LoopHere`):

```rust
    /// Park the flow and RE-ENTER this same node when the awaited async runtime
    /// response arrives (out-of-process conversational `dw.agent`). Like `Wait`
    /// it awaits a correlation-keyed response, but like `LoopHere` it resumes at
    /// THIS node (not the routing successor) so the conversational decision can
    /// run on the response. Resumed via the dispatch listener + RuntimeSessionResumer.
    AwaitHere {
        reason: Option<String>,
        correlation_id: String,
    },
```

- [ ] **Step 2: Add the `DispatchOutcome::await_here` constructor** mirroring `DispatchOutcome::wait` (locate `wait` in the `DispatchOutcome` impl; add a sibling that stores `NodeControl::AwaitHere`).

- [ ] **Step 3: Add the drive-loop handler.** In the control `match` arm block (alongside `NodeControl::Wait { reason } => { … }` at ~:820 and `NodeControl::LoopHere { reason } => { … }` at ~:838), add:

```rust
                NodeControl::AwaitHere { reason, correlation_id } => {
                    // Await the async agent response, but resume at THIS node so
                    // the conversational branch evaluates `terminated_by`. Snapshot
                    // is correlation-keyed (the NATS response resumes it), unlike
                    // LoopHere's session-keyed park. Mirror the remote-await Wait
                    // snapshot construction EXCEPT next_node = self.
                    let mut snapshot_state = state.clone();
                    snapshot_state.clear_egress();
                    let snapshot = FlowSnapshot {
                        pack_id: step_ctx.pack_id.to_string(),
                        flow_id: step_ctx.flow_id.to_string(),
                        next_flow: (current_flow_id != step_ctx.flow_id)
                            .then_some(current_flow_id.clone()),
                        next_node: node_id.as_str().to_string(), // SELF, not successor
                        state: snapshot_state,
                    };
                    let node_outputs = state.outputs_map();
                    // Finalize with None (render nothing here — the reply, if any,
                    // was already surfaced before the dispatch): match the
                    // remote-await Wait finalize semantics confirmed in Task 1.
                    let output_value = state.clone().finalize_with(None);
                    return Ok(FlowExecution::waiting(
                        output_value,
                        FlowWait { reason, snapshot },
                        node_outputs,
                    ));
                }
```

Note (CONFIRMED by Task 1 spike — findings §Q3): the correlation-keyed save/fetch keying is `(session_hint, ReplyScope.scope_hash)` via `build_store_ctx`/`FlowResumeStore::save` (`engine/runtime.rs:94-160`), **shared unchanged by every wait kind**. The `reason` string is cosmetic (never parsed as a key). So `AwaitHere` needs **no special keying** — it flows through the same `FlowWait` save path as `Wait`, and the response resumes it because `RuntimeSessionResumer` (`runtime_session_resumer.rs:125-183`) reconstructs the identical `(hint, scope_hash)` from the echoed `correlation_id`. The ONLY difference from `Wait` is `next_node = self`. Carry the `reason` through for audit/debug parity, but it is not load-bearing.

- [ ] **Step 4: Build + regression.** `cargo build -p greentic-runner-host` and `cargo test -p greentic-runner-host --lib runner::engine` (existing Wait/LoopHere/remote-dispatch tests stay green).

- [ ] **Step 5: Commit** `feat(engine): add NodeControl::AwaitHere (await async response, resume-at-self)`

---

## Task 4: Emit `AwaitHere` from the conversational NATS dispatch

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (`execute_remote_dispatch` — the `AwaitingResponse` → `DispatchOutcome::wait(output, Some(reason))` return site, **spike-confirmed at `engine.rs:1362-1372`**; the `correlation_id` is already built at `:1330-1341` in the same fn — reuse it)

**Interfaces:**
- Consumes: `DispatchOutcome::await_here` (Task 3).
- Produces: `execute_remote_dispatch` gains a `resume_at_self: bool` parameter (default path passes `false` → existing `DispatchOutcome::wait`; the conversational NATS caller passes `true` → `DispatchOutcome::await_here` with the same `correlation_id`/reason). All other callers (`sorla.call`, `agentic.call`, non-conversational `dw.agent`) pass `false` and are byte-identical.

- [ ] **Step 1: Thread `resume_at_self: bool`** through `execute_remote_dispatch`. At the `RemoteDispatchAction::AwaitingResponse { .. }` branch that today returns `Ok(DispatchOutcome::wait(output, Some(reason)))`, branch:

```rust
                if resume_at_self {
                    Ok(DispatchOutcome::await_here(output, Some(reason), correlation_id))
                } else {
                    Ok(DispatchOutcome::wait(output, Some(reason)))
                }
```

(The `correlation_id`/`reason` are already computed in this fn — reuse them; the spike Task 1 confirms `reason` is the `await-runtime:{correlation_id}` keying string.)

- [ ] **Step 2: Update the existing callers** to pass `resume_at_self: false` (mechanical — `sorla.call`, `agentic.call`, and the non-conversational `dw.agent` Nats arm). Grep `execute_remote_dispatch(` to find all call sites.

- [ ] **Step 3: Build + regression.** `cargo build -p greentic-runner-host`; `cargo test -p greentic-runner-host --lib` — existing remote-dispatch/sorla tests green (they use `resume_at_self: false`).

- [ ] **Step 4: Commit** `feat(engine): execute_remote_dispatch resume_at_self → AwaitHere for conversational`

---

## Task 5: Wire the conversational branch into the `DwAgentDispatch::Nats` arm

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (`dispatch_node` `NodeKind::DwAgent` → `DwAgentDispatch::Nats` arm ~:976-985)

**Interfaces:**
- Consumes: `mark_agent_await`/`take_agent_await` (Task 2), `execute_remote_dispatch(..., resume_at_self)` (Task 4), the existing `bump_park_turns`/`reset_park_turns`/`MAX_PARK_TURNS` (PR #554), `NodeControl::LoopHere`.

- [ ] **Step 1: Replace the Nats arm body** with the conversational phase machine. Today it is:

```rust
                    crate::runner::agent_node::DwAgentDispatch::Nats => {
                        let remote_payload = serde_json::json!({ "await": true, "input": payload });
                        self.execute_remote_dispatch(ctx, "agentic", agent_id, remote_payload)
                            .await
                    }
```

Replace with:

```rust
                    crate::runner::agent_node::DwAgentDispatch::Nats => {
                        if *conversational {
                            if state.take_agent_await(node_id) {
                                // Resuming with the agent's NATS response. SPIKE §Q2:
                                // the response is NOT in the `payload` argument (that
                                // is a freshly re-rendered request-mapping template) —
                                // it landed in `state.entry` as the envelope
                                // `{ok, output, events, error}`, so the agent output is
                                // `state.entry.output` (= `{reply, trail, terminated_by}`)
                                // and `terminated_by` is nested one level under `.output`.
                                // `state.entry` is readable here (same module; precedent
                                // `inject_card_locale(&mut payload, &state.entry)` at :897).
                                let agent_out = state
                                    .entry
                                    .get("output")
                                    .cloned()
                                    .unwrap_or(serde_json::Value::Null);
                                let output = crate::runner::agent_node::NodeOutput::new(agent_out.clone());
                                let ended = agent_out
                                    .get("terminated_by")
                                    .and_then(serde_json::Value::as_str)
                                    == Some("conversation_ended");
                                if ended {
                                    state.reset_park_turns(node_id);
                                    Ok(DispatchOutcome::complete(output))
                                } else {
                                    let turns = state.bump_park_turns(node_id);
                                    if turns >= MAX_PARK_TURNS {
                                        tracing::warn!(
                                            agent_id = %agent_id,
                                            turns,
                                            "conversational dw.agent (nats) hit park-loop cap ({MAX_PARK_TURNS}); force-advancing"
                                        );
                                        state.reset_park_turns(node_id);
                                        Ok(DispatchOutcome::complete(output))
                                    } else {
                                        Ok(DispatchOutcome::with_control(
                                            output,
                                            NodeControl::LoopHere {
                                                reason: Some(format!(
                                                    "conversational dw.agent `{agent_id}` (nats) awaiting next user message"
                                                )),
                                            },
                                        ))
                                    }
                                }
                            } else {
                                // Fresh user turn: dispatch to NATS, park awaiting the response.
                                state.mark_agent_await(node_id);
                                let remote_payload =
                                    serde_json::json!({ "await": true, "input": payload });
                                self.execute_remote_dispatch(
                                    ctx, "agentic", agent_id, remote_payload, /* resume_at_self */ true,
                                )
                                .await
                            }
                        } else {
                            // Non-conversational: unchanged single await → advance.
                            let remote_payload =
                                serde_json::json!({ "await": true, "input": payload });
                            self.execute_remote_dispatch(
                                ctx, "agentic", agent_id, remote_payload, /* resume_at_self */ false,
                            )
                            .await
                        }
                    }
```

(Exact `NodeOutput` constructor + `DispatchOutcome::with_control`/`complete` names: match what the in-process conversational branch above already uses — copy its idioms verbatim so the two branches stay observationally identical. Adjust the `output` construction to however the resumed payload is wrapped, per Task 1.)

- [ ] **Step 2: Build + clippy.** `cargo build -p greentic-runner-host`; `cargo clippy -p greentic-runner-host --all-targets --all-features -- -D warnings`.

- [ ] **Step 3: Commit** `feat(engine): conversational park-loop for NATS dw.agent dispatch`

---

## Task 6: Behavioral test (test-mock NATS) + full verify + doc-sync

**Files:**
- Test: `crates/greentic-runner-host/tests/` (new integration test, modeled on the existing conversational + NATS/sorla test harnesses) OR a `#[cfg(test)]` in `engine.rs` if the in-process conversational behavioral test (`conversational_dw_agent_force_advances_after_park_loop_cap`) can be parameterized for the Nats arm with a stub.
- Modify: `CLAUDE.md` (drop "the out-of-process (Nats) dispatch path is still a deferred follow-up").

- [ ] **Step 1: Write the behavioral test.** Drive a `conversational` `dw.agent` under `DwAgentDispatch::Nats` with a stub/test-mock agent (reuse the `aw-serve` `test-mock` pattern or the existing dispatch stub used by remote-dispatch tests): assert turn 1 dispatches and returns a `Waiting` (AwaitHere); a "not-ended" response resume returns `Waiting` (LoopHere) with the reply surfaced; a user-reply resume dispatches again; a `conversation_ended` response resume returns `Completed` (advanced to the successor). Assert the transcript is **identical** to the in-process conversational path for the same scripted responses. Add a cap case: past `MAX_PARK_TURNS`, a not-ended response force-advances (`Completed`).

- [ ] **Step 2: Run — RED then GREEN** as you implement any missing test-harness glue. `cargo test -p greentic-runner-host <test_name>`.

- [ ] **Step 3: Regression — non-conversational NATS + in-process conversational unchanged.** Run the existing conversational + remote-dispatch/sorla suites: `cargo test -p greentic-runner-host --lib runner::engine` and the relevant `tests/` binaries.

- [ ] **Step 4: Full local CI.** `bash ci/local_check.sh` (fmt → clippy → tests → package). If a step fails outside this change's scope, document it rather than hiding it.

- [ ] **Step 5: Doc-sync.** In `CLAUDE.md`, update the `dw.agent` conversational paragraph: the out-of-process (Nats) dispatch path now supports the conversational park-loop (via `AwaitHere` + `pending_agent_await`); remove it from the deferred-follow-ups sentence.

- [ ] **Step 6: Commit** `feat(engine): test + doc NATS conversational dw.agent park-loop`

---

## Self-Review

**Spec coverage:**
- Approach A / phase machine → Tasks 2 (marker), 3 (`AwaitHere`), 4 (emit), 5 (branch wiring). ✓
- `AwaitHere` resume-at-self, correlation-keyed → Task 3. ✓
- `pending_agent_await` marker, serde-persisted → Task 2. ✓
- `park_turns` cap reuse → Task 5 (bump/cap/reset). ✓
- Dual resume keying (correlation for AwaitHere, session for LoopHere) → Tasks 3 + 5; verified feasible by Task 1 spike. ✓
- Integration risk / spike-first → Task 1 (with NO-GO escalation). ✓
- Non-conversational + in-process byte-identical → Tasks 4 (resume_at_self:false), 5 (`else` arm), 6 (regression). ✓
- Testing (unit marker, behavioral NATS parity, cap, regression) → Tasks 2, 6. ✓
- No env flag/knob → Global Constraints; nothing adds one. ✓

**Placeholder honesty:** Task 1 is an explicit spike whose findings pin the exact resume-machinery loci that Tasks 3-4 wire; the resume internals are read live (they are `greentic-runner` internals and the resume-at-self delivery is the load-bearing unknown this plan de-risks first). Every INTERFACE the later tasks consume (`AwaitHere` shape, `DispatchOutcome::await_here`, `mark_/take_agent_await`, `execute_remote_dispatch(resume_at_self)`) is fully specified. The knowable code (ExecutionState marker+methods Task 2; the conversational branch decision tree Task 5) is given in full.

**Type consistency:** `pending_agent_await: HashMap<String,()>`, `mark_agent_await(&str)`, `take_agent_await(&str)->bool`, `NodeControl::AwaitHere { reason: Option<String>, correlation_id: String }`, `DispatchOutcome::await_here`, `execute_remote_dispatch(..., resume_at_self: bool)`, `MAX_PARK_TURNS`/`bump_park_turns`/`reset_park_turns` — identical across Tasks 2→6.
