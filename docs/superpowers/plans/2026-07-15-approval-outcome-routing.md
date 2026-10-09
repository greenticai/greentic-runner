# Approval Outcome Routing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an `approval.call` gate routable on its decision — `event` becomes `approved` / `denied` / `timeout` on every path (auto-approve, human approve/deny, timeout).

**Architecture:** Model A. `execute_approval_call` switches to `resume_at_self = true` so the node re-enters itself on resume (the shipped `AwaitHere` mechanism the conversational `dw.agent` already uses), and every completion sets `NodeOutput` `meta["outcome"]` — the canonical, live way to drive `event`, which wins unconditionally over the `ok`-derived fallback. A per-node marker on `ExecutionState` distinguishes "first entry" from "resumed with the response" and stops a stray inbound from re-dispatching a duplicate approval request. No snapshot migration: `AwaitHere` already writes `next_node = SELF`.

**Tech Stack:** Rust 1.95.0, `serde_json`, `crates/greentic-runner-host`.

## Global Constraints

- Rust 1.95.0 (pinned, `ci/local_check.sh:8`). Edition per crate manifest.
- English only in source, tests, comments.
- **Do NOT add Claude co-author attribution to commits or PRs** (`CLAUDE.md:304`).
- Conventional commits with a scope, matching repo style (`feat(runner):`, `fix(operala):`, `docs(runner):`).
- Gate: `ci/local_check.sh` (fmt → wit_sync → clippy → host_smoke → crate_tests → workspace_tests → conformance → package). `RUSTFLAGS="-Dwarnings"` is exported by the script.
  Fast subset while iterating: `LOCAL_CHECK_STEPS=fmt,clippy,workspace_tests ./ci/local_check.sh`
- `--all-features` matters: the conversational `dw.agent` paths are behind `#[cfg(feature = "agentic-worker")]`.
- **Line anchors below are verified against this branch's base (`research` @ `b4c86e29`).** If they drift, grep the symbol.
- Fail closed: an unrecognised or absent decision resolves to `denied`. A corrupt payload must never become a pass.
- Do NOT change `sorla.call` / `operala.call` / `agentic.call` / `telco-x.call` — they share `execute_remote_dispatch` but keep `resume_at_self = false`.
- Do NOT change `FlowSnapshot`.

---

### Task 1: Pure outcome mapper + the await marker

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs`
  - add two free functions next to `approval_requires_human` (`:3948`)
  - add a field + two methods to `ExecutionState` (field block at `:2417`, methods near `mark_agent_await`/`take_agent_await` at `:2517`/`:2526`)
  - add tests to the existing `mod approval_gate_tests` (`:3979`)

**Interfaces:**
- Consumes: nothing from earlier tasks (first task).
- Produces, relied on by Task 2:
  - `fn entry_is_approval_response(entry: &serde_json::Value) -> bool`
  - `fn approval_outcome_from_entry(entry: &serde_json::Value) -> &'static str` → `"approved" | "denied" | "timeout"`
  - `ExecutionState::mark_approval_await(&mut self, node_id: &str)`
  - `ExecutionState::take_approval_await(&mut self, node_id: &str) -> bool`

Rationale for the split: these are pure/state-local and unit-testable with no `FlowEngine`. There is **no** `FlowEngine` test harness in `engine.rs` (`grep FlowEngine::new` in tests → nothing), and `approval_requires_human` already establishes the "pure predicate + own test module" pattern. Keeping the decision logic out of the `&self` method is what makes it testable at all.

- [ ] **Step 1: Write the failing tests**

Add to `mod approval_gate_tests` (`crates/greentic-runner-host/src/runner/engine.rs:3979`). Extend its `use super::…` line to import the new fns:

```rust
    use super::{approval_outcome_from_entry, approval_requires_human, entry_is_approval_response};
```

then add:

```rust
    #[test]
    fn entry_is_a_response_only_when_the_envelope_has_ok() {
        // The dispatch response envelope always carries a top-level `ok`.
        assert!(entry_is_approval_response(
            &json!({ "ok": true, "output": { "decision": "approved" } })
        ));
        assert!(entry_is_approval_response(
            &json!({ "ok": false, "error": { "code": "timeout" } })
        ));
        // A user activity arriving mid-await has no top-level `ok`.
        assert!(!entry_is_approval_response(
            &json!({ "text": "hi", "metadata": { "action": "go" } })
        ));
        assert!(!entry_is_approval_response(&json!({})));
    }

    #[test]
    fn outcome_reads_the_decision() {
        assert_eq!(
            approval_outcome_from_entry(&json!({ "ok": true, "output": { "decision": "approved" } })),
            "approved"
        );
        assert_eq!(
            approval_outcome_from_entry(&json!({ "ok": true, "output": { "decision": "denied" } })),
            "denied"
        );
    }

    #[test]
    fn timeout_wins_over_the_decision() {
        // The watchdog envelope has output: null and error.code == "timeout".
        assert_eq!(
            approval_outcome_from_entry(
                &json!({ "ok": false, "output": null, "error": { "code": "timeout" } })
            ),
            "timeout"
        );
    }

    #[test]
    fn unknown_or_missing_decision_fails_closed_to_denied() {
        // Fail closed: a corrupt payload must never become a pass.
        assert_eq!(
            approval_outcome_from_entry(&json!({ "ok": true, "output": { "decision": "maybe" } })),
            "denied"
        );
        assert_eq!(
            approval_outcome_from_entry(&json!({ "ok": true, "output": {} })),
            "denied"
        );
        assert_eq!(approval_outcome_from_entry(&json!({ "ok": true })), "denied");
        assert_eq!(
            approval_outcome_from_entry(&json!({ "ok": false, "error": { "code": "nats_down" } })),
            "denied"
        );
    }

    #[test]
    fn decision_match_is_case_insensitive() {
        assert_eq!(
            approval_outcome_from_entry(&json!({ "ok": true, "output": { "decision": "Approved" } })),
            "approved"
        );
    }
```

Add to the `#[cfg(test)] mod tests` in the same file (the module that holds `pending_agent_await_mark_and_take`, near `:5330`):

```rust
    #[test]
    fn approval_await_mark_and_take() {
        let mut st = ExecutionState::new(json!({}));
        assert!(!st.take_approval_await("gate"), "unmarked node takes false");
        st.mark_approval_await("gate");
        assert!(st.take_approval_await("gate"), "marked node takes true");
        assert!(!st.take_approval_await("gate"), "take clears the mark");
    }

    #[test]
    fn approval_await_survives_snapshot_roundtrip() {
        let mut st = ExecutionState::new(json!({}));
        st.mark_approval_await("gate");
        let encoded = serde_json::to_string(&st).expect("serialize");
        let mut decoded: ExecutionState = serde_json::from_str(&encoded).expect("deserialize");
        assert!(
            decoded.take_approval_await("gate"),
            "the marker must survive a park/resume snapshot"
        );
    }

    #[test]
    fn approval_await_defaults_empty_for_old_snapshots() {
        // Snapshots persisted before this field exists must still decode.
        let mut decoded: ExecutionState =
            serde_json::from_str(r#"{"entry":{},"input":{}}"#).expect("old snapshot decodes");
        assert!(!decoded.take_approval_await("gate"));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p greentic-runner-host --all-features approval -- --nocapture`
Expected: FAIL to compile — `approval_outcome_from_entry`, `entry_is_approval_response`, `mark_approval_await`, `take_approval_await` do not exist.

- [ ] **Step 3: Add the pure functions**

In `crates/greentic-runner-host/src/runner/engine.rs`, immediately **after** `approval_requires_human` (its closing brace is just above `#[cfg(test)] mod approval_gate_tests` at `:3979`):

```rust
/// True when `entry` is a runtime-dispatch response envelope rather than a user
/// activity. The dispatch response always carries a top-level `ok`
/// (`{ok, output, events, error}`); an inbound activity never does. Mirrors the
/// conversational `dw.agent` discriminator (`state.entry.get("ok").is_some()`).
fn entry_is_approval_response(entry: &Value) -> bool {
    entry.get("ok").is_some()
}

/// Map an approval response envelope to the routing outcome that becomes
/// `event` via `NodeOutput.meta["outcome"]`.
///
/// A watchdog timeout arrives as `{ok: false, output: null, error: {code: "timeout"}}`
/// and wins over any decision. Otherwise the discriminator is `output.decision`
/// — NOT `ok`, which greentic-admin sets to `true` for approve *and* deny.
///
/// Fails closed: an unrecognised or absent decision is `denied`, mirroring
/// `approval_requires_human`'s own `_ => true` fail-safe. A corrupt payload must
/// never become a pass.
fn approval_outcome_from_entry(entry: &Value) -> &'static str {
    let timed_out = entry
        .pointer("/error/code")
        .and_then(Value::as_str)
        .is_some_and(|code| code.eq_ignore_ascii_case("timeout"));
    if timed_out {
        return "timeout";
    }
    match entry.pointer("/output/decision").and_then(Value::as_str) {
        Some(decision) if decision.eq_ignore_ascii_case("approved") => "approved",
        _ => "denied",
    }
}
```

- [ ] **Step 4: Add the marker to `ExecutionState`**

In the `ExecutionState` field block, immediately after `pending_agent_await: HashMap<String, ()>,` (`:2417`):

```rust
    /// Nodes that dispatched an approval request and are parked awaiting the
    /// decision. Set on dispatch, cleared when the response re-enters the node.
    /// Without it a stray inbound arriving mid-await would look like a first
    /// entry and re-dispatch — a duplicate approval request to the operator.
    #[serde(default)]
    pending_approval_await: HashMap<String, ()>,
```

In `ExecutionState::new`, next to `pending_agent_await: HashMap::new(),`:

```rust
            pending_approval_await: HashMap::new(),
```

And immediately after `take_agent_await` (`:2526-2528`):

```rust
    fn mark_approval_await(&mut self, node_id: &str) {
        self.pending_approval_await.insert(node_id.to_string(), ());
    }

    /// Check-and-clear: returns whether `node_id` dispatched an approval and is
    /// awaiting the decision.
    fn take_approval_await(&mut self, node_id: &str) -> bool {
        self.pending_approval_await.remove(node_id).is_some()
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p greentic-runner-host --all-features approval -- --nocapture`
Expected: all new tests PASS.

- [ ] **Step 6: Run the gate**

Run: `LOCAL_CHECK_STEPS=fmt,clippy ./ci/local_check.sh`
Expected: clean (exit 0). If clippy flags `pending_approval_await` as unused (Task 2 is its only consumer), add `#[allow(dead_code)]` to the two methods **with a comment naming Task 2**, and remove it in Task 2.

- [ ] **Step 7: Commit**

```bash
git add crates/greentic-runner-host/src/runner/engine.rs
git commit -m "feat(runner): add approval outcome mapper and await marker"
```

---

### Task 2: Wire `execute_approval_call` to re-enter and emit the outcome

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs`
  - `execute_approval_call` (`:1416-1436`)
  - the `NodeKind::ApprovalCall` dispatch arm (`:1246-1248`)

**Interfaces:**
- Consumes from Task 1: `entry_is_approval_response`, `approval_outcome_from_entry`, `ExecutionState::mark_approval_await`, `ExecutionState::take_approval_await`.
- Consumes (existing): `execute_remote_dispatch(&self, ctx, runtime: &str, target: &str, payload: Value, resume_at_self: bool)` (`:1541`); `NodeOutput::with_meta(payload, meta)` (`:2661`); `DispatchOutcome::complete(output)`; `DispatchOutcome::await_here(output, reason, correlation_id)` (`:2583`).
- Produces: `execute_approval_call(&self, ctx, node_id: &str, target: &str, payload: Value, state: &mut ExecutionState) -> Result<DispatchOutcome>`.

Why the signature change compiles: `dispatch_node` (`:950-958`) already takes `node_id: &str` and `state: &mut ExecutionState`, and matches on `&node.kind` — which borrows `node`, not `state`. The conversational `dw.agent` arm already calls `state.mark_agent_await(node_id)` inside that same match (`:1155`), which proves a `&mut state` reborrow is available there.

- [ ] **Step 1: Write the failing test**

The re-entry glue needs a `FlowEngine`, and this crate has no such test harness — so the observable contract is asserted at the routing layer, which is a pure function. Add to the `#[cfg(test)] mod tests` in `crates/greentic-runner-host/src/runner/engine.rs`, next to `multi_edge_node_routes_on_injected_event` (`:5431`):

```rust
    #[test]
    fn approval_outcomes_route_three_ways() {
        // The contract this feature exists for: an approval node's decision
        // selects the branch. `meta["outcome"]` wins over the ok-derived
        // default, so `ok: true` still routes to "denied" when denied.
        let raw_routing = json!([
            { "condition": "event == \"approved\"", "to": "do_it" },
            { "condition": "event == \"denied\"", "to": "reject" },
            { "condition": "event == \"timeout\"", "to": "escalate" }
        ]);
        let flow_ir = HostFlow {
            id: "flow.test".to_string(),
            start: None,
            nodes: IndexMap::new(),
            vars_init: JsonMap::new(),
        };
        let current = NodeId::from_str("gate").unwrap();
        let state = ExecutionState::new(json!({}));

        for (outcome, expected) in [
            ("approved", "do_it"),
            ("denied", "reject"),
            ("timeout", "escalate"),
        ] {
            let out = NodeOutput::with_meta(json!({}), json!({ "outcome": outcome }));
            match evaluate_custom_routing(&raw_routing, &out, &state, &flow_ir, &current) {
                CustomRoutingDecision::Next(nid) => assert_eq!(
                    nid.as_str(),
                    expected,
                    "outcome {outcome} must route to {expected}"
                ),
                other => panic!("outcome {outcome}: expected Next({expected}), got {other:?}"),
            }
        }
    }
```

- [ ] **Step 2: Run the test to verify it passes already**

Run: `cargo test -p greentic-runner-host --all-features approval_outcomes_route_three_ways -- --nocapture`
Expected: **PASS** — this test characterises the routing layer, which already supports custom events. It is the proof that the outcome names Task 2 emits are routable; it is not RED-first. Record it as characterisation, not TDD RED, in your report.

- [ ] **Step 3: Rewrite `execute_approval_call`**

Replace the whole function (`crates/greentic-runner-host/src/runner/engine.rs:1416-1436`) with:

```rust
    /// Human-in-the-loop approval gate.
    ///
    /// Sets `meta["outcome"]` to `approved` / `denied` / `timeout` on every path,
    /// so routing conditions (`event == "approved"`) work identically whether a
    /// human decided or the gate auto-approved. `ok` cannot serve as the
    /// discriminator: greentic-admin publishes `ok: true` for approve AND deny.
    ///
    /// Uses `resume_at_self = true` so the response re-enters THIS node and its
    /// own routing sees the decision. `pending_approval_await` distinguishes the
    /// first entry from the resume, and stops a stray inbound (which lands in the
    /// same wait slot — see the keying note on `NodeControl::AwaitHere`) from
    /// re-dispatching a duplicate approval request.
    async fn execute_approval_call(
        &self,
        ctx: &FlowContext<'_>,
        node_id: &str,
        target: &str,
        payload: Value,
        state: &mut ExecutionState,
    ) -> Result<DispatchOutcome> {
        if state.take_approval_await(node_id) {
            if entry_is_approval_response(&state.entry) {
                let outcome = approval_outcome_from_entry(&state.entry);
                let output = NodeOutput::with_meta(
                    state.entry.clone(),
                    serde_json::json!({ "outcome": outcome }),
                );
                return Ok(DispatchOutcome::complete(output));
            }
            // A user activity arrived while we were parked. Re-park without
            // re-dispatching; the correlation id is discarded by the AwaitHere
            // handler, so re-parking needs nothing from the original dispatch.
            state.mark_approval_await(node_id);
            return Ok(DispatchOutcome::await_here(
                NodeOutput::new(serde_json::json!({ "pending": true })),
                Some("awaiting approval decision".to_string()),
                String::new(),
            ));
        }

        let input = payload.get("input").cloned().unwrap_or(Value::Null);
        if !approval_requires_human(&input) {
            let output = NodeOutput::with_meta(
                serde_json::json!({
                    "ok": true,
                    "output": { "decision": "approved", "auto": true },
                    "error": serde_json::Value::Null,
                }),
                serde_json::json!({ "outcome": "approved" }),
            );
            return Ok(DispatchOutcome::complete(output));
        }

        state.mark_approval_await(node_id);
        self.execute_remote_dispatch(ctx, "approval", target, payload, true)
            .await
    }
```

- [ ] **Step 4: Update the dispatch arm**

At `crates/greentic-runner-host/src/runner/engine.rs:1246-1248`, replace:

```rust
            NodeKind::ApprovalCall { target } => {
                self.execute_approval_call(ctx, target, payload).await
            }
```

with:

```rust
            NodeKind::ApprovalCall { target } => {
                self.execute_approval_call(ctx, node_id, target, payload, state)
                    .await
            }
```

- [ ] **Step 5: Drop any `#[allow(dead_code)]` from Task 1**

If Task 1 added `#[allow(dead_code)]` to `mark_approval_await` / `take_approval_await`, remove it now — Task 2 is the consumer.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p greentic-runner-host --all-features approval -- --nocapture`
Expected: all PASS (Task 1's unit tests plus the routing test).

Run: `cargo test -p greentic-runner-host --all-features 2>&1 | tail -20`
Expected: no regressions. Pay attention to the wait/resume suite — `dispatch_outcome_await_here_stores_variant`, `evaluate_custom_routing_waits_when_conditional_falls_through`, and the conversational `dw.agent` park/loop tests must stay green.

- [ ] **Step 7: Run the gate (package-scoped)**

Run:
```bash
cargo fmt -p greentic-runner-host -- --check
cargo clippy -p greentic-runner-host --all-targets --all-features -- -D warnings
```
Expected: both exit 0.

Use the package-scoped commands, not `./ci/local_check.sh`. Verified in Task 1: `cargo fmt --all -- --check`
exits 1 on a **pre-existing** violation at `crates/greentic-aw-runtime/src/loop.rs:395` — a different
package this branch never touches, already broken at the base. Do not format or fix that package.

- [ ] **Step 8: Commit**

```bash
git add crates/greentic-runner-host/src/runner/engine.rs
git commit -m "feat(runner): route approval.call on approved/denied/timeout"
```

---

### Task 3: Integration test — approval await pauses and resumes at self

**Files:**
- Modify: `crates/greentic-runner-host/tests/runtime_call_nodes.rs`

**Interfaces:**
- Consumes from Task 2: the wired `execute_approval_call`. No new public surface.

This file already contains the await-pause contract tests for the sibling dispatch nodes — `operala_call_await_pauses_with_session_hint_correlation` (`:372`) and `agentic_call_await_pauses_with_session_hint_correlation` (`:494`). `approval.call` has **no** integration coverage at all today.

- [ ] **Step 1: Write the test**

Append to `crates/greentic-runner-host/tests/runtime_call_nodes.rs`, after the `agentic.call` tests. It reuses the file's existing helpers verbatim — `build_dispatch_pack` (`:156`, which builds the dispatch node under the id **`"call"`** with routing `{"next": {"node_id": "done"}}` plus a `"done"` successor), `host_config`, `build_engine`, `flow_ctx`, `RuntimeCapturingStub`, `RUNTIME`. Note the file's style: `#[test]` + `rt.block_on(...)`, not `#[tokio::test]`.

```rust
// ── approval.call tests ───────────────────────────────────────────────────────

#[test]
fn approval_call_await_parks_at_self_not_the_successor() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("approval-await.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    // `mode: always` → approval_requires_human → dispatch and park for a human.
    build_dispatch_pack(
        &pack_path,
        "approval.call",
        json!({ "await": true, "operation": "create", "input": { "mode": "always" } }),
        true,
    )?;

    let config = Arc::new(host_config(&bindings_path));
    let handler = Arc::new(RuntimeCapturingStub::default());
    let (pack, engine) = build_engine(&pack_path, Arc::clone(&config), Arc::clone(&handler))?;

    let ctx = flow_ctx(&config, pack.metadata().pack_id.as_str());
    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("await approval.call run")?;

    let wait = match execution.status {
        FlowStatus::Waiting(wait) => wait,
        FlowStatus::Completed => {
            anyhow::bail!("flow completed but should have paused awaiting the approval decision")
        }
    };

    // THE contract this feature exists for. Every sibling dispatch node
    // (sorla/operala/agentic/telco-x, resume_at_self = false) parks at the
    // routing successor — here that would be "done". approval.call re-enters
    // ITSELF so its own routing can see the decision.
    assert_eq!(
        wait.snapshot.next_node, "call",
        "approval.call must park at self, not at the successor"
    );

    let recorded = handler
        .last
        .lock()
        .unwrap()
        .clone()
        .expect("handler should have recorded a dispatch");
    assert_eq!(
        recorded.runtime, "approval",
        "runtime name must be 'approval', got '{}'",
        recorded.runtime
    );
    assert_eq!(recorded.mode, DispatchMode::Await);
    assert_eq!(recorded.target, "dep-1");
    Ok(())
}
```

If `FlowStatus` / `FlowWait` are not already imported in this file, add them to the existing `use greentic_runner_host::…` group (`FlowWait` is `{ reason, snapshot }`, `FlowStatus::Waiting(Box<FlowWait>)`, both at `src/runner/engine.rs:100-109`).

- [ ] **Step 2: Run the test**

Run: `cargo test -p greentic-runner-host --all-features approval_call_await_parks_at_self_not_the_successor -- --nocapture`
Expected: **PASS**, because Task 2 is already committed. This test is the end-to-end proof of Task 2's `resume_at_self = true`.

If it FAILS with `next_node == "done"`, Task 2's change did not take effect — that is a real defect in Task 2. **Report it; do not edit the test to match the behaviour.**

- [ ] **Step 4: Run the full suite**

Run: `cargo test -p greentic-runner-host --all-features 2>&1 | tail -20`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-runner-host/tests/runtime_call_nodes.rs
git commit -m "test(runner): cover approval.call await parking at self"
```

---

### Task 4: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run the package-scoped gate (the full one cannot pass — see below)**

Run:
```bash
cargo fmt -p greentic-runner-host -- --check
cargo clippy -p greentic-runner-host --all-targets --all-features -- -D warnings
```
Expected: both exit 0.

**Do not chase `./ci/local_check.sh` to green.** Verified during Task 1: `cargo fmt --all -- --check`
exits 1 on a **pre-existing** violation at `crates/greentic-aw-runtime/src/loop.rs:395` — a different
package that this branch never touches (every commit here is confined to
`crates/greentic-runner-host/`). It is broken at the base (`research` @ `b4c86e29`), not by us.

**Do NOT format or otherwise "fix" `greentic-aw-runtime`** — it is outside this branch's scope and
the repo's main checkout has unrelated work in flight there. Record the pre-existing failure in your
report so it reaches the PR summary (the repo rule is to document an out-of-scope failure, not hide
it and not silently absorb it).

For reference, the full script's other defaults: `host_smoke` is skipped (`RUN_HOST=never`) and
`conformance` is off (`RUN_CONFORMANCE=0`) — that is the normal local shape; do not force them on.

- [ ] **Step 2: Confirm no sibling dispatch node changed behaviour**

Run: `cargo test -p greentic-runner-host --all-features -- runtime_call_nodes resume_characterization multi_wait --nocapture 2>&1 | tail -15`
Expected: all PASS — `sorla` / `operala` / `agentic` / `telco-x` must still park at their successor (`resume_at_self = false`); only `approval.call` parks at self.

- [ ] **Step 3: Commit any verification fixes**

```bash
git add -A
git commit -m "fix(runner): address local_check findings for approval outcome routing"
```

## Notes for the implementer

- **Do not** change `resume_at_self` for `sorla.call` / `operala.call` / `agentic.call` / `telco-x.call`. They share `execute_remote_dispatch`; only the approval caller flips to `true`.
- **Do not** touch `FlowSnapshot`. `AwaitHere` already writes `next_node = SELF`; no migration is needed and in-flight parked sessions must keep decoding.
- The `AwaitHere` single-slot interleaving limitation (see the keying note in the `NodeControl::AwaitHere` handler) is **inherited, not fixed**. The stray-inbound re-park is what keeps this safe; do not "simplify" it away.
- `event` names here are deliberately custom (`approved`/`denied`/`timeout`), not the six `on_*` names. The runner does not validate `event`. Consequence, already accepted in the spec: `default_event` / `node_has_error_route` only recognise `on_*`, so a route `event == "denied"` does not register as an error route — harmless because we always set an explicit outcome, which wins.
