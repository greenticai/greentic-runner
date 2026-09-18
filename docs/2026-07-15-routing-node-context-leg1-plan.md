# Routing conditions: cross-node context — Leg 1 (runner) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a `conditional_branch` guard read any prior node's output via `node.<id>.<field>`, and make a guard that resolves to nothing visible instead of silent.

**Architecture:** `build_routing_context` gains one key — `node`, from the `state.outputs_map()` projection `template_context` already uses. Plus two honesty fixes: a test that currently hides the limit, and a `debug!` that hides the failure.

**Tech Stack:** Rust 1.94, edition 2024. No new dependencies.

**Spec:** `greentic-designer/docs/superpowers/specs/2026-07-15-routing-condition-node-context-design.md` (commit `df4b89da`).

## Global Constraints

- **Rust 1.94.0**, edition 2024 (runner pins 1.94 — NOT the designer's 1.95).
- **English only** — source, tests, doc comments, commit messages.
- **No Claude co-author attribution** on commits or PRs (`CLAUDE.md:304`).
- **`#![forbid(unsafe_code)]`** is the norm; `anyhow::Result` + `.context()`.
- No 500-line cap in this repo (`engine.rs` is already 8162 lines).
- Branch: `feat/routing-node-context`, based on `origin/research` (`6b4ff0f9`).
  Worktree: `greentic-runner/.worktrees/routing-node-ctx`.
- **Do NOT touch the fmt-drift base files** (`aw-event-bridge/jetstream.rs`,
  `greentic-aw-runtime/{dispatch_ledger,short_term}.rs`, runner-desktop,
  `graph_node.rs`, `mcp_flow_node.rs`) — pre-existing toolchain drift, not ours.
- Pre-existing CI reds on this repo are NOT yours: `ci.yml` clippy/test fail
  because CI cannot clone the private `chronicle-core` git dep, and fmt fails on
  the drift files above. Do not chase them.

## Why this is safe to change

`build_routing_context` (`engine.rs:3928`) already takes `&ExecutionState` and
reads `state.entry`. `state.outputs_map()` (`engine.rs:2515`) projects every
prior node through `node_output_view` — **the same projection**
`template_context` (`engine.rs:2945`) uses for `{{node.<id>.<field>}}`. So
adding `node` to the routing context makes conditions resolve **identically** to
params. Reader and writer agree by construction.

This is additive: no existing key changes, and the predecessor shorthand
(`q_age >= 18`, which resolves against the payload spread at top level) is
untouched.

## File Structure

| File | Responsibility | Task |
| --- | --- | --- |
| `crates/greentic-runner-host/src/runner/engine.rs` (**modify**, `:3928`) | `build_routing_context` gains `node`. | 1 |
| `crates/greentic-runner-host/src/runner/engine.rs` (**modify**, `:5819`) | Fix the test that hides the limit. | 2 |
| `crates/greentic-runner-host/src/runner/engine.rs` (**modify**, `:3730`) | `debug!` → `warn!` naming the unresolved path. | 3 |

---

### Task 1: Inject `node` into the routing context

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (`build_routing_context`, `:3928`)
- Test: inline `#[cfg(test)] mod tests` in the same file

**Interfaces:**
- Consumes: `ExecutionState::outputs_map()` (`:2515`, already `fn` on the struct).
- Produces: no signature change. `build_routing_context` still returns `Value`.

- [ ] **Step 1: Write the failing test**

Add to `mod tests`. **Read `build_routing_context`'s real signature first** (`:3928`) — it takes `(&NodeOutput, &ExecutionState, &str, &str)`; construct the fixture from the real types, and find how other tests in this module build an `ExecutionState` with populated `nodes` (grep `ExecutionState` inside `mod tests`).

```rust
    /// A routing condition must be able to read ANY prior node's output, not
    /// just the immediate predecessor's payload. `state.nodes` has always held
    /// them; the routing context simply never exposed them, so
    /// `node.<id>.<field>` resolved to nothing and the guard silently took the
    /// false branch on every input.
    ///
    /// This drives the REAL `build_routing_context` — see
    /// `condition_evaluator_supports_comparisons_and_contains` for why a
    /// hand-built context proves nothing here.
    #[test]
    fn routing_context_exposes_prior_node_outputs_under_node() {
        let mut state = ExecutionState::default();
        state
            .nodes
            .insert("register".into(), NodeOutput::new(json!({ "q_age": 21 })));

        let current = NodeOutput::new(json!({ "status": "ok" }));
        let ctx = build_routing_context(&current, &state, "on_success", "on_error");

        // The cross-node form, matching `{{node.<id>.<field>}}` in params.
        assert!(
            evaluate_simple_condition("node.register.q_age >= 18", &ctx),
            "a prior node's output must be readable via node.<id>.<field>: {ctx:?}"
        );
        // The node_io envelope resolves too — same projection as params.
        assert!(
            evaluate_simple_condition("node.register.data.q_age >= 18", &ctx),
            "the data envelope must resolve like it does in params: {ctx:?}"
        );
    }

    #[test]
    fn routing_context_keeps_the_predecessor_shorthand() {
        // Negative-ish: the existing form must not regress. The current node's
        // payload stays spread at the top level, which is what PR #665's
        // source-node prefix strip relies on.
        let mut state = ExecutionState::default();
        state
            .nodes
            .insert("register".into(), NodeOutput::new(json!({ "q_age": 21 })));

        let current = NodeOutput::new(json!({ "q_age": 30 }));
        let ctx = build_routing_context(&current, &state, "on_success", "on_error");

        assert!(
            evaluate_simple_condition("q_age >= 30", &ctx),
            "the bare form must still resolve against the current payload: {ctx:?}"
        );
    }

    #[test]
    fn routing_context_does_not_resolve_a_missing_node() {
        // Negative: a ref to a node with no output must NOT resolve — it must
        // stay false, not accidentally match something.
        let state = ExecutionState::default();
        let current = NodeOutput::new(json!({ "status": "ok" }));
        let ctx = build_routing_context(&current, &state, "on_success", "on_error");

        assert!(
            !evaluate_simple_condition("node.ghost.x == \"y\"", &ctx),
            "a missing node must not resolve: {ctx:?}"
        );
    }
```

If `ExecutionState::default()` does not exist or `nodes` is private, adapt to
how the module's existing tests build one — **do not** widen visibility just for
a test without saying so in your report.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p greentic-runner-host --lib routing_context_exposes -- --nocapture`
Expected: FAIL — `node.register.q_age` does not resolve.

- [ ] **Step 3: Implement**

In `build_routing_context` (`:3928`), after the `entry`/`in` inserts and before
or after the `response` block (order does not matter — keys are distinct), add:

```rust
    // Every prior node's output, keyed by id — the SAME projection
    // `template_context` exposes for `{{node.<id>.<field>}}`. Without this a
    // routing condition could only see its immediate predecessor's payload, so
    // `node.other.x` resolved to nothing and the guard silently took the false
    // branch on every input.
    ctx.insert("node".into(), Value::Object(state.outputs_map()));
```

**Do not remove or reorder any existing insert.** The current node's payload
must stay spread at the top level — PR #665's prefix strip depends on it, and
Task 1 Step 1's second test pins that.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p greentic-runner-host --lib routing_context -- --nocapture`
Expected: PASS, 3 tests.

- [ ] **Step 5: Run the whole engine suite for regressions**

Run: `cargo test -p greentic-runner-host --lib runner::engine -- --nocapture 2>&1 | tail -5`
Expected: PASS. **If a routing test breaks, STOP and report** — adding a key
should not change any existing resolution, and a break means it did.

- [ ] **Step 6: Lint**

Run: `cargo fmt --all -- --check && cargo clippy -p greentic-runner-host --all-features -- -D warnings`
Expected: clean. If fmt flags one of the known drift files, you touched
something you should not have — revert it.

- [ ] **Step 7: Commit**

```bash
git add crates/greentic-runner-host/src/runner/engine.rs
git commit -m "feat(engine): expose prior node outputs to routing conditions

A conditional_branch guard could only read its immediate predecessor's
payload. Anything else — including the cross-node form the catalog documents
as 'prior nodes'' outputs — resolved to nothing and silently took the false
branch on every input, indistinguishable from a guard that legitimately
evaluated false.

state.nodes has always held every prior output; the routing context simply
never exposed it, while template_context did. Insert the same outputs_map
projection under `node`, so node.<id>.<field> resolves in a condition exactly
as {{node.<id>.<field>}} does in params.

Additive: the current node's payload stays spread at the top level, so the
predecessor shorthand (and the designer's source-node prefix strip) is
unchanged."
```

---

### Task 2: Fix the test that hides the limit

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (`:5819`, `condition_evaluator_supports_comparisons_and_contains`)

**Why this task exists.** The test hand-builds a node-keyed context:

```rust
let ctx = json!({ "register": { "q_age": 18 }, … });
assert!(evaluate_simple_condition("register.q_age >= 18", &ctx));
```

and its doc comment claims it "backs the user-authored `conditional_branch`
expressions the catalog documents". Both halves mislead:

- The runtime never built that shape. `build_routing_context` spread the
  payload flat, with no node keying — so `register.q_age` never resolved that
  way in production.
- Reading the test, you would conclude cross-node conditions work. That belief
  is exactly why the limit went unnoticed.

As a **parser** test it is fine and worth keeping — `evaluate_simple_condition`
over an arbitrary context is a legitimate unit. The fix is to stop it claiming
to be more than that, and to point at the test that does cover the real shape.

**Do not delete it.** Do not change its assertions — the operators it pins
(`>=`, `<=`, `>`, `<`, `contains`) are real coverage from PR #486.

- [ ] **Step 1: Rewrite the doc comment honestly**

Replace the doc comment above `:5819` with something that says what it actually
tests. It must:
- describe it as an **expression-parser** test over an arbitrary context;
- state that the context here is hand-built and is **not** the shape
  `build_routing_context` produces;
- point at `routing_context_exposes_prior_node_outputs_under_node` (Task 1) as
  the test that covers the real routing context.

Keep the note about PR #486's operators — that part is true and useful.

- [ ] **Step 2: Rename the fixture keys so they cannot be mistaken for node ids**

The keys `register` / `submit` read as node ids, which is the whole illusion.
Rename them to something obviously arbitrary (e.g. `a` / `b` / `c`) and update
the assertions to match. The operators under test do not care about the names.

**If renaming makes any assertion fail, STOP and report** — that would mean the
test depends on the names, which would be news.

- [ ] **Step 3: Run**

Run: `cargo test -p greentic-runner-host --lib condition_evaluator -- --nocapture`
Expected: PASS, unchanged behaviour.

- [ ] **Step 4: Commit**

```bash
git add crates/greentic-runner-host/src/runner/engine.rs
git commit -m "test(engine): stop the condition test claiming end-to-end coverage

It hand-built a node-keyed context the runtime never produced, and its doc
comment said it backed the catalog's conditional_branch expressions. Read
together, that said cross-node conditions worked. They did not — and this
test is why nobody noticed.

Keep it as what it is: an expression-parser test over an arbitrary context.
The fixture keys no longer look like node ids, and the doc points at the test
that drives the real build_routing_context."
```

---

### Task 3: Make a fall-through visible

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (`:3730`)

**Why:** when no conditional route matches, the runner logs at `debug!` — invisible
at default levels — and returns `Wait`. With the designer's unconditional
false-path fallback the run does not even Wait: it takes the false branch on
every input. A broken guard is indistinguishable from a guard that is legitimately
false, and nothing in the logs says otherwise.

- [ ] **Step 1: Read the call site**

Read `engine.rs:3700-3740`. Note what is in scope: `flow_id`, `node_id`, and the
routing array. Decide what can be named without leaking payload values — **log
the unresolved condition string, not the resolved data**. A condition can carry
a literal (`status == "secret"`), but the condition text is authored, not user
input; the payload is user input. Do not log `ctx`.

- [ ] **Step 2: Upgrade the level and name the condition**

Change the `tracing::debug!` at `:3730` to `tracing::warn!`, and include the
condition string(s) that failed to match. Keep `flow_id` and `node_id`.

Match the style of the `warn!` a few lines above (`:3582`, "custom routing is
not an array; terminating") — same field order, same phrasing register.

- [ ] **Step 3: Verify nothing else regressed**

Run: `cargo test -p greentic-runner-host --lib runner::engine -- --nocapture 2>&1 | tail -5`
Expected: PASS. `evaluate_custom_routing_waits_when_conditional_falls_through`
asserts the **decision** (`Wait`), not the log level, so it must stay green
unedited. If it breaks, you changed behaviour, not logging — STOP and report.

- [ ] **Step 4: Lint + commit**

Run: `cargo fmt --all -- --check && cargo clippy -p greentic-runner-host --all-features -- -D warnings`

```bash
git add crates/greentic-runner-host/src/runner/engine.rs
git commit -m "fix(engine): warn when no conditional route matches

It logged at debug!, invisible by default. With the designer's unconditional
false-path fallback the run does not even pause — it takes the false branch on
every input, so a broken guard looks exactly like a guard that is legitimately
false. Name the condition that failed to match so the difference is visible.

Logs the condition text (authored), never the context (user data)."
```

---

### Task 4: Verify and open the PR

- [ ] **Step 1: Full local CI**

Run: `bash ci/local_check.sh`

**Expect pre-existing failures that are NOT yours** — `ci.yml` clippy/test cannot
clone the private `chronicle-core` dep, and fmt fails on known drift files
(`aw-event-bridge/jetstream.rs`, `greentic-aw-runtime/{dispatch_ledger,short_term}.rs`,
runner-desktop, `graph_node.rs`, `mcp_flow_node.rs`). Report them; do not fix them.

What must be green: `cargo test -p greentic-runner-host --lib runner::engine`
and clippy scoped to `-p greentic-runner-host`.

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin feat/routing-node-context
gh pr create --base research \
  --title "feat(engine): expose prior node outputs to routing conditions" \
  --body "$(cat <<'EOF'
## What

A `conditional_branch` guard could only read its **immediate predecessor's**
payload. Anything else resolved to nothing and **silently took the false branch
on every input** — indistinguishable from a guard that legitimately evaluated
false.

`state.nodes` has always held every prior node's output. The routing context
simply never exposed it — `template_context` did, `build_routing_context` did
not, and `outputs_map()` was one line away.

## How

One key: `ctx["node"] = state.outputs_map()`. That is the **same projection**
`template_context` uses, so `node.<id>.<field>` resolves in a condition exactly
as `{{node.<id>.<field>}}` does in params — one grammar, one mental model.

Additive. The current node's payload stays spread at the top level, so the
predecessor shorthand (`q_age >= 18`) and the designer's source-node prefix
strip (#665) are untouched — pinned by a test.

## Two honesty fixes

- `condition_evaluator_supports_comparisons_and_contains` hand-built a
  node-keyed context the runtime never produced, and its doc claimed it backed
  the catalog's expressions. Read together, that said cross-node conditions
  worked. **That test is why nobody noticed they did not.** It stays as an
  expression-parser test, with a doc that says so and fixture keys that no
  longer masquerade as node ids.
- The no-route-matched fall-through logged at `debug!` — invisible by default.
  Now `warn!`, naming the condition that failed to match.

## Scope

Leg 1 of 3. Designer (validator + catalog + pin bump) and greentic-start (pin
bump + deploy) follow. **Run Demo pins `805278fc` and production pins
`1df54513`**, so shipping this without both pin bumps would make Run Demo work
while deployed flows silently misroute — worse than today's uniform failure.

Spec: `greentic-designer/docs/superpowers/specs/2026-07-15-routing-condition-node-context-design.md`

## Verification

- `cargo test -p greentic-runner-host --lib runner::engine` green.
- Pre-existing CI reds (private `chronicle-core` clone, fmt drift files) are
  untouched and unrelated.
EOF
)"
```

- [ ] **Step 3: Report the PR url and stop.** Do not merge; await instruction.

---

## Self-Review

**Spec coverage (Leg 1 only):**

| Spec element | Task |
| --- | --- |
| `ctx["node"] = outputs_map()` | Task 1 |
| Cross-node condition test driving the REAL context | Task 1, Step 1 |
| Predecessor shorthand preserved | Task 1, Step 1 (second test) + Step 3's "do not reorder" |
| Negative: missing node does not resolve | Task 1, Step 1 (third test) |
| Fix the misleading test | Task 2 |
| `debug!` → `warn!` naming the path | Task 3 |

Out of Leg 1 by design: the designer validator + catalog + pin bump (Leg 2), and
the greentic-start pin bump + deploy (Leg 3).

**Placeholder scan:** No TBD/TODO. Task 1 Step 1 says to read the real signature
and adapt the fixture — that is an instruction to verify, not a gap.

**Type consistency:** `build_routing_context(&NodeOutput, &ExecutionState, &str, &str) -> Value`
is unchanged. `outputs_map(&self) -> JsonMap<String, Value>` (`:2515`) is wrapped
in `Value::Object`, matching how `template_context` (`:2945`) uses it.
`evaluate_simple_condition(&str, &Value) -> bool` takes the returned ctx directly.

**Ordering:** Task 1 → Task 2 (Task 2's doc points at Task 1's test). Task 3 is
independent.
