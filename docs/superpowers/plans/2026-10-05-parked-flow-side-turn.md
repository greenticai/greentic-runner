# Parked flow tool: side turn (PR-3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** With `on_text_while_parked = side_turn`, a typed message while a `flow:` tool is parked on a card is answered by the agent (RAG, tools, memory) WITHOUT cancelling the parked tool; the card is re-offered and a later card submit resumes the flow with its state intact.

**Architecture:** `take_pending` (`crates/greentic-aw-runtime/src/flow_suspend.rs:46`) gains a third outcome `Side`. At suspend the loop writes a placeholder `Tool{call_id, awaiting_user_input}` right after the assistant `tool_calls` turn so the transcript stays provider-valid while side Q/A is appended after it; resume/cancel PATCH that placeholder by `call_id` instead of pushing a duplicate. The side turn ends `awaiting_tool_input` again with the stored card, the engine re-marks the await, and renders `[reply, card]` as an array. Everything is behind a per-agent knob defaulting to today's cancel.

**Tech Stack:** Rust 1.95 edition 2024, `serde`, `chrono`; crates `greentic-aw-runtime`, `greentic-runner-host`.

**Spec:** `docs/superpowers/specs/2026-10-05-parked-flow-side-question-design.md` (D2-D6, section 6, section 8 decisions). PR-1 (classifier) is already committed on this branch; PR-2 (provider marker) is a separate plan.

## Global Constraints

- Conventional commits; NO Claude co-author attribution. No `unwrap()`/`panic!()` in production code (tests may).
- Default behaviour unchanged: knob default `cancel`; no placeholder is written unless the policy is `side_turn`.
- New persisted fields are `#[serde(default)]` (fleet compatibility, same reason as `a2a`/`pending_tool`).
- Side turns: all tools except `flow:` tools (refused with `BEHIND_SUSPENSION_ERROR`), transcript persisted, cap 20 side turns per park, expiry refreshed per side turn but never beyond 24h after `parked_at`.
- Narrow test filters only; gate with `cargo fmt --all --check` and `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`. No push/merge/tag/deploy.
- Before any cargo command: no `web/postcss.config.mjs`, `.vscode/`, `public/fonts/` in the tree (IOC check).

## Review Focus

- Provider pairing never breaks: every LLM request after any sequence (park, N side turns, resume, cancel, expiry, LLM error) has each assistant `tool_calls` id answered by a `Tool`, and no orphan `Tool` (Tasks 2, 3, 4 tests use a mock LLM that REJECTS broken pairing).
- `truncate_history` never splits an assistant/tool group nor drops the group holding the pending call (Task 2).
- Knob `cancel` is byte-for-byte today's behaviour including transcript order (Task 3, existing test `flow_tool_suspend.rs:386-408` stays green).
- Side turn that calls a `flow:` tool does not create a second park (Task 4).
- Cap and expiry: 21st side turn falls back to cancel; side turns cannot keep a card alive past 24h (Task 4).
- Guardrail-denied side message keeps the park (Task 4).
- Old stored state (pending_tool without `presentation`/`parked_at`/placeholder) still resumes and cancels (Task 1, 3).
- Engine renders reply AND card exactly once, and a following submit resumes (Task 5).

## File Structure

- Modify `crates/greentic-aw-runtime/src/config.rs` (knob), `state.rs` (fields, `truncate_history`), `flow_suspend.rs` (3-way take, patch, side helpers), `loop.rs` (wiring), `lib.rs`/`error.rs` (`AgentOutput.side_turn`).
- Modify `crates/greentic-runner-host/src/runner/agent_node.rs` (wire field), `runner/engine.rs` (render array), `http/agent_chat.rs` (reply text on side turn).
- Tests: `crates/greentic-aw-runtime/tests/flow_tool_side_turn.rs` (new, same harness as `tests/flow_tool_suspend.rs`), unit tests beside each change, engine tests in `runner/engine.rs` tests module.
- Docs: `CLAUDE.md` Agentic Workers section (one paragraph + knob).

---

### Task 1: Knob and persisted fields

**Files:** Modify `config.rs:136` (`AgentConfig`), `state.rs:112` (`PendingToolCall`), `lib.rs` (`AgentOutput`); Test: unit tests in the same files.

**Interfaces:**
- Produces: `pub enum ParkedTextPolicy { Cancel, SideTurn }` (`Default = Cancel`, serde `snake_case`, `Copy`); `AgentConfig.on_text_while_parked: ParkedTextPolicy`; `PendingToolCall.presentation: Option<Value>`, `.parked_at: Option<DateTime<Utc>>`, `.side_turns: u32` (all `#[serde(default)]`); `AgentOutput.side_turn: bool` (`#[serde(default, skip_serializing_if = "std::ops::Not::not")]`); consts `MAX_SIDE_TURNS: u32 = 20`, `PENDING_TOOL_MAX_AGE_SECS: i64 = 24*60*60` in `state.rs`.

- [ ] **Step 1: Failing tests** (config.rs tests module, state.rs tests module)

```rust
#[test]
fn on_text_while_parked_defaults_to_cancel_and_parses() {
    let base = r#"{"agent_id":"a","system_prompt":"s","tools":[],"llm":{"provider":"openai","model":"m","credential_ref":null}}"#;
    let cfg: AgentConfig = serde_json::from_str(base).unwrap();
    assert_eq!(cfg.on_text_while_parked, ParkedTextPolicy::Cancel);
    let with = base.replace("\"tools\"", "\"on_text_while_parked\":\"side_turn\",\"tools\"");
    let cfg: AgentConfig = serde_json::from_str(&with).unwrap();
    assert_eq!(cfg.on_text_while_parked, ParkedTextPolicy::SideTurn);
}

#[test]
fn a_pending_tool_stored_before_side_turns_still_deserialises() {
    let old = r#"{"call_id":"c","tool_name":"t","flow_ref":"f","flow_snapshot":{},"iterations_used":1,"expires_at":"2026-10-05T00:00:00Z"}"#;
    let p: PendingToolCall = serde_json::from_str(old).unwrap();
    assert!(p.presentation.is_none() && p.parked_at.is_none() && p.side_turns == 0);
}
```

- [ ] **Step 2: Run, expect compile FAIL** — `cargo test -p greentic-aw-runtime --lib -- on_text_while_parked a_pending_tool_stored_before`
- [ ] **Step 3: Implement.** Add the enum in `config.rs` next to `AgentConfig`, the field `#[serde(default)] pub on_text_while_parked: ParkedTextPolicy`, the three `PendingToolCall` fields, `AgentOutput.side_turn`. Fix every struct literal the compiler flags: `grep -rn "opening_message:" crates` and `grep -rn "PendingToolCall {" crates` and `grep -rn "pending_presentation:" crates`; add `on_text_while_parked: Default::default()`, `presentation: None, parked_at: None, side_turns: 0`, `side_turn: false`. In `loop.rs` park site (`~line 704`) set `presentation: Some(presentation.clone())`, `parked_at: Some(Utc::now())`, `side_turns: 0`; in `flow_suspend::resume_pending` `Waiting` arm set `pending.presentation = Some(presentation.clone())` (keep `parked_at`, reset `side_turns = 0`).
- [ ] **Step 4: Run, expect PASS** (same command) plus `cargo test -p greentic-aw-runtime --test flow_tool_suspend` (unchanged behaviour).
- [ ] **Step 5: Commit** `feat(aw-runtime): on_text_while_parked knob and side-turn fields (inert)`.

### Task 2: Pair-aware `truncate_history`

**Files:** Modify `state.rs:82-101`; Test: `state.rs` tests.

**Interfaces:** Consumes `ConversationState.messages`, `.pending_tool`. Produces: `truncate_history(&mut self, max: usize)` keeps its signature.

- [ ] **Step 1: Failing tests**

```rust
fn asst_calls(ids: &[&str]) -> ChatMessage { /* Assistant with one ToolCallRecord per id */ }
fn tool(id: &str) -> ChatMessage { ChatMessage::Tool { call_id: id.into(), content: serde_json::json!({}) } }

#[test]
fn truncate_never_leaves_an_orphan_tool_or_split_group() {
    let mut s = ConversationState::new(/* same ctor args as neighbouring tests */);
    s.messages = vec![user("a"), asst_calls(&["c1","c2"]), tool("c1"), tool("c2"), user("b"), asst_text("r")];
    s.truncate_history(3);
    assert!(pairing_is_valid(&s.messages));
    assert!(!matches!(s.messages.first(), Some(ChatMessage::Tool { .. })));
}

#[test]
fn truncate_keeps_the_group_holding_the_pending_call() {
    let mut s = /* state with pending_tool.call_id == "c1" */;
    s.messages = vec![user("a"), asst_calls(&["c1"]), tool("c1"), user("q1"), asst_text("r1"), user("q2"), asst_text("r2")];
    s.truncate_history(2);
    assert!(s.messages.iter().any(|m| matches!(m, ChatMessage::Assistant { tool_calls, .. } if tool_calls.iter().any(|c| c.call_id == "c1"))));
    assert!(pairing_is_valid(&s.messages));
}
```
with a test helper `pairing_is_valid(&[ChatMessage]) -> bool` (every assistant `tool_calls` id has exactly one following `Tool`, every `Tool` follows an assistant that called it). Reuse the existing helpers/ctor already in `state.rs` tests (read lines ~300-379 first).

- [ ] **Step 2: Run, expect FAIL** — `cargo test -p greentic-aw-runtime --lib -- truncate_never truncate_keeps`
- [ ] **Step 3: Implement.** Replace the one-message-at-a-time removal with group removal: find the oldest non-system index `i`; its group is `[i, j)` where, if `messages[i]` is `Assistant` with non-empty `tool_calls`, `j` extends over the immediately following `Tool` messages, else `j = i+1`; a leading `Tool` (orphan) is a group of its own. If the group contains the pending call id (`Assistant` carrying it or its `Tool`), skip to the next group. Repeat until non-system count `<= max` or only protected groups remain. Never panic; no `unwrap`.
- [ ] **Step 4: Run, expect PASS**, then `cargo test -p greentic-aw-runtime --lib -- state::` (existing truncate tests stay green).
- [ ] **Step 5: Commit** `fix(aw-runtime): truncate_history removes whole assistant/tool groups`.

### Task 3: Placeholder at suspend, patch on resume/cancel (policy-gated)

**Files:** Modify `flow_suspend.rs` (`take_pending`, `resume_pending`, new `patch_tool_result`), `loop.rs` park site and `take_pending` call; Test: `tests/flow_tool_side_turn.rs` (new) + unit tests in `flow_suspend.rs`.

**Interfaces:**
- Consumes: `ParkedTextPolicy`, `PendingToolCall` new fields (Task 1).
- Produces: `pub(crate) const AWAITING_PLACEHOLDER: &str = "awaiting_user_input"`; `pub(crate) fn patch_tool_result(state: &mut ConversationState, call_id: &str, content: Value)` (replace the `Tool` with that `call_id` in place; if none exists push, i.e. old behaviour); `pub(crate) enum Taken { Nothing, Resume(Resume), Cancelled(AgentStep), Side }` replacing the tuple; `take_pending(state, resume_payload, now, observer, policy) -> Taken`.

- [ ] **Step 1: Failing tests** in `flow_suspend.rs` (unit, no LLM):

```rust
#[test]
fn patch_replaces_the_placeholder_in_place() {
    let mut s = state_with(vec![asst_calls(&["c1"]), placeholder("c1"), user("q"), asst_text("r")]);
    patch_tool_result(&mut s, "c1", json!({"ok": true}));
    assert!(matches!(&s.messages[1], ChatMessage::Tool { content, .. } if content == &json!({"ok": true})));
    assert_eq!(s.messages.len(), 4);
}
#[test]
fn patch_pushes_when_no_placeholder_exists_old_state() {
    let mut s = state_with(vec![asst_calls(&["c1"])]);
    patch_tool_result(&mut s, "c1", json!({"status":"cancelled"}));
    assert_eq!(s.messages.len(), 2);
}
```
and in `tests/flow_tool_side_turn.rs`: (a) policy `side_turn`: after the park step the persisted transcript ends `[.., Assistant{c1}, Tool{c1, awaiting_user_input}]`; (b) policy `cancel` (default): transcript identical to today (no placeholder), `flow_tool_suspend.rs` stays green; (c) policy `side_turn`, then a card submit immediately: tool result replaces the placeholder (no duplicate id), final transcript pairing-valid. The mock LLM here is `RecordingLlm`-style but its `complete` returns `Err(LlmError::...)` (or panics inside the mock, asserted by the test) when the incoming history violates pairing — implement `fn pairing_is_valid(&[ChatMessage]) -> bool` once in the test file and call it in the mock.

- [ ] **Step 2: Run, expect FAIL** — `cargo test -p greentic-aw-runtime --lib --features test-mock -- patch_` and `--test flow_tool_side_turn`
- [ ] **Step 3: Implement.** (1) `patch_tool_result` as specified. (2) In `take_pending`: cancel and resume paths call `patch_tool_result` instead of `state.messages.push(Tool..)`; for `Resume`, `resume_pending` `Completed` arm also uses `patch_tool_result`. Return `Taken::Side` ONLY in Task 4 (for now the policy branch is unreachable: `Taken::Side` is constructed nowhere, keep `#[allow(dead_code)]` off by exercising it in Task 4). Update the `loop.rs:138` call site to the new enum. (3) At the park site in `loop.rs` (after setting `state.pending_tool`), when `config.on_text_while_parked == SideTurn` push `ChatMessage::Tool { call_id, content: json!({"status": AWAITING_PLACEHOLDER}) }`. Keep the suspension flow otherwise unchanged (the loop `continue`s/ends as today; later calls in the batch still get the `BEHIND_SUSPENSION_ERROR` Tool after the placeholder, which is valid).
- [ ] **Step 4: Run, expect PASS**, plus `cargo test -p greentic-aw-runtime --test flow_tool_suspend`.
- [ ] **Step 5: Commit** `feat(aw-runtime): placeholder tool result at park, patched on resume or cancel`.

### Task 4: The side turn

**Files:** Modify `flow_suspend.rs` (`take_pending` Side branch, `begin_side_turn`), `loop.rs` (after `take_pending`, tool dispatch, end of step), `error.rs`/`lib.rs` (`AgentOutput.side_turn` set); Test: `tests/flow_tool_side_turn.rs`.

**Interfaces:**
- Consumes: Task 1-3 items.
- Produces: `fn begin_side_turn(pending: &mut PendingToolCall, now: DateTime<Utc>)` — `side_turns += 1`; `expires_at = min(PendingToolCall::expiry_from(now), parked_at.unwrap_or(now) + 24h)`; `take_pending` returns `Taken::Side` iff `policy == SideTurn && resume_payload.is_none() && !expired && pending.side_turns < MAX_SIDE_TURNS`, and in that case the pending call stays in `state.pending_tool`; otherwise falls to the cancel behaviour (expired / cap / policy cancel / payload present = resume).

- [ ] **Step 1: Failing tests** (`tests/flow_tool_side_turn.rs`, policy `side_turn`, `ScriptedFlows`, pairing-checking mock LLM):
  1. `a_typed_message_is_answered_and_the_card_stays_parked`: park → typed text → LLM called once → `out.reply` is the LLM text, `out.terminated_by == AwaitingToolInput`, `out.side_turn`, `out.pending_presentation == Some(card)`, `state.pending_tool.is_some()`, `ScriptedFlows.resumed` empty, no `cancelled` Tool in transcript.
  2. `two_side_turns_then_a_submit_resumes`: park → text → text → submit; `ScriptedFlows.resumed` has exactly one call with the original snapshot; final transcript pairing-valid and the placeholder was replaced by the flow output.
  3. `a_side_turn_cannot_start_another_flow`: scripted LLM answers the side message with a `flow:form` tool call → the Tool result is `{"error": BEHIND_SUSPENSION_ERROR}`, `ScriptedFlows.invoked` stays at 1 invocation, still exactly one `pending_tool`.
  4. `the_21st_side_message_cancels`: after `MAX_SIDE_TURNS` side turns the next typed message cancels (Tool patched to `cancelled`, `pending_tool` None, `!out.side_turn`).
  5. `side_turns_do_not_extend_past_24h`: with a controllable `parked_at` (set 23h59m ago in the stored state) the refreshed `expires_at <= parked_at + 24h`; a message after 24h cancels as expired.
  6. `an_expired_pending_cancels_even_with_side_turn_policy` (idle 1h).
  7. `a_guardrail_denied_side_message_keeps_the_park` (mock guardrail denies; `pending_tool` still stored, `side_turns` unchanged).
  8. `an_llm_error_during_a_side_turn_keeps_a_valid_transcript` (LLM returns Err once; stored state pairing-valid; next typed message works).
  9. `an_old_state_without_a_placeholder_still_takes_a_side_turn`: pending_tool stored with dangling assistant `tool_calls` and no placeholder; side turn must insert the placeholder first (call `ensure_placeholder(state)` in `begin_side_turn`) so the request is pairing-valid.

- [ ] **Step 2: Run, expect FAIL** — `cargo test -p greentic-aw-runtime --features test-mock --test flow_tool_side_turn`
- [ ] **Step 3: Implement.** In `take_pending`, before cancelling: if the `Side` conditions hold, `begin_side_turn`, `ensure_placeholder` (push `Tool{call_id, awaiting}` right after the assistant message holding that id when absent), return `Taken::Side` WITHOUT `take()`-ing the pending call (restructure: peek with `state.pending_tool.as_mut()`, only `.take()` on resume/cancel). In `loop.rs`: for `Taken::Side` the inbound guardrail and `User` push run as a normal turn (the early `return Err` for a denied guardrail happens before any state save, so the park survives); set a local `side_pending = true`. Add the park-aware system hint (one sentence appended to the system prompt for this turn only: "A form step is waiting for the user; do not start that step again. Answer the question, then mention the form can be continued."). In the tool dispatch branch where `call.extension_id.strip_prefix("flow:")` is matched, if `side_pending` call `flow_suspend::refuse_behind_suspension(...)` and `continue`. At end of step (both exits at `loop.rs:~852` and `~908`), when `side_pending` and no LLM error: set `terminated_by = TerminationReason::AwaitingToolInput`, `suspension = state.pending_tool.as_ref().and_then(|p| p.presentation.clone())`, `out.side_turn = true`, then `truncate_history` and save as today. If `presentation` is `None` (very old state) end as a normal `FinalReply` and leave the park (no card to re-offer): ledger it.
- [ ] **Step 4: Run, expect PASS**; then `cargo test -p greentic-aw-runtime --features test-mock --test flow_tool_suspend --test flow_tool_side_turn`.
- [ ] **Step 5: Commit** `feat(aw-runtime): answer a typed message as a side turn while a flow tool is parked`.

### Task 5: Host: render reply and card, re-mark the await, sidecar

**Files:** Modify `runner/agent_node.rs` (~437: copy `side_turn` into node output), `runner/engine.rs:1489` (render), `http/agent_chat.rs` (`trace_to_response`, ~56-123); Test: `runner/engine.rs` tests module, `http/agent_chat.rs` tests.

**Interfaces:**
- Consumes: `AgentOutput.side_turn`, node output `{reply, trail, terminated_by, pending_presentation, side_turn}`.
- Produces: `fn rendered_park(payload: &Value) -> Value` in `engine.rs`: if `awaiting_tool_input(payload)` and `payload["side_turn"] == true` and a non-empty `reply` → `Value::Array(vec![payload_without_pending_presentation_and_side_turn, card])`; else today's `tool_presentation(payload).cloned().unwrap_or(payload.clone())`.

- [ ] **Step 1: Failing tests:** (a) `rendered_park` for a plain park returns only the card (unchanged); for a side-turn park returns a 2-element array whose first element has `reply` and second equals the card; (b) engine-level (reuse the harness of `a_typed_message_during_a_tool_park_is_not_a_resume_payload`, `engine.rs:~12988`): a side-turn output marks `pending_tool_await` again (`state.take_tool_await(node)` true on the next inbound) and a following card submit yields a `resume_payload`; (c) `agent_chat`: for a side-turn output `replies` contains the reply text AND `pending_card` is present.
- [ ] **Step 2: Run, expect FAIL** — `cargo test -p greentic-runner-host --lib -- rendered_park side_turn`
- [ ] **Step 3: Implement** as specified; `finalize_with` already passes `Value::Array` through (`engine.rs:3858-3879`), so nothing changes there. In `engine.rs` replace the three-line `rendered` computation at ~1489 with `rendered_park(&output.payload)`. Knob plumbing: confirm `AgentConfig.on_text_while_parked` reaches the runtime from the pack manifest and `<agent_id>.json` (it is part of `AgentConfig`, so serde does it; add a manifest-parse test in `agent_node.rs`).
- [ ] **Step 4: Run, expect PASS** plus `cargo test -p greentic-runner-host --lib -- runner::engine::tests http::agent_chat`.
- [ ] **Step 5: Commit** `feat(runner-host): render reply and card on a side turn and keep the await armed`.

### Task 6: Docs and gate

**Files:** Modify `CLAUDE.md` (Agentic Workers section: knob, default, semantics, failure modes in 3 short bullets), spec section 8 stays.

- [ ] **Step 1:** Add the paragraph. **Step 2:** `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo test -p greentic-aw-runtime --features test-mock --lib --test flow_tool_suspend --test flow_tool_side_turn`; `cargo test -p greentic-runner-host --lib -- runner::engine::tests http::agent_chat`. Expected: all pass/clean. **Step 3: Commit** `docs: on_text_while_parked`.

## Self-review

- Spec coverage: D2 (Task 4 policy branch), D3 (Tasks 2,3), D4 (Task 4 refusal, hint, cap), D5 (Tasks 4,5), D6 (Task 4), failure modes second side turn / expiry / LLM error / guardrail / old state (Task 4 tests); concurrency and restart are documented limitations (no code), store-disagreement handled by existing cancel paths. Open: manual repro against Cloud Run / Test chat after release.
- Rulings made while planning: array render instead of injecting text into the card body (engine already supports arrays); placeholder only when policy is `side_turn` to keep default byte-for-byte; `presentation`/`parked_at`/`side_turns` stored on `PendingToolCall` because the card is otherwise unrecoverable at re-offer time.
