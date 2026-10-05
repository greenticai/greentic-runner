# Parked flow tool: answer a free-text side question without losing the parked flow

Status: DRAFT for review (no code). Date: 2026-10-05.

Evidence base: `origin/develop` @ `050214fa` (read via `git archive`; this worktree's own
base `89860aba` is 856 commits stale and was NOT used), plus `greentic-start` develop
`cacf6bd` and `greentic-messaging-providers` develop `e4f7b389`. Nothing here was executed.
Everything not read directly from code is marked **UNVERIFIED**. Line numbers are
`develop@050214fa`; paths are relative to `crates/` unless noted.

## 1. Problem

A deployed worker (`outlook-helpdesk-assistant`, one `flow:` tool parked on a 26-card
deterministic flow) cannot answer a free-text side question while the card is parked:

- Deployed webchat: the next typed message "continues inside the flow" and never reaches RAG.
- Designer Test chat (runner sidecar): the same message is answered from knowledge (RAG).

Wanted behaviour:

1. Card submit (action/answers present) while parked: resume the flow, as today.
2. Free text while parked: normal agent turn (RAG, tools, memory); parked snapshot NOT discarded.
3. The flow stays resumable afterwards (card button or explicit affordance) with last state
   preserved (answers so far, current node).

## 2. Current behaviour (two separate problems)

### R1. Classification bug: typed webchat text can look like a card submit

- Gate: `greentic-runner-host/src/runner/engine.rs:1804-1812`:
  `resume_payload = (state.take_tool_await(node) && is_card_submit(&state.entry)).then(..)`.
- `is_card_submit` (`engine.rs:4393`) is true if `metadata.action` is non-blank, OR any
  envelope-root key outside `ENVELOPE_TRANSPORT_KEYS` (`engine.rs:4351`), OR any
  `metadata` key outside `CHANNEL_CONTEXT_METADATA_KEYS` (`engine.rs:4368` =
  `locale, team, env, autoStart` only).
- The webchat provider stamps on EVERY message, typed or clicked: `universal`, `tenant`,
  `route`, `tenant_channel_id` (`messaging-provider-webchat/src/ops/envelope.rs:28-39`),
  `user_id`, `user_verified` (`ops/ingest.rs:338,348`), `flow_hint` when the header is set
  (`ingest.rs:360`), and `extensions` / `channel.*` when non-empty (`envelope.rs:43,50`).
  None are in the runner allow-list.
- Therefore `metadata_input` is true for plain typed text, `is_card_submit` is true, and the
  text is delivered to the tool flow as its answer; the card re-parks. This matches the
  deployed symptom even on a build that contains #822. #822's tests use `metadata: {}`
  or only allow-listed keys (`engine.rs:~13020-13060`), so they do not model real provider output.
- UNVERIFIED at runtime (static read only). UNVERIFIED which runner build is deployed and
  whether the deployed start uses the legacy `runner_exec` path or the revision path.
  Pre-#822 builds are worse (any envelope counts as a submit).
- Provider drops activity `type` and strips `channelData.postBack` (`ingest.rs:534-548`); no
  `card_action` / `resume_payload` / `answers` / submit marker exists anywhere in
  start, provider or webchat SPA. A click with `data:{}` becomes text `"message"`
  (`ingest.rs:395-408`) and is indistinguishable from typed `message`.

### R2. Typed text is destructive by design (#811)

- Every step begins with `flow_suspend::take_pending` (`greentic-aw-runtime/src/flow_suspend.rs:46-76`,
  called at `loop.rs:138-143`). It always does `state.pending_tool.take()`. No payload, or
  expired (idle TTL 1h, `state.rs:107`, checked first): it appends
  `Tool{call_id, {"status":"cancelled", reason}}` and the turn continues as a normal agent
  turn. The parked tool is gone. (Order asserted by `tests/flow_tool_suspend.rs:386-408`.)
- `take_tool_await` also clears the engine marker on any inbound (`engine.rs:1809`).
- So even with R1 fixed, "free text -> agent answers" works but the park is lost. The #811
  commit body says this is intended ("A typed message instead reaches the agent as a message
  and cancels the tool").
- Guardrail-denied message returns before the save (`loop.rs:209-221`), so the park survives
  that one case (UNVERIFIED by test).

### Where state lives (two stores that can disagree)

| Store | What | Key | TTL | Backend |
|---|---|---|---|---|
| Wait store (outer `FlowSnapshot`, `next_node` = dw.agent node, `LoopHere`) | `engine/runtime.rs:176,178,1049,1205,1287-1292` | `{hint}::{scope_hash}`, `hint` = `tenant:provider:channel:conv:user` + `::pack=` | none in-memory; 24h on Redis (`GREENTIC_RUNNER_SESSION_WAIT_TTL_SECS`) | in-memory default; `GREENTIC_RUNNER_SESSION_BACKEND` |
| Agent state (`ConversationState.pending_tool.flow_snapshot`, inner tool-flow snapshot) | `aw/state.rs:54,112-133` | `aw:{tenant}:{env}:{session}:state` | idle 1h for `pending_tool`; 7d key | `GREENTIC_AW_STATE_BACKEND` / `GREENTIC_AW_REDIS_URL` (independent of #792) |

- Wait store is single-slot, overwrite on save, no lock/CAS (`runtime.rs:178`); there is no
  `register_wait_if_absent` (only "Phase D" comments). #826 is the WASM state-store, unrelated.
- Any inbound with the same key resumes the wait; there is no "new turn" while a wait exists
  (`runtime.rs:1341-1343`: `entry_node` loses to the snapshot).
- The inner snapshot never touches the wait store.

### Prior art

PARTIAL / none for this behaviour. Searches for "side question", "digression" are empty. Closest:
#811 (park + cancel-on-text, the opposite), #813 (`resume_payload`, `pending_card` on the
sidecar), #822 (typed text is not a submit), #823 (`on_message` route, flow-level only).
Issues #793-#796 are about approval parks. No prior request for this feature found in the
runner repo (designer repo checked by the requester: none).

### Why the Designer Test chat "works"

The SPA sends text as a new turn and holds the card client-side. Server side the same
`take_pending` cancels `pending_tool`, so a later button submit probably finds no
`pending_tool` (`take_pending` returns `None`, payload ignored). UNVERIFIED; needs a repro.
If true, Test chat is not actually resumable after a side question either.

## 3. Options

**(a) New turn, keep snapshot (recommended base).** On a non-submit message with a live
`pending_tool`, run an ordinary agent turn but do not cancel the park. Needs a third
`take_pending` outcome (`SideTurn`) and transcript handling (section 4).
+ Real RAG/tools/memory answer, parked state preserved, works for stateless clients.
- Touches aw-runtime transcript invariants; new failure modes (section 6).

**(b) Queue text as side turn, then re-offer the card.** Resume the card first (or hold the
text), answer after the flow finishes or on request.
+ No transcript branching.
- User waits through up to 26 cards for an answer; defeats the purpose. REJECTED as the
  primary mechanism. The "re-offer the card after the answer" part of (b) IS adopted inside (a).

**(c) Authoring-only `on_message` exit.** Add `{to, on_message: true}` routes (#823) to every
card of the tool flow.
+ No runner change.
- Works only for cards of an outer flow node; for a `flow:` tool parked by the agent the typed
  text never reaches that routing (it is cancelled at agent level, or R1 swallows it). Needs
  edits on every card (partner's flow has none), exits the flow, does not give a RAG answer
  unless the author wires an agent node, cannot preserve state generally. Complementary at best.

**Recommendation: (a) with card re-offer, behind a per-agent opt-in, and R1 fixed first as an
independent PR.**

## 4. Proposed design

### D1. Explicit submit discrimination (fixes R1, prerequisite for everything)

Order of evidence in `is_card_submit`:
1. Explicit marker, if present (new, emitted by the provider ONLY on submits, so older
   runners that ignore it are unaffected; typed text gets no new key).
2. Else `metadata.action`.
3. Else existing heuristic with an allow-list widened to the provider-stamped keys
   (`universal, tenant, route, tenant_channel_id, user_id, user_verified, flow_hint,
   extensions, channel.*, tenant_id, conversation_id`).

Known limits: a submit with `data:{}` and no marker stays ambiguous; card inputs named like
stamped keys collide. Hence the marker. Runner-only mitigation (step 3) can ship alone.

### D2. `take_pending` third outcome

`SideTurn` when: pending live, not expired, no submit payload, and agent config
`on_text_while_parked = "side_turn"` (default `"cancel"` = today). `flow_suspend.rs:46-76`
and `loop.rs:138-143` change; the cancel path is unchanged when the knob is off.

### D3. Transcript shape (keeps provider pairing valid)

At suspend (`loop.rs:700-718`) append a placeholder right after the assistant tool_calls turn:
`Tool{c1, {"status":"awaiting_user_input"}}`. Side turns append `User, Assistant` after it.
On resume (and on the existing cancel) REPLACE the placeholder by `call_id` instead of
pushing a new Tool (pushing would duplicate the id and OpenAI would reject it). Persisting the
side Q/A (not a throwaway branch) is what gives later turns conversation memory.
Required: make `truncate_history` (`state.rs:82-101`) pair-aware; today it drops single oldest
messages and can orphan a Tool or an assistant tool_calls turn (provider 400, and the
saved bad state then fails every later turn until the 7d TTL).

### D4. Side-turn rules

- Retrieval (RAG) runs as today. Tool access: all tools except flow tools. A flow tool call
  during a side turn is refused with the existing "another step is waiting for the user"
  result (`flow_suspend.rs:30-31,163-183`); single park slot is preserved.
- Park-aware system hint: a step is pending; do not call it again; answer, then say the
  form can be continued.
- Cap side turns per park (proposal: 20) to bound transcript growth.

### D5. Response and resumability

Side turn ends with `terminated_by = awaiting_tool_input`, a normal `reply`, and the SAME
`pending_presentation` re-emitted, so the existing engine path re-marks the await
(`engine.rs:1827`) and re-saves the `LoopHere` snapshot, and clients (webchat, sidecar
`pending_card` from #813) re-offer the card. The inner snapshot is untouched, so answers
entered so far and the current node are preserved; resume works through the normal
card submit. **Open (UNVERIFIED):** the engine renders `tool_presentation(&payload)` instead of
the payload (`engine.rs:4337`), which would drop the reply; the output must carry both the
text reply and the card (two replies).

### D6. Expiry

Side turns refresh `pending_tool.expires_at` (`expiry_from`), capped by an absolute max aligned
with the wait-store TTL (24h on Redis). Without the cap a chatty side conversation keeps a
dead card alive forever; without refresh a 1h side chat cancels the card silently.

### State machine

```
            park (tool returns Suspend)
 IDLE ─────────────────────────────────────▶ PARKED
                                              │ ▲
   card submit (D1 true) ──▶ RESUME ──────────┘ │ tool parks again (new card, expiry reset)
                              │ completes       │
                              ▼                 │ text, knob=side_turn
                            IDLE            SIDE_TURN ── answer + re-offer card ─▶ PARKED
   text, knob=cancel (today) ──▶ CANCELLED ─▶ IDLE (Tool{cancelled}, text handled as normal turn)
   expiry passes (checked on next inbound) ──▶ CANCELLED(expired), submit payload ignored
```

## 5. Compatibility and rollout

- Knob `on_text_while_parked: cancel | side_turn`, default `cancel`; per `AgentConfig`
  (`config.rs`), overridable by manifest. Zero behaviour change for existing workers.
- Provider marker is additive and submit-only; old runner ignores it. Old provider + new
  runner falls back to heuristic (D1 step 3).
- Older greentic-start pins (`greentic-runner-host >=1.2.0-dev.37225724381`): whether that
  includes #822 is UNVERIFIED. Old designer pins: sidecar contract (#813) unchanged; a side turn
  returns `pending_card` again, same shape.
- Two start paths have different snapshot keys and stores (legacy `runner_exec` file
  `{pack}:{flow}:{conv}` vs revision path host `SessionStore`, `greentic-start/src/runner_exec.rs`,
  `revision_serve.rs`). The feature targets the revision path; legacy is UNVERIFIED/out of scope.
- Suggested PR split: PR-1 R1 fix (runner allow-list, tests with real provider fixtures; small,
  ships first). PR-2 provider marker (other repo). PR-3 side turn behind the knob.

## 6. Failure modes

| Case | Expected | Note |
|---|---|---|
| Second / many side turns | append more Q/A after placeholder; cap 20 | bounded by `max_history_turns` (default 20, message-counted) |
| Inner expired, outer alive | submit is cancelled as "expired", payload ignored (today) | re-render a fresh card or tell the user; decide |
| Outer gone, inner alive (restart, in-memory session + Redis aw) | fresh execute; `take_pending` cancels stale tool | old card orphaned on screen |
| Outer alive, inner absent (Redis session + memory aw) | `take_pending` returns None, payload ignored | UNVERIFIED exact behaviour |
| Runner restart, both in-memory | everything lost (flow restarts at entry) | document; durable backends needed for this feature |
| Two tabs, same conversation | aw lock (5s then `LockTimeout`); wait store last-writer-wins, no CAS | side turn re-saves identical LoopHere; submit racing side turn is UNVERIFIED |
| LLM error mid side turn | state saved with placeholder, pairing still valid | `loop.rs:463-467` |
| Guardrail denies side message | park survives (returns before save) | UNVERIFIED by test |
| Side turn LLM calls a flow tool | refused, no second park | D4 |
| Submit for a different/old card after a side turn | matched by `call_id` / snapshot; stale -> ignored | decide UX |

## 7. Test plan (narrow filters only; never the full suite locally)

- `is_card_submit` unit tests using REAL provider output fixtures: typed text with all stamped keys
  -> false; submit with `action` -> true; submit with fields -> true; `data:{}` with marker -> true;
  without marker -> documented ambiguous case.
- aw-runtime (style of `tests/flow_tool_suspend.rs`), mock LLM that REJECTS broken tool pairing:
  side turn keeps `pending_tool`; transcript valid at every step; resume after 1 and after N side
  turns completes the tool and replaces the placeholder; flow-tool call in a side turn refused;
  expiry (inner) with and without refresh/cap; knob off == today's cancel test unchanged;
  `truncate_history` never orphans a Tool/assistant pair (property test).
- Engine test: side turn re-marks `pending_tool_await`, re-saves `LoopHere`, reply and
  presentation both emitted.
- Store-disagreement tests (D-cases in section 6) at the host layer.
- Provider ingest test for the marker (other repo). Designer/Test chat: manual repro that a
  button submit after a side question completes the tool.
- Gate: `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  (CI does not run tests on PRs); run targeted `cargo test -p <crate> <filter>` locally.

## 8. Decisions (2026-10-05, from review)

1. Deployed greentic-start uses the **revision** path (legacy `runner_exec` out of scope).
   Deployed runner build/tag is still UNKNOWN: R1 as the live cause stays unconfirmed until a repro.
2. PR split approved: PR-1 R1 fix first (runner allow-list widen) AND PR-2 provider submit marker.
3. Side turns may use all tools except flow tools (D4 unchanged).
4. Side Q/A is persisted in the transcript (D3).
5. Expiry: refresh on each side turn with an absolute cap of 24h (D6).
6. One response carries both text reply and the re-offered card; engine output contract must change
   (`engine.rs:4337`) and sidecar/webchat renderers must accept two replies.
7. Knob `on_text_while_parked` defaults to `cancel`.

## 9. Still open

- Deployed runner tag (needed to confirm R1 vs an older bug).
- Reproduce R1 and the Test-chat "submit after side question" case before PR-1 is merged.
- Next step: implementation plan (writing-plans) for PR-1, rebased on `origin/develop` (this worktree base is stale).
