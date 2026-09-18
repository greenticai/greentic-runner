# Approval outcome routing — make `approved` / `denied` / `timeout` routable

**Date:** 2026-07-15
**Status:** Design — approved, pending plan
**Repo:** `greentic-runner` (runner-only; the designer follow-up is out of scope here)
**Area:** `crates/greentic-runner-host/src/runner/engine.rs`

> **Line references.** The inline `engine.rs:NNN` refs in the prose below were first
> taken against a *different* branch tip (the main checkout, mid-WIP) and are offset
> by roughly 60-80 lines. The anchors verified against this branch's base
> (`research` @ `b4c86e29`) are the authoritative ones:
>
> | Item | `engine.rs` |
> |---|---|
> | `execute_approval_call` | **1416** |
> | `NodeKind::ApprovalCall` dispatch arm | **1246** |
> | `execute_remote_dispatch` (`resume_at_self` param) | **1541** |
> | `DispatchOutcome::await_here` | **2583** |
> | `NodeOutput::with_meta` | **2661** |
> | `outcome_meta` | **2759** |
> | `ExecutionState.pending_agent_await` | **2417** |
> | `mark_agent_await` / `take_agent_await` | **2517** / **2526** |
> | `approval_requires_human` | **3948** |
> | `mod approval_gate_tests` | **3979** |
>
> The implementation plan carries only re-verified refs. Grep the symbol, not the
> line, if they drift again.

## Problem

An `approval.call` gate cannot branch on its own decision. Today an operator can
express only "pause for a human, then continue to one successor" — approve and
deny take the same edge, which defeats the entire point of an approval gate.

Verified in-tree:

- **Routing on a wait node is evaluated at dispatch time**, before the human
  decides (`engine.rs:797-829`). `output.payload` is only
  `{pending: true, correlation_id}`. It must resolve to exactly **one**
  successor, frozen into `snapshot.next_node`; the node is never re-entered
  (resume starts at that successor, `engine.rs:602`). A condition that does not
  match collapses to `None` and hard-errors:
  `bail!("session.wait node … requires a non-empty route")` (`engine.rs:814-819`).
- **`event` cannot discriminate.** greentic-admin publishes both decisions as
  `{ok: true, output: {decision: "approved"|"denied", …}, events: [], error: null}`
  (`greentic-designer-admin/src/approval_bridge.rs:123-128`). `event` falls back
  to `NodeOutput.ok` (`engine.rs:3841-3854`), so it is `on_success` for approve
  **and** deny — and for the auto-approve path, and at the successor after a
  timeout.
- **The two paths expose the decision differently.** The human path replaces
  `state.entry` (`engine.rs:600-601`) so the decision reads at `in.output.decision`
  **at the successor**; the auto-approve path (`engine.rs:1364-1374`) returns
  `DispatchOutcome::complete` and never replaces `entry`, so at the successor that
  path is unresolved — its decision sits at bare `output.decision` **on the
  approval node itself**. One routing encoding cannot cover both.
- Consequently the designer's palette ports (`approved`/`denied`/`timeout`) are a
  fiction: nothing can route on them.

There is **no** 3-way branch off any dispatch node anywhere in this repo, and
**no test at all** covers either branch of `execute_approval_call` (only the pure
`approval_requires_human` predicate is tested, `engine.rs:3897-3924`).

## Why this is cheap: both mechanisms already exist

This needs no new engine concepts and **no snapshot migration**.

1. **Re-entering a node on resume is already shipped.** `execute_remote_dispatch`
   takes `resume_at_self` (`engine.rs:1489`, documented `:1473-1480`) and branches
   at `:1568-1574`: `false` → `DispatchOutcome::wait` (resume at successor — what
   `execute_approval_call` passes today, `:1376`); `true` →
   `DispatchOutcome::await_here` (**re-enter this node**), used by the
   conversational out-of-process `dw.agent`. Three of the four `next_node` writers
   already park at SELF (`engine.rs:778`, `:850`, `:887`).
2. **`meta["outcome"]` is the canonical, live way to set `event`.**
   `outcome_meta` (`engine.rs:2677-2682`) + `NodeOutput::with_meta`
   (`engine.rs:2579-2585`), consumed at `engine.rs:3833-3856` where an explicit
   outcome **wins unconditionally** over the `ok`-derived fallback. Pinned by
   `outcome_meta_surfaces_component_emitted_outcome` (`engine.rs:5569`) and by the
   routing-context test at `engine.rs:5364-5365`, which proves `ok: true` +
   `meta.outcome = "on_error"` yields `event == "on_error"` — i.e. `meta` can
   contradict `ok`.
3. **`event` is a free string in the runner** — no allowlist, no validation
   (`condition_event_eq`, `engine.rs:3779-3785`; `build_routing_context`, `:3839-3843`).
4. **No migration.** `AwaitHere` already writes `next_node = node_id` (SELF), so
   `FlowSnapshot` is unchanged. (It is persisted as JSON with no version field and
   a hard decode error, `engine/runtime.rs:71-77` — so *not* changing it matters.)

## Decisions

1. **Model A — re-enter the approval node on resume** and set `meta["outcome"]`
   from the decision. (Rejected: normalising the auto path so a *successor* could
   branch. Dispatch fns receive no `&mut ExecutionState` and `DispatchOutcome`
   carries no state channel, so it is not reachable without a signature change —
   and replacing `state.entry` from a completion would defeat the conversational
   `dw.agent`'s `state.entry.get("ok").is_some()` discriminator (`engine.rs:1035`),
   drop card locale (`inject_card_locale`, `:937`), and empty `response.*` —
   exactly the "every condition fail, looping the user back to the wait point
   forever" failure the `resume` comment warns about (`engine.rs:592-595`).)
2. **Custom event names: `approved` / `denied` / `timeout`.** They match the
   designer's palette ports exactly, so no translation layer is ever needed. The
   runner accepts them (free string).
3. **Fail closed.** An unrecognised or missing decision resolves to `denied`,
   mirroring `approval_requires_human`'s own fail-safe (`_ => true` — unknown mode
   still requires a human, `engine.rs:3891`). A corrupt payload must never become
   a pass.

## Design

### Behaviour of `execute_approval_call`

| Situation | Outcome |
|---|---|
| First entry, `!approval_requires_human(input)` | `DispatchOutcome::complete` with `meta {"outcome": "approved"}` |
| First entry, human required | mark `pending_approval_await(node_id)`; dispatch with `resume_at_self = true` → `DispatchOutcome::await_here` → park at SELF |
| Re-entry, marker set, entry is a response (`entry.ok` present), `error.code == "timeout"` | complete, `meta {"outcome": "timeout"}` |
| Re-entry, marker set, entry is a response, `output.decision == "approved"` | complete, `meta {"outcome": "approved"}` |
| Re-entry, marker set, entry is a response, `output.decision == "denied"` | complete, `meta {"outcome": "denied"}` |
| Re-entry, marker set, entry is a response, decision unrecognised/absent | complete, `meta {"outcome": "denied"}` (**fail closed**) |
| Re-entry, marker set, entry is **not** a response (stray inbound) | re-park (`await_here`) **without re-dispatching**; marker stays set |

The stray-inbound row is load-bearing, not defensive padding: `AwaitHere` parks in
the same single `(session_hint, scope_hash)` slot as every other wait kind
(`build_store_ctx` strips correlation from the key — `engine.rs:866-880`), so an
inbound arriving mid-await *does* re-enter this node. Without the marker it would
look like a first entry and **re-dispatch — a duplicate approval request to the
operator**.

Result: `event` is `approved` / `denied` / `timeout` on **every** path — human,
auto-approve, and timeout — so `condition: event == "approved"` works uniformly
and the palette's three ports become real.

### The marker

Mirror the `dw.agent` precedent exactly: a per-node set on `ExecutionState`.

```rust
#[serde(default)]
pending_approval_await: HashMap<String, ()>,
```

`ExecutionState`'s fields are all `#[serde(default)]` and old snapshots are covered
by round-trip tests (`execution_state_vars_default_empty_for_old_snapshots`,
`engine.rs:5901`; `pending_agent_await_survives_snapshot_roundtrip`, `:5254`), so
this is additive and safe for in-flight parked sessions.

**No correlation id is stored.** Verified: `NodeControl::AwaitHere { reason,
correlation_id: _ }` (`engine.rs:861-863`) **discards** it — the comment states it
"is NOT part of the key — it only drives how the NATS response reconstructs the
hint/scope". So a re-park needs nothing from the original dispatch, and the marker
can be a set (`HashMap<String, ()>`) exactly like `pending_agent_await`
(`engine.rs:2350`).

### State access

`execute_approval_call` currently takes `(ctx, target, payload)` and has no
`state`; `FlowContext` does not carry it (`engine.rs:7641-7655`). Re-entry needs to
**read** `state.entry` and **take/set** the marker. Two options, both local — the
plan picks whichever the borrow checker actually allows, since the dispatch match
already holds `state`:

- add a `&mut ExecutionState` parameter to `execute_approval_call` (preferred —
  keeps `drive_flow` tidy), or
- inline the arm in `drive_flow`'s dispatch match, as the conversational `dw.agent`
  does precisely because it needs `state` in scope (`engine.rs:1035`, `:1044-1045`).

### No park-loop cap

Unlike the conversational `dw.agent` (which loops and needs `MAX_PARK_TURNS`,
`engine.rs:2322`), approval re-enters **once**: dispatch → park → resume →
complete. A stray inbound re-parks without advancing, which cannot loop
unboundedly on its own — each re-park requires a fresh inbound. No cap is added.

## Testing

The area has **zero** coverage today, so these are written from scratch:

- Auto-approve (`mode: above_risk`, risk below threshold) → completes with
  `meta.outcome == "approved"`.
- Human approve → re-entry with `{ok:true, output:{decision:"approved"}}` →
  `meta.outcome == "approved"`.
- Human deny → `meta.outcome == "denied"`.
- Timeout (`{ok:false, error:{code:"timeout"}}`) → `meta.outcome == "timeout"`.
- Unrecognised/absent decision → `meta.outcome == "denied"` (fail closed).
- Stray inbound while parked → re-parks, marker still set, **no second dispatch**.
- Marker round-trips through a snapshot (mirroring
  `pending_agent_await_survives_snapshot_roundtrip`).
- Routing integration: a node with three `event == "…"` routes lands on the right
  successor for each outcome.

Gate: `ci/local_check.sh` (fmt → wit_sync → clippy → crate_tests → workspace_tests
→ package). `--all-features` matters — the conversational `dw.agent` paths are
behind `#[cfg(feature = "agentic-worker")]`.

## Consequences to carry forward (not defects, but do not discover them later)

- **The designer's catalog must gain the three events.** `catalog.baseline.yaml`
  declares a fixed, additive-only event vocabulary (`:33-42`) that has no
  `approved`/`denied`/`timeout`; its own comment calls adding one "a separate
  review event". That is designer work, out of scope here.
- **`default_event` / `node_has_error_route` only understand the six `on_*` names**
  (`engine.rs:3707`, `:3713`, `:3721-3739`, `:3752-3775`). A route
  `event == "denied"` will not register as an error route and will not participate
  in port-priority defaulting. Harmless for approval (we always set an explicit
  outcome, which wins), but it means a partially-wired approval node falls back to
  the `ok`-derived default rather than to a `denied` branch.
- **The `AwaitHere` single-slot interleaving limitation is inherited, not fixed**
  (`engine.rs:866-880`, tracked in-tree as a follow-up). The stray-inbound re-park
  is what keeps it safe here.
- **⚠️ The `timeout` outcome can never fire without `deadline_ms`.** The watchdog is
  spawned only for `(DispatchMode::Await, Some(deadline_ms))`
  (`runner/remote_dispatch.rs:145-185`), and `deadline_ms` is read from the node input
  (`engine.rs`, in `execute_remote_dispatch`). An approval node authored without it can
  emit only `approved` / `denied` — so the designer's third palette port would be **dead
  wiring**. The designer follow-up must either require `deadline_ms` when the `timeout`
  branch is connected, or surface that the port is inert without it. (Found in the final
  review; recorded here so it does not resurface as a "why does this port never fire" bug.)
- **Inherited watchdog caveat.** The runner's cancellation-less, nonce-less dispatch
  watchdog (documented in `CLAUDE.md`, the reverted `dw.agent` deadline) applies here too.
  Lower risk for approval than for a looping node — it parks once — and a spurious timeout
  now routes to the `timeout` branch rather than silently taking the success edge, which is
  arguably an improvement. No action; worth a line in the PR summary.

## Non-goals

- Any greentic-designer change (palette ports, catalog vocabulary,
  `inject_approval_nodes`, `node_kind` arm). Tracked separately — the designer
  branch `feat/restore-approval-call-palette` is parked pending this.
- Applying `resume_at_self = true` to `sorla.call` / `operala.call` /
  `agentic.call` / `telco-x.call`. They share `execute_remote_dispatch` and have the
  same limitation, but nothing asks for it yet.
- Fixing the `AwaitHere` interleaving limitation.
- greentic-admin changes — its response shape is fine; we adapt to it.
