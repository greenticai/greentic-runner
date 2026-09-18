# Agent-Run Metering Emit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Emit one `greentic.runner.metering.agent_run` event per completed in-process `dw.agent` run, best-effort over the existing `AuditSink`, so admin can aggregate usage (EPIC-D, unit = per-agent-run). Mirrors the EPIC-B B-3 agent-audit emit.

**Architecture:** A new `metering_subject` + `build_agent_run_metering_event` in `trace/audit_event.rs` (mirroring `agent_audit_subject` + `build_agent_audit_event` + the shared `base_event`), and one best-effort `sink.emit(...)` call at `RuntimeAgentNodeHandler::execute`'s `Ok(output)` arm in `runner/agent_node.rs`. Off by default (no `AuditSink` → no emit).

**Tech Stack:** Rust (edition 2024); `greentic_types::EventEnvelope`; the existing `AuditSink`/`trace` module.

## Global Constraints
- **Crate:** greentic-runner-host only. Files: `src/trace/audit_event.rs`, `src/runner/agent_node.rs`.
- **Off by default:** the emit is gated by `self.audit_sink` being `Some` — identical gate to the B-3 agent audit. No sink → byte-identical to today. Do NOT add new NATS wiring (the `agent_audit_sink` at `runtime.rs:315` already feeds this handler).
- **Best-effort:** `sink.emit(...)` already drops-on-full + never blocks/errors/panics. The metering emit must never affect the agent run's returned result — emit, then return the same `Ok(json!{...})` as today.
- **Unit = per-agent-run:** payload `{ "unit": "agent_run", "quantity": 1, "agent_id": <id>, "steps": <trail.len()> }`. No tokens (not on `AgentOutput`).
- **Mirror the B-3 precedent exactly:** `build_agent_audit_event` (`audit_event.rs:136`), `base_event` (`:59`), `agent_audit_subject` (`:50`), and the emit test `execute_with_audit_sink_routes_through_step_with_observer_and_enqueues_events` (`agent_node.rs:1491`).
- **Conventional commits, NO Claude co-author.** Target `research`.
- **Build discipline (shared machine, disk ample):** cargo `-j2` + `CARGO_BUILD_JOBS=2`, FOREGROUND; if OOM/SIGKILL retry `-j1`; never pkill/kill or delete another worktree's target/.

---

### Task 1: metering subject + builder + emit at run completion

**Files:**
- Modify: `crates/greentic-runner-host/src/trace/audit_event.rs` (add `metering_subject` + `build_agent_run_metering_event`)
- Modify: `crates/greentic-runner-host/src/runner/agent_node.rs` (emit at the `Ok(output)` arm ~:248)
- Test: inline `#[cfg(test)]` in both files

**Interfaces:**
- Produces:
  - `pub fn metering_subject(tenant: &str) -> String` → `format!("audit.{tenant}.metering.agent_run")` (mirror `agent_audit_subject`).
  - `pub fn build_agent_run_metering_event(tenant: &greentic_types::TenantCtx, agent_id: &str, steps: usize, now: DateTime<Utc>, id: EventId) -> EventEnvelope` — type `greentic.runner.metering.agent_run`, payload `{unit:"agent_run", quantity:1, agent_id, steps}`, built via `base_event` exactly like `build_agent_audit_event`. (READ `build_agent_audit_event` + `base_event` for the exact `base_event` signature — `now`/`id` may be produced internally; match its shape, don't invent parameters.)
- Consumes: `self.audit_sink: Option<AuditSink>`, `tenant_ctx_for_audit(tenant_id, env_id)`, `agent_id`, `output.trail` — all already in scope at `agent_node.rs` `execute`.

- [ ] **Step 1: Read** `trace/audit_event.rs` `base_event` (:59), `agent_audit_subject` (:50), `build_agent_audit_event` (:136) — note the EXACT way `now`/`EventId` are produced (internally vs passed) and mirror it; `runner/agent_node.rs` `execute` :213-252 (the `Ok(output)` arm :248 + how `tenant_ctx_for_audit`/`AgentAuditObserver` use the sink :232-239); the emit test :1491 (how it uses `AuditSink::from_sender` + a receiver to assert enqueued events).
- [ ] **Step 2: Write the failing builder test** — `build_agent_run_metering_event(&ctx, "agent1", 3, ...)` yields `event.r#type == "greentic.runner.metering.agent_run"`, `payload["unit"] == "agent_run"`, `payload["quantity"] == 1`, `payload["steps"] == 3`, `payload["agent_id"] == "agent1"`, tenant from ctx. And `metering_subject("t1") == "audit.t1.metering.agent_run"`.
- [ ] **Step 3: Run — expect FAIL** (`CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host -j2 audit_event`).
- [ ] **Step 4: Implement** `metering_subject` + `build_agent_run_metering_event` (mirror the audit builders exactly, matching `base_event`'s real signature).
- [ ] **Step 5: Wire the emit.** In `agent_node.rs` `execute`, in the `Ok(output) =>` arm (:248), BEFORE returning, when `let Some(sink) = &self.audit_sink`: `sink.emit(metering_subject(tenant_id), &build_agent_run_metering_event(&tenant_ctx_for_audit(tenant_id, env_id), agent_id, output.trail.len(), ...))`. Then return the SAME `Ok(json!({...}))` as today (unchanged). Do NOT emit on the error/guardrail arms.
- [ ] **Step 6: Emit test.** Mirror `execute_with_audit_sink_routes_through_step_with_observer_and_enqueues_events` (:1491): run `execute` with a `from_sender` sink + receiver, assert the receiver observes an event whose subject is `audit.<tenant>.metering.agent_run` (or whose decoded type is `greentic.runner.metering.agent_run`) with `quantity == 1`. Add a no-sink assertion (nothing emitted) if not already implied.
- [ ] **Step 7: Gate + commit.** `cargo fmt --all`; `CARGO_BUILD_JOBS=2 cargo clippy -p greentic-runner-host -j2 --all-targets -- -D warnings`; `CARGO_BUILD_JOBS=2 cargo test -p greentic-runner-host -j2 -- audit_event agent_node`. Commit (`feat(metering): emit per-agent-run metering event over the audit sink (EPIC-D D-1)`). Then finishing-a-development-branch → PR to `research` noting: admin aggregation (`usage_metering` table + subscriber branch + query route) is the follow-up D-2; tokens/graph/serve/team deferred.

## Self-Review
- **Coverage:** subject+builder (Step 4) + emit (Step 5) + tests (Steps 2, 6).
- **Placeholder scan:** "read base_event's real signature + the emit test" are deliberate — the exact `base_event`/`now`/`EventId` shape must be mirrored from the repo. No TBD.
- **Type consistency:** `build_agent_run_metering_event` returns `EventEnvelope`; `sink.emit(String, &EventEnvelope)` matches `AuditSink::emit`. `metering_subject` returns `String`.
- **Scope:** 2 files, additive, off-by-default; error/guardrail arms unchanged; no new NATS wiring.
