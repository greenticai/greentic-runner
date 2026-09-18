# EPIC-D — Per-Agent-Run Metering — Design Spec

**Status:** Draft — 2026-07-06
**Initiative:** Agentic platform coverage — EPIC-D "metering → billing". Unit = **per-agent-run** (quantity = 1 per completed `dw.agent` run). An internal NATS→admin metering spine that mirrors the EPIC-B audit spine.

## 1. Problem & goal
The platform has no internal usage-metering: there is an *external* `HttpBillingMeter` (`greentic-aw-runtime/src/billing.rs`) that POSTs per-LLM-iteration token batches to a cloud-commerce API (gated on `GREENTIC_BILLING_BASE_URL`), but nothing internal counts agent runs per tenant for the operator's own usage/billing view. EPIC-D adds a lightweight **internal** metering event, emitted once per completed agent run and aggregated in greentic-admin — a parallel, self-contained alternative to the external billing HTTP path (kept conceptually separate: unit `agent_run`, not `token`, so no double-counting).

**Goal (this spec):** emit one `greentic.runner.metering.agent_run` event per completed in-process `dw.agent` run, best-effort over the existing `AuditSink`, so admin can aggregate usage. (Admin aggregation = the follow-up slice D-2.)

## 2. Why a clean mirror of the EPIC-B audit spine
The runner already has an `AuditSink` (`trace/audit_sink.rs`: bounded mpsc, drop-on-full, fire-and-forget NATS publish, never blocks/panics), constructed once at `runtime.rs:315` (`agent_audit_sink = audit_nats_client.map(AuditSink::new)`, `None` when NATS is unset) and threaded into the `dw.agent` handler. The admin already subscribes to `audit.>` (`audit_ingest.rs`). So a metering event published under the `audit.<tenant>.…` prefix rides the **existing** sink and the **existing** subscriber — no new NATS wiring on either side.

## 3. Scope
**In (D-1, this spec's slice):** emit `greentic.runner.metering.agent_run` at the single clean run-completion point — `RuntimeAgentNodeHandler::execute`'s `Ok(output)` arm (`agent_node.rs:248`), where `tenant`/`agent_id`/`output.trail` are in hand and `self.audit_sink` is already present. Off by default (no sink → no emit, byte-identical to today).

**Out (documented):**
- **Admin aggregation** (`usage_metering` table + subscriber branch + query route) — the follow-up slice **D-2**.
- **Tokens on the record:** `AgentOutput` carries no token counts (they live only as a local in `aw-runtime/src/loop.rs:317`); surfacing them is a cross-crate change. v1 meters **quantity = 1 + steps (`trail.len()`)** — the reliable signal at this seam.
- **Graph + `agentic.call` serve paths:** the graph path is per-*turn* not per-run; the out-of-process serve path has no `AuditSink`. Deferred — v1 meters the in-process `dw.agent` node only.
- **Team:** absent at this seam (only `tenant_id`/`env_id` strings reach `execute`) — `team` is left as the audit path leaves it (null in admin), a documented follow-up.

## 4. The metering event
- **Subject:** `audit.<tenant>.metering.agent_run` (via a new `metering_subject(tenant)` helper next to `agent_audit_subject`) — under the `audit.` prefix so the existing `audit.>` admin subscriber receives it.
- **Type:** `greentic.runner.metering.agent_run`.
- **Payload:** `{ "unit": "agent_run", "quantity": 1, "agent_id": "<id>", "steps": <trail.len()> }`.
- **Envelope:** built by a new `build_agent_run_metering_event(tenant_ctx, agent_id, steps, now, id) -> EventEnvelope`, mirroring `build_agent_audit_event` + the shared `base_event` helper in `trace/audit_event.rs`.
- **Emission:** best-effort `sink.emit(metering_subject(tenant), &envelope)` — the sink already drops-on-full and never blocks/errors. A metering emit never affects the agent run's result.

## 5. Testing (offline)
- **Builder:** `build_agent_run_metering_event` produces type `greentic.runner.metering.agent_run`, payload `unit=agent_run`/`quantity=1`/`steps=N`, tenant from the ctx (mirror the `build_agent_audit_event` test).
- **Emit:** extend/mirror `execute_with_audit_sink_routes_through_step_with_observer_and_enqueues_events` (`agent_node.rs:1491`) — after a successful run with a sink, a metering event is enqueued on the sink (assert the `from_sender` receiver observes a `…metering.agent_run` subject/type + quantity 1). With **no** sink, nothing is emitted (off-by-default).

## 6. Rollout
Additive, off-by-default (rides the existing `agent_audit_sink` gate). Files: `trace/audit_event.rs` (subject + builder), `runner/agent_node.rs` (the emit call). Target `research`. Follow-up: **D-2** admin `usage_metering` table + subscriber branch + aggregate query route (mirrors `audit_activity`); then tokens-on-`AgentOutput`, graph/serve metering, and team plumbing.
