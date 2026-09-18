# Slice 1 — Runner: guardrail enforce/monitor mode + violation events

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Let a guardrail policy be `enforce` (block on deny — today's behaviour) or `monitor` (do not block; record the denial), and emit a best-effort violation event for both outcomes over the existing NATS audit rail.

**Architecture:** `mode` rides the policy payload exactly like the existing `mandatory` flag: `GuardrailRef` → `ResolvedGuardrail` → `run_chain`. `run_chain` stays pure (no tenant, no sink): it *returns* observations, and `run_step` — which owns `observer: Arc<dyn StepObserver>` and spans both guardrail call sites — notifies the observer. `AgentAuditObserver` turns an observation into an `EventEnvelope` on the existing `AuditSink`, mirroring the metering event.

**Tech Stack:** Rust 2024, `greentic-aw-runtime` + `greentic-runner-host`, serde, tracing, existing NATS `AuditSink`.

## Global Constraints

- Repo: `greentic-runner`, worktree `.worktrees/guardrail-mode`, branch `feat/guardrail-mode-monitor`, base `origin/research` (**NOT `main`** — main is a diverged lineage).
- Gate: `ci/local_check.sh` (fmt → clippy `-D warnings` → `cargo test --workspace --all-targets --all-features`).
- **The WIT contract and the guardrail components are NOT touched.** The component never learns a mode exists.
- **Backward compatibility is mandatory**: existing admin payloads and pack manifests carry no `mode`. Absent ⇒ `Enforce`. Never change behaviour for a payload that predates this change.
- `mandatory` (fail-open/fail-closed on evaluator *error*) is orthogonal to `mode` (what to do on an explicit *deny*). Do not conflate them.
- Events are **best-effort**: `AuditSink::emit` drops on a full channel and never blocks. Off entirely when no sink is configured — same as metering.
- Conventional Commits. No AI attribution.
- Spec: `greentic-designer-admin/docs/superpowers/specs/2026-07-15-guardrails-mode-and-observability-design.md`.

## File Structure

```
crates/greentic-aw-runtime/src/config.rs      MODIFY  GuardrailMode enum + GuardrailRef.mode
crates/greentic-aw-runtime/src/guardrail.rs   MODIFY  ResolvedGuardrail.mode, resolve_one, ChainOutcome, run_chain, GuardrailObservation
crates/greentic-aw-runtime/src/lib.rs         MODIFY  StepObserver::on_guardrail (default no-op)
crates/greentic-aw-runtime/src/loop.rs        MODIFY  notify observer at both guardrail sites
crates/greentic-runner-host/src/trace/audit_event.rs  MODIFY  violation_subject + build_guardrail_violation_event
crates/greentic-runner-host/src/trace/agent_audit.rs  MODIFY  AgentAuditObserver::on_guardrail
```

---

### Task 1: `GuardrailMode` + carry it to the chain

**Files:** `crates/greentic-aw-runtime/src/config.rs`, `crates/greentic-aw-runtime/src/guardrail.rs`

**Interfaces produced:**
- `pub enum GuardrailMode { #[default] Enforce, Monitor }` (in `config.rs`), `Serialize`/`Deserialize` as `"enforce"`/`"monitor"`, `Clone, Copy, Debug, PartialEq, Eq, Default`.
- `GuardrailRef.mode: GuardrailMode` (`#[serde(default)]`)
- `ResolvedGuardrail.mode: GuardrailMode`

- [ ] **Step 1: Write the failing test**

In `config.rs`'s test module:

```rust
#[test]
fn guardrail_ref_without_mode_defaults_to_enforce() {
    // Every policy payload in the wild today omits `mode`. It must mean Enforce.
    let r: GuardrailRef = serde_json::from_str(r#"{"cap_id":"greentic:guardrail/pii"}"#).unwrap();
    assert_eq!(r.mode, GuardrailMode::Enforce);
}

#[test]
fn guardrail_ref_parses_monitor_mode() {
    let r: GuardrailRef =
        serde_json::from_str(r#"{"cap_id":"greentic:guardrail/pii","mode":"monitor"}"#).unwrap();
    assert_eq!(r.mode, GuardrailMode::Monitor);
}

#[test]
fn guardrail_mode_serializes_lowercase() {
    assert_eq!(serde_json::to_string(&GuardrailMode::Monitor).unwrap(), r#""monitor""#);
}
```

- [ ] **Step 2: Run it — expect FAIL**

Run: `cd .worktrees/guardrail-mode && cargo test -p greentic-aw-runtime guardrail_ref_ 2>&1 | tail -20`
Expected: compile error — `GuardrailMode` not found.

- [ ] **Step 3: Implement**

In `config.rs`, above `GuardrailRef`:

```rust
/// What to do when a guardrail returns an explicit `deny` verdict.
///
/// Orthogonal to [`ResolvedGuardrail::mandatory`], which decides fail-open vs
/// fail-closed when the evaluator *errors*. This decides what an explicit deny
/// *means*. The guardrail component never sees this value — a verdict is a
/// detection result; acting on it is the runner's policy decision.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GuardrailMode {
    /// Deny blocks the turn. The historical (and default) behaviour: a payload
    /// that predates this field means Enforce.
    #[default]
    Enforce,
    /// Deny is recorded but does NOT block; the content passes through unchanged.
    Monitor,
}
```

Add to `GuardrailRef`:

```rust
    #[serde(default)]
    pub mode: GuardrailMode,
```

In `guardrail.rs`, add `pub mode: GuardrailMode,` to `ResolvedGuardrail` (import it from `crate::config`), and copy it in `resolve_one`'s `Some(binding) => Some(ResolvedGuardrail { … })` arm:

```rust
            mode: guardrail_ref.mode,
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test -p greentic-aw-runtime guardrail_ 2>&1 | tail -20`
Expected: the three new tests pass. Existing `ResolvedGuardrail` literals in tests will fail to compile — fix each by adding `mode: GuardrailMode::Enforce` (that is the correct value: those tests assert today's blocking behaviour).

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-aw-runtime/src/config.rs crates/greentic-aw-runtime/src/guardrail.rs
git commit -m "feat(aw-runtime): carry a guardrail enforce/monitor mode on the policy ref"
```

---

### Task 2: Honor the mode in `run_chain` + return observations

**Files:** `crates/greentic-aw-runtime/src/guardrail.rs`

**Interfaces consumed:** `GuardrailMode`, `ResolvedGuardrail.mode` (Task 1).
**Interfaces produced:**
- `pub struct GuardrailObservation { pub cap_id: String, pub extension_id: String, pub direction: GuardrailDirection, pub code: String, pub message: String, pub action: GuardrailAction }`
- `pub enum GuardrailAction { Blocked, Monitored }` (serializes `"blocked"` / `"monitored"`)
- `ChainOutcome::Pass { content: String, observations: Vec<GuardrailObservation> }` (was `Pass(String)`)
- `ChainOutcome::Denied { info, direction, observation: GuardrailObservation }`

- [ ] **Step 1: Write the failing tests**

In `guardrail.rs`'s test module (follow the existing fake-evaluator pattern already there):

```rust
#[test]
fn monitor_mode_does_not_block_and_records_an_observation() {
    // A denying guardrail in Monitor mode must pass the ORIGINAL content
    // through untouched and surface exactly one Monitored observation.
    let chain = vec![resolved("ext-pii", "greentic:guardrail/pii", GuardrailMode::Monitor)];
    let evaluator = DenyingEvaluator { code: "pii".into(), message: "blocked pii".into() };
    match run_chain(&chain, GuardrailDirection::Inbound, "hello".into(), &ctx(), &evaluator) {
        ChainOutcome::Pass { content, observations } => {
            assert_eq!(content, "hello");
            assert_eq!(observations.len(), 1);
            assert_eq!(observations[0].action, GuardrailAction::Monitored);
            assert_eq!(observations[0].cap_id, "greentic:guardrail/pii");
            assert_eq!(observations[0].code, "pii");
        }
        other => panic!("monitor mode must not block, got {other:?}"),
    }
}

#[test]
fn enforce_mode_still_blocks_and_reports_a_blocked_observation() {
    let chain = vec![resolved("ext-pii", "greentic:guardrail/pii", GuardrailMode::Enforce)];
    let evaluator = DenyingEvaluator { code: "pii".into(), message: "blocked pii".into() };
    match run_chain(&chain, GuardrailDirection::Inbound, "hello".into(), &ctx(), &evaluator) {
        ChainOutcome::Denied { info, observation, .. } => {
            assert_eq!(info.code, "pii");
            assert_eq!(observation.action, GuardrailAction::Blocked);
        }
        other => panic!("enforce mode must block, got {other:?}"),
    }
}

#[test]
fn monitor_mode_does_not_stop_the_chain() {
    // A Monitor deny must not short-circuit: a later guardrail still runs and
    // can still Update the content.
    let chain = vec![
        resolved("ext-a", "greentic:guardrail/pii", GuardrailMode::Monitor),
        resolved("ext-b", "greentic:guardrail/topic", GuardrailMode::Enforce),
    ];
    let evaluator = ScriptedEvaluator::new(vec![
        ("ext-a", GuardrailVerdict::Deny(deny_info("pii"))),
        ("ext-b", GuardrailVerdict::Update("rewritten".into())),
    ]);
    match run_chain(&chain, GuardrailDirection::Inbound, "hello".into(), &ctx(), &evaluator) {
        ChainOutcome::Pass { content, observations } => {
            assert_eq!(content, "rewritten", "the chain must continue past a monitored deny");
            assert_eq!(observations.len(), 1);
        }
        other => panic!("expected pass, got {other:?}"),
    }
}

#[test]
fn mandatory_evaluator_error_still_fails_closed_regardless_of_monitor_mode() {
    // `mandatory` is orthogonal to `mode`: an evaluator ERROR on a mandatory
    // guardrail must still fail closed even when the mode is Monitor.
    let chain = vec![ResolvedGuardrail {
        extension_id: "ext-pii".into(),
        cap_id: "greentic:guardrail/pii".into(),
        mandatory: true,
        mode: GuardrailMode::Monitor,
        config: serde_json::Value::Null,
    }];
    let evaluator = ErroringEvaluator;
    match run_chain(&chain, GuardrailDirection::Inbound, "hello".into(), &ctx(), &evaluator) {
        ChainOutcome::Denied { info, .. } => assert_eq!(info.code, "internal"),
        other => panic!("mandatory error must fail closed, got {other:?}"),
    }
}
```

Add whatever `resolved(...)`, `ctx()`, `deny_info(...)`, `DenyingEvaluator`, `ErroringEvaluator`, `ScriptedEvaluator` helpers the existing test module lacks — **reuse the existing fakes if they already exist**; read the module first.

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test -p greentic-aw-runtime guardrail 2>&1 | tail -25`
Expected: compile errors (`ChainOutcome::Pass` shape, `GuardrailObservation` missing).

- [ ] **Step 3: Implement**

Add above `ChainOutcome`:

```rust
/// What the runner did about an explicit `deny` verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum GuardrailAction {
    /// Enforce mode: the turn was blocked.
    Blocked,
    /// Monitor mode: recorded only; the content passed through.
    Monitored,
}

/// A single guardrail denial, recorded whether or not it blocked. This is the
/// payload the host turns into a best-effort violation event; `run_chain`
/// itself neither knows the tenant nor owns a sink.
#[derive(Clone, Debug, PartialEq)]
pub struct GuardrailObservation {
    pub cap_id: String,
    pub extension_id: String,
    pub direction: GuardrailDirection,
    pub code: String,
    pub message: String,
    pub action: GuardrailAction,
}
```

Change `ChainOutcome`:

```rust
pub enum ChainOutcome {
    Pass {
        content: String,
        observations: Vec<GuardrailObservation>,
    },
    Denied {
        info: GuardrailDenyInfo,
        direction: GuardrailDirection,
        observation: GuardrailObservation,
    },
}
```

In `run_chain`: declare `let mut observations = Vec::new();` before the loop; replace the `Deny` arm with:

```rust
            Ok(GuardrailVerdict::Deny(info)) => match g.mode {
                GuardrailMode::Enforce => {
                    let observation = GuardrailObservation {
                        cap_id: g.cap_id.clone(),
                        extension_id: g.extension_id.clone(),
                        direction,
                        code: info.code.clone(),
                        message: info.message.clone(),
                        action: GuardrailAction::Blocked,
                    };
                    return ChainOutcome::Denied { info, direction, observation };
                }
                GuardrailMode::Monitor => {
                    // Monitor: record and keep going with the content UNCHANGED.
                    tracing::info!(
                        extension_id = %g.extension_id,
                        cap_id = %g.cap_id,
                        code = %info.code,
                        "guardrail denied in monitor mode; not blocking"
                    );
                    observations.push(GuardrailObservation {
                        cap_id: g.cap_id.clone(),
                        extension_id: g.extension_id.clone(),
                        direction,
                        code: info.code,
                        message: info.message,
                        action: GuardrailAction::Monitored,
                    });
                }
            },
```

The mandatory-error arm keeps returning `Denied` — give it an observation with `code: "internal"` and `action: Blocked` so the shape is uniform. End the function with `ChainOutcome::Pass { content, observations }`.

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test -p greentic-aw-runtime 2>&1 | tail -25`
Expected: all pass. `loop.rs` will not compile yet (it matches `Pass(text)`); that is Task 3 — you may fix the two match arms minimally here to keep the crate compiling (`ChainOutcome::Pass { content, .. } => content`).

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-aw-runtime/src/guardrail.rs crates/greentic-aw-runtime/src/loop.rs
git commit -m "feat(aw-runtime): honor monitor mode on guardrail deny and record observations"
```

---

### Task 3: Surface observations through `StepObserver`

**Files:** `crates/greentic-aw-runtime/src/lib.rs`, `crates/greentic-aw-runtime/src/loop.rs`

**Interfaces consumed:** `GuardrailObservation`, `ChainOutcome` (Task 2).
**Interfaces produced:** `StepObserver::on_guardrail(&self, obs: &GuardrailObservation)` — **default no-op**, so `NoopStepObserver`, `SseForwardObserver` and `CompositeObserver` need no change unless they want it.

- [ ] **Step 1: Write the failing test**

In `loop.rs`'s test module, using the existing runtime/test harness there (read it first — mirror how other `run_step` tests build `AgentRuntime` and a fake observer):

```rust
#[tokio::test]
async fn run_step_notifies_the_observer_for_a_monitored_denial() {
    // A recording observer proves the observation escapes run_chain and
    // reaches the seam the host emits from.
    let observer = Arc::new(RecordingObserver::default());
    // …build a runtime whose guardrail chain has one Monitor guardrail that denies…
    let out = run_step(&runtime, tenant(), "s1", "a1", input("hi"), observer.clone()).await;
    assert!(out.is_ok(), "monitor mode must not fail the turn");
    let seen = observer.guardrails.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].action, GuardrailAction::Monitored);
}

#[tokio::test]
async fn run_step_notifies_the_observer_for_a_blocked_denial() {
    let observer = Arc::new(RecordingObserver::default());
    // …one Enforce guardrail that denies…
    let out = run_step(&runtime, tenant(), "s1", "a1", input("hi"), observer.clone()).await;
    assert!(matches!(out, Err(AgentError::GuardrailDenied { .. })));
    let seen = observer.guardrails.lock().unwrap();
    assert_eq!(seen.len(), 1, "a blocked denial must still be observed");
    assert_eq!(seen[0].action, GuardrailAction::Blocked);
}
```

If building a full `AgentRuntime` in a unit test is impractical here, say so in your report and instead assert the same two behaviours directly against `run_chain` + a hand-rolled notify helper — but do NOT drop the "blocked denial is still observed" assertion; that is the regression this task exists to prevent.

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test -p greentic-aw-runtime run_step_notifies 2>&1 | tail -20`

- [ ] **Step 3: Implement**

In `lib.rs`, add to `trait StepObserver`:

```rust
    /// Called for every guardrail denial — blocked (Enforce) or merely recorded
    /// (Monitor). Default no-op: only the audit observer forwards these.
    fn on_guardrail(&self, _obs: &crate::guardrail::GuardrailObservation) {}
```

In `loop.rs`, at BOTH guardrail sites (inbound ~176, outbound ~656), replace the match:

```rust
        crate::guardrail::ChainOutcome::Pass { content, observations } => {
            for obs in &observations {
                observer.on_guardrail(obs);
            }
            content
        }
        crate::guardrail::ChainOutcome::Denied { info, direction, observation } => {
            observer.on_guardrail(&observation);
            return Err(AgentError::GuardrailDenied {
                direction,
                code: info.code,
                message: info.message,
                details: info.details,
            });
        }
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test -p greentic-aw-runtime 2>&1 | tail -25`

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-aw-runtime/src/lib.rs crates/greentic-aw-runtime/src/loop.rs
git commit -m "feat(aw-runtime): surface guardrail observations through StepObserver"
```

---

### Task 4: Emit the violation event from the audit observer

**Files:** `crates/greentic-runner-host/src/trace/audit_event.rs`, `crates/greentic-runner-host/src/trace/agent_audit.rs`

**Interfaces consumed:** `StepObserver::on_guardrail`, `GuardrailObservation`, `GuardrailAction`.
**Interfaces produced:** `violation_subject(tenant) -> String`, `build_guardrail_violation_event(...) -> EventEnvelope`.

- [ ] **Step 1: Write the failing test**

In `audit_event.rs`'s test module (mirror the existing metering-event tests):

```rust
#[test]
fn guardrail_violation_event_has_the_expected_shape() {
    let ev = build_guardrail_violation_event(
        &tenant_ctx("acme", "production"),
        "a1",
        Some("s1"),
        &obs_blocked(),                    // cap_id greentic:guardrail/pii, inbound, code "pii"
        Utc::now(),
        "evt-1".to_string(),
    );
    assert_eq!(ev.r#type, "greentic.runner.guardrail.violation");
    assert_eq!(ev.source, "runner");
    assert_eq!(ev.payload["cap_id"], "greentic:guardrail/pii");
    assert_eq!(ev.payload["direction"], "inbound");
    assert_eq!(ev.payload["code"], "pii");
    assert_eq!(ev.payload["action"], "blocked");
    assert_eq!(ev.payload["agent_id"], "a1");
}

#[test]
fn guardrail_violation_subject_is_tenant_scoped() {
    assert_eq!(violation_subject("acme"), "audit.acme.guardrail.violation");
}

#[test]
fn monitored_violation_is_tagged_monitored() {
    let ev = build_guardrail_violation_event(
        &tenant_ctx("acme", "production"), "a1", None, &obs_monitored(), Utc::now(), "e".into(),
    );
    assert_eq!(ev.payload["action"], "monitored");
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test -p greentic-runner-host guardrail_violation 2>&1 | tail -20`

- [ ] **Step 3: Implement the builders**

In `audit_event.rs`, beside the metering pair:

```rust
/// Subject for one guardrail denial: `audit.<tenant>.guardrail.violation`.
pub fn violation_subject(tenant: &str) -> String {
    format!("audit.{tenant}.guardrail.violation")
}

/// Best-effort record of one guardrail denial — blocked (Enforce) or recorded
/// only (Monitor). Emitted alongside, never instead of, the other audit events.
///
/// This is TELEMETRY, not an audit log: `AuditSink` drops events when its
/// channel saturates and the NATS publish is fire-and-forget.
pub fn build_guardrail_violation_event(
    tenant: &TenantCtx,
    agent_id: &str,
    session_id: Option<&str>,
    obs: &GuardrailObservation,
    now: DateTime<Utc>,
    id: String,
) -> EventEnvelope {
    let payload = json!({
        "cap_id": obs.cap_id,
        "extension_id": obs.extension_id,
        "direction": match obs.direction {
            GuardrailDirection::Inbound => "inbound",
            GuardrailDirection::Outbound => "outbound",
        },
        "code": obs.code,
        "action": obs.action,          // GuardrailAction serializes lowercase
        "agent_id": agent_id,
    });

    base_event(
        tenant,
        "greentic.runner.guardrail.violation",
        "runner",
        format!("agent:{agent_id}"),
        session_id.map(str::to_string),
        payload,
        now,
        id,
    )
}
```

Read `base_event`'s real signature before writing this and match it exactly (the metering builder above it is the reference). Do NOT put `obs.message` in the payload — it can contain user content.

- [ ] **Step 4: Wire the observer**

In `agent_audit.rs`, implement on `AgentAuditObserver` (mirror how its other methods reach the sink and how `agent_node.rs` builds `tenant_ctx_for_audit` + `generate_audit_event_id`):

```rust
    fn on_guardrail(&self, obs: &GuardrailObservation) {
        // Best-effort, same as every other audit emission: no sink ⇒ nothing built.
        self.sink.emit(
            violation_subject(&self.tenant_id),
            &build_guardrail_violation_event(
                &self.tenant_ctx,
                &self.agent_id,
                Some(&self.session_id),
                obs,
                Utc::now(),
                generate_audit_event_id(),
            ),
        );
    }
```

Adapt to the struct's actual fields — read `agent_audit.rs:53-89` first.

- [ ] **Step 5: Run — expect PASS**

Run: `cargo test -p greentic-runner-host 2>&1 | tail -25`

- [ ] **Step 6: Commit**

```bash
git add crates/greentic-runner-host/src/trace/audit_event.rs crates/greentic-runner-host/src/trace/agent_audit.rs
git commit -m "feat(runner-host): emit best-effort guardrail violation events"
```

---

### Task 5: Full gate

- [ ] **Step 1:** `cd .worktrees/guardrail-mode && cargo fmt --all --check`
- [ ] **Step 2:** `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] **Step 3:** `cargo test --workspace --all-targets --all-features 2>&1 | tail -20`
- [ ] **Step 4:** `ci/local_check.sh` — if it fails outside this change's scope, document it, do not hide it.
- [ ] **Step 5:** Commit any fixes; report.

## Self-Review

- **Spec coverage:** mode carried (T1) · monitor honored + observations (T2) · observer seam (T3) · event emitted for blocked AND monitored (T4) · gate (T5). WIT/components untouched throughout.
- **Backward compat:** asserted directly by `guardrail_ref_without_mode_defaults_to_enforce`; every pre-existing `ResolvedGuardrail` literal is updated to `Enforce`.
- **The trap to avoid:** `mandatory` vs `mode`. T2's fourth test pins that an evaluator *error* on a mandatory guardrail still fails closed even in Monitor mode.
- **Privacy:** the violation payload carries `code`, never `message` or content.
