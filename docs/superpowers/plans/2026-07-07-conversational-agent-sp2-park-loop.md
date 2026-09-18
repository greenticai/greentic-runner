# SP2 — Park-loop conversational `dw.agent` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a conversational `dw.agent` flow node park-and-loop (re-enter itself on each user message) until the agent calls `end_conversation` (`terminated_by == "conversation_ended"`), then advance to its successor.

**Architecture:** All changes in `crates/greentic-runner-host/src/runner/engine.rs`. Add a `conversational: bool` to `NodeKind::DwAgent`, a new `NodeControl::LoopHere` park variant (re-enter THIS node + render the reply, mirroring the proven `NextDecision::Wait` snapshot), and a conversational branch in the in-process `dw.agent` dispatch arm that inspects the agent output's `terminated_by` string.

**Tech Stack:** Rust 1.94 (toolchain 1.95), edition 2024, Tokio, `async_trait`, serde_json. Tests use in-module engine construction + a stub `AgentNodeHandler`.

**Spec:** `docs/superpowers/specs/2026-07-07-conversational-agent-sp2-park-loop-design.md`
**Epic:** `docs/superpowers/specs/2026-07-07-conversational-agent-chat-segment-epic-design.md`
**Depends on:** SP1 (merged to research, runner `0757855`) — the agent output payload carries `terminated_by: "conversation_ended"`.

## Global Constraints

- **Rust edition 2024**, toolchain pinned. `match` on `NodeControl` / `NodeKind` is exhaustive — adding a variant/field forces every arm to be updated (compiler-enforced).
- **Feature gating:** the conversational dispatch branch lives ONLY on the `agentic-worker` in-process path (`DwAgentDispatch::InProcess`). The non-feature `execute_dw_agent` `bail!`s, so leave that path unchanged. The `NodeControl::LoopHere` variant + its drive-loop arm are NOT feature-gated (the enum + drive loop always compile), so they must compile in both builds.
- **Decouple from aw-runtime:** read the exit signal as a string — `payload.get("terminated_by").and_then(Value::as_str) == Some("conversation_ended")`. Do not import `TerminationReason`.
- **Test command:** `cargo test -p greentic-runner-host --features agentic-worker,test-mock <name-substring>` (single substring filter). Also confirm the non-feature build compiles: `cargo check -p greentic-runner-host`.
- **Lint/format:** `cargo fmt --all --check`; `cargo clippy -p greentic-runner-host --features agentic-worker,test-mock -- -D warnings` before the final commit.
- **Env note:** a disk-cleanup watchdog + a concurrent designer dev-stack kill full `cargo test` runs — always run FILTERED tests, never the whole unfiltered suite.
- **Git:** conventional commits, **no `Co-Authored-By: Claude`** / no "Generated with Claude". Branch `feat/conversational-agent-sp2` → PR to `research`.
- **Backward compatible:** `conversational` defaults false ⇒ existing `dw.agent` flows byte-identical (`DispatchOutcome::complete`).

---

### Task 1: `NodeKind::DwAgent` gains a `conversational` field

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs`
  - `NodeKind::DwAgent { agent_id: String }` enum variant → add `conversational: bool`
  - dispatch match arm `NodeKind::DwAgent { agent_id } => { ... }` → bind `conversational` (unused for now: `conversational: _` is NOT allowed if a later task needs it — bind it as `conversational` and prefix `_` only if unused this task; simplest: keep the field ignored this task via `NodeKind::DwAgent { agent_id, conversational: _ }` and revisit in Task 2)
  - loader arm `"dw.agent" => NodeKind::DwAgent { agent_id: ... }` → add `conversational: false` with a `// SP3 will populate from the flow doc` comment
  - reverse-map `NodeKind::DwAgent { .. } => "dw.agent"` uses `..` → unaffected

**Interfaces:**
- Produces: `NodeKind::DwAgent { agent_id, conversational }` — Task 2 reads `conversational` in the dispatch arm.

- [ ] **Step 1: Add the field to the enum**

Find `enum NodeKind` and the `DwAgent` variant:

```rust
    DwAgent {
        agent_id: String,
        /// SP2: opt into multi-turn conversation-segment park-loop behaviour.
        /// SP3 will populate this from the flow doc; the loader defaults it false.
        conversational: bool,
    },
```

- [ ] **Step 2: Fix the loader construction site**

In the flow-loader match (the `"dw.agent" => NodeKind::DwAgent { .. }` arm):

```rust
                "dw.agent" => NodeKind::DwAgent {
                    agent_id: raw_operation.clone().unwrap_or_default(),
                    conversational: false, // SP3 will populate from the flow doc
                },
```

- [ ] **Step 3: Fix the dispatch match arm binding**

In `dispatch_node`, change `NodeKind::DwAgent { agent_id } => {` to `NodeKind::DwAgent { agent_id, conversational: _ } => {` (Task 2 replaces `_` with a real binding). The reverse-map arm `NodeKind::DwAgent { .. } =>` needs no change.

- [ ] **Step 4: Verify it compiles (both feature settings)**

Run: `cargo check -p greentic-runner-host --features agentic-worker,test-mock`
Then: `cargo check -p greentic-runner-host`
Expected: both compile — no "missing field" / non-exhaustive-match errors.

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-runner-host/src/runner/engine.rs
git commit -m "feat(engine): add conversational flag to NodeKind::DwAgent"
```

---

### Task 2: `NodeControl::LoopHere` + conversational park-loop dispatch

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs`
  - `enum NodeControl` → add `LoopHere { reason: Option<String> }`
  - drive-loop `match control { ... }` (the block with `NodeControl::Continue`, `NodeControl::Wait`, `NodeControl::Jump`, `NodeControl::Respond` arms) → add a `NodeControl::LoopHere { reason }` arm
  - dispatch arm `NodeKind::DwAgent { agent_id, conversational: _ }`, in-process path → inspect `terminated_by`
- Test: same file's `#[cfg(test)] mod tests` (add `#[cfg(feature = "agentic-worker")]` tests)

**Interfaces:**
- Consumes: `NodeKind::DwAgent.conversational` (Task 1); the DwAgent output payload's `terminated_by` string (SP1).
- Produces: conversational park-loop behaviour (`Waiting` re-entering the same node on a normal reply; `Completed`/advance on `conversation_ended`).

- [ ] **Step 1: Add the `LoopHere` variant**

In `enum NodeControl`, after `Wait`:

```rust
enum NodeControl {
    Continue,
    Wait {
        reason: Option<String>,
    },
    /// Park the flow and RE-ENTER this same node on the next inbound activity
    /// (conversational `dw.agent` loop), rendering the node output first.
    /// Unlike `Wait` (which resumes at the routing successor and renders
    /// nothing), `LoopHere` sets the resume target to the current node and
    /// renders the reply.
    LoopHere {
        reason: Option<String>,
    },
    Jump(JumpControl),
    Respond {
        text: Option<String>,
        card_cbor: Option<Vec<u8>>,
        needs_user: Option<bool>,
    },
}
```

- [ ] **Step 2: Add the drive-loop arm**

In the drive loop's `match control` block, add a `NodeControl::LoopHere` arm next to `NodeControl::Wait`. Mirror the `NextDecision::Wait` snapshot construction (search for `next_node: node_id.as_str().to_string()` — that existing block is the template), so `next_node` is the CURRENT node and the reply is rendered:

```rust
                NodeControl::LoopHere { reason } => {
                    // Conversational dw.agent: park and re-enter THIS node so the
                    // next user message drives the next turn. Render the reply
                    // (finalize_with Some) — unlike NodeControl::Wait, which
                    // resumes at the successor and finalizes with None.
                    let mut snapshot_state = state.clone();
                    snapshot_state.clear_egress();
                    let snapshot = FlowSnapshot {
                        pack_id: step_ctx.pack_id.to_string(),
                        flow_id: step_ctx.flow_id.to_string(),
                        next_flow: (current_flow_id != step_ctx.flow_id)
                            .then_some(current_flow_id.clone()),
                        next_node: node_id.as_str().to_string(),
                        state: snapshot_state,
                    };
                    let output_value = state.finalize_with(Some(output.payload.clone()));
                    return Ok(FlowExecution::waiting(
                        output_value,
                        FlowWait { reason, snapshot },
                    ));
                }
```

(Confirm the surrounding variable names — `step_ctx`, `current_flow_id`, `node_id`, `state`, `output` — match the `NextDecision::Wait` block you are mirroring; they are in the same scope.)

- [ ] **Step 3: Add the conversational branch in the dispatch arm**

In `dispatch_node`, the `NodeKind::DwAgent { agent_id, conversational }` arm (rename the `conversational: _` from Task 1 to `conversational`). On the in-process path only, replace `.map(DispatchOutcome::complete)` with an inspect-and-branch. Keep the Nats arm and the `#[cfg(not(feature = "agentic-worker"))]` path unchanged:

```rust
                    crate::runner::agent_node::DwAgentDispatch::InProcess => {
                        let output = self.execute_dw_agent(ctx, agent_id, payload).await?;
                        if *conversational {
                            let ended = output
                                .payload
                                .get("terminated_by")
                                .and_then(serde_json::Value::as_str)
                                == Some("conversation_ended");
                            if ended {
                                Ok(DispatchOutcome::complete(output))
                            } else {
                                Ok(DispatchOutcome::with_control(
                                    output,
                                    NodeControl::LoopHere {
                                        reason: Some(format!(
                                            "conversational dw.agent `{agent_id}` awaiting next user message"
                                        )),
                                    },
                                ))
                            }
                        } else {
                            Ok(DispatchOutcome::complete(output))
                        }
                    }
```

- [ ] **Step 4: Write the failing tests**

Add to the engine `#[cfg(test)] mod tests`. These are `#[cfg(feature = "agentic-worker")]` (they use `agent_node_handler`). Put the stub + helper near the other engine drive tests (e.g. after `vars_survive_park_and_resume_end_to_end`). Imports the test module already has: `NodeId`, `IndexMap`/`indexmap`, `RwLock`, `StdHashMap`, `FlowKey`, `ValidationConfig`/`ValidationMode`, `FlowContext`, `RetryConfig`, `Runtime`, `json`, `Arc`, `Routing`. Add `use std::str::FromStr as _;` if not present.

```rust
    #[cfg(feature = "agentic-worker")]
    struct StubAgentHandler {
        payload: serde_json::Value,
    }
    #[cfg(feature = "agentic-worker")]
    #[async_trait::async_trait]
    impl crate::runner::agent_node::AgentNodeHandler for StubAgentHandler {
        async fn execute(
            &self,
            _tenant_id: &str,
            _env_id: &str,
            _agent_id: &str,
            _session_id: &str,
            _flow_input: &serde_json::Value,
        ) -> anyhow::Result<serde_json::Value> {
            Ok(self.payload.clone())
        }
    }

    /// Build a 2-node flow: a `dw.agent` node (id "agent", conversational as
    /// given) routing to an emit "thanks" node that ends the flow.
    #[cfg(feature = "agentic-worker")]
    fn conversational_dw_flow(conversational: bool) -> HostFlow {
        let mut nodes = IndexMap::new();
        let agent_id = NodeId::from_str("agent").unwrap();
        let thanks_id = NodeId::from_str("thanks").unwrap();
        nodes.insert(
            agent_id.clone(),
            HostNode {
                kind: NodeKind::DwAgent {
                    agent_id: "a".to_string(),
                    conversational,
                },
                component: "dw.agent".to_string(),
                component_id: "dw.agent".to_string(),
                operation_name: Some("a".to_string()),
                operation_in_mapping: None,
                payload_expr: json!({ "user_text": "hi" }),
                routing: Routing::Next { node_id: thanks_id.clone() },
            },
        );
        nodes.insert(
            thanks_id.clone(),
            HostNode {
                kind: NodeKind::BuiltinEmit { kind: EmitKind::Response },
                component: "emit.response".to_string(),
                component_id: "emit.response".to_string(),
                operation_name: None,
                operation_in_mapping: None,
                payload_expr: json!({ "text": "thanks" }),
                routing: Routing::End,
            },
        );
        HostFlow {
            id: "conv.flow".to_string(),
            start: Some(agent_id),
            nodes,
            vars_init: JsonMap::new(),
        }
    }

    /// Build an engine holding `flow` with a stub agent handler returning `payload`.
    /// Mirrors the FlowEngine literal in `vars_survive_park_and_resume_end_to_end`.
    #[cfg(feature = "agentic-worker")]
    fn conv_engine(flow: HostFlow, payload: serde_json::Value) -> FlowEngine {
        FlowEngine {
            packs: Vec::new(),
            flows: Vec::new(),
            flow_sources: StdHashMap::new(),
            flow_cache: RwLock::new(StdHashMap::from([(
                FlowKey { pack_id: "test-pack".to_string(), flow_id: "conv.flow".to_string() },
                flow,
            )])),
            default_env: "local".to_string(),
            validation: ValidationConfig { mode: ValidationMode::Off },
            cross_pack_resolver: None,
            remote_dispatch_handler: None,
            dw_agent_dispatch: crate::runner::agent_node::DwAgentDispatch::InProcess,
            agent_node_handler: Some(std::sync::Arc::new(StubAgentHandler { payload })),
            graph_node_handler: None,
            mcp_tool_source: None,
        }
    }

    #[cfg(feature = "agentic-worker")]
    fn conv_ctx<'a>() -> FlowContext<'a> {
        FlowContext {
            tenant: "demo",
            pack_id: "test-pack",
            flow_id: "conv.flow",
            node_id: None,
            tool: None,
            action: None,
            session_id: Some("sess-conv"),
            provider_id: None,
            reply_scope: None,
            retry_config: RetryConfig { max_attempts: 1, base_delay_ms: 1 },
            attempt: 1,
            observer: None,
            mocks: None,
        }
    }

    #[cfg(feature = "agentic-worker")]
    #[test]
    fn conversational_dw_agent_parks_and_loops_on_normal_reply() {
        let engine = conv_engine(
            conversational_dw_flow(true),
            json!({ "reply": "hello there", "trail": [], "terminated_by": "final_reply" }),
        );
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(engine.execute(conv_ctx(), Value::Null)).unwrap();
        let snapshot = match result.status {
            FlowStatus::Waiting(w) => w.snapshot,
            other => panic!("expected Waiting (park-loop), got {other:?}"),
        };
        assert_eq!(snapshot.next_node, "agent", "must re-enter the dw.agent node itself");
        // The reply is rendered in the parked output.
        assert!(
            serde_json::to_string(&result.output).unwrap().contains("hello there"),
            "the agent reply must be rendered before parking: {:?}",
            result.output
        );
    }

    #[cfg(feature = "agentic-worker")]
    #[test]
    fn conversational_dw_agent_advances_on_conversation_ended() {
        let engine = conv_engine(
            conversational_dw_flow(true),
            json!({ "reply": "bye", "trail": [], "terminated_by": "conversation_ended" }),
        );
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(engine.execute(conv_ctx(), Value::Null)).unwrap();
        assert!(
            matches!(result.status, FlowStatus::Completed),
            "conversation_ended must advance to the successor and complete, got {:?}",
            result.status
        );
    }

    #[cfg(feature = "agentic-worker")]
    #[test]
    fn non_conversational_dw_agent_never_loops() {
        // Even with terminated_by == conversation_ended, a non-conversational
        // node just routes onward (today's one-shot behaviour) — never parks.
        for tb in ["final_reply", "conversation_ended"] {
            let engine = conv_engine(
                conversational_dw_flow(false),
                json!({ "reply": "x", "trail": [], "terminated_by": tb }),
            );
            let rt = Runtime::new().unwrap();
            let result = rt.block_on(engine.execute(conv_ctx(), Value::Null)).unwrap();
            assert!(
                matches!(result.status, FlowStatus::Completed),
                "non-conversational must complete (route onward) for terminated_by={tb}, got {:?}",
                result.status
            );
        }
    }
```

- [ ] **Step 5: Run the tests (RED before the dispatch/LoopHere edits, GREEN after)**

If you write the tests first (recommended), before Steps 1-3 they fail to compile (`NodeControl::LoopHere` / field missing). After Steps 1-3:

Run: `cargo test -p greentic-runner-host --features agentic-worker,test-mock conversational_dw_agent_parks_and_loops_on_normal_reply`
Run: `cargo test -p greentic-runner-host --features agentic-worker,test-mock conversational_dw_agent_advances_on_conversation_ended`
Run: `cargo test -p greentic-runner-host --features agentic-worker,test-mock non_conversational_dw_agent_never_loops`
Expected: all PASS.

- [ ] **Step 6: Confirm the non-feature build still compiles**

Run: `cargo check -p greentic-runner-host`
Expected: compiles (the `LoopHere` arm is not feature-gated; the dispatch branch is inside the `agentic-worker` cfg).

- [ ] **Step 7: Commit**

```bash
git add crates/greentic-runner-host/src/runner/engine.rs
git commit -m "feat(engine): park-loop conversational dw.agent on non-ConversationEnded turns"
```

---

### Task 3: Docs + final verification + PR

**Files:**
- Modify: `CLAUDE.md` (repo root) — the "Agentic Workers" section already says the park-loop is SP2; update to reflect it shipped.

**Interfaces:** none (docs + release).

- [ ] **Step 1: Update the CLAUDE.md note**

In the "Agentic Workers (`dw.agent` node)" section, replace the trailing sentence
"the flow park-and-loop reaction to `ConversationEnded` is SP2 (runner engine)." with:

```markdown
A conversational `dw.agent` node (`NodeKind::DwAgent.conversational`, default false) is a
multi-turn segment: after each agent turn the engine parks and re-enters the same node
(`NodeControl::LoopHere`) on the next inbound message, until the agent's output carries
`terminated_by == "conversation_ended"`, at which point the flow advances to the node's
successor (SP2). Non-conversational `dw.agent` is unchanged (one-shot). The flow-doc
`conversational` flag wiring is SP3; the out-of-process (Nats) dispatch path and a
`max_turns` safety cap are deferred follow-ups.
```

- [ ] **Step 2: Final gates**

```bash
cargo fmt --all --check
cargo clippy -p greentic-runner-host --features agentic-worker,test-mock -- -D warnings
cargo test -p greentic-runner-host --features agentic-worker,test-mock conversational_dw_agent_parks_and_loops_on_normal_reply
cargo test -p greentic-runner-host --features agentic-worker,test-mock conversational_dw_agent_advances_on_conversation_ended
cargo test -p greentic-runner-host --features agentic-worker,test-mock non_conversational_dw_agent_never_loops
cargo check -p greentic-runner-host
```
Expected: fmt clean, clippy clean, 3 tests pass, non-feature build compiles.

- [ ] **Step 3: Commit docs**

```bash
git add CLAUDE.md
git commit -m "docs(engine): document conversational dw.agent park-loop (SP2)"
```

- [ ] **Step 4: Push + PR to research**

```bash
# SSH port 22 is blocked on this machine — push over HTTPS:
gh auth setup-git
git push -u "https://github.com/greenticai/greentic-runner.git" feat/conversational-agent-sp2
gh pr create --base research --head feat/conversational-agent-sp2 \
  --title "feat(engine): SP2 park-loop conversational dw.agent (conversational-agent epic)" \
  --body "SP2 of the in-flow conversational chat-segment epic. A conversational dw.agent node parks and re-enters itself (NodeControl::LoopHere) each turn until terminated_by == conversation_ended, then advances to its successor. Additive & backward compatible (conversational defaults false; non-feature/Nats paths + max_turns deferred). Spec: docs/superpowers/specs/2026-07-07-conversational-agent-sp2-park-loop-design.md"
```
(No Claude attribution.)

---

## Self-Review

**Spec coverage:**
- `NodeKind::DwAgent.conversational` (loader default false) → Task 1. ✅
- `NodeControl::LoopHere` (re-enter self + render reply) → Task 2 Steps 1-2. ✅
- Conversational dispatch branch on `terminated_by` string, in-process only → Task 2 Step 3. ✅
- Nats path + non-feature path unchanged → not modified (Task 2 Step 3 scopes to InProcess; Step 6 checks non-feature compiles). ✅
- `max_turns` deferred → not in plan; documented in spec + CLAUDE.md. ✅
- Backward compat (default false, non-conversational unchanged) → `non_conversational_dw_agent_never_loops` test. ✅
- 3 spec test cases (continue / advance / regression) → Task 2 Step 4. ✅
- Resume-scope correctness (next_node == node id) → asserted in `conversational_dw_agent_parks_and_loops_on_normal_reply`. ✅

**Placeholder scan:** none. The one "confirm surrounding variable names match the mirrored block" note (Task 2 Step 2) points at a concrete existing block (`next_node: node_id.as_str().to_string()`) with the exact variables named.

**Type consistency:** `NodeControl::LoopHere { reason: Option<String> }` used in Task 2 Step 1 (def), Step 2 (drive arm), Step 3 (dispatch). `NodeKind::DwAgent { agent_id, conversational }` consistent Task 1 → Task 2. `terminated_by` string `"conversation_ended"` matches SP1's serde value. Stub `AgentNodeHandler::execute` signature matches the trait (`&str ×4, &Value → Result<Value>`).
