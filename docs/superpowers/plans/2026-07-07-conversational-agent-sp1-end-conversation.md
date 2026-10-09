# SP1 — `end_conversation` exit signal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the agentic-worker Plan-Act-Observe loop an agent-driven "conversation is over" signal: a host built-in `end_conversation` tool + `TerminationReason::ConversationEnded`, offered only to conversational agents.

**Architecture:** All changes live in `crates/greentic-aw-runtime`. Add an opt-in `AgentConfig.conversational` flag, a new additive `TerminationReason` variant, a new `end_conversation` host-tool module (mirroring `short_term`/`long_term`), and two minimal `loop.rs` edits (advertise + note; short-circuit termination reusing the existing guarded-return path). Downstream reaction to the new signal is SP2 (out of scope here).

**Tech Stack:** Rust 1.94.0, edition 2024, Tokio, serde_json, `MockLlmBackend` scripted-response test harness.

**Spec:** `docs/superpowers/specs/2026-07-07-conversational-agent-sp1-end-conversation-design.md`
**Epic:** `docs/superpowers/specs/2026-07-07-conversational-agent-chat-segment-epic-design.md`

## Global Constraints

- **Rust 1.94.0, edition 2024** — pinned via `rust-toolchain.toml`; let-chains (`if a && let Some(x) = ...`) are available and already used in `loop.rs`.
- **Test command (per task gate):** `cargo test -p greentic-aw-runtime --features test-mock` — NOT `--all-features`.
- **Zero-warning lint:** `cargo clippy -p greentic-aw-runtime --features test-mock -- -D warnings` and `cargo fmt --all --check` before the final commit.
- **Downstream compile:** adding a required `AgentConfig` field breaks every literal constructor in the workspace (incl. `greentic-runner-host` test modules). Task 2 fixes them all; verify with `cargo test -p greentic-runner-host --no-run`.
- **Git:** conventional commits, **no `Co-Authored-By: Claude`** and no "Generated with Claude" trailer. Branch `feat/conversational-agent-sp1` → PR to `research`.
- **Output contract unchanged:** `AgentOutput { reply, trail, terminated_by }` — only a new `terminated_by` value may appear.
- **Backward compatible:** `conversational` defaults `false`; non-conversational agents are byte-identical (no tool, no note, unchanged loop).

---

### Task 1: `TerminationReason::ConversationEnded` variant

**Files:**
- Modify: `crates/greentic-aw-runtime/src/error.rs:135-141` (the `TerminationReason` enum)
- Test: same file's `#[cfg(test)] mod tests`

**Interfaces:**
- Produces: `TerminationReason::ConversationEnded` (serde `"conversation_ended"`) — consumed by Task 5 and by SP2.

- [ ] **Step 1: Write the failing serde test**

Add to `error.rs` tests module:

```rust
#[test]
fn conversation_ended_serde_snake_case() {
    let json = serde_json::to_string(&TerminationReason::ConversationEnded).unwrap();
    assert_eq!(json, "\"conversation_ended\"");
    let back: TerminationReason = serde_json::from_str("\"conversation_ended\"").unwrap();
    assert_eq!(back, TerminationReason::ConversationEnded);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p greentic-aw-runtime --features test-mock conversation_ended_serde_snake_case`
Expected: FAIL — `no variant named ConversationEnded` (compile error).

- [ ] **Step 3: Add the variant**

In `error.rs`, extend the enum (keep the existing `#[serde(rename_all = "snake_case")]`):

```rust
pub enum TerminationReason {
    FinalReply,
    MaxIterations,
    Timeout,
    Error,
    TokenBudgetExceeded,
    ConversationEnded,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p greentic-aw-runtime --features test-mock conversation_ended_serde_snake_case`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-aw-runtime/src/error.rs
git commit -m "feat(aw): add TerminationReason::ConversationEnded"
```

---

### Task 2: `AgentConfig.conversational` field + workspace compile-fix

**Files:**
- Modify: `crates/greentic-aw-runtime/src/config.rs:106-119` (add field)
- Modify (add `conversational: false,` to each literal — insert after the last field `knowledge: ...`):
  - `crates/greentic-aw-runtime/src/config.rs:314`
  - `crates/greentic-aw-runtime/src/config_provider.rs:155`
  - `crates/greentic-aw-runtime/src/dw.rs:125` *(production: `agent_config_from_dw_manifest`)*
  - `crates/greentic-aw-runtime/src/error.rs:149`
  - `crates/greentic-aw-runtime/src/knowledge.rs:188`
  - `crates/greentic-aw-runtime/src/layered_provider.rs:54`
  - `crates/greentic-aw-runtime/src/long_term.rs:227`
  - `crates/greentic-aw-runtime/src/loop.rs:730`
  - `crates/greentic-aw-runtime/src/manifest_provider.rs:111`
  - `crates/greentic-aw-runtime/src/serve.rs:317` *(production: serve invoker)*
  - `crates/greentic-aw-runtime/src/short_term.rs:74`
  - `crates/greentic-aw-runtime/tests/component_loop.rs:96`
  - `crates/greentic-aw-runtime/tests/guardrail_e2e.rs:370`
  - `crates/greentic-aw-runtime/tests/guardrail_loop.rs:42`, `:139`, `:281`
  - `crates/greentic-aw-runtime/tests/loop_scripted.rs:25`
  - `crates/greentic-aw-runtime/tests/mcp_local_loop.rs:110`
  - `crates/greentic-aw-runtime/tests/mcp_loop.rs:78`
  - `crates/greentic-aw-runtime/tests/mcp_store_pull_e2e.rs:262`
  - `crates/greentic-runner-host/src/runner/agent_node.rs:1601`, `:1633`
  - `crates/greentic-runner-host/src/runner/engine.rs:4155`
  - `crates/greentic-runner-host/src/runner/graph_node.rs:1104`, `:1276`, `:2529`
- Test: `crates/greentic-aw-runtime/src/config.rs` tests module

**Interfaces:**
- Produces: `AgentConfig.conversational: bool` (default `false`) — read by Task 3's `conversational_active` and Task 4/5's loop gating.

**Note on helpers:** `loop_scripted.rs` `cfg_with_long_term`/`cfg_with_short_term` build on the base `cfg()` (no own literal) — only the base `cfg()` at `:25` needs the field. `short_term.rs` and `long_term.rs` use `let mut c = AgentConfig { ... }` — the literal still needs the field. `manifest_provider.rs`, `agent_node.rs`, `engine.rs`, `graph_node.rs` literals are in `#[cfg(test)]` modules; the real production manifest path deserializes via serde (`#[serde(default)]` covers it — no change).

- [ ] **Step 1: Write the failing default-deserialize test**

Add to `config.rs` tests module:

```rust
#[test]
fn conversational_defaults_false_when_absent() {
    let json = r#"{
        "agent_id": "a",
        "system_prompt": "s",
        "tools": [],
        "llm": { "provider": "openai", "model": "m" }
    }"#;
    let cfg: AgentConfig = serde_json::from_str(json).unwrap();
    assert!(!cfg.conversational);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p greentic-aw-runtime --features test-mock conversational_defaults_false_when_absent`
Expected: FAIL — `no field conversational on type AgentConfig` (compile error), and every literal site errors "missing field".

- [ ] **Step 3: Add the field**

In `config.rs`, add to `AgentConfig` (after `knowledge`):

```rust
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knowledge: Option<KnowledgeSettings>,
    /// Opt-in: when true this agent is a multi-turn conversation segment and is
    /// offered the host built-in `end_conversation` tool (SP1). Threaded from the
    /// flow node's `conversational` flag by SP2/SP3. Default false = today's
    /// one-shot behaviour.
    #[serde(default)]
    pub conversational: bool,
}
```

- [ ] **Step 4: Fix every literal constructor**

For each file/line in the **Files** list above, add `conversational: false,` as the last field of the `AgentConfig { ... }` literal (immediately after the `knowledge: ...,` line). Example (from `loop.rs:730` `cfg()`):

```rust
        memory: None,
        knowledge: None,
        conversational: false,
    }
```

- [ ] **Step 5: Verify aw-runtime compiles + test passes**

Run: `cargo test -p greentic-aw-runtime --features test-mock conversational_defaults_false_when_absent`
Expected: PASS (and the crate compiles — no "missing field" errors remain).

- [ ] **Step 6: Verify downstream (runner-host) test targets compile**

Run: `cargo test -p greentic-runner-host --no-run`
Expected: compiles with no "missing field `conversational`" errors. (`--no-run` builds test targets without executing — no Redis/network needed.)

- [ ] **Step 7: Commit**

```bash
git add crates/greentic-aw-runtime crates/greentic-runner-host
git commit -m "feat(aw): add AgentConfig.conversational opt-in flag"
```

---

### Task 3: `end_conversation` host-tool module

**Files:**
- Create: `crates/greentic-aw-runtime/src/end_conversation.rs`
- Modify: `crates/greentic-aw-runtime/src/lib.rs` (add `pub mod end_conversation;` in the alphabetical mod list, between `dw` and `error`)
- Test: inline `#[cfg(test)] mod tests` in the new module

**Interfaces:**
- Consumes: `AgentConfig` (Task 2), `crate::llm::LlmToolSchema`.
- Produces:
  - `pub(crate) const HOST_EXTENSION_ID: &str = "host"`
  - `pub(crate) const END_CONVERSATION_TOOL: &str = "end_conversation"`
  - `pub(crate) fn conversational_active(config: &AgentConfig) -> bool`
  - `pub(crate) fn end_conversation_tool_schema() -> LlmToolSchema`
  - `pub(crate) fn augment_system_prompt(base: &str) -> String`

- [ ] **Step 1: Write the failing tests**

Create `crates/greentic-aw-runtime/src/end_conversation.rs` with the tests first:

```rust
//! Host built-in `end_conversation` tool for conversational agents (SP1).
//!
//! Mirrors the `short_term`/`long_term` host-tool pattern: a reserved `"host"`
//! extension id + an LLM-facing schema, advertised only when the agent is
//! `conversational`. When the model calls it, the Plan-Act-Observe loop
//! (`crate::r#loop`) terminates the turn with
//! `TerminationReason::ConversationEnded`.

use crate::config::AgentConfig;
use crate::llm::LlmToolSchema;

/// Reserved extension id for host-provided built-in tools (shared with memory).
pub(crate) const HOST_EXTENSION_ID: &str = "host";
/// Host built-in tool: end the current conversation segment.
pub(crate) const END_CONVERSATION_TOOL: &str = "end_conversation";

/// The tool is offered only to conversational agents.
pub(crate) fn conversational_active(config: &AgentConfig) -> bool {
    config.conversational
}

/// LLM-facing schema for the host built-in `end_conversation` tool.
pub(crate) fn end_conversation_tool_schema() -> LlmToolSchema {
    LlmToolSchema {
        extension_id: HOST_EXTENSION_ID.to_string(),
        tool_name: END_CONVERSATION_TOOL.to_string(),
        description: "End the current conversation when it has reached a natural \
            conclusion — the user's goal is met, they say goodbye, or there is \
            nothing left to do. Optionally include a brief closing message."
            .to_string(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "final_message": {
                    "type": "string",
                    "description": "Optional short closing message shown to the user."
                }
            }
        }),
    }
}

/// Append the conversational system-prompt note so the model knows the tool
/// exists. Applied only for conversational agents.
pub(crate) fn augment_system_prompt(base: &str) -> String {
    format!(
        "{base}\n\nWhen the conversation has reached a natural end — the user's goal \
         is met, they say goodbye, or there is nothing left to do — call the \
         `end_conversation` tool with a brief `final_message`. Do not call it while \
         the user still needs help."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(conversational: bool) -> AgentConfig {
        AgentConfig {
            agent_id: "a".into(),
            system_prompt: "sys".into(),
            tools: vec![],
            guardrails: vec![],
            llm: crate::config::LlmProviderRef {
                provider: "openai".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: crate::config::AgentLimits::default(),
            memory: None,
            knowledge: None,
            conversational,
        }
    }

    #[test]
    fn active_only_when_conversational() {
        assert!(conversational_active(&cfg(true)));
        assert!(!conversational_active(&cfg(false)));
    }

    #[test]
    fn schema_shape() {
        let s = end_conversation_tool_schema();
        assert_eq!(s.extension_id, HOST_EXTENSION_ID);
        assert_eq!(s.tool_name, END_CONVERSATION_TOOL);
        // final_message is optional: no "required" array (or it omits final_message).
        let required = s
            .parameters
            .get("required")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
            .unwrap_or_default();
        assert!(!required.contains(&"final_message"));
        assert!(
            s.parameters["properties"]
                .get("final_message")
                .is_some()
        );
    }

    #[test]
    fn augment_appends_note() {
        let out = augment_system_prompt("BASE");
        assert!(out.starts_with("BASE"));
        assert!(out.contains("end_conversation"));
    }
}
```

- [ ] **Step 2: Register the module**

In `crates/greentic-aw-runtime/src/lib.rs`, add between `pub mod dw;` and `pub mod error;`:

```rust
pub mod end_conversation;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p greentic-aw-runtime --features test-mock end_conversation`
Expected: PASS — `active_only_when_conversational`, `schema_shape`, `augment_appends_note`.

- [ ] **Step 4: Commit**

```bash
git add crates/greentic-aw-runtime/src/end_conversation.rs crates/greentic-aw-runtime/src/lib.rs
git commit -m "feat(aw): add end_conversation host-tool module"
```

---

### Task 4: Advertise the tool + inject the system-prompt note (conversational only)

**Files:**
- Modify: `crates/greentic-aw-runtime/src/loop.rs` — compute `conv_active` near `lt_active`/`st_active`; append the note to `system_prompt`; push the schema in the per-iteration `tools_schema` block (next to `st_active` pushes)
- Test: `crates/greentic-aw-runtime/tests/loop_scripted.rs`

**Interfaces:**
- Consumes: `crate::end_conversation::{conversational_active, end_conversation_tool_schema, augment_system_prompt}` (Task 3).
- Produces: LLM requests for conversational agents carry the `end_conversation` tool + the note; non-conversational requests do not.

- [ ] **Step 1: Write the failing tests**

In `tests/loop_scripted.rs`, add (use the existing `cfg(...)` helper + set the flag; `MockLlmBackend` records `seen_tool_names` and `seen_system_prompts`):

```rust
#[tokio::test]
async fn conversational_agent_is_offered_end_conversation_tool() {
    let llm = std::sync::Arc::new(MockLlmBackend::new(vec![Ok(final_reply("hi"))]));
    let mut c = cfg(4, 5_000, vec![], None);
    c.conversational = true;
    let out = run_with(llm.clone(), c, "hello").await.unwrap();
    assert_eq!(out.terminated_by, TerminationReason::FinalReply); // no tool called here
    let tools = llm.seen_tool_names.lock().unwrap();
    assert!(tools[0].iter().any(|t| t == "end_conversation"));
    let prompts = llm.seen_system_prompts.lock().unwrap();
    assert!(prompts[0].contains("end_conversation"));
}

#[tokio::test]
async fn non_conversational_agent_has_no_end_conversation_tool() {
    let llm = std::sync::Arc::new(MockLlmBackend::new(vec![Ok(final_reply("hi"))]));
    let c = cfg(4, 5_000, vec![], None); // conversational defaults false
    let _ = run_with(llm.clone(), c, "hello").await.unwrap();
    let tools = llm.seen_tool_names.lock().unwrap();
    assert!(!tools[0].iter().any(|t| t == "end_conversation"));
    let prompts = llm.seen_system_prompts.lock().unwrap();
    assert!(!prompts[0].contains("end_conversation"));
}
```

**If a `run_with(llm, cfg, text)` helper does not already exist in `loop_scripted.rs`**, add this thin wrapper next to the existing helpers (adapt arg names to the file's existing runtime-construction helper — mirror how the current scripted tests build `AgentRuntime` and call `.step(...)`):

```rust
async fn run_with(
    llm: std::sync::Arc<MockLlmBackend>,
    config: AgentConfig,
    text: &str,
) -> Result<greentic_aw_runtime::AgentOutput, greentic_aw_runtime::AgentError> {
    let store = std::sync::Arc::new(MockAgentStateStore::new());
    let telemetry = std::sync::Arc::new(MockTelemetry::new());
    let cp = MockConfigProvider::new();
    let tc = TenantContext::new("acme", "prod");
    cp.insert(&tc, &config.agent_id, config.clone());
    let cp = std::sync::Arc::new(cp);
    let ext = std::sync::Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test());
    let token_meter = std::sync::Arc::new(greentic_aw_runtime::cost::MockTokenMeter::new(0));
    let ledger = std::sync::Arc::new(greentic_aw_runtime::mock::NoopToolLedger);
    let runtime = AgentRuntime::new(
        cp, store, ext, llm, telemetry, token_meter, ledger, None,
    );
    runtime
        .step(tc, "sess-conv", &config.agent_id, AgentInput { text: text.into() })
        .await
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p greentic-aw-runtime --features test-mock conversational_agent_is_offered_end_conversation_tool non_conversational_agent_has_no_end_conversation_tool`
Expected: FAIL — the conversational assertion fails (tool/note absent).

- [ ] **Step 3: Wire advertising + note in `loop.rs`**

Near the `lt_active` / `st_active` bindings, add:

```rust
    let conv_active = crate::end_conversation::conversational_active(&config);
```

After the long-term / knowledge `system_prompt` augmentation blocks (just before the MCP catalog resolution), append the note when conversational:

```rust
    let system_prompt = if conv_active {
        crate::end_conversation::augment_system_prompt(&system_prompt)
    } else {
        system_prompt
    };
```

In the per-iteration `tools_schema` assembly, after the `st_active` pushes:

```rust
        if conv_active {
            tools_schema.push(crate::end_conversation::end_conversation_tool_schema());
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p greentic-aw-runtime --features test-mock conversational_agent_is_offered_end_conversation_tool non_conversational_agent_has_no_end_conversation_tool`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-aw-runtime/src/loop.rs crates/greentic-aw-runtime/tests/loop_scripted.rs
git commit -m "feat(aw): advertise end_conversation tool + prompt note for conversational agents"
```

---

### Task 5: Short-circuit termination on `end_conversation` + guarded-return

**Files:**
- Modify: `crates/greentic-aw-runtime/src/loop.rs` — short-circuit inside the `if !response.tool_calls.is_empty()` block (before the assistant-tool_calls push + per-call loop); change the guarded-return gate from `== FinalReply` to `matches!(FinalReply | ConversationEnded)`
- Test: `crates/greentic-aw-runtime/tests/loop_scripted.rs`

**Interfaces:**
- Consumes: `crate::end_conversation::END_CONVERSATION_TOOL`, `conv_active` (Task 4), `TerminationReason::ConversationEnded` (Task 1).
- Produces: for a conversational agent whose model calls `end_conversation`, `AgentOutput { reply = final_message ?? accompanying content ?? "", terminated_by = ConversationEnded }`, with the closing message run through the outbound guardrail + saved.

- [ ] **Step 1: Write the failing tests**

In `tests/loop_scripted.rs`, add a helper to script an `end_conversation` tool-call response (adapt to the file's `LlmResponse`/`ToolCallRecord` shape — mirror the existing `tool_call(...)` helper), then the behaviour tests:

```rust
fn end_conversation_call(call_id: &str, final_message: Option<&str>, content: Option<&str>) -> LlmResponse {
    let args = match final_message {
        Some(m) => serde_json::json!({ "final_message": m }),
        None => serde_json::json!({}),
    };
    LlmResponse {
        content: content.map(str::to_string),
        tool_calls: vec![ToolCallRecord {
            call_id: call_id.into(),
            extension_id: "host".into(),
            tool_name: "end_conversation".into(),
            args,
        }],
        tokens_in: 5,
        tokens_out: 5,
    }
}

#[tokio::test]
async fn end_conversation_terminates_with_final_message() {
    let llm = std::sync::Arc::new(MockLlmBackend::new(vec![
        Ok(end_conversation_call("c1", Some("Goodbye!"), None)),
    ]));
    let mut c = cfg(4, 5_000, vec![], None);
    c.conversational = true;
    let out = run_with(llm, c, "bye").await.unwrap();
    assert_eq!(out.terminated_by, TerminationReason::ConversationEnded);
    assert_eq!(out.reply, "Goodbye!");
}

#[tokio::test]
async fn end_conversation_falls_back_to_accompanying_content() {
    let llm = std::sync::Arc::new(MockLlmBackend::new(vec![
        Ok(end_conversation_call("c1", None, Some("Take care."))),
    ]));
    let mut c = cfg(4, 5_000, vec![], None);
    c.conversational = true;
    let out = run_with(llm, c, "bye").await.unwrap();
    assert_eq!(out.terminated_by, TerminationReason::ConversationEnded);
    assert_eq!(out.reply, "Take care.");
}

#[tokio::test]
async fn end_conversation_empty_reply_when_neither_present() {
    let llm = std::sync::Arc::new(MockLlmBackend::new(vec![
        Ok(end_conversation_call("c1", None, None)),
    ]));
    let mut c = cfg(4, 5_000, vec![], None);
    c.conversational = true;
    let out = run_with(llm, c, "bye").await.unwrap();
    assert_eq!(out.terminated_by, TerminationReason::ConversationEnded);
    assert_eq!(out.reply, "");
}

#[tokio::test]
async fn end_conversation_ignored_when_not_conversational() {
    // Non-conversational agent: the tool is not intercepted. The model's call
    // is not in the allow-list, so it is blocked (not terminated); the second
    // scripted response is the real final reply.
    let llm = std::sync::Arc::new(MockLlmBackend::new(vec![
        Ok(end_conversation_call("c1", Some("nope"), None)),
        Ok(final_reply("real reply")),
    ]));
    let c = cfg(4, 5_000, vec![], None); // conversational = false
    let out = run_with(llm, c, "hi").await.unwrap();
    assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    assert_eq!(out.reply, "real reply");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p greentic-aw-runtime --features test-mock end_conversation_terminates end_conversation_falls_back end_conversation_empty_reply end_conversation_ignored`
Expected: FAIL — no interception yet; the conversational cases treat `end_conversation` as an unknown/blocked tool.

- [ ] **Step 3: Add the short-circuit in `loop.rs`**

At the very top of the `if !response.tool_calls.is_empty() {` block — **before** the `state.messages.push(ChatMessage::Assistant { ... tool_calls ... })` line and the `for call in response.tool_calls` loop:

```rust
        if !response.tool_calls.is_empty() {
            // --- Host built-in: `end_conversation` (conversational agents) ---
            // Agent-driven exit signal (SP1). Short-circuit BEFORE recording the
            // multi-tool assistant message so saved history carries no dangling
            // tool_call. Any co-occurring tool calls are ignored — the agent
            // chose to end the conversation.
            if conv_active
                && let Some(end_call) = response
                    .tool_calls
                    .iter()
                    .find(|c| c.tool_name == crate::end_conversation::END_CONVERSATION_TOOL)
            {
                observer.on_tool_call(&end_call.tool_name, &end_call.call_id);
                let closing = end_call
                    .args
                    .get("final_message")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .filter(|s| !s.is_empty())
                    .or_else(|| response.content.clone())
                    .unwrap_or_default();
                let ok = serde_json::json!({ "ok": true });
                observer.on_tool_result(&end_call.tool_name, &end_call.call_id, &ok);
                trail.push(AgentStep::ToolCall {
                    name: end_call.tool_name.clone(),
                    call_id: end_call.call_id.clone(),
                    result: ok,
                });
                reply = closing;
                terminated_by = TerminationReason::ConversationEnded;
                break;
            }

            // Record the assistant's tool-call turn BEFORE the tool results.
            state.messages.push(ChatMessage::Assistant {
                // ... existing code unchanged ...
```

- [ ] **Step 4: Extend the guarded-return gate**

Change the post-loop gate (currently `if terminated_by == TerminationReason::FinalReply {`) to:

```rust
    if matches!(
        terminated_by,
        TerminationReason::FinalReply | TerminationReason::ConversationEnded
    ) {
```

This routes the closing message through the existing outbound-guardrail → push `Assistant` reply → truncate → save → telemetry → `return Ok(AgentOutput { reply, trail, terminated_by })` block. The returned `terminated_by` is the variable's value (`ConversationEnded`).

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p greentic-aw-runtime --features test-mock end_conversation_terminates end_conversation_falls_back end_conversation_empty_reply end_conversation_ignored`
Expected: PASS.

- [ ] **Step 6: Add the outbound-guardrail regression test**

Confirm the closing message is guarded. In `tests/guardrail_loop.rs`, mirror the existing outbound-deny test but drive termination via `end_conversation`. Use that file's existing runtime+guardrail construction helper and a deny guardrail; script a single `end_conversation` response with `final_message: "blocked farewell"` on a `conversational = true` config, and assert the call returns `Err(AgentError::GuardrailDenied { direction: Outbound, .. })`. (Follow the exact construction the file's current outbound-deny test uses; only the scripted response + `conversational = true` differ.)

- [ ] **Step 7: Run the guardrail regression + full crate suite**

Run: `cargo test -p greentic-aw-runtime --features test-mock`
Expected: PASS — all new tests plus the pre-existing suite (no regressions).

- [ ] **Step 8: Commit**

```bash
git add crates/greentic-aw-runtime/src/loop.rs crates/greentic-aw-runtime/tests/loop_scripted.rs crates/greentic-aw-runtime/tests/guardrail_loop.rs
git commit -m "feat(aw): terminate loop with ConversationEnded on end_conversation call"
```

---

### Task 6: Docs + final verification

**Files:**
- Modify: `crates/greentic-runner-host/CLAUDE.md` *(if present)* and/or the repo-root `CLAUDE.md` "Agentic Workers" section — one paragraph on `AgentConfig.conversational` + `TerminationReason::ConversationEnded` + the `end_conversation` built-in (SP1 of the conversational-agent epic; downstream park-loop is SP2).

**Interfaces:** none (documentation only).

- [ ] **Step 1: Document the new primitive**

Add to the "Agentic Workers (`dw.agent` node)" section of the greentic-runner `CLAUDE.md` (root `CLAUDE.md` in this repo):

```markdown
An agent may be marked `conversational` (`AgentConfig.conversational`, default
false). Conversational agents are offered a host built-in `end_conversation`
tool (reserved `host` extension id) and a system-prompt note; when the model
calls it, the loop terminates the turn with `TerminationReason::ConversationEnded`
and the closing message (`final_message` arg, else the accompanying reply) is the
final reply. This is SP1 of the in-flow conversational chat-segment epic; the
flow park-and-loop reaction to `ConversationEnded` is SP2 (runner engine).
```

- [ ] **Step 2: Final lint + format + full crate gate**

```bash
cargo fmt --all --check
cargo clippy -p greentic-aw-runtime --features test-mock -- -D warnings
cargo test -p greentic-aw-runtime --features test-mock
cargo test -p greentic-runner-host --no-run
```
Expected: all clean; downstream test targets compile.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(aw): document conversational agents + end_conversation (SP1)"
```

- [ ] **Step 4: Push + open PR to research**

```bash
git push -u origin feat/conversational-agent-sp1
gh pr create --base research --head feat/conversational-agent-sp1 \
  --title "feat(aw): SP1 end_conversation exit signal (conversational-agent epic)" \
  --body "SP1 of the in-flow conversational chat-segment epic. Adds TerminationReason::ConversationEnded + AgentConfig.conversational + host built-in end_conversation tool + loop exit + system-prompt note. Additive & backward compatible (conversational defaults false). Spec: docs/superpowers/specs/2026-07-07-conversational-agent-sp1-end-conversation-design.md"
```
(No Claude attribution in the commit or PR body.)

---

## Self-Review

**Spec coverage:**
- Config flag `AgentConfig.conversational` → Task 2. ✅
- `TerminationReason::ConversationEnded` (serde snake_case) → Task 1. ✅
- `end_conversation` tool (reserved `host` id, optional `final_message`) → Task 3. ✅
- Advertise only when conversational → Task 4. ✅
- System-prompt note only when conversational → Task 4. ✅
- Short-circuit termination + final-reply resolution (final_message ?? content ?? "") → Task 5. ✅
- Reuse guarded-return path (outbound guardrail + save) via gate change → Task 5 Step 4. ✅
- Clean history / co-occurring tool calls ignored (known limitation) → Task 5 Step 3 comment. ✅
- Backward-compat (defaults false; non-conversational unchanged; blocked-not-terminated) → Task 5 `end_conversation_ignored_when_not_conversational`. ✅
- All 7 spec test cases → Tasks 1/3/4/5. ✅
- greentic-dw-providers pin risk → base is research; verified via crate build (Task 2/6). ✅
- max_turns deferred to SP2 → stated in spec non-goals; not in this plan. ✅

**Placeholder scan:** No TBD/TODO. The two "adapt to the file's existing helper" notes (Task 4 `run_with`, Task 5 guardrail test) point at concrete existing patterns in the named files with the exact differing inputs spelled out — not open-ended.

**Type consistency:** `conversational_active` / `end_conversation_tool_schema` / `augment_system_prompt` / `END_CONVERSATION_TOOL` used identically in Tasks 3→4→5. `TerminationReason::ConversationEnded` consistent Tasks 1→5. `AgentConfig.conversational: bool` consistent Tasks 2→3→4→5.
