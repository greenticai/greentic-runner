# Card-submit classifier: ignore provider-stamped metadata (PR-1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A plain typed message from the webchat provider is no longer classified as a card submit by `is_card_submit`, so a parked `flow:` tool is not fed typed text as its answer.

**Architecture:** `is_card_submit` (`crates/greentic-runner-host/src/runner/engine.rs:4393`) counts any `metadata` key outside `CHANNEL_CONTEXT_METADATA_KEYS` as submitted input. The webchat provider stamps extra context keys on every message, typed or clicked. We extend the context allow-list with those keys (exact names plus the `channel.` prefix) and pin it with tests built from the provider's real output.

**Tech Stack:** Rust 1.95 / edition 2024, `serde_json`, crate `greentic-runner-host` (feature `agentic-worker`, default-on).

**Spec:** `docs/superpowers/specs/2026-10-05-parked-flow-side-question-design.md` (section 2 R1, D1 step 3). PR-2 (provider marker) and PR-3 (side turn) are NOT in this plan.

## Global Constraints

- Conventional commits; NO Claude co-author attribution (repo CLAUDE.md).
- No `unwrap()`/`panic!()` in production code (tests may assert).
- Run ONLY narrow test filters locally; never the full suite. Before any cargo command: confirm no `web/postcss.config.mjs`, `.vscode/`, `public/fonts/` exist and `git log -1 --format=%cn` is `GitHub`/own commit on top of `origin/develop` (IOC scan done 2026-10-05, tree clean).
- Do not push, merge, tag, bump or deploy.
- Final gate (compile, tests are not run on PR CI): `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`.
- `-p` reductions are not faithful (feature unification); use workspace clippy for the gate.

## Review Focus

- Typed webchat text with ALL provider-stamped keys is not a submit (Task 1 test).
- A real click carrying only card inputs (`room`) is still a submit, even beside stamped keys (Task 1 test).
- A click with `metadata.action` stays a submit regardless of stamped keys (Task 1 test).
- Card input whose name equals an allow-listed key (e.g. `route`) is now invisible to the classifier when it is the ONLY input: documented limitation, pinned so a change is deliberate (Task 1 test; real fix is the PR-2 marker).
- `channel.<k>` keys match by prefix only, not substring (`mychannel.x` is still input) (Task 1 test).

## File Structure

- Modify: `crates/greentic-runner-host/src/runner/engine.rs` (allow-list const ~4368, `is_card_submit` ~4416, tests module ~13035). Single-file change; the file is large but this follows the existing #822 pattern, no restructuring.

---

### Task 1: Treat provider-stamped metadata as context, not input

**Files:**
- Modify: `crates/greentic-runner-host/src/runner/engine.rs:4368` (const), `:4416` (predicate)
- Test: same file, tests module next to `a_typed_message_in_a_channel_envelope_is_not_a_card_submit` (~13035)

**Interfaces:**
- Consumes: existing `is_card_submit(&Value) -> bool`, `wrapped_typed_message(&str) -> Value` (test helper, ~13011), `resolve_entry_metadata`.
- Produces: `CHANNEL_CONTEXT_METADATA_KEYS: &[&str]` (extended), `CHANNEL_CONTEXT_METADATA_PREFIX: &str = "channel."`, private `fn is_channel_context_key(key: &str) -> bool`. No public API change.

- [ ] **Step 1: Write the failing tests** (add after `a_typed_message_in_a_channel_envelope_is_not_a_card_submit`)

```rust
    /// What the webchat provider stamps on EVERY message (typed or clicked):
    /// `messaging-provider-webchat` `ops/envelope.rs` (universal, tenant,
    /// tenant_channel_id, route, extensions, channel.*) and `ops/ingest.rs`
    /// (user_id, user_verified, flow_hint, locale). Verified against
    /// greentic-messaging-providers develop e4f7b389.
    #[cfg(any(feature = "agentic-worker", test))]
    fn provider_stamped_metadata() -> serde_json::Value {
        json!({
            "universal": "true",
            "env": "prod",
            "tenant": "acme",
            "tenant_channel_id": "chan-1",
            "route": "conv-1",
            "user_id": "user-1",
            "user_verified": "true",
            "flow_hint": "main",
            "locale": "en-US",
            "extensions": "{\"caller\":{\"sub\":\"user-1\"}}",
            "channel.channel_data": "{}"
        })
    }

    #[test]
    fn a_typed_message_with_provider_stamped_metadata_is_not_a_card_submit() {
        let mut typed = wrapped_typed_message("how do I get a mail signature?");
        typed["input"]["metadata"] = provider_stamped_metadata();
        assert!(!is_card_submit(&typed));
        // Same envelope delivered flat.
        let flat = typed["input"].clone();
        assert!(!is_card_submit(&flat));
    }

    #[test]
    fn a_click_is_still_a_card_submit_beside_provider_stamped_metadata() {
        let mut by_action = wrapped_typed_message("");
        let mut meta = provider_stamped_metadata();
        meta["action"] = json!("start_request");
        by_action["input"]["metadata"] = meta;
        assert!(is_card_submit(&by_action));

        let mut by_values = wrapped_typed_message("");
        let mut meta = provider_stamped_metadata();
        meta["room"] = json!("101");
        by_values["input"]["metadata"] = meta;
        assert!(is_card_submit(&by_values));
    }

    #[test]
    fn channel_context_prefix_matches_only_the_channel_namespace() {
        let mut typed = wrapped_typed_message("hi");
        typed["input"]["metadata"] = json!({ "mychannel.x": "1" });
        assert!(is_card_submit(&typed));
        typed["input"]["metadata"] = json!({ "channel.x": "1" });
        assert!(!is_card_submit(&typed));
    }

    /// Known limitation (spec D1): a card whose ONLY input shares a name with
    /// a provider-stamped key cannot be told from typed text without the
    /// PR-2 submit marker. Pinned so changing it is deliberate.
    #[test]
    fn a_lone_card_input_named_like_a_stamped_key_is_not_seen_as_a_submit() {
        let mut click = wrapped_typed_message("");
        click["input"]["metadata"] = json!({ "route": "billing" });
        assert!(!is_card_submit(&click));
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p greentic-runner-host --lib -- a_typed_message_with_provider_stamped a_click_is_still channel_context_prefix a_lone_card_input`
Expected: `a_typed_message_with_provider_stamped_metadata_is_not_a_card_submit`, `channel_context_prefix_...` and `a_lone_card_input_...` FAIL; `a_click_is_still...` passes already.

- [ ] **Step 3: Implement**

Replace the const at `engine.rs:4368` and add the helper right below it:

```rust
#[cfg(any(feature = "agentic-worker", test))]
const CHANNEL_CONTEXT_METADATA_KEYS: &[&str] = &[
    "locale",
    "team",
    "env",
    "autoStart",
    // Stamped by the webchat provider on every activity (typed or clicked).
    "universal",
    "tenant",
    "tenant_id",
    "tenant_channel_id",
    "route",
    "conversation_id",
    "user_id",
    "user_verified",
    "flow_hint",
    "extensions",
];

/// Provider passthroughs are flattened as `channel.<key>`.
#[cfg(any(feature = "agentic-worker", test))]
const CHANNEL_CONTEXT_METADATA_PREFIX: &str = "channel.";

#[cfg(any(feature = "agentic-worker", test))]
fn is_channel_context_key(key: &str) -> bool {
    CHANNEL_CONTEXT_METADATA_KEYS.contains(&key) || key.starts_with(CHANNEL_CONTEXT_METADATA_PREFIX)
}
```

Update the doc comment above the const to name the webchat provider as the source. In `is_card_submit` replace the `metadata_input` closure body:

```rust
    let metadata_input = metadata
        .is_some_and(|meta| meta.keys().any(|key| key != "action" && !is_channel_context_key(key)));
```

Check no other use of `CHANNEL_CONTEXT_METADATA_KEYS` was left stale: `grep -n CHANNEL_CONTEXT_METADATA_KEYS crates/greentic-runner-host/src/runner/engine.rs`.

- [ ] **Step 4: Run to verify they pass, plus the existing #822 tests**

Run: `cargo test -p greentic-runner-host --lib -- is_card_submit card_submit typed_message_during_a_tool_park typed_envelope_message`
Expected: all PASS.

- [ ] **Step 5: Format and lint gate**

Run: `cargo fmt --all --check` then `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/greentic-runner-host/src/runner/engine.rs
git commit -m "fix(runner-host): provider-stamped metadata is context, not a card submit"
```

---

## Self-review

- Spec coverage: covers R1 / D1 step 3 only; D1 steps 1-2 marker is PR-2; D2-D6 are PR-3. No gap for this PR.
- Placeholders: none. Types: `is_channel_context_key`, `CHANNEL_CONTEXT_METADATA_PREFIX` used consistently.
- Caveat for the reviewer: the allow-list now also exempts `tenant`/`route`/`flow_hint` from "input" status; provider skips value keys starting `user_`/`greentic_` (`ingest.rs`), so `user_id`/`user_verified` cannot be forged by card data. Whether card `value.route` can overwrite the stamped `route` is UNVERIFIED (ordering in `ingest.rs`); irrelevant for classification, relevant only if `route` is later trusted.
- Open before merge: repro against a real provider payload (captured envelope fixture) and confirm deployed runner tag; if the deployed build predates #822 this PR alone does not fix the deployment.
