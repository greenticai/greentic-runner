# SP3 — `conversational` flow-doc flag → IR → runner loader — Design

**Status:** approved (brainstorm), pending spec review
**Epic:** [In-flow Conversational Chat Segment](2026-07-07-conversational-agent-chat-segment-epic-design.md) — sub-project 3 of 4
**Depends on:** SP2 (merged to research, runner `a98f943`) — the runner loader currently hardcodes `NodeKind::DwAgent.conversational = false`.
**Scope:** CROSS-REPO — `greentic-types` (foundation) + `greentic-flow` + `greentic-runner-host`.
**Release model (user decision):** `[patch.crates-io]` dev-deps to prove end-to-end locally; **publish to crates.io + pin bump are DEFERRED to a coordinated release**. The runner-host loader change cannot merge to research until greentic-types+greentic-flow are published, so its PR is held as **draft/blocked-pending-publish**; the greentic-types and greentic-flow PRs proceed on their own branches (not published).

## Goal

Let a flow author mark a `dw.agent` node conversational in the flow doc:

```yaml
nodes:
  chat:
    dw.agent: my-agent
    conversational: true          # NEW — opt into the SP2 park-loop
    routing: [{ to: thanks }]
```

The flag flows: flow doc → `greentic_types::Node.conversational` (parsed by greentic-flow) →
runner loader → `NodeKind::DwAgent { conversational }` (SP2's park-loop). Absent/false ⇒
today's one-shot node.

## Background — the type cascade (grounded)

- The runner loader (`greentic-runner-host/src/runner/engine.rs`, `"dw.agent" =>` arm) builds
  `NodeKind::DwAgent` from a `greentic_types::Node` (imported `use greentic_types::{Flow, Node, ...}`).
  It currently sets `conversational: false` (SP2 placeholder).
- `greentic_types::Node` (`greentic-types/src/flow.rs:93`) is a typed struct (id, component, input,
  output, err_map, routing, telemetry) — **no generic config map**. So the flag needs a typed field.
- greentic-flow parses the flow-doc YAML into `NodeDoc` (`greentic-flow/src/model.rs`, which has a
  `#[serde(flatten)] raw: IndexMap<String, Value>` catch-all) and then constructs
  `greentic_types::Node` (`greentic-flow/src/lib.rs:176`). The node-loop already iterates
  `for (k, v) in node_doc.raw { match k { "in_map" => ..., ... } }` (~lib.rs:131) — the
  `conversational` key lands there.
- Both foundation crates are pinned exact from crates.io (`greentic-flow = "=1.1.3"`,
  greentic-types likewise), so a real field addition needs publishes + pin bumps (deferred).

## Design

### 1. greentic-types — add the field (`src/flow.rs`)

```rust
pub struct Node {
    // ... existing fields ...
    pub telemetry: TelemetryHints,
    /// SP3: opt this node into conversational chat-segment behaviour (the runner's
    /// SP2 park-loop for a `dw.agent` node). Default false = today's one-shot node.
    #[cfg_attr(feature = "serde", serde(default))]
    pub conversational: bool,
}
```

- `serde(default)` ⇒ existing flow docs / serialized `Node`s deserialize as `false`. Additive.
- `Node` derives `Clone, Debug, PartialEq` (+ optional serde/schemars) — a bool field is fine.
- Every literal `Node { ... }` constructor in greentic-types (tests) must add `conversational: false`
  (no `..Default`; the compiler enumerates them).

### 2. greentic-flow — thread the flag (`src/lib.rs`)

In the node-construction loop, before the `for (k, v) in node_doc.raw` match, add
`let mut conversational = false;`. In the match, add:

```rust
                "conversational" => {
                    conversational = v.as_bool().unwrap_or(false);
                }
```

Add `conversational,` to the `Node { ... }` literal (~lib.rs:176). Fix any other
`greentic_types::Node { ... }` literal in greentic-flow (tests). To build/test against the
unpublished greentic-types field, greentic-flow uses a **local `[patch.crates-io]`** (uncommitted)
pointing greentic-types at the sibling checkout.

### 3. greentic-runner-host — read the flag (`src/runner/engine.rs`)

Loader `"dw.agent" =>` arm:

```rust
                "dw.agent" => NodeKind::DwAgent {
                    agent_id: raw_operation.clone().unwrap_or_default(),
                    conversational: node.conversational,   // was: false (SP2 placeholder)
                },
```

`node` is the `greentic_types::Node` in scope. `From<Flow> for HostFlow` needs no change (the loader
reads `node` fields directly).

**Local-only patch (NOT committed):** the worktree's root `Cargo.toml` gets a
`[patch.crates-io]` pointing greentic-types + greentic-flow at the local sibling checkouts so the
loader compiles + the test runs end-to-end. This patch is reverted before the runner-host commit;
the committed loader change is held (draft PR) until the coordinated publish lands the field in the
pinned crates.io versions.

### 4. Validation (epic risk: conversational-terminal-node)

The epic asks: "a conversational node SHOULD have a successor; warn if terminal." Realize as a
**non-fatal warning** at load time (greentic-flow validation or the runner loader): if
`conversational && routing has no forward target (End/Reply only)`, log a warning
("conversational dw.agent `<id>` has no successor; the segment cannot advance after
end_conversation"). Non-blocking; keep minimal. If clean placement isn't obvious, defer to a
follow-up and note it (do not force a fragile hook).

## Backward compatibility

- `conversational` defaults false everywhere (serde default + loader) ⇒ existing flows byte-identical.
- Additive typed field; no behavioural change until a doc sets `conversational: true` AND the runner
  runs SP2 (already on research).

## Testing

- **greentic-types:** a serde round-trip test — a `Node` JSON without `conversational` deserializes
  to `false`; with `true` → `true`.
- **greentic-flow:** compile a flow doc with `conversational: true` on a `dw.agent` node ⇒ the
  produced `greentic_types::Node.conversational == true`; absent ⇒ false. (Uses the local patch to
  build against the new greentic-types field.)
- **greentic-runner-host:** with the local patch, a loader test — a `Flow` whose `dw.agent` node has
  `conversational: true` ⇒ `NodeKind::DwAgent { conversational: true }` (assert via the reverse path
  or a dispatch that parks). Combined with SP2's park-loop tests, this proves the doc→behaviour path.

## Risks / realities

- **Deferred publish:** the runner-host loader change does NOT compile against the pinned crates.io
  greentic-types 1.1.3. It is proven locally via `[patch.crates-io]` and its PR is **held** until a
  coordinated publish of greentic-types + greentic-flow (+ runner-host pin bump). Only the
  greentic-types and greentic-flow branch PRs proceed now.
- **Foundation-crate blast radius:** greentic-types is the foundation ("changes may break all
  downstream"). The change is purely additive (new defaulted field) — low risk, but the eventual
  publish must be coordinated across the 0.4.x/1.1.x train.
- **Branch model:** greentic-types is on `fix/types-main-1.1.3`, greentic-flow on
  `fix/flow-main-1.1.4` — SP3 branches off each submodule's current integration tip (confirm before
  branching), NOT research (research is a runner concept).
- **Worktree patch paths:** the local `[patch.crates-io]` must use paths that resolve from the
  runner-host worktree to the sibling greentic-types/greentic-flow checkouts (absolute paths are
  safest); it is never committed.
