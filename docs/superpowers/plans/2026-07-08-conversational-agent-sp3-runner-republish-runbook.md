# SP3 runner adoption — coordinated republish runbook

**Context:** SP1+SP2 are merged to runner `research`. The SP3 `conversational` flag is authored
(greentic-types field, greentic-flow parse) and the runner loader one-liner is ready. The runner
cannot compile against the field yet because of a version-lane tangle. This runbook is the ordered
sequence to finish it. Everything below stays on the **1.2.x-research** lane (per directive).

## Done (already landed)

- **greentic-types** research → `1.2.0-research.2` (`Node.conversational` + CBOR round-trip). PR #171
  (2465a7b). Reverted the bad #168 bump that had jumped it to `1.3.0-research.0`.
- **greentic-flow** research → `1.2.0-research.2` (parses the `conversational` node key; loader
  `reserved` lists updated). PR #275 (fcad2dd). Reverted the bad #272 `1.3.0-research.0` bump; dep
  pins restored to `interfaces-host/qa-spec/qa-lib/interfaces-wasmtime = =1.2.0-research.1`,
  `greentic-types/distributor-client = =1.2.0-research.2`; carries a temporary
  `[patch.crates-io] greentic-types = { git rev 2465a7b }` until types publishes.
- **greentic-runner** loader read `conversational: node.conversational` — commit 5b9dcfe on branch
  `feat/conversational-agent-sp3` (draft PR #540). Trivial field read; compiles once the field is
  reachable.

## Root cause of the remaining block

The runner pins its ENTIRE foundation at `=1.2.0-research.0` (session/state/config/interfaces/pack/
distributor) + greentic-types `=1.2.0-research.1`. Empirically bumping the runner to consume
types/flow `1.2.0-research.2` resolves the interfaces/session/state/config layer (their research is
`1.2.0-research.1` and already pins types `=1.2.0-research.2`), but then hits:

```
error: failed to select a version for `greentic-types`  (and `greentic-flow`)
    ... required by package `greentic-pack-lib v1.2.0-research.0`
```

**`greentic-pack-lib` is the wall.** The published `greentic-pack-lib 1.2.0-research.0` pins the OLD
greentic-types + greentic-flow, and the `greentic-pack` repo's research branch is on the
**`1.1.0-research.0`** lane — i.e. there is no pack version that consumes types/flow `1.2.0-research.2`.
So the runner cannot adopt the field via any bounded `[patch.crates-io]`; pack must move onto the
1.2-research `.2` line first. `greentic-config-types` and `greentic-secrets-lib` are also not local
(cannot be git-patched ad hoc).

## Republish sequence (pipeline / release owner)

Publish in dependency order, all on the **1.2.x-research** lane; each step's crate pins the prior
step's just-published version.

1. **greentic-types `1.2.0-research.2`** → crates.io. (PR #171 merged; the code is on research.)
   Carries `Node.conversational`. Everyone downstream already pins `=1.2.0-research.2`.
2. **greentic-flow `1.2.0-research.2`** → crates.io. (PR #275 merged.) Once #1 is published, drop the
   temporary `[patch.crates-io] greentic-types` from greentic-flow (its `=1.2.0-research.2` pin now
   resolves from the registry) and publish.
3. **greentic-pack-lib → `1.2.0-research.2`**: bring `greentic-pack` off the `1.1.0-research.0` lane
   onto 1.2-research, bump its greentic-types + greentic-flow deps to `=1.2.0-research.2`, rebuild
   against the parse, publish `greentic-pack-lib 1.2.0-research.2`. (This is the crate that currently
   blocks the runner; it must consume the new flow to carry `conversational` through pack load.)
4. **Any other `.0`→`.2` foundation stragglers** the runner pins that still require old types/flow —
   e.g. confirm `greentic-config-types`, `greentic-secrets-lib`, `greentic-interfaces*`,
   `greentic-session/state/config`, `greentic-distributor-client` all have a `1.2.0-research`
   version pinning types `=1.2.0-research.2`, and publish any that don't. (interfaces/session/state/
   config research are `1.2.0-research.1` and already pin types `.2`; distributor research is
   `1.2.0-research.2`.)
5. **greentic-runner** (`feat/conversational-agent-sp3`, #540): bump the workspace pins to the
   `.2`-consistent set — `greentic-types = =1.2.0-research.2`, `greentic-flow = =1.2.0-research.2`,
   `greentic_pack = =1.2.0-research.2`, and interfaces/session/state/config/distributor to whichever
   `1.2.0-research.{1,2}` now resolves. Remove any leftover git-patches. `cargo build -p
   greentic-runner-host --features agentic-worker` must resolve + compile. Then merge #540 to research.

## Verify after the runner lands

Reuse the SP2 park-loop path end-to-end: a flow doc with `conversational: true` on a `dw.agent` node
→ greentic-flow parses it → `greentic_types::Node.conversational = true` → runner loader
`NodeKind::DwAgent.conversational = true` → SP2 `NodeControl::LoopHere` parks/loops until
`end_conversation`. (SP4 adds the designer toggle that authors the flag.)

## Note

Alternatively, if the whole research foundation is intended to move to `1.3-research`, do that
migration for the FULL graph (including pack) instead — but the directive was to keep 1.2.x-research,
so this runbook stays on 1.2.
