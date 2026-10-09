# SP3 `.3` foundation cutover runbook (publish the conversational field)

**Supersedes** the earlier `…-runner-republish-runbook.md`, which assumed the field could ship at
`1.2.0-research.2`. It cannot: **`greentic-types 1.2.0-research.2` is already published on crates.io
WITHOUT `Node.conversational`** (published before the field landed; crates.io versions are
immutable). The whole research foundation pins `greentic-types = "=1.2.0-research.2"`, so it builds
against that fieldless crate. Shipping the field requires a NEW version, `1.2.0-research.3` (verified
free), rolled through the entire foundation atomically.

## State (done)

- **greentic-types**: research bumped to `1.2.0-research.3` (+ `Node.conversational`). PR #172 MERGED
  (research `9d71cde`). CI should auto-publish `greentic-types 1.2.0-research.3` (~80 min dev-publish
  cadence). **This is the first domino.**
- SP1/SP2 merged; greentic-flow parse merged (research, still carries a temp
  `[patch.crates-io] greentic-types = git`); SP4 designer green (#972); designer dep-skew fixed (#973).

## Cutover order (each: bump `greentic-types` pin `=1.2.0-research.2 → =1.2.0-research.3`, bump own
## version to `1.2.0-research.3`, bump any sibling-foundation pins to `.3`, PR → merge → CI publishes).
## Do NOT start a crate until every dep it pins is published at `.3`.

1. **greentic-types → 1.2.0-research.3** — DONE (#172). Wait for the crates.io publish.
2. **greentic-flow → 1.2.0-research.3**: bump own version + `greentic-types` pin → `.3`; **DROP the
   temporary `[patch.crates-io] greentic-types = { git … }`** (now that types .3 publishes, the pin
   resolves from the registry); bump its other foundation pins
   (`greentic-interfaces-host/qa-spec/qa-lib/interfaces-wasmtime/distributor-client`) to their `.3`
   once those publish. Build clean (no patch), PR → merge → publish.
3. **greentic-interfaces (+ -host/-guest/-wasmtime), greentic-session, greentic-state,
   greentic-config, greentic-distributor-client**: each bumps own version + `greentic-types` pin →
   `.3` (+ any inter-foundation pins). These are LOCAL submodules. Order among them by their own
   dep edges (interfaces before session/state/config; distributor after interfaces).
4. **greentic-config-types, greentic-secrets-lib**: same bump — **NOT local submodules here**; the
   release owner must PR/publish these. The runner pins both, so it cannot resolve `.3` until they are
   published at `.3`.
5. **greentic-pack** (repo research = `1.2.0-research.4`; member crate `greentic-pack-lib`): bump its
   `greentic-flow` pin `=1.2.0-research.1 → =1.2.0-research.3` (+ types `.3`), bump the member/pack
   version, PR → merge → publish `greentic-pack-lib` at the new version. (This is the crate whose old
   flow pin blocked the runner in the earlier empirical test.)
6. **greentic-runner #540** (branch `feat/conversational-agent-sp3`, loader commit `5b9dcfe`): once
   every foundation crate above is published at `.3`, bump the runner workspace pins —
   `greentic-types = "=1.2.0-research.3"`, `greentic-flow = "=1.2.0-research.3"`,
   `greentic_pack`/`greentic-distributor-client`/`greentic-interfaces*`/`greentic-session`/`-state`/
   `-config`/`-config-types`/`-secrets-lib` to their published `.3` — remove any git-patches, then
   `cargo build -p greentic-runner-host --features agentic-worker` must resolve + compile. Merge #540.

## Verifying a downstream crate BEFORE its deps publish (optional)

To build/verify a crate against the not-yet-published `.3` of a dependency, add a temporary
`[patch.crates-io] <dep> = { git = "…", rev = "<research .3 commit>" }` (the crates.io version at that
rev must equal the pin, e.g. `1.2.0-research.3`). Drop the patch before the real publish (it is a
build-time override only; crates.io strips `[patch]` on publish, so a published crate would otherwise
depend on the registry version that doesn't exist yet). This is the pattern greentic-flow used for
types; it is why flow's publish is currently stuck (its committed git-patch must be dropped once
types `.3` is on the registry — step 2).

## Blockers for autonomous execution (why this is a release-owner op)

- **No crates.io token** in this environment (`~/.cargo/credentials` absent) → cannot `cargo publish`.
  Publishing happens via CI on merge-to-research, or with the owner's token.
- **`greentic-config-types` / `greentic-secrets-lib` are not local submodules** → cannot be PR'd here.
- The already-published fieldless `1.2.0-research.2` should probably be `cargo yank`ed (owner decision)
  so nothing new resolves the fieldless crate.

## Net

All feature code (SP1–SP4 + the designer dep-skew fix) is merged/green. What remains is purely this
`.3` version cutover + publish chain — a coordinated release across ~10 crates (2 non-local) gated on
CI publishes and a crates.io token. greentic-types `.3` (the keystone) is already merged and
publishing; the rest follows the order above as each publish completes.
