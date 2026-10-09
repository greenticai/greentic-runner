# SP3 conversational-agent — 1.3-research lane cutover ledger

User chose the **1.3-research lane** for the whole foundation. Goal: land greentic-runner
**#540** (`feat/conversational-agent-sp3`, the SP3 `dw.agent` loader) on a coherent
`greentic-types 1.3.0-research.1` lineage.

## Hard rule (why the naive cascade broke)

Foundation crates use **exact pins** (`=X.Y.Z`). A crate published at `.1` that still pins
`greentic-types =1.3.0-research.0` is INCOMPATIBLE with `types 1.3.0-research.1` in the same
tree (unresolvable exact-pin conflict). So EVERY foundation crate the runner pulls must sit on
the **same** `types .1` lineage. crates.io versions are immutable → a mis-pinned published
crate needs a NEW version, not a fix-in-place.

## Ground truth (2026-07-08)

Correct on `types .1` lineage (research + published):
- greentic-types `1.3.0-research.1` ✅ · greentic-interfaces(+host/guest/wasmtime) `1.3.0-research.1` ✅
- greentic-session `1.3.0-research.1` ✅ · greentic-state `1.3.0-research.1` ✅
- greentic-config + greentic-config-types `1.3.0-research.1` ✅
- greentic-qa — **must go to `.1`, NOT stay `.0`**: `qa-lib .0 → component-qa .0 → interfaces-guest =1.3.0-research.0`, which conflicts with distributor `.1`'s `interfaces-guest =.1` in flow/pack trees. The transitive `interfaces-guest` exact-pin is the trap (qa-lib/qa-spec themselves have no direct interfaces dep, so an index check on them alone misses it — you must look at component-qa). So bump greentic-qa `.0 → .1` and pin qa-lib/qa-spec `=.1` in flow AND pack.

WRONG lineage — must be republished at a new version:
- **greentic-pack-lib `1.3.0-research.1`** pins `types =.0` + `flow =.0` → needs **pack `1.3.0-research.2`** pinning types .1 + flow .1.

Behind (`.0`, drag `types =.0`) — must go to `.1`:
- **greentic-distributor-client** `1.3.0-research.0` → `.1`
- **greentic-secrets** (secrets-lib/core) `1.3.0-research.0` → `.1`
- **greentic-flow** `1.2.0-research.2` → `1.3.0-research.1` (branch `chore/rejoin-1.3-research.1`, WIP)

## Cutover order (publish each before the next; verify sparse index)

1. **distributor-client** .0 → 1.3.0-research.1: bump own version + `types`/`interfaces-guest`/
   `config-types` pins → the published `.1`. build → PR research → merge → `git tag v1.3.0-research.1 <tip> && git push <https> v...`.
2. **flow** → 1.3.0-research.1 (branch `chore/rejoin-1.3-research.1`): own ver .1; `types`/
   `interfaces-host`/`interfaces-wasmtime` = `.1`; `qa-lib` = `.0`; **`distributor-client` = `.1`**
   (was mistakenly `.0` → the conflict that failed the first build); dropped the `[patch.crates-io]
   greentic-types` git-patch. build → merge → tag-publish.
3. **secrets** .0 → 1.3.0-research.1: workspace foundation pins (`types`/`config`/`config-types`/
   `interfaces*`) → `.1`. build → merge → tag-publish (publishes secrets-lib + core `.1`).
4. **pack** → 1.3.0-research.2: bump pack-lib version to `.2`; pins `types`/`flow` → `.1`,
   `config`/`config-types` → `.1`, `qa-lib` = `.0`, `distributor-client` → `.1`. build → merge → tag-publish.
5. **runner #540** (`feat/conversational-agent-sp3`, loader commit `5b9dcfe`): re-pin the whole
   foundation from `=1.2.0-research.0` → the coherent `.1` set — types/interfaces(+guest/host/
   wasmtime) `.1`, flow `.1`, session `.1`, state `.1`, config `.1`, config-types `.1`,
   secrets-lib `.1`, distributor-client `.1`, greentic-pack-lib **`1.3.0-research.2`**; leave
   telemetry range as-is (out of lane). Drop the types + flow git-patches. Build
   `cargo build -p greentic-runner-host --features agentic-worker`. Merge #540.

## Publish mechanism

research does NOT auto-publish on merge. After merge: `git fetch origin research` then
`git tag vX.Y.Z-research.N <research-tip> && git push https://github.com/greenticai/<repo>.git vX.Y.Z-research.N`
→ the tag fires `crates-publish.yml` (idempotent; skips already-published). Verify via the
sparse index `https://index.crates.io/<aa>/<bb>/<crate>` (the JSON API lags minutes).
Confirm a downstream dep's `greentic-types` req with the index `deps[]` before pinning it.

## Expanded closure (discovered when building runner-host) — STILL TODO before #540

The runner tree pulls MORE crates that sit on the stale `types .0` lineage. Each must go to
`.1` (exact-pin drag). Remaining, in order:

1. **greentic-component → 1.3.0-research.1**: published only at `.0` (pins `types =.0`).
   dw-providers pins it. **Local submodule is UNINITIALIZED** (`git submodule status` shows
   leading `-`) → `git submodule update --init greentic-component` (or clone) before editing.
2. **greentic-mcp → 1.3.0-research.1**: research pins `types =.0` + `interfaces-wasmtime =.0`
   (own ver `1.3.0-research.0`). Bump both + own → `.1`. NOTE runner git-patches
   `greentic-mcp-exec` to a greentic-mcp branch rev — after publishing mcp `.1`, either move
   that patch rev or drop it if the registry `.1` suffices.
3. **greentic-dw-providers research → coherent .1/.2** (local sibling, **push via SSH**
   `git@github.com:greenticai/greentic-dw-providers.git`, branch `research`, tip `8545497`):
   lines ~104-108 `greentic-component`/`greentic-types`/`greentic-interfaces`/`greentic-state`
   `=1.3.0-research.0 → =.1`, `greentic-pack =1.3.0-research.1 → =.2`. Runner pulls 6 crates
   from it by git branch (`greentic-dw-embedding`, `-openai-compatible`, `-memory-chronicle`,
   `-llm`, `-llm-openai-compatible`, `-providers-common`).
4. **runner #540**: foundation re-pins ALREADY STAGED in worktree
   `.worktrees/conversational-sp3/Cargo.toml` (flow/session/state/config/config-types/
   secrets-lib/distributor/interfaces* = `.1`, types = `.1`, pack-lib = `.2`). The runner's
   `[patch.crates-io]` keeps `greentic-extension-sdk-contract` + `greentic-mcp-exec` (NOT
   foundation). After (1)-(3): `cargo update -p greentic-dw-embedding …` to move the git dep
   onto the new dw-providers research commit, then `cargo build -p greentic-runner-host
   --features agentic-worker`, then merge #540.

SAFETY NOTE: `greentic-component` is an uninitialized submodule; running git there falls
through to the PARENT monorepo. Verified this session that no parent state was harmed
(stray `git stash -u` was a no-op; `stash@{0}` "research: park tracked submodule-pointer…"
is a PRE-EXISTING human stash — do NOT drop it).

## BLOCKER discovered at runner #540 resolve — greentic-ext-runtime (designer-extensions)

`greentic-runner-host` (agentic-worker feature) pulls **`greentic-ext-runtime`** as a git dep:
`crates/greentic-runner-host/Cargo.toml:99` →
`git = "https://github.com/greentic-biz/greentic-designer-extensions", rev = "c47a0738…"`.
That rev is `greentic-ext-runtime v1.2.24` pinning `greentic-types = "=1.2.0-research.1"` →
conflicts with the `types .1` lineage. The canonical repo `greenticai/greentic-designer-extensions`
research tip (`e979160`) is on `1.3.0-research.0` (NOT `.1`), and the runner pins the **greentic-biz
MIRROR at a specific rev** — so finishing #540 needs: (a) designer-extensions research (its whole
workspace + transitive SDK deps) moved to `types 1.3.0-research.1`; (b) that landed on the
greentic-biz mirror (or the runner repointed to greenticai); (c) the runner's ext-runtime `rev`
bumped to the new `.1` commit. Depth beyond that is unquantified (ext-runtime pulls the extension
SDK + wasmtime stack). This is a further cross-repo/fork migration, NOT a mechanical pin bump —
paused here for a user decision.

### Runner #540 staged so far (in the worktree, uncommitted)
- Foundation workspace pins → `.1` (flow/session/state/config/config-types/secrets-lib/distributor/
  interfaces*/types), `greentic_pack` → `=1.3.0-research.2`.
- `[patch.crates-io] greentic-mcp-exec` rev → `9cb2a3e…` (mcp research `.1` tip).
- `crates/greentic-aw-runtime/Cargo.toml:60` `greentic-mcp-exec` → `=1.3.0-research.1`.
- STILL TODO on #540 after ext-runtime is resolved: `cargo update` the 6 `greentic-dw-*` git deps to
  dw-providers research tip `8a9d6b5` (currently lock-stuck at old `b29a7a6c`), then
  `cargo build -p greentic-runner-host --features agentic-worker`, then merge.
- Cargo.lock was regenerated-then-restored from `/tmp/runner_cargo.lock.bak` (worktree intact).

## FINAL two blockers to #540 (version cutover DONE; these are code/feature)

The runner #540 dependency graph now RESOLVES CLEANLY on the coherent types-.1 lineage
(`cargo generate-lockfile` EXIT 0). Two non-mechanical blockers remain:

### Blocker A — mcp-exec `list_tools`/`ToolDef` reverted on mcp research
`greentic-aw-runtime/src/mcp_local.rs:13` imports `ToolDef` + `list_tools` from
`greentic-mcp-exec`. The old runner patch pinned mcp rev `f0d2f02b` **because it carries the
public `list_tools` API** (feat commit `f40fdce` "expose public list_tools for wasix:mcp
routers"). But mcp **research REVERTED that feature**: `5b1332a Revert "Merge …
feat/local-wasm-mcp-transport"`. So mcp research `.1` (my bump) has NO public `list_tools`/
`ToolDef` (only the crate-private `try_list_tools_router` in `router.rs`). Moving the runner's
mcp-exec patch to research `.1` (types-.1) therefore breaks the local-wasm MCP transport
compile. Resolution options (a DECISION):
  1. Re-apply (un-revert) the local-wasm-mcp-transport feature on greentic-mcp research, bump
     to a new `.1`, republish; point the runner patch there. (Keeps the shipped feature.)
  2. Cut a greentic-mcp branch off the `.1` tip that cherry-picks `f40fdce` (public list_tools)
     and point the runner's `[patch]` rev at it (mirrors the original f0d2f02b arrangement, now
     on types-.1).
  3. Change the runner's `mcp_local.rs` to use the crate-private router path instead of the
     removed public API — a code change on the SP3 branch.

### Blocker B — greentic-dw research clean merge + greentic-dw-authoring
`greentic-dw` sdk-pin bump is on branch `chore/adopt-sdk-1.3.0-research.1` (commit `4490278`,
pushed). The runner only needs `greentic-dw-manifest` (uses the workspace sdk pin → `.1`,
clean), so the runner is temporarily pinned at rev `4490278` (aw-runtime line 45). But
greentic-dw's FULL workspace build fails: its `greentic-dw-cli` member pulls
`greentic-dw-authoring` (git rev `645980b7`, **NOT cloned locally**) which still pins
`sdk-contract =1.2.19-research`. To land greentic-dw research cleanly (so the runner can use
`branch = "research"` again instead of the temp rev), greentic-dw-authoring must also adopt
sdk `.1` (another repo, may chain further). The runner does NOT need authoring.

### Runner #540 worktree state (staged, uncommitted, VERIFIED to resolve)
`.worktrees/conversational-sp3`: Cargo.toml (foundation `.1`, pack `.2`, sdk-contract patch tag
`v1.3.0-research.1`, mcp-exec patch rev `9cb2a3e`), `crates/greentic-runner-host/Cargo.toml:99`
ext-runtime rev `82d1bb2`, `crates/greentic-aw-runtime/Cargo.toml` (mcp-exec `=1.3.0-research.1`
L60, sdk-contract `=1.3.0-research.1` L51, ext-runtime rev `82d1bb2` L50, dw-manifest TEMP rev
`4490278` L45), regenerated Cargo.lock. `cargo build -p greentic-runner-host --features
agentic-worker` gets PAST resolution and fails ONLY on Blocker A (mcp_local.rs imports).

## ✅ RUNNER #540 BUILDS GREEN
`cargo build -p greentic-runner-host --features agentic-worker` = EXIT 0 on the coherent
types-.1 lineage, with mcp `list_tools` restored (mcp 1.3.0-research.2) and the SP3
`Node.conversational` field integrated (added `conversational: false` to the two pre-SP3 Node
literals: `pack.rs:2945`, `runner/flow_adapter.rs:178` — the flag flows via the greentic_flow
parse path, not these adapters). Runner worktree currently pins greentic-dw-manifest at TEMP
rev `4490278` (my greentic-dw branch), mcp-exec patch rev `a4ad9e4` (mcp .2), aw-runtime
mcp-exec pin `=1.3.0-research.2`.

## REMAINING for a clean #540 merge — greentic-dw structural tangle (needs a decision)
To flip the runner from the temp rev to `greentic-dw-manifest { branch = "research" }`,
greentic-dw research must carry the sdk-.1 bump. But greentic-dw's workspace sdk pin is shared:
`greentic-dw-manifest` (needs `=1.3.0-research.1` for the runner) uses `sdk-contract =
{ workspace = true }`, and so does `greentic-dw-cli` → which pulls `greentic-dw-authoring`
(rev `645980b7`, sdk `1.2.19`). Bumping the workspace pin to `.1` breaks cli/authoring;
leaving it at `1.2.19` breaks the runner. And **greentic-dw-authoring itself won't build on
.1**: it has a git dep on `greentic-aw-runtime` (the runner's own crate) → circular, plus its
own types `1.2.0-research.1` + sdk `1.2.19` pins. This is pre-existing greentic-dw structural
debt, NOT needed by the runner (runner only pulls `greentic-dw-manifest`, which is clean on
sdk `.1`). Options to land #540: (a) merge greentic-dw's sdk bump to research and pin the
runner at `branch = "research"`, accepting greentic-dw's `cli` member goes red until authoring
is untangled (follow-up); (b) decouple `greentic-dw-manifest` to pin sdk-contract directly
(not workspace) so the greentic-dw workspace can stay 1.2.19 for cli/authoring; (c) keep the
runner pinned at a MERGED greentic-dw research commit by rev (not branch). authoring bump WIP:
cloned sibling `greentic-dw-authoring`, branch `chore/adopt-1.3.0-research.1` (types/sdk → .1)
— build blocked on the aw-runtime circular dep.

## Done this session
- session/state/config(+config-types) `.1` PR'd+merged+tag-published (real progress).
- qa: redundant `.1` PR #71 CLOSED + mis-pointed tag DELETED (qa stays `.0`).
- flow branch `chore/rejoin-1.3-research.1` committed+pushed but build FAILED (distributor pin
  was `.0` → types-.0 conflict). Fix = step 2's distributor `.1` pin, then rebuild.
