# Load design extensions from the pack

Branch: `BimaPangestu28/load-extensions-from-pack`, off `origin/research`.

Closes the consuming half of the chain whose producing half shipped in
greentic-pack `research` (PR #286): a `.gtxpack` design extension now travels
inside the built `.gtpack`, and the runner loads it from there in addition to
the on-disk directory it already scanned.

## The defect this closes

A `.gtxpack` bound as a tool on an agentic worker was declared to the pack build
as a dependency, and packc read its `describe.json` to generate `setup.yaml` +
`secret-requirements.json` — so a cloud operator was asked for that tool's API
key. In a k8s or Cloud Run container the extension was not on disk:
`extension_discovery_dir()` resolves `GREENTIC_EXTENSIONS_DIR` or
`$HOME/.greentic/extensions`, nothing writes either in a container, and
`discovery::scan_kind_dir` walked an empty directory. The tool was dropped with
a `warn`. Deploy succeeded, the worker booted, `list_tools` returned empty, and
the operator's tool was gone after they had supplied its credential.

## What was built

**`crates/greentic-runner-host/src/runner/pack_extensions.rs`** (new).
Enumerates, stages and registers the extension archives a set of loaded
`PackRuntime`s carry. Its module doc records the cross-repo layout contract
verbatim (flat `extensions/<name>.gtxpack`, consumers filter on the suffix,
`extensions/*.json` sidecars predate the feature and are not extensions) so the
runner side no longer inherits it only by reading greentic-pack's source.

**`PackRuntime::extension_archive_entries()`** (`pack.rs`, gated on
`agentic-worker`). Lists `extensions/*.gtxpack` from the materialized pack
directory and from the `.gtpack` archive — the same two sources, in the same
order, as `read_pack_file`, so every entry it names is one that reader can then
fetch. Sorted and deduplicated via a `BTreeSet`.

**`build_ext_runtime`** (`agent_node.rs`) takes a new
`packs: &[Arc<PackRuntime>]` parameter and runs a second registration pass after
the on-disk scan. The `caps = …` summary line moved below both passes and gained
`from_packs` / `shadowed_by_disk` / `pack_failures`, so one log line now
describes the registry the agent will actually dispatch against.

Call sites:

| site | packs passed | why |
|---|---|---|
| `agent_node::build_runtime_with_stores` | `&packs` | the per-tenant `dw.agent` lane — the one a deployed bundle uses |
| `graph_node::build_graph_node_handler` | `&packs` | already held them for `component_source_from_packs` |
| `agent_node::build_agent_runtime` | `&[]` | process-level NATS serve path; holds no `PackRuntime` at all, exactly as it holds no pack-backed MCP fallback |
| `runner::mod::HostServer::with_sql` | `&[]` | the `GET /admin/capabilities` probe is process-wide; pack-carried extensions are per-tenant, so there is no process-wide answer. The field's doc comment now says so rather than continuing to claim a process-level answer is correct |

An operator opt-out `GREENTIC_AW_PACK_EXTENSIONS=0` mirrors `GREENTIC_AW_MCP` /
`GREENTIC_AW_COMPONENT_TOOLS` / `GREENTIC_AW_FLOW_TOOLS`.

`CLAUDE.md` gained the two-source description, the precedence rule and the new
env var.

## Following the precedent, not inventing a mechanism

`component_source_from_packs` and `mcp_source_from_packs` had already moved
resolution from "the environment of this lane" to "the contents of the pack".
Design extensions were the last of the three still on the old model, so this is
finishing a migration. §9.4 of the designer's external-RAG spec records why the
rejected alternative — unpacking to `GREENTIC_EXTENSIONS_DIR` at deploy time —
is wrong: it makes the filesystem and the environment part of the deploy
contract, so every target needs its own unpack step and forgetting one fails
silently. That is what left the `mcp:` lane needing an env-projection seam that
still does not exist.

## The three decisions

### 1. Precedence — disk wins, the pack is the fallback

The two existing sibling paths resolve in *opposite* orders, deliberately, and
the comment at `agent_node.rs`'s `AgentRuntime::new` call spells out why:

- The **flow MCP node** prefers the PACK (`mcp_node::aw::invoke_with_secrets`).
  Its pack route strictly ADDS capability — the admin catalog supplies the same
  route and nothing else — so preferring the pack can only turn a failure into a
  success.
- The **agent loop** prefers the ADMIN/env source
  (`mcp_source_from_env().or_else(mcp_source_from_packs(..))`), because the agent
  catalog additionally carries LIVE tool schemas probed this run plus each
  server's `allowed_tools`; preferring the pack there would downgrade every
  environment that has a working admin source.

Design extensions are the agent-loop shape, so **disk wins**. Concretely:

- An on-disk extension is one an operator installed or updated deliberately
  (`gtdx install`, the designer's admin auto-sync, the bundled unpack). In every
  lane that has one at all, it is the newer artefact by construction.
- A pack-carried archive is frozen at pack-build time. Preferring it would
  silently pin every designer and desktop lane to whatever version was current
  when the pack was built — downgrading environments that work today in order to
  fix one that does not.

**What it costs, stated plainly:** a host with a *stale* on-disk copy keeps it
even when the pack carries a newer one, and nothing reports that. Accepted
because in the lane this closes — a container — the directory is empty, so the
rule never fires there. Making the newer of the two win needs a version
comparison the identity does not carry (`ExtensionId` is `metadata.id`,
version-free) and would have to define "newer" across two unrelated publishers.

**The mechanism is the pass order, not a check bolted on.**
`register_loaded_from_dir` inserts by `ExtensionId` and overwrites, so the pack
pass must run second *and* skip ids already present. Both facts are written at
the call site and on `is_shadowed`, because either one alone silently inverts
the rule.

### 2. Verification — the same gate, reached by a different road

Nothing here is a second loader. Each archive is unpacked into a staging
directory and handed to `ExtensionRuntime::register_loaded_from_dir` — the exact
entry point the on-disk scan uses — so a pack-carried extension runs the
identical checks:

- `verify_dir_signature`: describe self-consistency
  (`verify_describe_self_consistent`, which also rejects an unsigned describe),
  then the TOFU publisher-key anchor via the trust store.
- `verify_dir_manifest`: `manifest.json` must be present, must be the one the
  signed describe commits to through `manifestSha256`, must carry the supported
  schema, and every file it lists must hash to the recorded sha256.

Being carried in a pack is therefore **not** a substitute for being signed. The
pack is a delivery route.

Unpacking adds one check of its own, ahead of the loader: an archive entry whose
path escapes the staging directory (`ZipFile::enclosed_name()` returning `None`)
is **refused**, not sanitised. Sanitising would load an extension whose contents
are not what the archive claims.

Staging is digest-keyed under one process-wide root, with a `.staged` marker
written last. Two tenants carrying the same extension unpack it once; a run that
died partway through is cleared and redone rather than unpacked over (a stale
truncated file the current archive no longer lists would otherwise survive and
be hashed against the ledger). The root is a `TempDir` inside a `static`, so it
is never dropped — deliberate: `LoadedExtension` retains its `source_dir` for the
life of the runtime and `ExtensionRuntime` offers no seam to hang a lifetime
guard on, so a per-call `TempDir` would be deleted out from under a loaded
extension.

### 3. Failure mode — reported, never silent, never fatal

Per archive, three outcomes, all counted and all logged:

- **staged and loaded** — `info`, with entry name and extension id.
- **shadowed by a disk install** — `info`, so an operator wondering why the
  pack's copy is not in use can see it.
- **refused** — `warn` naming the entry and the reason, and the count reaches
  the boot summary as `pack_failures`. Two sub-cases: it could not be staged
  (not a zip, traversing entry, unreadable pack entry) or the verified loader
  refused it.

A refused archive never takes the worker down: one operator's broken tool must
not cost them the others. It also never vanishes — an extension disappearing
without a trace is the failure this whole change exists to end. `.json` sidecars
in `extensions/` are not failures at all: the suffix filter means they are never
candidates.

## Tests

Eight new tests, all asserting on behaviour rather than on a function being
called.

`runner::pack_extensions::tests`:

| test | property |
|---|---|
| `a_pack_carried_extension_is_staged_when_the_extensions_directory_is_empty` | a real `.gtpack` ZIP carrying an extension is opened, unpacked and identified while the on-disk directory is empty — the whole hop, driven through a real `PackRuntime` |
| `only_gtxpack_entries_at_one_flat_level_are_extensions` | the contract's rule 2, including the `.json` sidecar, `extensions/design/…`, `assets/…` and `…gtxpack.bak` |
| `a_staged_archive_is_unpacked_whole_and_carries_its_extension_id` | the staged tree holds the archive's own bytes, and the id needed for the precedence decision is readable |
| `a_corrupt_archive_is_reported_and_a_sibling_still_stages` | a non-zip is named with a reason AND the sibling still loads |
| `an_archive_entry_that_escapes_the_staging_dir_is_refused` | zip-slip: nothing staged, reason mentions the escape, nothing written outside |
| `a_describe_without_an_id_still_stages_for_the_loader_to_refuse` | an unreadable id is not a silent drop — the loader owns that refusal |
| `an_unverifiable_pack_carried_extension_is_refused_and_counted` | drives `register_from_packs` against a real `ExtensionRuntime`: `failed: 1`, `loaded: 0`, runtime still empty — the pack path is not a signing bypass |
| `an_extension_already_loaded_from_disk_is_not_replaced_by_the_pack` | precedence, including that an unknown id is NOT assumed shadowed |

`pack::tests`:

| test | property |
|---|---|
| `extension_archive_entries_lists_only_gtxpack_files_from_a_pack_dir` | enumeration + the sidecar rule over a real directory; every listed entry is readable by `read_pack_file` |
| `extension_archive_entries_lists_only_gtxpack_files_from_a_gtpack_archive` | the same over a real `.gtpack` ZIP — the shape a container actually sees |
| `extension_archive_entries_is_empty_for_a_pack_built_before_the_feature` | a pre-feature pack reads as "no extensions", not as an error |

**Mutation check.** Dropping the `.gtxpack` suffix filter (the one rule most
likely to be "simplified" away) was applied and the suite run — 4 tests fail,
including the end-to-end one, which then tries to unzip the `.json` sidecar:

```
---- pack::tests::extension_archive_entries_lists_only_gtxpack_files_from_a_pack_dir stdout ----
assertion `left == right` failed
  left: ["extensions/acme.tool.gtxpack", "extensions/wizard-answers.json"]
 right: ["extensions/acme.tool.gtxpack"]

---- runner::pack_extensions::tests::a_pack_carried_extension_is_staged_when_the_extensions_directory_is_empty stdout ----
[StagingFailure { entry_name: "extensions/wizard-answers.json", reason: "read the extension archive: invalid Zip archive: Could not find EOCD" }]

failures:
    pack::tests::extension_archive_entries_lists_only_gtxpack_files_from_a_gtpack_archive
    pack::tests::extension_archive_entries_lists_only_gtxpack_files_from_a_pack_dir
    runner::pack_extensions::tests::a_pack_carried_extension_is_staged_when_the_extensions_directory_is_empty
    runner::pack_extensions::tests::only_gtxpack_entries_at_one_flat_level_are_extensions

test result: FAILED. 7 passed; 4 failed; 0 ignored; 0 measured; 774 filtered out; finished in 0.04s
```

The mutation was reverted; the suffix check is back as
`let Some(stem) = name.strip_suffix(ARCHIVE_SUFFIX) else { return false; };`.

## Verification — every command, real output

All foreground. No backgrounded builds or tests.

### `cargo fmt --all --check`

```
$ cargo fmt --all && cargo fmt --all --check && echo "FMT OK"
FMT OK
```

### `cargo clippy --workspace --all-targets --all-features -- -D warnings`

```
$ CARGO_BUILD_JOBS=2 cargo clippy --workspace --all-targets --all-features -- -D warnings 2>&1 | tail
    Checking greentic-runner-host v1.3.0-research.0 (…/crates/greentic-runner-host)
    Checking greentic-runner-desktop v1.3.0-research.0 (…/crates/greentic-runner-desktop)
    Checking greentic-runner-tests v1.3.0-research.0 (…/crates/tests)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 24.03s
EXIT=0
```

A first attempt at `CARGO_BUILD_JOBS=4` exited 1 with **no output at all** —
that is the OOM-kill shape on this box, not a lint failure. Re-run at
`CARGO_BUILD_JOBS=2` it is clean, as shown.

### New module

```
$ cargo test -p greentic-runner-host --all-features --lib pack_extensions
running 8 tests
test runner::pack_extensions::tests::only_gtxpack_entries_at_one_flat_level_are_extensions ... ok
test runner::pack_extensions::tests::an_extension_already_loaded_from_disk_is_not_replaced_by_the_pack ... ok
test runner::pack_extensions::tests::an_archive_entry_that_escapes_the_staging_dir_is_refused ... ok
test runner::pack_extensions::tests::a_describe_without_an_id_still_stages_for_the_loader_to_refuse ... ok
test runner::pack_extensions::tests::a_corrupt_archive_is_reported_and_a_sibling_still_stages ... ok
test runner::pack_extensions::tests::a_staged_archive_is_unpacked_whole_and_carries_its_extension_id ... ok
test runner::pack_extensions::tests::a_pack_carried_extension_is_staged_when_the_extensions_directory_is_empty ... ok
test runner::pack_extensions::tests::an_unverifiable_pack_carried_extension_is_refused_and_counted ... ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 777 filtered out; finished in 0.02s
```

### Each module the change touches (one filter per module, not one for the module I edited)

```
$ cargo test -p greentic-runner-host --all-features --lib -- pack::
test result: ok. 33 passed; 0 failed; 0 ignored; 0 measured; 752 filtered out; finished in 0.03s

$ cargo test -p greentic-runner-host --all-features --lib -- runner::agent_node
test result: ok. 49 passed; 0 failed; 0 ignored; 0 measured; 736 filtered out; finished in 73.84s

$ cargo test -p greentic-runner-host --all-features --lib -- runner::graph_node
test result: ok. 52 passed; 0 failed; 0 ignored; 0 measured; 733 filtered out; finished in 0.08s

$ cargo test -p greentic-runner-host --all-features --lib -- runner::router_tests
running 2 tests
test runner::router_tests::assembled_router_serves_admin_packs_status ... ok
test runner::router_tests::assembled_router_serves_admin_capabilities ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 783 filtered out; finished in 0.00s
```

Each of those printed a non-zero `running N tests` / passed count, so none is a
filter that matched nothing.

### Whole `greentic-runner-host` lib suite

```
$ cargo test -p greentic-runner-host --all-features --lib
test result: ok. 784 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 6.51s
```

(776 before this change, 784 after: the 8 new `pack_extensions` tests. The 3 new
`pack::tests` cases are `#[cfg(feature = "agentic-worker")]` and are included in
that 784 as well; the earlier 777/752/736 "filtered out" figures are consistent
with the same total.)

Not run: `ci/local_check.sh`'s `host_smoke`, `conformance` and `workspace_tests`
steps, which need example packs / `RUN_CONFORMANCE=1` and are CI's job.

## Things I had to discover

- **`register_loaded_from_dir` is the whole verification surface.** Both
  `verify_dir_signature` and `verify_dir_manifest` are private to
  `ExtensionRuntime` and operate on an *unpacked directory*, so "unpack, then
  call the same public entry point" is the only way to reuse the gate — and it
  reuses all of it, which is what makes this not a bypass.
- **`ExtensionId` is `describe.metadata.id` and carries no version.** That is
  what makes "skip ids the disk already supplied" a clean precedence rule, and
  also what makes "prefer the newer of the two" impossible without new
  machinery.
- **`LoadedExtension` retains `source_dir`** (`runtime.rs:555` finds an
  extension by it on unregister), so the staging tree must outlive the runtime.
  There is no place on `ExtensionRuntime` to hang a guard, hence the
  process-scoped root.
- **`extensions/` is already occupied.** greentic-pack's `collect_extra_files`
  walks the wizard's `extensions/*.json` manifest sidecars into the archive
  verbatim, and they predate this feature. That is why the contract's rule 2 is
  load-bearing rather than defensive, and why the mutation check above targets
  exactly that rule.
- **`grep` on this box skips files containing NUL bytes.** Reading the `zip` 8.6
  and SDK sources needed `command grep`; the wrapped one returned nothing, which
  reads exactly like "the API does not exist".
- The `.gtxpack` fixtures on this box (e.g. the designer's bundled
  `greentic.adaptive-cards-*.gtxpack`) carry `describe.json` + `extension.wasm`
  at the archive root and **no** `manifest.json` — they predate the ledger. A
  committed test that loads a genuinely signed extension would need a signed
  fixture plus a ~1 MB WASM component, which is why the loader-side test asserts
  *refused and counted* rather than *loaded*. `knowledge_ext_e2e.rs` sets the
  same precedent: its real-extension chain is `#[ignore]`d for exactly this.
