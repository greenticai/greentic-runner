# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

greentic-runner is the production runtime for the Greentic platform. It loads `.gtpack` archives containing WebAssembly components and flow definitions, normalizes inbound messaging-provider events (Telegram, Teams, Slack, WebChat, Webex, WhatsApp, generic webhook, timer/cron) into flow runs, and executes flows with pause/resume session semantics. The workspace produces three binaries (`greentic-runner`, `greentic-runner-cli`, `greentic-gen-bindings`) and a library crate (`greentic_runner`). Workspace version: see `[workspace.package]` in `Cargo.toml`.

## Build & Test

```bash
# Full local CI mirror (fmt → clippy → tests → package dry-run)
ci/local_check.sh

# Individual steps
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features

# Single test
cargo test -- test_name_here

# Crate-scoped tests
cargo test -p greentic-runner
cargo test -p greentic-runner-host

# Host smoke test (requires example packs)
RUN_HOST=always ci/local_check.sh

# Conformance suite
RUN_CONFORMANCE=1 ci/local_check.sh

# Heavy WASM fixture tests (the #[ignore]d test must be asked for explicitly)
cargo test -p greentic-runner --test contract_cli contract_pack_rejects_mismatched_artifact -- --ignored
# ...or, as part of the local CI mirror:
GREENTIC_HEAVY_WASM=1 ci/local_check.sh

# Select specific CI steps
LOCAL_CHECK_STEPS=fmt,clippy ci/local_check.sh
```

The `workspace_tests` step (and `host_wasi_p2_roundtrip`) needs `wasm-tools` on PATH (`cargo install wasm-tools --locked`); `ci/local_check.sh` fails early without it. `GREENTIC_HEAVY_WASM=1` has no effect on a bare `cargo test --workspace`: the heavy test is `#[ignore]`d, so it runs only when named with `-- --ignored` (as above), from the `crate_tests` step of `ci/local_check.sh` when that variable is set, or in the nightly `heavy_wasm` CI job.

`ci/local_check.sh` steps: `fmt`, `dependency_sanity`, `wit_sync` (vendored extension-provider WIT vs pinned upstream; soft-skips offline outside CI), `clippy`, `host_smoke`, `crate_tests`, `workspace_tests`, `conformance`, `package`.

The `dependency_sanity` step detects multiple wasmtime versions in the dependency tree (local workspace crates vs published greentic-* version skew) and fails early before clippy/tests hit confusing trait-mismatch errors.

GitHub CI (`ci.yml`) is thinner than `ci/local_check.sh`: it runs `cargo fmt --check`, `cargo clippy --all-targets --all-features --workspace --locked -- -D warnings`, a warning-only toolchain-drift check, and (nightly/dispatch only) the heavy WASM fixture tests. The clippy job is the compile gate — it builds every target including test code, so feature-gated code compiles on PRs — but **tests are not executed on PRs**; the full suite runs in Nightly Coverage against `main`. A red `ci/local_check.sh` can still be pre-existing on the base branch; reproduce on a pristine checkout before assuming your change caused it.

The compile gate exists on `develop` and `main` but **not on `research`**: research carries 38 git dependencies, 6 of them on the cross-org private `greentic-biz` org, so cargo dies at dependency-fetch there without a cross-org deploy key. `develop` and `main` have zero git dependencies (`grep -c 'source = "git' Cargo.lock` → 0), which is why the gate runs.

Note that `-p <crate>` is not a faithful reduction of a workspace build here: feature unification means `cargo clippy -p greentic-aw-runtime --all-features` can pass while the workspace build of the same crate fails, because `greentic-runner-host` turns on features that pull in extra code paths. Reproduce workspace failures with the workspace command.

## Workspace Layout

```
crates/
  greentic-runner/           # Binary + public library (thin CLI wrapper)
  greentic-runner-host/      # Core runtime (~29K lines): pack loading, flow engine,
                             #   ingress normalization (adapt_*), session/state, admin API
  greentic-runner-desktop/   # Desktop CLI integration
  runner-core/               # Pack resolution, signing verification, cache helpers
  greentic-aw-runtime/       # Agentic-worker runtime (Plan-Act-Observe loop) +
                             #   `serve` mode (NATS) + `aw-serve` test-mock bin
  aw-event-bridge/           # NATS bridge: consumes greentic.agentic.request.v1,
                             #   dispatches to AgentDispatchInvoker (agentic.call side)
  telco-x-event-bridge/      # NATS bridge: consumes greentic.telco-x.request.v1,
                             #   dispatches to TelcoXDispatchInvoker (telco-x.call side).
                             #   Phase 2 transport scaffold + EchoInvoker placeholder +
                             #   telco-x-serve bin (real ops = a TelcoXDispatchInvoker impl)
  greentic-a2a/              # A2A protocol client/types (agent-card cache, SendMessage)
  tests/                     # Integration test harness
```

The `greentic-runner` crate is the thin entrypoint. Almost all runtime logic lives in `greentic-runner-host`.

i18n is handled inline in runner-host (`crates/greentic-runner-host/src/runner/i18n.rs`), not as a separate workspace crate.

## Architecture

### Runtime Hierarchy

`RunnerHost` → manages multiple `TenantRuntime`s (one per tenant) → each owns `PackRuntime`s (loaded from `.gtpack` archives) → each contains Wasmtime `Component`s, flows, and templates.

### Flow Execution Pipeline

1. Ingress adapter normalizes raw provider payload into canonical schema (`Activity`)
2. Router selects tenant + flow via bindings config
3. Session key derived: `{tenant}:{provider}:{conversation}:{user}` — checked for suspended `FlowSnapshot`
4. `FlowEngine` interprets flow DAG, executing nodes in sequence
5. Each node may: invoke a WASM component, call a provider, emit a response, or `session.wait` (pause)
6. On `session.wait`: snapshot persisted → next inbound activity resumes from that point

### Agentic Workers (`dw.agent` node)

A flow node keyed `dw.agent.<agent_id>` (component `dw.agent`, operation = agent_id)
dispatches to the agentic-worker runtime (`crates/greentic-aw-runtime`, Plan-Act-Observe
loop). Gated behind the `agentic-worker` feature (default-on). Tools come from
`.gtxpack` design-extensions and from per-tenant MCP servers; the OpenAI backend
encodes tool function names as `<ext>_FN_<tool>` (OpenAI rejects dots).

Design extensions load from **two** sources, in this order
(`agent_node::build_ext_runtime`):

1. `GREENTIC_EXTENSIONS_DIR/design/` on disk (`discovery::scan_kind_dir`).
2. The loaded packs' own `extensions/*.gtxpack` entries
   (`runner::pack_extensions::register_from_packs`).

**Disk wins**; the pack is the fallback, same direction as
`mcp_source_from_env().or_else(mcp_source_from_packs(..))`. The order of the two
passes is what enforces it — `register_loaded_from_dir` inserts by
`ExtensionId`, so running the pack pass first would overwrite an operator's
installed copy with one frozen at pack-build time. See
`pack_extensions::is_shadowed` for the reasoning and the cost.

The pack source exists because nothing writes `GREENTIC_EXTENSIONS_DIR` inside a
k8s or Cloud Run container: the directory scanned there is empty, so every
extension tool an operator bound used to be dropped with a `warn` after the
deploy reported success and after they had already supplied its credential.
Unpacking happens BEFORE that gate, so `unpack_archive` is bounded (64 MiB/entry, 256 MiB total, 4096
entries; real bytes streamed through a `take`, not just the declared size), and the process-wide staging root
reclaims superseded digests of the same extension id and crashed partial unpacks after a 1h grace
(`sweep_stale_slots`; every stage re-touches its slot).
Pack-carried archives go through the SAME `register_loaded_from_dir` gate
(signature + `manifest.json` ledger) — the pack is a delivery route, not a
verification bypass. The in-pack layout is a cross-repo contract with
greentic-pack and is recorded at the top of `runner::pack_extensions`.
Needs a state backend — `memory` (default, ephemeral) / `disk` (redb) / `redis` (durable +
multi-instance), selected via `GREENTIC_AW_STATE_BACKEND` — plus an LLM key
(`GREENTIC_LLM_API_KEY`/`OPENAI_API_KEY`). When nothing is set the runtime auto-selects an
in-memory backend, so `dw.agent` runs with no Redis; multi-instance HA still requires `redis`
(the `memory`/`disk` backends give single-process locking only).
Worker config is an `AgentConfig` (`greentic-aw-runtime/src/config.rs`), supplied via pack
manifest, `<agent_id>.json` in `GREENTIC_AGENT_MANIFESTS_DIR`, or the admin endpoint.

An agent may be marked `conversational` (`AgentConfig.conversational`, default false).
The host `end_conversation` tool is offered when EITHER that config opts in OR the
invocation does — `AgentInput.conversational`, set from the flow node's `conversational`
flag (SP3). So a flow node marked conversational makes the agent able to end the segment
the engine park-loop maintains, even when the agent's own config default is false (the
node, not just the agent config, can opt in; the two are OR-ed in the loop as `conv_active`).
Conversational agents are offered a host built-in `end_conversation` tool (reserved `host`
extension id) plus a system-prompt note; when the model calls it, the loop terminates the
turn with `TerminationReason::ConversationEnded` and the closing message (`final_message`
arg, else the accompanying reply) becomes the final reply — routed through the same
outbound-guardrail/save path as a normal `FinalReply`. This is SP1 of the in-flow
conversational chat-segment epic (`docs/superpowers/specs/2026-07-07-conversational-agent-chat-segment-epic-design.md`).
**Tool-failure blocker guard:** if any tool the agent tried during the turn failed
(dispatch error or allow-list block), an `end_conversation` request is downgraded to a
normal `FinalReply` (the turn parks) instead of `ConversationEnded`. This keeps a
conversational segment open — the closing message, which explains the failure, is shown
and the flow does not silently advance past the blocker. The `end_conversation`
system-prompt note also steers the model not to end on tool/backend failures.

A conversational `dw.agent` node (`NodeKind::DwAgent.conversational`, default false) is a
multi-turn segment: after each agent turn the engine parks and re-enters the same node
(`NodeControl::LoopHere`) on the next inbound message, until the agent's output carries
`terminated_by == "conversation_ended"`, at which point the flow advances to the node's
successor (SP2). Non-conversational `dw.agent` is unchanged (one-shot). The flow-doc
`conversational` flag wiring is SP3. A safety backstop caps the park-loop at
`MAX_PARK_TURNS` (100) consecutive parked turns per node: an agent that never emits
`conversation_ended` force-advances to the successor after the cap (per-node counter
`ExecutionState.park_turns`, persisted in the park/resume snapshot; a plain constant,
no env var / config knob).

**Typed text while a `flow:` tool is parked (`AgentConfig.on_text_while_parked`).** A `dw.agent`
whose `flow:` tool parked on a card ends the turn `awaiting_tool_input`
(`pending_presentation` = the card). By default (`cancel`) a typed message cancels the
parked tool (`flow_suspend::take_pending`). With `on_text_while_parked: side_turn` the
agent instead answers the message (RAG, tools, memory) and the tool STAYS parked: the
card is re-offered, the turn ends `awaiting_tool_input` with `side_turn: true`, and the
engine renders `[reply, card]` (`engine::rendered_park`), so a later card submit resumes
the flow with its state intact. Rules that fail silently if changed: (1) a
`Tool{status: "awaiting_user_input"}` placeholder is written at park ONLY under
`side_turn`, so side Q/A can follow it without breaking provider tool-call pairing —
resume and cancel PATCH it by `call_id` (`flow_suspend::patch_tool_result`), never push a
second result; (2) `ConversationState::truncate_history` removes whole assistant/tool
groups and never the group holding the pending call; (3) a side turn refuses `flow:` tool
calls (one park slot), is capped at `MAX_SIDE_TURNS` (20), and refreshes the 1h idle
expiry only up to `parked_at + 24h`; (4) a park with no stored card (`presentation`),
an expired park, or the cap falls back to the cancel behaviour. Whether a message is a
card submit at all is `engine::is_card_submit`: provider-stamped context keys
(`universal`, `user_id`, `flow_hint`, `extensions`, `channel.*`, and `route`/`tenant` only
when equal to the envelope identity) are NOT input. The order is: the provider's explicit
`metadata.greentic_submit == "true"` marker (stamped by `messaging-provider-webchat` only on a
message activity with object `value`, so a `data: {}` submit still counts), then
`metadata.action`, then that key heuristic (the fallback for providers that do not stamp it). Design: `docs/superpowers/specs/2026-10-05-parked-flow-side-question-design.md`.

The out-of-process (`DwAgentDispatch::Nats`) dispatch path supports the same
conversational park-loop, identical in outcome to the in-process path. A fresh user turn
marks a pending-await marker (`ExecutionState.pending_agent_await`, serde-persisted) and
dispatches to the `agentic` NATS runtime with `resume_at_self: true`, which parks via
`NodeControl::AwaitHere` — a correlation-keyed wait that resumes at the node itself
(not the routing successor) once the async response arrives. On resume, the agent's
response is read from `state.entry.output` (the `{ok, output: {reply, trail,
terminated_by}, events, error}` envelope the NATS response listener builds), not from the
node's re-rendered request payload; `terminated_by == "conversation_ended"` completes the
node (advances to the successor), otherwise it loops (`NodeControl::LoopHere`, session-keyed,
awaiting the next user message) — with the same `MAX_PARK_TURNS` cap and force-advance
behavior as the in-process path. Only a resume whose `state.entry` carries an `"ok"` key is
treated as that response (an interleave guard — a user message arriving before the real
response instead falls through and re-dispatches as a fresh turn), and an `{ok:false, ...}`
error envelope shows the partial `output.reply` when non-empty, otherwise the localized
user-safe line `runner.flow.execution_failed` (never the machine `error.message`, which
stays on the payload's `error` for operators), and re-parks (`LoopHere`) without
bumping the `MAX_PARK_TURNS` cap.

**Known limitation:** the await has no deadline. A lost/never-arriving `aw-serve` response
currently wedges the segment indefinitely (the interleave guard above lets a *new* user
message re-dispatch a fresh turn, but the original stuck wait is never cleaned up). A
bounded deadline was tried and reverted: the naive version set a fixed-timeout watchdog on
every fresh dispatch, but the NATS correlation id is deterministic per-conversation (no
per-dispatch nonce), the watchdog is fire-and-forget with no cancellation, and
`FlowResumeStore` overwrites the wait slot by `scope_hash` on every turn — so a watchdog from
an earlier turn fires later and injects a spurious `{ok:false, error:{code:"timeout"}}`
into whatever turn is parked at that moment, producing a bogus timeout reply mid-conversation.
A correct bounded deadline needs a per-dispatch correlation nonce plus watchdog cancellation
(likely shared with `sorla.call`); tracked as a follow-up, not yet implemented.

**`approval.call` alone carries a per-dispatch nonce** (greentic-runner#793). Its
correlation id ends with `::n=<32 lowercase hex>`, appended LAST so every other segment
keeps its position, and `RuntimeSessionResumer` strips it FIRST (`split_dispatch_nonce`),
so the park is still keyed per conversation. The id each pending approval was published
under is recorded in `ExecutionState::pending_approval_await`, and the resumer DROPS (with
a `warn`) a nonced response that does not match the parked one — an earlier gate's late
decision, or its watchdog `timeout`, no longer lands on a later gate. It is scoped to the
approval runtime on purpose: the agentic/telco bridges reuse the correlation id as the
downstream SESSION id when the input names none, so a nonce on `agentic.call` would give
every turn a fresh agent memory. The watchdog is still never cancelled; the nonce makes
its late `timeout` harmless for approvals, not for the other runtimes.

**The `approval.call` autonomy gate fails closed** (`approval_requires_human`). `above_risk` /
`above_confidence` auto-approve ONLY when both the value and its threshold are finite JSON numbers; a missing,
`null`, string or non-finite value or threshold goes to a human (it used to read as `0.0` / `1.0` and
auto-approve). `always` and unknown modes already required a human.

**`approval.call` is issued a single-use `decision_token`** (greentic-runner#794; approval
rail contract v2 §4, `docs/approval-rail-contract-v2.md` in greentic-designer). Each
dispatch mints 32 random bytes, base64url without padding (`runner/approval_token.rs`),
publishes it at `routing.decision_token` in the request body, and stores ONLY
`sha256(token)` in `ExecutionState::pending_approval_token` beside the correlation id. The
response must carry it back at `output.decision_token`. Two checks, both needed:
`RuntimeSessionResumer` (`admit_response`) drops a response whose token is missing or wrong
BEFORE the runtime runs, and runs for every response, nonced or not — dropping the `::n=`
suffix is exactly the guessable-id attack, so it must not skip the check; the gate itself
(`response_authenticates` in `execute_approval_call`) is the authoritative check, because
anything else that lands an `ok`-carrying entry on the node must not decide it either — a
refused response re-parks like a stray inbound. Taking the park removes the fingerprint,
so a token is spent on first use. The token is stripped from the node output, and never
logged. The runner's own deadline watchdog echoes the token in its `timeout`
(`build_timeout_response_message_with_token`), or the `timeout` branch could never fire.
**Legacy parks** (a snapshot with no fingerprint, parked by a runner predating #794) resume
without a token, as before; the exemption drains as those parks do.

**The HTTP approval rail** (`runner/approval_http/`; design in greentic-designer
`docs/superpowers/specs/2026-09-29-approval-rail-http-design.md`) serves `approval.call` on
lanes with no NATS broker. greentic-start installs it with
`RevisionHostOptions::with_approval_inbox(ApprovalInboxTarget { .. })`; the wire types live
in ONE place, `approval_http/wire.rs`. Four things fail silently if changed:
- **Its own engine slot.** It goes into `FlowEngine::approval_dispatch_handler`, read only for
  the approval runtime. `remote_dispatch_configured()` reads the shared slot alone, so the
  inbox never turns a missing SoR route into a `sorla` dispatch — `sorla.call` still fails
  `sorla_route_missing`. NATS wins when `GREENTIC_EVENTS_NATS_URL` connects.
- **No token on the wire.** The request body is a `RuntimeDispatchRequest` (no `routing`),
  the token is held in memory, and a fetched decision is resumed with the held token injected
  at `output.decision_token`, so the gate's usual check spends it.
- **Resume at most once, never into a fresh run.** The first `200` removes the id before the
  resume; the resume goes through `RuntimeSessionResumer::strict`, where a NONCED id with no
  park recorded under exactly that id is `WrongGate` (the lenient NATS resumer would start a
  fresh run from the entrypoint).
- **Resume runs on its own task.** The single poller task only fetches decisions and enforces deadlines;
  `poller::spawn_resume` runs each resume (the whole flow turn after the decision) on a separate task and logs a
  `JoinError`, so a slow or panicking resume cannot stall or kill the poller. The poller also clears
  `poller_started` when its loop exits, so the next registration can start a replacement.
- **Owned by the `TenantRuntime`.** Its `Drop` calls `HttpApprovalDispatcher::shutdown`, which
  cancels the poller, forgets held tokens and drops the resumer (breaking the
  resumer → runtime → engine → dispatcher cycle). A restart loses pending polls even when the
  park survives in a durable store.

### `secrets://` URIs are never built by hand from config values

Every segment of a `secrets://` URI built from config/pack-controlled input goes through
`greentic_aw_runtime::scoped_secrets::secret_uri` (or, where the `agentic-worker` feature is
off, `secrets_broker::is_safe_secret_segment`): only `[A-Za-z0-9._-]`, no leading dot, no
`..`. That includes `AgentConfig.llm.credential_ref` (category `llm`), the provider/key of
`agent_tool_secret_uri` (unsafe -> `None`) and env/tenant/team in `scoped_secret_path_for_pack`
(unsafe -> `Err`; the pack id is normalised to `[a-z0-9_-]` there). A `credential_ref` such as
`vault://acme/x` or `../other` is therefore REJECTED, not looked up. In
`secret_uri_candidates` an unsafe unit candidate (e.g. a key ending in `.`) is skipped and the
team/tenant candidates still apply (the first skip per process logs a `warn!`, later ones `debug!`: the
lookup has silently widened to a broader scope).

A tenant id is checked once against the strictest of these rules at config load
(`config::tenant_ctx_for` -> `secrets_broker::validate_tenant_segment`: `[A-Za-z0-9._-]`, at most 128 bytes, no
leading `.`, no `..`). A `.hidden` / `a..b` tenant used to load and then fail every secret read; it now fails the
load with a message naming the tenant.

### Broker URLs are logged redacted

`GREENTIC_EVENTS_NATS_URL` may carry `user:pass@`. Anything that logs it or puts it in an error
context goes through `greentic_aw_runtime::url_redact::redact_url` (masks userinfo, handles
comma-separated lists); see `serve::serve_with_ledger` and `maybe_spawn_inproc_agentic_serve`.

**Outbound literal-IP guard (A2A and knowledge index).** `hyper-util` never consults the
DNS resolver for an IP-literal host, so `GuardedResolver` alone does not stop
`http://169.254.169.254/...`. The A2A client (`a2a_source::source::check_target_literal`, before any
secret read) and the knowledge-index client (`knowledge_index::client::check_literal`, on the
embeddings URL and each index search URL) therefore also call `HttpGuard::check_url_literal`. Known
gap: `greentic-a2a`'s `CardCache` agent-card GET uses its own unguarded client. `runtime.rs`'s broker
client talks to fixed provider endpoints, not pack-supplied URLs.

### Agent state and lock invariants (greentic-aw-runtime)

Rules that fail silently if changed. (1) A state `load` error must never start an empty turn: only `Ok` or a
missing key does; a backend error is `AgentError::StateUnavailable` (its `Display` carries
`STATE_UNAVAILABLE_MARKER`, which `aw-event-bridge` / `telco-x-event-bridge` `is_lock_contention` matches to
redeliver or answer `session_busy`), undecodable / too-new state is non-retryable `StateLoad`. (2) A session lock
refresh that finds the key absent is `LockLost`, never a re-acquire: another turn may have run and saved in the
gap. `ensure_lock_held` runs before every end-of-turn save, and `keep_alive` wraps every long await (LLM, `flow:`
tool, generic tool dispatch). (3) Guardrail WASM goes through `guardrail::run_chain_blocking` (blocking pool,
fail-closed on a task panic); never call `run_chain` directly from async code. (4) Detached side effects go
through `background::BackgroundGate` (cap + timeout), not a bare `tokio::spawn`. (5) The core-NATS bridges cap
in-flight requests (`MAX_INFLIGHT`) and bound each handler. (6) A tool result over `MAX_TOOL_RESULT_BYTES`
(32 KiB serialised) is replaced in the history by `{truncated, original_bytes, preview}`; the model never sees the
rest, so `state::cap_tool_result_for` logs the cut with the tool name and size: a `warn!` at most once a minute per process (`OVERSIZE_WARN`), `debug` otherwise. See
`docs/audit-breaking-changes.md`.

### Knowledge retrieval backends

An agent's `Knowledge` implementation is a chain of wrappers around whatever is
mounted first (the pack-shipped `knowledge_corpus` backend, or none). Each
wrapper delegates to the one before it for a binding it does not own — and for
every `ingest` call, unconditionally, so a boot-ingested corpus keeps working
underneath. Two such wrappers exist:

- `knowledge_ext` (provider id `provider.knowledge.extension`) routes a bound
  agent's retrieval to a customer's own HTTP service through a design
  extension's tool (`<extension_id>/<tool_name>`).
- `knowledge_index` (`crates/greentic-runner-host/src/runner/knowledge_index/`,
  provider id **`provider.knowledge.chronicle-index`**) routes retrieval to a
  Greentic-operated Chronicle index server instead — the worker's documents
  live in one or more named indexes built by the designer's sync, not in the
  pack and not behind a customer's own service.

**Binding params**, every key prefixed `provider.knowledge.chronicle-index.`:
`endpoint` (the index server's base URL), `tenant` (the DESIGNER tenant slug,
sent as the `X-Greentic-Tenant` header on every server call), `team` (sent as
`X-Greentic-Team`; blank/absent defaults to `general` on the wire),
`index_ids` (non-empty array — every index is searched and the hits merged),
`embedding_provider` (only `openai` is supported today), `embedding_model`,
and `embedding_base_url` (optional, defaults to `https://api.openai.com/v1`).
`tenant` and `team` must match `[A-Za-z0-9_-]{1,64}` (they become headers).
Any param that is missing or invalid is a `Backend` error naming its full key.

**Credential destinations.** The index key goes to `endpoint` and the
embedding key goes to `embedding_base_url` — hosts the pack's binding names,
which is the same trust boundary as the A2A contract §9 (the pack is trusted
input and chooses where a credential is sent). That is why both must be
absolute URLs with a host and scheme `https`; plain `http` is accepted only
for a loopback host (`localhost`, `127.0.0.1`, `::1`), so a key never crosses
a network in cleartext. The index server must be served at the endpoint's
ROOT: the request path is always `/v1/indexes/{index_id}/search`, so any base
path in `endpoint` (and its query/fragment) is dropped.

**Secrets** are sealed under category `knowledge` as `chronicle_index_key`
(bearer for the index server) and `embedding_key` (bearer for the embedding
API), read via `scoped_secrets::read_secret_for_unit` team-first-then-`_` —
**never** unit-scoped. Both are read under the RUNTIME tenant (the tenant the
host's own secrets are scoped by), not the binding's `tenant` param, which
names the designer tenant and is only ever sent to the index server. There is
no unit-scoped candidate because the deployed secret stores (greentic-deployer's
dev-store writer, greentic-start's reader) canonicalise the secret name to
`[a-z0-9_]` for every category except `mcp`/`a2a`, so a `<key>.unit-<id>` name
could never be written or found there — this is also why the key names use
`_` rather than `-`.

**Per-call timeout** is `GREENTIC_KNOWLEDGE_INDEX_TIMEOUT_MS` (milliseconds,
must be a positive integer or the 5000 ms default applies). It is read ONCE,
when the process-wide HTTP client is built on the first bound search — every
instance shares that client, because the graph lane mounts a fresh backend on
every agent turn and building a client per turn would put TLS setup on each
one. A client that cannot be built makes every bound search a `Backend`
error; it never falls back to an untimed client.

**Failure is always `KnowledgeError::Backend`** — the turn loop already warns,
records a failed retrieval trace step, and runs the turn without knowledge.
One index among several failing is only a `tracing::warn!`; the other
indexes' hits are still returned. Only when every bound index fails does the
whole retrieval become a `Backend` error. Error messages never carry a
credential, the query vector, or chunk text. Hits from every index are merged
by score (best first), truncated to the query's limit, and capped by the same
per-chunk/total character budgets as `knowledge_ext` (`MAX_CHUNK_CHARS` 4 000,
`MAX_TOTAL_CHARS` 24 000).

This backend is mounted at the same three production sites that mount
`knowledge_ext`: the in-process `dw.agent`/`agentic.call` construction
(`agent_node::build_runtime_with_stores`), the out-of-process NATS serve path
(`agent_node::build_agent_runtime`, used by `serve_agentic`), and the
agent-graph lane (`graph_node.rs`, which runs each turn under a synthetic
`TenantCtx` and so captures the real runtime tenant once, in `IndexMount`,
rather than reading it off that synthetic context). A deep worker
(`operala.call`) mounts nothing of its own — it borrows the in-process
runtime that `agent_node` already built.

**Server contract**: `POST {endpoint}/v1/indexes/{index_id}/search`, served by
`greentic-chronicle-ext`'s `chronicle-index-server`.

### A worker's A2A conversation with a remote agent is multi-turn

An `a2a:<agent_id>` tool call is a message to an external agent, and the
remote may answer `input-required` — "which city?" — rather than an answer.
Until this landed every `SendMessage` carried `contextId: None`, so the reply
to that question opened a NEW remote task and the remote asked again. The
protocol types had modelled `TaskState::InputRequired` from the start;
nothing used it.

**What the conversation remembers.** `ConversationState.a2a`
(`a2a_source::A2aContinuations`, `#[serde(default)]`) holds, per remote
agent, the `contextId` and — only while the task is open — the `taskId`.
It rides in the worker's own per-session state, keyed
`aw:{tenant}:{env}:{session}:state`, rather than in a store of its own, so it
survives a restart on the durable backends and is shared across instances on
Redis.

**The two ids have different lifetimes, and conflating them is the bug this
is shaped to avoid** (design §7). `contextId` is the conversation with that
agent; `taskId` is ONE remote execution. A `taskId` is dropped the moment
`TaskState::progress()` says the task is no longer `Open` — a completed task
will not accept another message — while the `contextId` survives, so the next
question lands in the same remote thread rather than a fresh one.

**The lifetime rule** is idle expiry: every successful call refreshes the
entry, and one hour (`CONTINUATION_IDLE_TTL_SECS`) after the last call it is
dropped and the next call starts fresh. Sending a reference the remote has
already collected is worse than sending none — depending on the agent that is
an error, or it silently opens a new task while we believe we resumed one.
At most 16 agents are remembered per conversation, LRU-evicted, because the
map is serialised on every state save.

**Two conversations never share a remote context.** They are different
`ConversationState`s, so they hold different maps. Each entry ALSO records
the tenant and env it was minted under and is discarded if they do not match
the running `TenantContext` — the key protects the store, this protects the
value, which matters because `ConversationState` is public and callers
(greentic-designer) build their own.

**The tool result is a tagged object, with one rule worth more than the rest
of the shape: `reply` appears if and only if `status` is `completed`.**
`input_required` carries `question` plus a `next_step` telling the model the
remote is waiting; `working` carries `detail`; `failed` carries `error` and
the remote `state` (`failed`/`canceled`/`rejected`/`auth_required`); `error`
is OUR failure to reach the agent at all, which is a different fact and sends
an operator to a different system. The old shape flattened every one of these
into `{"reply": <prose about our own polling support>}`, which a model read
as the agent's answer. Pinned by
`a2a_source::outcome::tests::only_a_completed_outcome_carries_a_reply`.

**Two call sites deliberately have NO continuation**, and both open a fresh
remote context every time: `A2aToolCatalog::dispatch` (kept for one-shot
callers) and the agent-graph tool node, which holds no `ConversationState` —
closing the latter means teaching the graph executor's checkpoint to carry
the map. `ToolSession` (the in-process deep worker behind `operala.call`)
keeps one in memory for the life of the session only.

### A plain flow can call an A2A agent too (`component == "a2a"`)

Everything above is the WORKER lane: `a2a:<agent_id>` is a tool an agentic
worker's model may choose. A plain flow had no way to ask an external agent at
all — there was no flow-node executor for one. There is now, and it is
deliberately the same shape the `mcp` flow node already solved once
(`runner/mcp_node.rs`): a native node the engine dispatches itself, with the
node's own payload as the source of truth.

**LOCKED ENCODING v1**, `component == "a2a"`, payload/config:

```yaml
nodes:
  ask_recipe:
    a2a:
      agent: 8d2f0c1e-recipe     # the admin row id, as the sidecar names it
      message: "{{ entry.text }}"
      output: answer             # optional state key to bind the result under
    routing: [ ... ]
```

`agent` is read from the payload; `operation: "<agent_id>"` and an
`a2a:<agent_id>` component ref are the fallbacks, exactly as the `mcp` node's
legacy `operation`/`mcp:` forms are. `output` binds the result under that key
(`{{node.ask_recipe.answer.reply}}`), absent binds it as the node payload.
Both nodes go through one `bind_node_result`, so they cannot answer `output:`
differently. `a2a` is in `flow_adapter::NATIVE_OP_KEYS` and `a2a:` is one of
`is_native_op_key`'s prefixes — without both, the loader wraps the node as a
generic component and the engine arm becomes unreachable, silently.

**The node sends ONE message and completes. It never parks.** Implementation:
`runner/a2a_node.rs` plus `FlowEngine::execute_a2a`.

Four things decide how it behaves, and each fails silently if changed:

- **Nothing here classifies an outcome.** `A2aToolCatalog::dispatch_in_conversation`
  is called verbatim, so the flow lane and the worker lane render a remote
  task's state through the same code — including **`reply` appears if and only
  if `status` is `completed`**, pinned by the one test
  (`a2a_source::outcome::tests::only_a_completed_outcome_carries_a_reply`) plus
  its flow-lane twin in `tests/a2a_flow_node.rs`. A second classifier here is
  how a flow and a worker would start disagreeing about what `input-required`
  means. The node's value always carries `status`, one of `completed`,
  `input_required`, `working`, `failed` (the remote decided) or `error` (we
  could not ask it) — a flow ROUTES on it, because a flow cannot stop
  mid-node to ask a human.
- **The continuation is per `(tenant, env, session, FLOW, agent)`**, held as
  the same [`A2aContinuations`] map the worker keeps in its
  `ConversationState` — the type, not a copy of its rules, so the idle TTL, the
  LRU cap and the tenant/env re-check are shared. It lives in the flow's own
  durable state store under prefix `runner-a2a`, deliberately NOT
  `STATE_PREFIX`: a flow author owns every key `state.set` writes, so sharing
  that namespace would let an ordinary `state.set` read or overwrite a remote
  task reference. Every call ONE FLOW makes to one agent in one session
  therefore continues the same remote conversation, which is the only shape in
  which a flow can use `input-required` at all — route the question to a card,
  come back to the node, and the remote's own question gets ANSWERED instead of
  re-asked.

  **The flow is in the key for CORRECTNESS, not for confidentiality.** Two
  flows in one session are the same tenant, the same end user and the same
  remote agent, which has already seen both — there is nothing to disclose.
  What there is, is a wrong answer: a support flow and a billing flow sharing
  one remote thread would have the billing flow's question answered conditioned
  on the support flow's turns. Nothing the `input-required` loop needs survives
  across flows, so the continuity buys nothing to set against that.

  **The cost, stated rather than left to be found: a `flow.goto` hand-over
  starts a fresh remote context.** An exchange mid-`input_required` when the
  turn is handed to another flow cannot be resumed there — the receiving flow
  asks the agent from scratch. Carrying it across would mean keying on
  something the two flows share, which is the cross-flow bleed above.

  The key is `continuations/<flow_id>/<session hint>` and its two segments
  cannot be confused: a `FlowId` is `[A-Za-z0-9._-]+`, so the flow segment can
  never contain the `/` that ends it, and `execute_a2a` has already resolved
  the flow through `pack_for_flow` before the key is built. A test pins that
  charset rule, so a relaxation upstream reports itself rather than letting
  `("a/b", "c")` and `("a", "b/c")` collide.

  **A run with no session hint gets no continuation** (each call is its own
  context, nothing is stored), and so does a runtime with no state store. Both
  are degradations to "a fresh remote context", never a node failure.
- **A missing credential fails loudly and sends nothing.** A route with
  `requires_auth` and no resolvable token refuses in
  `a2a_source::auth::CredentialScope::header_for` — before anything is sent,
  naming the agent and every `secrets://…/a2a/<agent_id>` scope it tried.
  There is no unauthenticated retry and no empty reply.
- **The agents come from THIS flow's pack**, not from every pack the runtime
  loaded. An agent the pack does not declare is an error naming it, never a
  fallback to another agent — the same fail-closed rule
  `operala_tools::resolve_tool_agent` applies to a deep worker's agent id.
  `GREENTIC_AW_A2A=0` disables the flow node as well as the worker tools: an
  operator who turned outbound A2A off must not have it re-enabled by a pack.

**Only the two `error`-carrying statuses mark the node failed**, and only when
it has an error route to send the failure down — the gate `mcp_node_output`
already applies, for the same reason (a routing array that matches none of its
conditions PARKS the run, which is worse than a value the flow can still read).
`failed` becomes `a2a_agent_failed` and `error` becomes `a2a_call_failed`,
because the two send an operator to different systems. `completed`,
`input_required` and `working` are never errors. The envelope is built by
`normalize_mcp_tool_error`, the single definition of what a failed node's
`{errors}` output looks like, so the `output:` binding key and `status` survive
beside it.

**Still needed before an operator can use this:** nothing in greentic-designer
emits an `a2a` node yet — no palette entry, no config panel — and the
`assets/a2a-routes.json` sidecar a flow pack needs is written today only for
packs that carry a WORKER binding an `a2a:` tool. A flow-only pack gets no
sidecar, so every `a2a` node in it refuses with the "declares no agents"
message. That is the designer half, and it is a separate change.

### A plain flow can create a file (`component == "media.create"`)

A flow with no agentic worker can render a `txt`, `md`, `csv` or `pdf` file and
store it as an artifact. **LOCKED ENCODING v1**, `component == "media.create"`,
payload/config (already template-rendered when the node runs):

```yaml
make_pdf:
  media.create:
    format: pdf              # txt | md | csv | pdf (required)
    name: "Claim Summary"    # file stem, sanitised (`[A-Za-z0-9._-]`, 64 chars)
    text: "{{vars.summary}}" # txt / md / pdf; blank is an `invalid_input` value
    rows: [["a", "b"]]       # csv only: array of arrays of scalars
    output: file             # optional state key to bind the result under
  routing: end
```

The result is the C5 value of `docs/media-contract-v1.md` in the designer repo:
`{ok:true, artifact:{id,mime_type,name}, artifacts:[..], warnings:[..]}` or
`{ok:false, error:{code,message}}`, codes `invalid_input`, `unsupported_format`,
`too_large`, `artifact_unavailable`, `artifact_rejected`, `unauthorized`.
**Delivery does not depend on `output:` or on what follows the node.** Every
successful `media.create` records its file(s) in the turn's created-files list
(`ExecutionState.created_artifacts`, fed from `NodeOutput.created`), and
`finalize_with` folds that list into the turn's FINAL reply as a reserved
`created_artifacts: [{id, mime_type, name}]` key (`media_node::attach_created`;
an object gets the key, an array of emitted payloads gets it on its LAST
element, a string becomes `{text, created_artifacts}`, `null` becomes a
file-only `{created_artifacts}`). greentic-start (>= the build after this
change) delivers every listed id its own artifact port stored, in addition to
the older top-level `artifact` / `response.artifact` rule. Without `output:`
the result still IS the node output, so the old rule keeps working as before.

Rules, each silent if broken:

- **The list is the runner's word, never the flow's.** `attach_created` always
  removes a `created_artifacts` key already present in a reply (a node, an HTTP
  echo or a template cannot name files), and a failure value never lists a file.
  The list is per TURN: it is `#[serde(skip)]` and cleared with the egress on
  every snapshot, so a parked run does not deliver a file again when it
  resumes. A turn that fails terminally (the metadata-only error reply) lists
  nothing. A failed node elsewhere in the flow makes `lift_first_node_error_from_nodes`
  wrap an ARRAY reply under `payload`, which hides the list from the host: known,
  rare, and the file is not delivered then.
- **At most 20 files per turn are listed** (`MAX_CREATED_PER_TURN`, mirrored in `runner/engine.rs`). Files beyond
  the cap are stored but not listed in the reply (so not delivered); `note_created_artifacts` logs a `warn!` with the
  dropped count.
- **A failure is a value.** The node is marked failed (errors envelope via
  `normalize_mcp_tool_error`) only when it has an `on_error` edge; otherwise it
  completes with the failure value, exactly like `a2a`.
- **The port is the agent runtime's.** `FlowEngine::set_ext_artifact_port` is
  called in `runtime.rs` with the same `ext_artifact_port` the agent runtime
  gets as `host.artifact.put`. No port gives an `artifact_unavailable` value.
  `ArtifactPort::put` is synchronous, so it runs on `spawn_blocking`; the real
  `HttpArtifactPort` refuses a current-thread runtime. Nested flow-tool
  engines get no port yet.
- **The renderer is ported, not shared.** `runner/media_render/` is a copy of
  `component-media-ext` (`document.rs`, `pdf.rs`, `input.rs`, `names.rs`,
  `limits.rs`, `output.rs`); limits are identical (10 MB, text 2,000,000
  characters, PDF over 2000 pages is `too_large`, CSV formula escaping, Latin-1
  replacement warning). Recorded debt: change both or neither.
- Registered like `a2a`: `NATIVE_OP_KEYS`, the `is_builtin` prefix (`media.`),
  `NodeKind::MediaCreate`, the dispatch arm. The loader additionally needs
  greentic-flow's `RUNTIME_NATIVE_CALL_KINDS` to carry `media.create`.

### Async runtime dispatch (`sorla.call` node)

**A route document is the primary transport, and NATS is now the fallback (amendment
A1).** The transport rule, verbatim: *if the route document resolves, use HTTP,
synchronously. If it does not and NATS is configured, use NATS as before. If neither, the
node fails with `sorla_route_missing`. A route document always wins over NATS.*
`runner/engine.rs::execute_sorla_call` always tries
`runner/sorla_node.rs::execute_sorla_http` first: `Ok(Some(result))` completes the node in
this same step (no publish, no wait, no `RemoteDispatchHandler`); `Ok(None)` means no route
document exists at all for the SoR, and only THAT case falls through to the NATS path below
(and only when `RemoteDispatchHandler` is actually configured — otherwise the node fails
with ``sorla_route_missing: no route document for SoR `<sor>` and no NATS configured``);
`Err(_)` (a malformed document, or the secrets backend itself failing to answer) is a hard
failure and never falls through to NATS.

#### Route documents (`secrets://…/sorla/<sor>`)

Each SoR is addressed by a route document read from the secrets manager at
`secrets://default/<tenant>/<team|_>/sorla/<sor>` — never carried in a pack, never cached on
an invoker, and re-read on EVERY call (`runner/sorla_route.rs::resolve_route`,
`crates/greentic-aw-runtime/src/scoped_secrets.rs::secret_uri_candidates` for the
unit-then-team-then-tenant candidate order), so a restarted local SoR child on a new port,
or a redeployed service, is followed without rebuilding anything. The document is UTF-8
JSON:

```json
{ "url": "https://…", "token": "…", "tenant": "…" }
```

`token` is optional. An absent or blank `token` resolves to `None`, and a call built from
that document sends **no** `Authorization` header — never `Bearer ` with an empty value.
`resolve_route` distinguishes three outcomes, and only one of them means "no route
document, try NATS": `SorlaRouteError::Missing` (`NotFound` at every scope tried),
`SorlaRouteError::Invalid` (a document exists but is malformed — its value is never echoed,
since it may carry a token), and `SorlaRouteError::Unavailable` (the secrets backend itself
could not answer — denied, unreachable, ...). Only `Missing` reads as "no route document";
`Invalid` and `Unavailable` are hard errors, because a backend that cannot answer says
nothing about whether a route document exists — folding either into "no route" would let a
SoR with a real, unreadable route silently fall back to NATS (or misreport
`sorla_route_missing`).

**The SoR key is the capability-URI pack segment** — the `<pack>` in `sorla:<pack>` and in
`cap://greentic/business-functions/<pack>/<action>/v<ver>` (also
`cap://greentic/agent-endpoints/<pack>/<id>/v<ver>`) — **never** the
`sorla_deployments.pack_id` column (e.g. `supplier-sor-0.1.0`). Discovery
(`sorx_invoker::routed::SorxRoutedInvoker::discover_all`, and the single-call
`sorla_node::execute_sorla_http`) drops any offer whose `cap://` pack segment does not match
the SoR it was discovered through — logged by SoR and offered pack, never by token or
document value — so a shared or misbehaving SoR cannot leak another pack's operations
through this SoR's route document/credential.

#### `assets/sorla-routes.json` — requirements only, never an address

A pack declares the SoRs its flows and workers need in `assets/sorla-routes.json`
(`runner/sorla_pack_routes.rs::PackSorlaRoutes`), a JSON array of records:

```json
[{ "sor": "landlord", "actions": ["record_rent_payment"] }]
```

`actions` is informational and defaults to `[]`. **This sidecar never carries a URL, a
token, or a secrets URI** — only which SoRs and (optionally) which of their actions a pack
uses; the address and credential are resolved at call time from each SoR's own route
document, so one pack build runs unchanged in every environment. A record that fails to
parse, or names a blank SoR, is dropped; a duplicate SoR keeps the first record; a document
that is not a JSON array at all parses to `None`.

#### Worker sorla tool source: the env override wins

A worker's sorla tool source is built as
`sorla_source_from_env().or_else(|| sorla_source_from_packs(&packs, &tenant, secrets,
unit))` (`agent_node.rs`). The single-SoR `GREENTIC_AW_SORX_URL` development override —
token-less, tenant from `GREENTIC_AW_SORX_TENANT` (default `"default"`) — **wins whenever
it is set, for worker tools only**; only when it is unset does the source come from the
loaded packs' `assets/sorla-routes.json` sidecars, each declared SoR resolved through its
own route document via `SorxRoutedInvoker` (one discovery fetch per bound SoR at
construction, re-resolved and re-discovered per `invoke` call — never cached — so a
restarted SoR child is followed the same way `sorla.call` is). `sorla_source_for_sors` is
the same seam, re-exported as `greentic_runner_host::sorla_source_for_sors` for a caller
that already knows a worker's bound SoRs directly and has no `PackRuntime` to read a
sidecar from — the designer's in-process Test chat. `GREENTIC_AW_SORLA_TOOLS=0` disables
both the env-URL and the pack-routed path.

Note the `sorla.call` flow-node transport rule above and this worker-tool precedence are
two different mechanisms reached from two different call sites — a flow node always prefers
its own route document over NATS regardless of `GREENTIC_AW_SORX_URL`; that env var only
ever substitutes for a worker's whole pack-routed tool catalog.

#### The historical NATS transport (fallback only)

A native flow node `sorla.call` (component `sorla.call`, operation = the sorx target) can
still dispatch work to the separate `greentic-sorx` runtime over NATS pub/sub, when no route
document resolves for the named SoR. Node input:
`{ "await": true|false, "operation": "<op>", "deadline_ms": <u64?>, "input": {...} }`.
`await: true` PAUSES the flow (reuses `FlowResumeStore`/ingress resume) and resumes when the
runtime's response arrives; `await: false` continues immediately. Implemented natively
(`runner/remote_dispatch.rs` `RemoteDispatchHandler`/`NatsDispatcher`, `runner/engine.rs`
`execute_sorla_call`, `runner/dispatch_listener.rs` response-listener,
`runner/runtime_session_resumer.rs`), wired in `runtime.rs` when `GREENTIC_EVENTS_NATS_URL`
is set. Subjects: `greentic.sorla.request.v1` / `greentic.sorla.response.v1`; correlation id
= `<bare session hint>::pack=<id>::flow=<id>`. Contract in `greentic-types::runtime_dispatch`;
sorx side is the `sorx-event-bridge` crate. Same pattern is intended for `agentic.call` /
`operala.call`. Waits from an inbound with a non-empty thread/reply_to ARE resumable: the
node appends opaque `::thread=<t>::reply=<r>` markers to the correlation id (omitted when
empty) and `RuntimeSessionResumer` parses them back into the synthesized `ReplyScope` so
`FlowResumeStore::fetch` recomputes the same `scope_hash` as `save`. The sorx bridge echoes
the correlation verbatim, so no sorx change is required.

**Multi-replica response delivery.** Response listeners join a per-tenant queue group
(`runner-{tenant}-{runtime}`) with a tenant filter, so one replica resumes each response:
the park read (`FlowResumeStore::fetch`) and clear are NOT one atomic take, so letting every
replica see every response (no queue group) could resume a wait twice. The deadline watchdog map
(`remote_dispatch::WATCHDOGS`) is process-local, so the replica that admits a response may not be
the one that armed the watchdog. The admitting replica therefore announces the correlation id on
`greentic.dispatch.watchdog-cancel.v1` (`WatchdogCancel`), and every replica's
`run_watchdog_cancel_listener` (plain subscription, tenant-filtered, ignoring its own `origin`)
cancels the matching watchdog. Residual window: a notice that arrives after another turn of the
same conversation armed a new watchdog on the receiving replica would retire it (the call then
has no timeout fallback, the documented no-deadline limitation). Re-checking the park before
publishing a `timeout` is not enough: a later wait parks under the same deterministic id.

### Agentic dispatch serve mode (`agentic.call` runtime side)

The `agentic.call` node uses runtime name `"agentic"` → subjects
`greentic.agentic.request.v1` / `greentic.agentic.response.v1` (same contract,
headers, and correlation-marker rules as `sorla.call`). The runtime-side consumer
is the `aw-event-bridge` crate: it shares the `greentic-types::runtime_dispatch`
contract directly (aw-runtime pins the same types lineage as the runner, so no
mirroring) and exposes an `AgentDispatchInvoker` seam + `run_bridge`. The
production invoker (`greentic_aw_runtime::serve::RuntimeAgentDispatchInvoker`)
maps `target` → agent id, `input.user_text` → `AgentInput`, the correlation hint
→ session id, and serialises `AgentOutput` to `{reply, trail, terminated_by}`
(identical to the in-process `dw.agent` node output). **Session id length:** the
runner's `::cnv64=` / `::thr64=` / `::rpl64=` markers lengthen the correlation
hint, and `validate_session_id` caps a session id at 512 bytes. `resolve_session_id`
therefore replaces an over-long id with `h:` + sha256 hex (66 chars, deterministic,
so one conversation keeps one memory); an id with a control character is still
rejected. Ids at or under the cap are unchanged. Conversations whose id was over
the cap were rejected every turn before, so none has memory to lose; see
`docs/audit-breaking-changes.md`. Serve entries:
`greentic_aw_runtime::serve::serve(nats_url, runtime)` and the host-level
`agent_node::serve_agentic(nats_url, merged_agents)` (reuses the shared
`build_agent_runtime` so in-process and serve paths build an identical runtime).
For a credit-free live e2e there is an `aw-serve` bin (features `serve,test-mock`):
`GREENTIC_EVENTS_NATS_URL=nats://127.0.0.1:4222 cargo run -p greentic-aw-runtime
--features serve,test-mock --bin aw-serve` (env `AW_SERVE_AGENT_ID`,
`AW_SERVE_REPLY`). `dw.agent` (in-process) stays untouched — `agentic.call` is the
out-of-process path.

**Dispatch ledger key.** Redelivery dedup (`DispatchLedger`) is keyed
`{tenant}:{env}:{dispatch_id}`, where `dispatch_id` is the random `Greentic-Dispatch-Id`
header `remote_dispatch::build_request` mints once per publish (stable when the broker redelivers
that message, different for the next turn). It is a separate header on purpose: the correlation
id / `Greentic-Idempotency-Key` is the downstream SESSION id for the agentic/telco bridges, so a
nonce there would reset agent memory every turn, and keying the ledger on it replayed turn 1's
reply as turn 2's for up to an hour. A request with no (or a malformed) dispatch id from an older
runner is not deduplicated at all. The bridge hands it to invokers through
`AgentDispatchInvoker::invoke_identified` (default delegates to `invoke`).

**Opt-in in-process co-host (`GREENTIC_AGENTIC_SERVE_INPROC`)**: a single-node
deployment can co-host the agentic-worker service inside the main runner process
instead of running a separate `aw-serve`. **Default OFF** — distributed deploys
run `aw-serve` standalone so the agentic service scales independently. When
`GREENTIC_AGENTIC_SERVE_INPROC` is truthy (`1`/`true`/`yes`/`on`) AND
`GREENTIC_EVENTS_NATS_URL` is set, `greentic_runner_host::run()` spawns
`serve_agentic` exactly once per process (NOT per-tenant — a per-tenant spawn
would put multiple competing subscribers on `greentic.agentic.request.v1`). The
gate is the pure `agent_node::should_serve_agentic_inproc`; the spawn is
`maybe_spawn_inproc_agentic_serve` in `lib.rs`, feature-gated behind
`agentic-worker`. Process-level base agent configs come ONLY from
`GREENTIC_AGENT_MANIFESTS_DIR` (`<agent_id>.json` full `AgentConfig` files, loaded
by `agent_node::load_process_agent_configs`) — pack-embedded and per-tenant
`HostConfig.agents` are not visible at process startup. Skips with a warning (and
the runner continues normally) when no agents are configured, or when the runtime
cannot be built (no usable state backend — e.g. `GREENTIC_AW_STATE_BACKEND=redis` with no
`GREENTIC_AW_REDIS_URL`, or a Redis connect failure — or no LLM key). With no backend env set
it defaults to the in-memory backend, so the runtime builds without Redis.

**Event-bridge scope headers and JetStream budgets (round 13).** Both bridges (`aw-event-bridge`,
`telco-x-event-bridge`) treat a blank `Greentic-Env` as absent (default `default`) and echo `Greentic-Tenant` /
`Greentic-Env` on every rejection reply whenever the ids fit the runner's charset (`is_echoable_scope_id`:
`[A-Za-z0-9._-]`, at most 128 bytes, `..` allowed). `validate_scope_id` still rejects `..` because it guards ids used
to build keys; the two rules differ on purpose. Without the echo the runner's tenant-scoped response listener drops
the rejection (it also drops, with a `warn!` naming the correlation id, any response with no `Greentic-Tenant`), and
the awaiting flow stays parked until its watchdog fires (forever with no deadline). On JetStream a busy session is
redelivered while the message is younger than `busy_budget(step_timeout)` (handle timeout + 60 s, at most 31 min),
measured from the message's publish time; every Nak counts against the server's `max_deliver`, so the consumer's is
now the backstop `CONSUMER_MAX_DELIVER` (100) and `MAX_DELIVER` (6) is the application cap for handler ERRORS only. Handler errors are
counted per stream sequence in-process (`jetstream::ErrorAttempts`, least-recently-recorded eviction), not from the delivery count, so busy Naks do not spend
that budget; if `info()` fails the delivery is settled as the last one.

### What a component is told about its caller

A `component.exec` node runs under two different identities, and they must not be confused:

- **The host scope** (`ExecCtx.tenant`, built by `runner/engine.rs::component_exec_ctx`). `user` is the provider id (`messaging-webchat`, `greentic-run-demo`, or the literal `"provider"` that `IngressEnvelope::canonicalize` substitutes when none was stamped) and `team` is `None`. This is a STORAGE key: `pack.rs::tenant_ctx_from_v1` feeds it to greentic-state, whose FQN is `env:tenant:team:user`. Changing it would re-key every component's existing state. Secrets have no user segment.
- **What the component is told** (`component_api.rs::presented_user` / `presented_team`, reached through the `*_for` conversions and `PackRuntime::invoke_component_for`). With a provider-verified caller (`FlowContext::caller`, `user_verified: true`) the component is told the caller's `sub` / `team` (and `caller.*` attributes in 0.5, `metadata_cbor` in 0.6). **With no verified subject it is told NO user** (`None`) whenever the host user is the provider id — so an anonymous caller of a deployed environment no longer looks like a user named after the provider, and in particular not like Run Demo. The provider is presented in the 0.5 `TenantCtx.provider_id` slot instead; 0.4 and 0.6 have no provider slot. This matches the JSON `InvocationEnvelope` path (`runner/invocation.rs`), which has always left `user` empty for an unverified caller (pinned in `tests/component_exec.rs`).

`None`, not a literal `"anonymous"`: it is an already-exercised shape and cannot collide with a real subject of that name. A host user that is NOT the provider id (an embedder that put a real user on `ExecCtx`, `greentic_x_provider`'s actor) is still presented unchanged — only the provider-as-user substitution is withheld. `GREENTIC_PRESENT_PROVIDER_AS_USER=1` restores the pre-change fallback for a component that keyed behaviour on it; it changes only what is presented, never the host scope.

Known gap, not closed here: `tenant_ctx_from_v1` lets a component override `team` / `user` on its state calls from the `TenantCtx` it passes back, so "a caller never re-keys host scope" is a convention, not an enforced rule. Withholding the provider-as-user does not widen it — a `None` user in the passed-back ctx does not override the host user.

### WASM Component Model

- **Target**: `wasm32-wasip2` (WASI Preview 2, Component Model)
- **Wasmtime 45** with `component-model` + `cranelift`
- Components export `greentic:component@0.6.0` world
- Host links: WASI-p2, WASI-HTTP, WASI-TLS, state/session/secrets/telemetry/OAuth helpers
- Components run on dedicated threads via `tokio::task::block_in_place` to avoid blocking the async runtime
- Two-tier cache: memory LRU + disk, keyed by `EngineProfile`

### Guest sandbox, limits and outbound guard

Each tenant's packs get `/state`, `/cache`, `/logs` backed by `<host dir>/tenants/<tenant>` (`RunnerWasiPolicy::for_tenant`; tenant ids must be `[A-Za-z0-9._-]{1,128}`, not `.`/`..`), and `env_allow` is only that tenant's `env_passthrough`. The pack-artifact cache (`<cache>/packs`) is never visible to guests. Data under the old host-wide dirs is not migrated. Execution limits live in `wasm_limits.rs` (a grow past the memory/table cap is DENIED, not trapped: the guest sees `-1` from `memory.grow`/`table.grow` and a Rust guest usually aborts on the failed allocation; `GuestLimiter` logs a `warn!` for the first denial per PROCESS, later denials at `debug`, each store at most once); the outbound SSRF/size guard in `http_guard.rs` (host `http-client`: checked in the client's DNS resolver, so it covers the connect address and redirects; `wasi:http`: the name is resolved and validated just before the upstream connector re-resolves it, so a DNS-rebinding race remains — egress-filter at the network layer if that matters).

### Desktop session snapshots (legacy adoption)

`greentic-runner-desktop` persists a parked run as `<escaped id>.snapshot.json`, wrapped with the
owning `session_id`. Builds before `%xx` escaping used a lossy name (every unsafe byte became `_`)
and wrote bare snapshots with no owner. For an id with unsafe characters the loader falls back to
that legacy name when the current file is absent; a wrapped file must record the same id, but a
BARE legacy file is accepted by whichever session maps to its name, so `a/b` can adopt a pre-upgrade
park saved for `a_b`. Accepted residual risk, behaviour intentionally unchanged (see
`load_legacy_session_snapshot`); it drains as old parks resume or expire, and nothing writes bare
files any more.

**Pack/config-controlled URLs in the agentic lane use the same policy.** The pure IP classification and
env policy (`HttpGuard`: link-local/metadata refused, loopback/private allowed by default,
`GREENTIC_HTTP_ALLOW_METADATA` / `GREENTIC_HTTP_DENY_PRIVATE`) live in `runner_core::net_guard`, shared by
`http_guard.rs` and `greentic-aw-runtime::outbound_guard`. Applied to: MCP `transport_url` (pack
`mcp-routes.json` or admin row) via `check_endpoint` right before `McpHttpClient::new`; the A2A client
(guarded resolver, redirects off); and the `knowledge_index` client (guarded resolver + redirect policy).
`McpHttpClient` builds its own `reqwest` client with no hook, so for MCP the check is a resolve-then-validate
pre-flight: a DNS-rebinding answer that differs on connect, and redirects, are NOT covered until
`greentic-mcp-client` exposes a resolver/redirect seam (egress-filter at the network layer if that matters).
The resolver adapters are per-crate because host uses reqwest 0.13 and aw-runtime 0.12. Other clients
(admin/billing/OAuth/sorx route docs/approval inbox) target operator-set URLs and are not guarded.

### Round 20 notes

- Tool-secret env deny-list also covers `AWS_BEARER_*`, `AWS_SECURITY_*`, `AWS_ACCESS_KEY*`, `ARM_*`, `CLOUDSDK_*`, `GOOGLE_OAUTH_*`, `AZURE_*_CONNECTION_STRING`, `AZURE_CREDENTIALS`, `AZURE_DEVOPS_EXT_PAT`; `AWS_REGION`, `AWS_PROFILE`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and third-party `*_API_KEY` stay usable.
- JetStream pull errors (`aw-event-bridge`): only a description naming `MaxAckPending` is the quiet 500 ms retry; other `409`s warn and back off; `Consumer Deleted` re-runs `ensure_consumer` (the "consumer not found" branch was dead code, removed in Round 21). `ensure_consumer` only RAISES a finite `max_ack_pending`; **Round 20 claimed it "never lowers" it, which was wrong** for an unlimited (`-1`/`0`) value and on the `get_or_create` error path (both fixed in Round 21), so a rolling deploy does not flap it.
- Error text for MCP store pulls uses `url_redact::redact_http_url(url)` (no userinfo/query/fragment; superseded in Round 21/22: origin only, see below); reqwest body/decode errors use `without_url()` before `to_string()`.
- See `docs/audit-breaking-changes.md` (Round 20).

### Round 21 notes

- `aw-event-bridge` JetStream: a durable deleted while the loop is idle (or blocked in `acquire_batch`) is never answered `Consumer Deleted`; the pull gets `503`/no responders. `classify_pull_error` now returns `PullFault::Unavailable` for that, and `ConsecutiveFaults` re-runs `ensure_consumer` after `RECREATE_AFTER_FAULTS` (3) pull failures in a row (`Other`/`Unavailable` items or a failed `.messages()` open). A delivered message, a clean empty batch or a `409 MaxAckPending` clears the count; a failed re-creation keeps it. Acks of messages in flight at re-creation hit the old consumer's delivery and are ignored: the message is redelivered and absorbed by the dispatch ledger (only with a real ledger: Noop unless `GREENTIC_AW_REDIS_URL` + `redis` backend, where `agent_node::serve_agentic` installs `RedisDispatchLedger`; `serve()` / `RuntimeAgentDispatchInvoker::new` default to Noop; and the old-delivery acks are ignored only if the durable was really deleted) or held off by the session lock; the delivery count restarts, the busy age budget does not.
- `ensure_consumer` converges from the durable's REAL config on the error path (`consumer_info` -> `converged_consumer_config`), never `agentic_consumer_config()` blindly; `converged_consumer_config` keeps an unlimited `max_ack_pending` (`<= 0`, sent as `-1`) and any value above 512.
- `url_redact::redact_http_url` now returns the ORIGIN only (`scheme://[redacted]@host:port`, plus `/…` if there was a path/query/fragment); the "last `@`" rule was superseded in Round 22. `HttpConfigProvider` (`http_provider.rs`) strips the URL from reqwest errors.
- `runner_core::packs::resolver::strip_url_suffix` no longer counts parentheses: the URL ends at the first `"): "` or a closing `)` at the end of the message, else everything from ` for url (` on is dropped.
- Tool-secret env deny-list adds `OTEL_EXPORTER_OTLP_*HEADERS`, `IDENTITY_HEADER`/`IDENTITY_ENDPOINT`, `MSI_SECRET`/`MSI_ENDPOINT`, the `VAULT_` prefix, `REDIS_PASSWORD`, `PGPASSFILE`, `GOOGLE_CREDENTIALS`, `GOOGLE_ACCESS_TOKEN`, `MONGODB_URI`, `AMQP_URL`. `*_API_KEY`, `GITHUB_TOKEN`/`GH_TOKEN`/`HF_TOKEN`, `AWS_REGION`/`AWS_PROFILE`, `AZURE_TENANT_ID`/`AZURE_CLIENT_ID` stay usable.
- The directory-pack `read_pack_file` 64 MiB cap (`MAX_ARCHIVE_ENTRY_READ_BYTES`, same as archive entries) came from `c573decf` (Round 19), not Round 20.
- See `docs/audit-breaking-changes.md` (Round 21).

### Round 22 notes

- `aw-event-bridge` JetStream: `recreate_consumer` no longer resets the backoff `attempt` (`recreate_backoff` escalates on success and failure); only a received message, a cleanly ended pull or `409 MaxAckPending` resets it. A persistent non-durable fault now climbs to the 30 s cap instead of cycling every ~2 s.
- `url_redact::redact_http_url`: authority ends at the first `/?#`; an `@` after it makes the URL ambiguous and only `scheme://[redacted]` is shown (never a host or suffix). `@` all inside the authority is userinfo (`scheme://[redacted]@host`).
- Tool-secret env deny-list adds `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` (case-folded), `SENTRY_DSN`, `GIT_ASKPASS`, `DOCKER_CONFIG`; `NO_PROXY` stays allowed.
- `HttpConfigProvider::with_client` lets the URL-leak test use a `.no_proxy()` client.
- See `docs/audit-breaking-changes.md` (Round 22).

### Round 26 notes

- Any code that shows a URL without its credentials uses `runner_core::url_split::split_url` (authority ends at the first `/?#`; an `@` after it is ambiguous, show `scheme://[redacted]` only; scheme must be valid). `redact_locator`, `log_safe_origin` and the run-outcome redactor do; `url_redact::redact_http_url` keeps its own copy (aw-runtime copy of the same rule).
- The A2A and `knowledge_index` clients are `.no_proxy()`: the SSRF guard is in the DNS resolver, which a proxy bypasses. The `runtime.rs` broker/OAuth/mail client deliberately keeps the env proxy.
- `unit_pack_segment`: a lossy pack id carries a 40-hex hash so its pack part is longer than any lossless id (<= 40 bytes). Pack-extension staging also has a per-pass unpacked-bytes budget (1 GiB) and sweeps idle complete slots of any id. See `docs/audit-breaking-changes.md` (Round 26).

### Engine and tool-secret rules (round 11)

- **Tool-secret env fallback has a deny-list.** `secret://a/b` maps to env var `A_B` for local runs
  (`agent_node::env_secret_lookup`), but host configuration resolves as not found
  (`env_secret_name_is_denied`): the exact names `ADMIN_TOKEN`, `OPERATOR_TOKEN`, `PACK_PUBLIC_KEY`,
  `SLACK_SIGNING_SECRET`, `WEBEX_WEBHOOK_SECRET`, `WHATSAPP_VERIFY_TOKEN`, `WHATSAPP_APP_SECRET`,
  `TELEGRAM_BOT_TOKEN`, `REDIS_URL`, `OTLP_ENDPOINT`, `OTLP_HEADERS`, `PGPASSWORD`, `KUBECONFIG`, `NPM_TOKEN`,
  `CARGO_REGISTRY_TOKEN`, `SSH_AUTH_SOCK`, and any name with the prefix `GREENTIC_`,
  `TENANT_JWT_`, `SECRETS_`, `PACK_`, or (ambient cloud/infra credentials, narrowed in round 19) the exact names
  `AWS_ACCESS_KEY_ID`, `AZURE_CLIENT_SECRET`, `AZURE_USERNAME`, `AZURE_PASSWORD`, `GOOGLE_APPLICATION_CREDENTIALS`
  and the prefixes `AWS_SECRET_`, `AWS_SESSION_`, `AWS_CONTAINER_`, `AWS_WEB_IDENTITY_`, `AWS_SHARED_`,
  `AZURE_FEDERATED_`, `AZURE_CLIENT_CERTIFICATE_`, `AZURE_STORAGE_`, `GOOGLE_CLOUD_`, `GCP_`, `DATABASE_`, `NATS_`,
  `POSTGRES_`, `MYSQL_`, `KUBERNETES_`. Third-party API keys sharing a vendor prefix (`GOOGLE_API_KEY`,
  `AZURE_OPENAI_API_KEY`, `GITHUB_TOKEN`, `GH_TOKEN`) and the non-secret `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` are NOT denied. Extend `ENV_SECRET_DENY_EXACT` / `ENV_SECRET_DENY_PREFIXES` when a new
  host-level env secret is introduced. `OPENAI_API_KEY` is deliberately NOT denied.
- **Tool-extension HTTP client** (`build_tool_http_client`): `redirect::Policy::none()` plus the `http_guard`
  address policy on DNS (a reqwest-0.12 resolver, since the ext-runtime pins 0.12 and `GuardedResolver` targets 0.13).
  The client is built once per process and cached even when it is `None` (the blackhole listener could not bind, the
  builder failed): every later tool HTTP call then fails, so each failure site logs an `error!` with the reason.
- **Pack JSON Schemas never fetch**: `schema_validator::compile_validator` installs a retriever that refuses every
  external `$ref`.
- **Playbook turns** (`playbook_turn::build_turn`) get the calling worker's guardrail policy, billing meter
  (`PlaybookTurnHost.billing_meter`) and MCP source.
- **Persistence after side effects is never a reason to re-run them.** `resume.clear` / `save_with_run` after a
  flow ran go through `persist_after_effects` (retries the write only) and fail as
  `RunnerError::PostEffectPersistence`, which `retry_with_jitter` does not retry.
- **A failed effectful node is terminal too.** `drive_flow` sets its effects flag when a `performs_external_effect` node
  succeeds AND when one fails (except `flow.call`, whose nested `drive_flow` classifies itself), so a transient failure
  after a partial effect is never whole-turn retried. The default is conservative (an unmarked error sets the flag), but
  a failure wrapped with `engine::pre_effect(err)` (`PreEffectError`: proven to precede every effect, e.g. no handler
  configured, `provider.invoke` before `invoke_provider`, a NATS request that was never published) does not, so a
  transient one still retries. Mark only what is provable; an earlier completed effect still makes the turn terminal.
  `terminal_after_effects` rebuilds the error as a `TerminalFlowError` whose `Display` is only the OUTERMOST message
  (it reaches `metadata.error_message`, user facing) but whose `source()` keeps the original causes, so `{err:#}` /
  `anyhow_chain_text` (operator log, redacted run error, `FlowTerminal.reason`) still see the root cause; never put
  `err:#` into a user-facing string. Note `FlowTerminal.reason` (and the non-terminal `AdapterCall.reason`) carries the
  full chain and is returned to ingress callers via `Err`: it is operator-side text, not the user envelope (which uses
  `to_string()`), so do not forward it verbatim to an end user. The full chain is also logged at `warn`. A deterministic pre-effect error whose
  message echoes input (`tenant_ctx_for_audit`) is a `terminal_flow_error` so `should_retry`'s text heuristic cannot
  retry it; other deterministic errors containing "internal"/"unavailable"/"timeout" can still be retried (known).
- **`redirect_count` (`flow.goto`) is per turn**, reset on resume. `drive_flow` also caps one turn at
  `MAX_STEPS_PER_TURN` (10,000) nodes.
- **`RunnerHost` installs `DiscardSessionHost`** as the engine-level `SessionHost`; the store-backed host was
  write-only and leaked a row per turn. Parked snapshots live in `FlowResumeStore`, not there.

### Credentials in URLs and errors (round 18)

Rules that fail silently if changed. (1) A `reqwest::Error`'s `Display` appends ` for url (<full url>)`; never format one
into a log line, error context or guest-facing message without `without_url()` (it consumes the error, so compute
`is_blocked_error` first). The host `http-client` logs only `log_safe::log_safe_origin` (scheme+host+port; the PATH can
hold a token, so a path-preserving helper is not enough there; `log_safe_url` was removed). A body-read `io::Error` wraps a reqwest error: use
`http_guard::body_read_reason`. (2) A pack locator may be a presigned S3 / Azure SAS URL: put it in an error or log only
through `runner_core::packs::resolver::redact_locator`, and reload failures through `watcher::redact_chain`
(`HealthState::last_error`, hence `/admin/packs/status`, is redacted too). (3) `run_outcome::error::redact` decides
whether a `key=value` key is a credential by its LAST `-`/`_` segment (`is_sensitive_key`), not an exact list; add a new
credential word to `SENSITIVE_SUBSTRINGS` / `SENSITIVE_SEGMENTS`. (4) The SQLite SQL gateway accepts a single
`SELECT`/`WITH`/`VALUES` statement (`execute::check_sqlite_read_only_sql`): `read_only(true)` alone does not stop
`VACUUM INTO`, `ATTACH` or stacked statements. See `docs/audit-breaking-changes.md` (Round 18).

### Per-session turn lock

`FlowResumeStore::fetch` does not consume a parked snapshot (it is cleared after
a completed turn or overwritten by a new park), so two concurrent messages for
one parked session used to both resume it and repeat its effects. Both
`PackFlowAdapter::call_traced` and `RuntimeSessionResumer::resume_checked`
(admission check + resume as one unit) now run under `engine::turn_lock`, an
in-process keyed async mutex (key = `env:tenant:<derived store user id>`, the
identity the park is stored under). Different sessions run in parallel; the
guard is released on drop (cancellation/panic); map entries are swept when
unused. The held keys ride a task-local, so a nested run for the same session on
the same task (the resumer's inner `handle`) skips the lock; a `tokio::spawn`
between them would NOT inherit it. Lenient-resumer semantics are unchanged: the
loser of the race finds no park and starts a fresh run. The state machine takes the same lock around the WHOLE
`retry_with_jitter` loop of `execute_adapter_step` (key from `engine::runtime::turn_key_for_payload`, the one
derivation shared with `call_traced`), so another message cannot consume the parked snapshot between a failed
attempt and its retry; the inner per-attempt acquisition skips through the task-local. **Limit: in-process
only.** Replicas sharing a Redis session store need a store-level
compare-and-set/lease, which the session store does not expose today.

### Whole-turn retry rules (round 13)

Two retry layers re-run a failed turn from its start: the engine's own loop (`execute_with_options`,
`FlowRetryConfig`) and the state machine's `retry_with_jitter`. Both stop on a `TerminalFlowError`.

- **After an effect, a failure is terminal.** `drive_flow` tracks whether a node that performs an external effect
  has succeeded in THIS run (`NodeKind::performs_external_effect`: `provider.invoke`, `flow.call`, `dw.agent`,
  `dw.agent_graph`, `sorla/operala/agentic/telco-x/approval.call`, `mcp`, `a2a`, `media.create`); any later failure
  of the run is rewrapped by `terminal_after_effects` (outermost message as `Display`, causes kept as `source()`) so neither layer
  repeats the effect. Failures BEFORE any such node keep their classification, so transient failures still retry.
  Not counted, on purpose: `emit.*` (only appends to the turn's egress, dropped on failure), `var.set`, `state.get`,
  `state.set` (an overwrite is idempotent), `session.wait`, `flow.goto`. **Known limitations:** generic component
  nodes (`Exec` / `PackComponent`) are opaque to the host, so a component that wrote to an external system is NOT
  counted (counting them would turn the whole-turn retry into a no-op for nearly every flow); a `state.set` that
  is a read-modify-write (a counter) re-applies on retry; and the node that itself fails may have partly acted.
  If you add a node kind with an external effect, add it to `performs_external_effect`.
- **Deterministic jump failures are terminal**: `unknown_flow` (flow not registered), `unknown_node`, "no start node"
  and a malformed `greentic_control` block (`parse_component_control`). A `load_flow` I/O failure stays retryable.
- **A RESUMED run that fails after its own effect clears the park (round 26).** `fetch` does not consume a snapshot, so
  a resume that ended in `Err` used to leave the wait in place and the next message repeated the effect.
  `terminal_after_effects` now stamps `TerminalFlowError.after_effects` (also on an already-terminal error, e.g. a
  step budget hit after an effect); `is_after_effects_failure` reads it. The top-level turn path resumes through
  `FlowEngine::resume_surfacing_failures`, which turns such a failure into the same metadata-only `Ok` envelope a
  fresh run gets (`surfaced_failure_execution`), so `call_traced` takes its ordinary `Completed` branch:
  `persist_after_effects(resume.clear)`, `PostEffectPersistence` semantics unchanged, turn lock still held. Every
  OTHER resume failure stays an `Err` and KEEPS the park: a deterministic pre-effect one (`unknown_node`, step
  budget before any effect) and a transient pre-effect one, so the user can retry; a deterministic one therefore
  keeps failing on each message until the pack is fixed (known, unchanged). Plain `FlowEngine::resume` (nested flow
  tools, `pack.rs`) is unchanged and still returns `Err`: a flow tool's caller must SEE the failure.

**Watchdog registration and state migration (round 26).** `remote_dispatch::arm_watchdog` holds the `WATCHDOGS` lock across
spawn + insert, so a near-zero deadline cannot deregister before its entry exists (it used to leave a finished
`JoinHandle`, which `cancel_watchdog` then reported as "cancelled"). `StateStoreHost::get_json` migrates a pre-escaping
legacy key with `set_json_if_absent` (a concurrent `set_json` on the current key wins and is what the caller sees);
a store whose `set_json_if_absent` errors (its default) keeps the old unconditional write, so the stale-overwrite
window remains only there and is normally closed by the turn lock anyway.

**Known limitation: a failed resume after admission leaves the wait without a timeout.** `RuntimeSessionResumer::resume_checked` retires the deadline watchdog at admission, before `handle` runs. If `handle` then fails with an infrastructure error (resume-store read/save, `PostEffectPersistence`) the response is spent and no watchdog remains, so a still-parked wait has no timeout and the user hears nothing until they message again (`dispatch_to_resumer_outcome` logs it at `error`). Not re-armed on purpose: the original deadline is not recorded, a synthetic `timeout` would resume the park and repeat effects after a post-effect failure, and an approval would also need its nonce/`decision_token`. A fix needs the deadline persisted with the park.

**Backpressure (aw-event-bridge JetStream path).** `run_bridge_jetstream` takes a `MAX_INFLIGHT` semaphore slot before each pull (`fetch().max_messages(<slots>)`), so unread messages stay in the stream; the durable's `max_ack_pending` is `2 * MAX_INFLIGHT` and `ensure_consumer` converges a stale one. That limit is CONSUMER-WIDE, so it is a fleet-wide ceiling shared by every replica, not a per-replica cap; the headroom is for Nak'd (busy/error) messages, which hold ack-pending without a permit. A pull answered `409` (`Exceeded MaxAckPending`, async-nats surfaces it as a stream error item) is retried after a fixed 500 ms (`classify_pull_error`), at `debug`, without escalating the backoff; a pull that returns no message pauses 50-100 ms (`empty_batch_delay`) so an idle stream cannot hot-loop. Pulling a message without a slot would let it sit unacked, with no progress acks, past `ack_wait` and run twice.

**Bounded intake of pack-carried extensions and store artifacts.** `pack_extensions::stage_from_packs` stages one archive at a time (read, stage, release) and caps a pass at 64 entries / 512 MiB total (`stage_bounded`; over-cap entries become `StagingFailure`s). `mcp_store_pull::download_artifact` streams the body through `collect_capped` against `MAX_ARTIFACT_BYTES`; never use `response.bytes()` there.

### Pack Hot-Reload

Full list of user-visible and breaking changes from the audit branch: `docs/audit-breaking-changes.md`.

Pack index (JSON, local/HTTPS/cloud) polled at `PACK_REFRESH_INTERVAL` (default 30s). On change: resolve locators → verify digest/signature → cache artifacts → atomically swap `TenantRuntime` via `ArcSwap`. Overlays independently deployable per tenant.

Three things that fail silently if changed: (1) the superseded runtime's cron timers are stopped with a per-runtime `CancellationToken` that `adapt_timer::drive_schedule` observes ONLY while sleeping — never `JoinHandle::abort`, which would cancel a tick mid-`handle()` after its effects but before the snapshot save; (2) the digest used for signatures and the watcher's applied-digest comparison is the one COMPUTED from the cached file (`CachedPack.digest`, canonical lowercase), never the index's spelling of the pin; (3) `PackCache::store` prunes idle sibling digest dirs, but a loaded `PackRuntime` keeps its archive PATH and re-opens it lazily, so pruning is gated on mtime (cache hits refresh it), keeps the newest 2, and must never delete anything recently used. The idle gate is `max(1h, 2 x PACK_REFRESH_INTERVAL)`
(`runner_core::packs::prune_min_idle`, applied through `PackManager::with_refresh_interval` by the watcher); `touch`
opens the archive READ-ONLY (a 0444 cached archive used to fail the write open, so its mtime never refreshed); and a
runtime kept because its republish failed to build, because the tenant's pack resolve failed, or because it is in
backoff has its archives touched (`watcher::keep_archives_alive`; the resolve-failure branch was missing it until round 26).
`ActivePacks::insert_pack` / `insert_revision` / `remove_revision` cancel the cron timers of the runtime they replace or
remove (not when the same `Arc` is re-inserted), so an embedder still holding it does not double-fire a tick.

Whole-turn retry (`retry_with_jitter`) never re-runs a `RunnerError::FlowTerminal`/`PostEffectPersistence`. Deterministic or post-effect flow failures are raised through `runner::engine::terminal_flow_error` (`TerminalFlowError`: step limit, `redirect_limit`, unknown node/flow, missing required var); `should_retry` also returns false for them.

The pack cache path is `<name>/<version>/<sha256>/pack.gtpack` (the content digest is always in the key, so equal `name@semver` with different bytes coexist). A pinned-digest pack whose cached file still hashes to it is not re-downloaded on refresh. HTTP pack/index downloads are capped (`GREENTIC_PACK_MAX_DOWNLOAD_MB` 1024, `GREENTIC_PACK_INDEX_MAX_MB` 32, `0` disables) and the total deadline is `GREENTIC_PACK_DOWNLOAD_TIMEOUT_SECS` (900), not `read_timeout_ms`. `pack.lock` is looked up only beside the archive, and pack-authored asset/component paths are confined to the pack root.

**Known limitation: gtbind `pack_locator` packs cannot be verified.** `maybe_write_gtbind_index` generates the index from `*.gtbind` bindings, and a pack binding carries no signature (nor a digest for a semver `pack_ref`). With a trust key configured the loader requires a signature for every entry, and strict mode a pinned digest for remote packs, so such packs are refused; startup logs a warning when it generates an index with a key set. Verification is deliberately not relaxed. Use a signed pack index for verified deployments, or leave the trust key unset in development.

### Key Modules in `greentic-runner-host`

**Core lifecycle & runtime**

| Module | Responsibility |
|--------|---------------|
| `host.rs` | Multi-tenant builder, `RunnerHost` lifecycle |
| `boot.rs` | Startup bootstrap sequence |
| `runtime.rs` | `TenantRuntime`, atomic pack swapping via `ArcSwap` |
| `runtime_refs.rs` | Shared runtime reference handles |
| `runtime_wasmtime.rs` | Wasmtime engine/linker setup, WASI linkage |
| `config.rs` | Runtime configuration loading and defaults |
| `watcher.rs` | Pack index polling and hot reload |

**Pack & component loading**

| Module | Responsibility |
|--------|---------------|
| `pack.rs` | Component loading, flow/template discovery, Wasmtime linking |
| `component_api.rs` | Component invoke API (describe, invoke) |
| `verify.rs` | Ed25519 signature verification |
| `wasi.rs` | WASI host-import wiring |
| `cache/` | Component artifact caching (memory LRU + disk, singleflight dedup) |

**Flow engine (`engine/`)**

| Module | Responsibility |
|--------|---------------|
| `engine/mod.rs` | `FlowEngine` entry, DAG walker |
| `engine/state_machine.rs` | State transitions, pause/resume orchestration; holds the turn lock across the whole retry loop |
| `engine/turn_lock.rs` | In-process per-session async mutex (task-local reentrant); see "Per-session turn lock" |
| `engine/runtime.rs` | `PackFlowAdapter`, `FlowResumeStore`, `IngressEnvelope`, `turn_key_for_payload` |
| `engine/builder.rs` | Engine builder pattern |
| `engine/policy.rs` | Execution policy enforcement |
| `engine/registry.rs` | Node-type registry |
| `engine/glue/`, `engine/shims/` | Adapter glue and host shims for component calls |

**Runner (invocation pipeline)**

| Module | Responsibility |
|--------|---------------|
| `runner/engine.rs` | Flow DAG interpretation, node execution |
| `runner/operator.rs` | Built-in operators (emit, wait, flow call, provider invoke) |
| `runner/invocation.rs` | Invocation envelope construction and dispatch |
| `runner/flow_adapter.rs` | Flow-to-runtime adapter |
| `runner/schema_validator.rs` | Runtime schema validation |
| `runner/contract_cache.rs` | Cached component contract descriptors |
| `runner/contract_introspection.rs` | v0.6 component introspection support |
| `runner/templating.rs` | Handlebars template rendering |
| `runner/i18n.rs` | Inline i18n locale handling |
| `runner/adapt_timer.rs` | Timer/cron ingress normalization |
| `runner/adapt_events_email.rs` | Email event ingress normalization |

**HTTP layer (`http/`)**

| Module | Responsibility |
|--------|---------------|
| `http/mod.rs` | Module declarations only (`admin`, `agent_chat`, `agent_stream`, `auth`, `health`). The Axum router is assembled in `runner/mod.rs::router`: `/healthz`, `/admin/*`, `/agent/chat*`, `/operator/op/invoke`, `/sql/*`. Provider webhook ingress is NOT registered there; the per-provider `Activity` normalization lives in the `runner/adapt_*` modules and `activity.rs`, and the HTTP receivers for provider webhooks are hosted outside this router |
| `http/admin.rs` | Admin API endpoints |
| `http/auth.rs` | `AdminAuth` holds the configured admin and operator tokens (config, not an extractor); `AdminGuard` is the extractor that gates admin/chat routes (bearer token, else loopback only); `require_operator` is the route layer for `POST /operator/op/invoke` |
| `http/health.rs` | Health/readiness probes |

**Cross-cutting**

| Module | Responsibility |
|--------|---------------|
| `activity.rs` | Canonical `Activity` type for ingress normalization |
| `routing.rs` | Tenant + flow routing from inbound requests |
| `identify_hint.rs` | Provider identification hints for routing |
| `provider.rs` | Provider abstraction layer |
| `provider_core.rs` | Provider-core schema integration |
| `provider_core_only.rs` | Guard enforcing provider-core-only mode |
| `oauth.rs` | OAuth token brokering for components |
| `secrets.rs` | Secrets host-import implementation |
| `storage/` | Session + state backend adapters (in-memory, Redis) |
| `metrics.rs` | Prometheus metrics collectors |
| `operator_metrics.rs` | Per-operator metric instrumentation |
| `operator_registry.rs` | Operator registry (node-type → handler mapping) |
| `telemetry.rs` | OpenTelemetry span setup |
| `telemetry_scan.rs` | Telemetry configuration scanning |
| `greentic_x_provider.rs` | Greentic-X extension provider (behind `greentic-x-provider` feature) |
| `gtbind.rs` | Bindings generation helpers |
| `http_guard.rs` | Outbound SSRF/size guard shared by the host `http-client` and `wasi:http` (`GREENTIC_HTTP_*`) |
| `http_timeout_hooks.rs` | Ceiling on outbound `wasi:http` request timeouts (`HttpTimeoutHooks`) |
| `wasm_limits.rs` | Guest execution deadline (epoch interruption) and linear-memory/table caps (`GuestLimiter`) |
| `log_safe.rs` | `log_safe` / `log_safe_max`: strip control and bidi characters from peer-supplied text before logging |
| `secrets_broker.rs` | `BrokerSecretsManager` (broker HTTP client) and `is_safe_secret_segment` / `validate_tenant_segment` |
| `caller_identity.rs` | The one place a provider-verified caller enters the runtime (`FlowContext::caller`) |
| `extension_provider.rs` | Host adapter for the `greentic:extension-provider` worlds (0.1.0 and 0.2.0) |
| `sql/` | In-runtime SQL gateway (`/sql/<conn>/schema` and `/query`) over read-only sqlx pools |
| `run_outcome/` | Deployed run audit: one outcome event per flow turn (`RunOutcomeReporter`) |
| `fault/` | Fault injection framework (behind `fault-injection` feature) |
| `testing/` | Test utilities and mock helpers |
| `trace/` | Distributed tracing helpers |
| `validate/` | Runtime validation utilities |

## Conventions

- **Rust 1.95.0**, edition 2024, pinned via `rust-toolchain.toml`
- **YAML**: Uses `serde_yaml_gtc` (imported as `serde_yaml_bw`), not `serde_yaml`
- **Error handling**: `anyhow::Result<T>` with `.context()`; `thiserror` for domain errors
- **Serialization**: JSON for messages, CBOR for components/caching, YAML for config
- **Concurrency**: `ArcSwap` for lock-free runtime swaps, `parking_lot::Mutex` preferred over `std::sync::Mutex`
- **Dynamic dispatch**: `dyn SessionHost`, `dyn StateHost`, `dyn SecretsManager` for pluggable backends
- **i18n**: Locale bundles embedded at compile time via `build.rs`; user-facing strings use i18n keys, never hardcoded
- **Provider-core only**: Packs use `greentic:provider/schema-core@1.0.0` components; legacy typed provider worlds are not supported
- `GREENTIC_PROVIDER_CORE_ONLY=1` is set by default in CI

## Public API Surface

```rust
// Mirror the CLI
greentic_runner::run_http_host(RunnerConfig) -> Result<()>

// Embedded host (no HTTP server) for tests/tools
greentic_runner::start_embedded_host(HostBuilder) -> Result<RunnerHost>
```

## Binaries

| Binary | Purpose |
|--------|---------|
| `greentic-runner` | Main HTTP host (pack watcher + ingress) |
| `greentic-runner-cli` | CLI companion (`required-features = ["legacy-gen-bindings"]`) |
| `greentic-gen-bindings` | Inspects a `.gtpack` and emits a `bindings.yaml` seed (`required-features = ["legacy-gen-bindings"]`) |

## Key Environment Variables

| Variable | Purpose |
|----------|---------|
| `PACK_REFRESH_INTERVAL` | Watcher cadence (e.g. `30s`, `5m`) |
| `PACK_CACHE_DIR` | **Not read by the runner.** The pack cache is `packs.cache_dir` in greentic-config (default `<cache_dir>/packs`); only a test sets this variable. Same for `PACK_SOURCE` / `PACK_INDEX_URL`: the index comes from `packs.source` or the generated gtbind index. |
| `PACK_PUBLIC_KEY` | **Not read as an env var.** Trust keys come from `packs.trust.public_keys` in greentic-config (several are joined for rotation); the name only appears in error messages. A configured key makes every pack entry need a valid signature, regardless of `PACK_VERIFY_STRICT`. |
| `PACK_VERIFY_STRICT` | Truthy (`1`/`true`/`yes`/`on`) turns strict verification on (also on via `packs.trust.require_signatures`): a key is required at startup, signatures must be canonical (see `PACK_ALLOW_LEGACY_SIGNATURES`), remote packs need a pinned digest, and `fs://` locators are confined (`PACK_FS_ALLOWED_ROOTS`). It can only add checks; `0`/`false` does NOT disable verification when a trust key is configured. |
| `GREENTIC_TRACE_OUT` | Path of the trace file. Setting it to any value turns trace capture ON; unset means `Off` (default) and nothing is written. `--trace` / `--trace-out` override it (`--trace-out` alone also turns it on). |
| `GREENTIC_TRACE_CAPTURE_INPUTS` | Exactly `1` records each node's full invocation input (`invocation_json`) in the trace, which may contain user data; any other value, or unset, leaves inputs out. `--trace-capture-inputs on|off` overrides it. |
| `PORT` | HTTP server port (also `--port` CLI flag) |
| `DEFAULT_TENANT` | Fallback tenant for routing (blank = unset) |
| `TENANT_RESOLVER` | Routing mode: host/header/jwt/env. An unsupported value (typo) fails startup via `RoutingConfig::startup_check` |
| `ADMIN_TOKEN` | Bearer token for `/admin` endpoints (loopback-only when unset) |
| `OPERATOR_TOKEN` | Bearer token for `POST /operator/op/invoke`; takes precedence over `ADMIN_TOKEN` for that route. With neither set the route stays open and startup logs a warning. The token is accepted from EITHER `Authorization: Bearer <token>` OR the dedicated `X-Operator-Token: <token>` header (needed when `Authorization` already carries a tenant JWT under `TENANT_RESOLVER=jwt`); both are compared in constant time |
| `TENANT_JWT_SECRET` | HS256 shared secret for `TENANT_RESOLVER=jwt`: verifies signature, requires and checks `exp` (unless `TENANT_JWT_ALLOW_NO_EXP`) and `nbf` (60s leeway) and optional `iss`; failures (bad signature, expired, not-yet-valid, wrong `iss`/`alg`, missing or non-bearer `Authorization`) are answered `401`; a verified token lacking the `tenant` claim and other malformed routing input stay `400`. Only `alg: HS256` is accepted (`none`/RS256 rejected). RS256/ES256 (PEM) is not implemented |
| `TENANT_JWT_ALLOW_NO_EXP` | Truthy (`1`/`true`/`yes`/`on`): accept verified-mode JWTs that carry no `exp` claim. **Breaking change:** by default (unset) a token without `exp` is now rejected (401), because a non-expiring bearer is a permanent credential. A present `exp` is always enforced |
| `TENANT_JWT_ISSUER` | If set, the token `iss` must equal it (jwt mode with `TENANT_JWT_SECRET`) |
| `TENANT_JWT_TRUST_GATEWAY` | Truthy (`1`/`true`/`yes`/`on`): explicit opt-in for `TENANT_RESOLVER=jwt` without a key, i.e. decode-only because an upstream gateway already verified the token (startup warning). Without a key and without this, startup fails |
| `GREENTIC_EVENTS_NATS_URL` | NATS bus URL; enables `sorla.call`/`operala.call`/`agentic.call`/`telco-x.call` dispatch + in-proc agentic serve. The runner registers response listeners for `sorla`/`operala`/`agentic`/`telco-x`; the `telco-x-event-bridge` crate serves `greentic.telco-x.request.v1` (Phase 2 transport scaffold). A credit-free `telco-x-serve` bin runs the bridge with the built-in `EchoInvoker` (`cargo run -p telco-x-event-bridge --bin telco-x-serve`); real Telco-X operations need a production `TelcoXDispatchInvoker` impl. Until then `telco-x.call` only echoes. See `docs/superpowers/specs/2026-06-21-telco-x-runtime-dispatch-design.md` in the workspace root |
| `GREENTIC_AGENTIC_SERVE_INPROC` | Opt-in (default OFF): co-host the agentic-worker NATS service in-process; truthy (`1`/`true`/`yes`/`on`) + `GREENTIC_EVENTS_NATS_URL` set |
| `GREENTIC_AGENT_MANIFESTS_DIR` | Dir of `<agent_id>.json` full `AgentConfig` files; process-level agent source for in-proc serve |
| `GREENTIC_AW_PACK_EXTENSIONS` | Set to `0` to ignore design extensions carried inside a `.gtpack` (`extensions/*.gtxpack`) and keep the on-disk scan as the only source. Mirrors `GREENTIC_AW_MCP` / `GREENTIC_AW_COMPONENT_TOOLS` / `GREENTIC_AW_FLOW_TOOLS`. |
| `GREENTIC_AW_A2A` | Set to `0` to ignore the `assets/a2a-routes.json` sidecar a `.gtpack` carries, so a worker's `a2a:` tools AND every `component == "a2a"` flow node resolve to nothing. Mirrors `GREENTIC_AW_MCP`. A route with `requires_auth` reads its token per call from `secrets://default/<tenant>/<auth_team\|_>/a2a/<agent_id>` (unit-scoped `<agent_id>.unit-<segment>` first). |
| `GREENTIC_AW_STATE_BACKEND` | AW state backend selector: `redis` \| `memory` \| `disk`. Unset → `redis` if `GREENTIC_AW_REDIS_URL` is set, else `memory` (ephemeral, in-process). `memory`/`disk` give single-process locking only — multi-instance HA needs `redis`. |
| `GREENTIC_AW_STATE_PATH` | On-disk (redb) file path when `GREENTIC_AW_STATE_BACKEND=disk` (default `~/.greentic/aw-state.redb`, falling back to `/var/lib/greentic/aw-state.redb`). |
| `GREENTIC_AW_REDIS_URL` | Agentic-worker Redis state store. **Optional** — the worker defaults to an in-memory backend when unset; set this (or `GREENTIC_AW_STATE_BACKEND=disk`) for durable / multi-instance state. |
| `GREENTIC_SESSION_REDIS_URL` | Opt-in (default unset = in-memory): Redis URL for the parked-flow SESSION store. Needs a keyspace: `GREENTIC_RUNNER_SESSION_NAMESPACE` or `GREENTIC_ENV` (-> `greentic:session:<env>`); wait TTL `GREENTIC_RUNNER_SESSION_WAIT_TTL_SECS` (default 24h, `0` = none). Independent of the state var. Connect failure or missing keyspace is a hard startup error, never a memory fallback. An embedder's `HostBuilder::with_storage` wins over env. **Multi-replica deployments need this (and the state var) for `session.wait` to resume on a different pod.** |
| `GREENTIC_STATE_REDIS_URL` | Opt-in (default unset = in-memory): Redis URL for the flow STATE store (keys already scoped by env/tenant by greentic-state). Independent of the session var; same hard-error and embedder-wins rules. |
| `GREENTIC_FLOW_STATE_TTL_SECS` | Seconds before a flow-state entry written through `StateStoreHost` expires. Unset/`0` = never expires (on Redis that leaves permanent keys for abandoned conversations). A value above `u32::MAX` is clamped to `u32::MAX` with a warning; an unparsable value is warned about and ignored |
| `SECRETS_BACKEND` | Secrets manager for host secrets: `env` (default; only allowed when `GREENTIC_ENV` is `local`/`dev`/`test`, or unset, which assumes `local`) or `broker`. Anything else is an error. MCP credentials only resolve under `broker` (or a manager the host injects) |
| `SECRETS_BROKER_ENDPOINT` | Base URL of the greentic-secrets-broker, required when `SECRETS_BACKEND=broker` (trailing `/` trimmed). Requests go to `/v1/<env>/<tenant>/<team>/<category>/<name>`; every segment is validated (no `.`/`..`, `/`, `?`, `#`, `%`, `\\` or control characters) and percent-encoded |
| `SECRETS_BROKER_TOKEN` | Bearer token sent to the broker (may be empty for an unauthenticated broker) |
| `SECRETS_CACHE_TTL_SECS` | TTL of the in-process secrets read cache (`CachingSecretsManager`); default `300` |
| `SECRETS_CACHE_MAX_ENTRIES` | LRU capacity of that cache; default `512`, `0`/invalid falls back to the default |
| `GREENTIC_OAUTH_BROKER_SHARED_SECRET` | Shared secret sent as `Authorization: Bearer` on OAuth broker token requests (including the email-send path). Overrides `oauth.shared_secret` from the host config; a blank value is treated as unset. Never logged |
| `GREENTIC_PRESENT_PROVIDER_AS_USER` | Opt-in (default OFF): restore the legacy presentation in which a component invoked by a flow with no verified caller is told the PROVIDER id as its `user` / `user_id`. See "What a component is told about its caller" above. Truthy = `1`/`true`/`yes`/`on`. |
| `GREENTIC_WASM_MAX_EXEC_SECS` | Per-invocation wall-clock deadline for a guest component (epoch interruption, 1s granularity). Default `300`; `0` disables. A trap surfaces as an ordinary invocation error. |
| `GREENTIC_WASM_MAX_MEMORY_MB` | Cap on any one guest linear memory AND on the total linear memory across all memories in one store (`wasm_limits::GuestLimiter`; multi-memory is enabled). Default `1024`; `0` disables both. At most 16 memories per store. Table/instance counts are always capped at generous fixed values. A grow past the cap is denied (`memory.grow` returns `-1`, no trap); the first denial per process is a `warn!`, later ones `debug`. |
| `GREENTIC_SHUTDOWN_GRACE_SECS` | Seconds in-flight HTTP requests get to drain after SIGTERM/ctrl-c before being cancelled (axum graceful shutdown). Default `30`; `0` aborts at once. `host.stop()` runs on every exit path. |
| `GREENTIC_HTTP_ALLOW_METADATA` | Truthy disables the default outbound block on link-local (`169.254/16`, `fe80::/10`) and cloud-metadata addresses (`169.254.169.254`, `fd00:ec2::254`, `100.100.100.200`, Azure WireServer `168.63.129.16`, Oracle `192.0.0.192`). |
| `GREENTIC_HTTP_DENY_PRIVATE` | Opt-in (default OFF): also block loopback, `0.0.0.0/8`, RFC1918, CGNAT and ULA destinations. |
| `GREENTIC_HTTP_MAX_RESPONSE_MB` | Cap on an outbound response body (host `http-client` and `wasi:http`). Default `64`; `0` disables. |
| `GREENTIC_WASI_HTTP_ENFORCE_POLICY` | Opt-in (default OFF): make `wasi:http` honour a tenant's `http_enabled = false` (that flag is true only when the tenant has a `messaging` binding, so default enforcement would break other tenants' wasi-http components). |
| `GREENTIC_STATE_MAX_VALUE_MB` | Cap on one guest `state.write` / `write-if-absent` value. Default `16`; `0` disables. Over the cap the guest gets a normal `too-large` state error. |
| `GREENTIC_HTTP_MAX_REQUEST_MB` | Cap on one guest outbound `http-client` request body. Default `64`; `0` disables. Over the cap the guest gets a `too-large` error and nothing is sent. (`wasi:http` request bodies are streamed and not covered.) |
| `GREENTIC_HTTP_REQUEST_TIMEOUT_SECS` | Wall-clock limit on every inbound HTTP route except `LONG_RUNNING_PATHS` (`/agent/chat`, `/agent/chat/stream`, `/operator/op/invoke`: their work has its own deadlines, the agent loop limit and `GREENTIC_WASM_MAX_EXEC_SECS`, and a 408 + client retry could repeat a side effect); exceeding it answers 408 on `/admin/*`, `/healthz`, `/sql/*`. Default `60`; `0` disables. Implemented as a small axum middleware (`tower-http` is not a dependency). There is no header-read timeout: `axum::serve` does not expose one, so front the runner with a proxy if slowloris matters. |
| `GREENTIC_HTTP_OUTBOUND_TIMEOUT_SECS` | Ceiling, in whole seconds, on every phase timeout (connect / first-byte / between-bytes) of an outbound `wasi:http` request (`http_timeout_hooks.rs`). Default `30`; `0`, empty or unparseable values fall back to `30` rather than failing. |
| `PACK_FS_ALLOWED_ROOTS` | Platform path list (`:` on Unix) of extra roots, besides the working directory, that absolute `fs://` pack locators may point into. Setting it turns confinement on; confinement is also on whenever `PACK_VERIFY_STRICT` is set. When unset and not strict, absolute paths are unrestricted (dev behaviour). |
| `PACK_ALLOW_LEGACY_SIGNATURES` | Opt-in (default OFF; truthy = `1`/`true`/`yes`/`on`, case-insensitive): accept legacy digest-only signatures while `PACK_VERIFY_STRICT` is set. They do not bind the pack name/version, so strict mode rejects them otherwise. |
| `GREENTIC_CACHE_DIR` | Root of the compiled-component cache (default `.greentic/cache/components`, relative to the working directory); entries live under `v1/<engine profile id>`. |
| `GREENTIC_NO_CACHE` | Truthy (`1`/`true`/`TRUE`/`yes`/`YES`, exact match) disables both the memory and disk component caches. The CLI `--no-cache` flag sets it. |
| `GREENTIC_DISABLE_CACHE` | Alias of `GREENTIC_NO_CACHE` with identical semantics; either one disables the caches. |

**Blank env vars.** `GREENTIC_ENV` and `DEFAULT_TENANT` are read through `config::greentic_env()` / `config::non_blank` (trim, blank = unset) at every site; never add a raw `std::env::var("GREENTIC_ENV")`. The env-secrets gate treats blank like unset (`local` + warning). The same rule holds elsewhere with its own helper: `storage/config.rs::named()` (the Redis URL / namespace vars), `routing::env_nonempty` (`TENANT_JWT_*`), and `GREENTIC_OAUTH_BROKER_SHARED_SECRET` (a blank value does not override the YAML `shared_secret`). Boolean knobs parse case-insensitively and trimmed (`1`/`true`/`yes`/`on`; `routing::is_truthy_value` for `TENANT_JWT_TRUST_GATEWAY` / `TENANT_JWT_ALLOW_NO_EXP`).

**Auth hardening.** `POST /operator/op/invoke` is guarded by a route layer (`http/auth.rs::require_operator`) that demands `Authorization: Bearer` (or `X-Operator-Token`) equal to `OPERATOR_TOKEN`, else the admin token (constant-time compare, 401 otherwise); with no token it stays open for backward compatibility and `HostServer::serve` logs a warning. `TENANT_RESOLVER=jwt` now verifies HS256 tokens when `TENANT_JWT_SECRET` is set (`TenantResolver::JwtVerified`); with no key it fails startup (`RoutingConfig::startup_check`) unless `TENANT_JWT_TRUST_GATEWAY=1` keeps the legacy decode-only `TenantResolver::Jwt`.

**Provider secrets are not env vars the runner reads.** `SLACK_SIGNING_SECRET`, `WEBEX_WEBHOOK_SECRET`, `WHATSAPP_VERIFY_TOKEN`, `WHATSAPP_APP_SECRET` and `TELEGRAM_BOT_TOKEN` appear nowhere in the runner's source apart from test fixtures: no host code reads them. They are only secret NAMES a provider component may request through the `secrets-store` import, which canonicalizes the key (`TELEGRAM_BOT_TOKEN` -> `telegram_bot_token`, `secrets::canonicalize_secret_key`) and resolves it through the configured `SECRETS_BACKEND`.

**Route guards.** `/admin/packs/status`, `/admin/packs/reload`, `/admin/capabilities`, `POST /agent/chat` and `POST /agent/chat/stream` all take the `AdminGuard` extractor: `ADMIN_TOKEN` bearer when set, otherwise loopback peers only (403 for anything else). `/healthz` is open, `/operator/op/invoke` uses the operator guard above, and `/sql/*` authenticates through the SQL gateway.

## Workspace Features

Default-on features, by crate: `greentic-runner-host` = `verify`, `agentic-worker`, `state-disk`, `session-redis`;
`greentic-runner` = `verify`, `agentic-worker` (which also turns on the host's `state-disk`);
`greentic-runner-desktop` = `agentic-worker`; `greentic-aw-runtime` = `state-disk`.

- `verify` (default) — validate pack files before loading
- `agentic-worker` (default) — `dw.agent` / `agentic.call` support (pulls `greentic-aw-runtime` and `greentic-ext-runtime`); disable for a lean publishable build
- `state-disk` (default) — on-disk (redb) agent state backend, `GREENTIC_AW_STATE_BACKEND=disk`; without it, `disk` falls back to in-memory with a warning (host feature forwards to `greentic-aw-runtime/state-disk`)
- `dev-allow-unsigned` — lets `GREENTIC_EXT_ALLOW_UNSIGNED=1` skip extension signature verification. LOCAL DEV ONLY; deliberately not pulled in by `desktop-agent-ephemeral`
- `greentic-aw-runtime` own features: `serve` (NATS serve mode: pulls `aw-event-bridge` + `async-nats`), `test-mock` (canned-reply runtime types; required with `serve` by the `aw-serve` bin; never in a server build), `state-disk`, `greentic-llm-backend`, `dev-allow-unsigned`
- `telemetry` — explicit span annotation via `greentic-telemetry` (telemetry crate always linked; feature gates `annotate_span` calls)
- `fault-injection` — testing fault injection
- `session-redis` (host default) — Redis session storage backend
- `component-v0-6-introspection` — v0.6 component inspection
- `greentic-x-provider` — Greentic-X extension runtime integration (host crate; pulls `greentic-x-runtime` + `greentic-x-types`)
- `legacy-gen-bindings` — gates the `greentic-gen-bindings` binary (runner crate only)
- `operala-in-process` — in-process deep-worker (`operala.call`) runtime: enables `agentic-worker` plus the greentic-dw operala invoker/bridge and `greentic-llm`. Server-safe. `GREENTIC_OPERALA_DISPATCH=nats` keeps NATS dispatch at run time.
- `desktop-agent-ephemeral` — ephemeral in-memory desktop-agent mode: enables `agentic-worker`, `greentic-llm-backend`, `operala-in-process` and `greentic-aw-runtime`'s `test-mock` (not for production; `dev-allow-unsigned` must be named separately)
- `greentic-llm-backend` — in-process LLM backend for the agentic worker: enables `agentic-worker` plus `greentic-aw-runtime/greentic-llm-backend`

## Git Conventions

Do NOT add Claude co-author attribution to commits or PRs.
