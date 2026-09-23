# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

greentic-runner is the production runtime for the Greentic platform. It loads `.gtpack` archives containing WebAssembly components and flow definitions, exposes HTTP ingress adapters for messaging providers (Telegram, Teams, Slack, WebChat, Webex, WhatsApp, generic webhook, timer/cron), and executes flows with pause/resume session semantics. The workspace produces three binaries (`greentic-runner`, `greentic-runner-cli`, `greentic-gen-bindings`) and a library crate (`greentic_runner`). Workspace version 1.2.0-dev.0.

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

# Heavy WASM fixture tests
GREENTIC_HEAVY_WASM=1 cargo test --workspace

# Select specific CI steps
LOCAL_CHECK_STEPS=fmt,clippy ci/local_check.sh
```

`ci/local_check.sh` steps: `fmt`, `dependency_sanity`, `clippy`, `host_smoke`, `crate_tests`, `workspace_tests`, `conformance`, `package`.

The `dependency_sanity` step detects multiple wasmtime versions in the dependency tree (local workspace crates vs published greentic-* version skew) and fails early before clippy/tests hit confusing trait-mismatch errors.

GitHub CI (`ci.yml`) is thinner than `ci/local_check.sh`: it runs `cargo fmt --check`, `cargo clippy --all-targets --all-features --workspace --locked -- -D warnings`, a warning-only toolchain-drift check, and (nightly/dispatch only) the heavy WASM fixture tests. The clippy job is the compile gate — it builds every target including test code, so feature-gated code compiles on PRs — but **tests are not executed on PRs**; the full suite runs in Nightly Coverage against `main`. A red `ci/local_check.sh` can still be pre-existing on the base branch; reproduce on a pristine checkout before assuming your change caused it.

The compile gate exists on `develop` and `main` but **not on `research`**: research carries 38 git dependencies, 6 of them on the cross-org private `greentic-biz` org, so cargo dies at dependency-fetch there without a cross-org deploy key. `develop` and `main` have zero git dependencies (`grep -c 'source = "git' Cargo.lock` → 0), which is why the gate runs.

Note that `-p <crate>` is not a faithful reduction of a workspace build here: feature unification means `cargo clippy -p greentic-aw-runtime --all-features` can pass while the workspace build of the same crate fails, because `greentic-runner-host` turns on features that pull in extra code paths. Reproduce workspace failures with the workspace command.

## Workspace Layout

```
crates/
  greentic-runner/           # Binary + public library (thin CLI wrapper)
  greentic-runner-host/      # Core runtime (~29K lines): pack loading, flow engine,
                             #   ingress adapters, session/state, admin API
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
  greentic-i18n/             # Compile-time i18n (embedded locale bundles)
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
error envelope surfaces `error.message` as the reply and re-parks (`LoopHere`) without
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

### Async runtime dispatch (`sorla.call` node)

A native flow node `sorla.call` (component `sorla.call`, operation = the sorx target)
dispatches work to the separate `greentic-sorx` runtime over NATS pub/sub. Node input:
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
(identical to the in-process `dw.agent` node output). Serve entries:
`greentic_aw_runtime::serve::serve(nats_url, runtime)` and the host-level
`agent_node::serve_agentic(nats_url, merged_agents)` (reuses the shared
`build_agent_runtime` so in-process and serve paths build an identical runtime).
For a credit-free live e2e there is an `aw-serve` bin (features `serve,test-mock`):
`GREENTIC_EVENTS_NATS_URL=nats://127.0.0.1:4222 cargo run -p greentic-aw-runtime
--features serve,test-mock --bin aw-serve` (env `AW_SERVE_AGENT_ID`,
`AW_SERVE_REPLY`). `dw.agent` (in-process) stays untouched — `agentic.call` is the
out-of-process path.

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

### Pack Hot-Reload

Pack index (JSON, local/HTTPS/cloud) polled at `PACK_REFRESH_INTERVAL` (default 30s). On change: resolve locators → verify digest/signature → cache artifacts → atomically swap `TenantRuntime` via `ArcSwap`. Overlays independently deployable per tenant.

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
| `engine/state_machine.rs` | State transitions, pause/resume orchestration |
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
| `http/mod.rs` | Axum router assembly (ingress adapters, admin, health) |
| `http/admin.rs` | Admin API endpoints |
| `http/auth.rs` | `AdminAuth` extractor: bearer-token or loopback-only gating for admin routes |
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
| `PACK_CACHE_DIR` | Artifact cache directory (default `.packs`) |
| `PACK_PUBLIC_KEY` | Ed25519 public key for signature verification |
| `PACK_VERIFY_STRICT` | Fail on missing/invalid signatures |
| `PORT` | HTTP server port (also `--port` CLI flag) |
| `DEFAULT_TENANT` | Fallback tenant for routing |
| `TENANT_RESOLVER` | Routing mode: host/header/jwt/env |
| `ADMIN_TOKEN` | Bearer token for `/admin` endpoints (loopback-only when unset) |
| `GREENTIC_EVENTS_NATS_URL` | NATS bus URL; enables `sorla.call`/`operala.call`/`agentic.call`/`telco-x.call` dispatch + in-proc agentic serve. The runner registers response listeners for `sorla`/`operala`/`agentic`/`telco-x`; the `telco-x-event-bridge` crate serves `greentic.telco-x.request.v1` (Phase 2 transport scaffold). A credit-free `telco-x-serve` bin runs the bridge with the built-in `EchoInvoker` (`cargo run -p telco-x-event-bridge --bin telco-x-serve`); real Telco-X operations need a production `TelcoXDispatchInvoker` impl. Until then `telco-x.call` only echoes. See `docs/superpowers/specs/2026-06-21-telco-x-runtime-dispatch-design.md` in the workspace root |
| `GREENTIC_AGENTIC_SERVE_INPROC` | Opt-in (default OFF): co-host the agentic-worker NATS service in-process; truthy (`1`/`true`/`yes`/`on`) + `GREENTIC_EVENTS_NATS_URL` set |
| `GREENTIC_AGENT_MANIFESTS_DIR` | Dir of `<agent_id>.json` full `AgentConfig` files; process-level agent source for in-proc serve |
| `GREENTIC_AW_PACK_EXTENSIONS` | Set to `0` to ignore design extensions carried inside a `.gtpack` (`extensions/*.gtxpack`) and keep the on-disk scan as the only source. Mirrors `GREENTIC_AW_MCP` / `GREENTIC_AW_COMPONENT_TOOLS` / `GREENTIC_AW_FLOW_TOOLS`. |
| `GREENTIC_AW_A2A` | Set to `0` to ignore the `assets/a2a-routes.json` sidecar a `.gtpack` carries, so a worker's `a2a:` tools resolve to nothing. Mirrors `GREENTIC_AW_MCP`. A route with `requires_auth` reads its token per call from `secrets://default/<tenant>/<auth_team\|_>/a2a/<agent_id>` (unit-scoped `<agent_id>.unit-<segment>` first). |
| `GREENTIC_AW_STATE_BACKEND` | AW state backend selector: `redis` \| `memory` \| `disk`. Unset → `redis` if `GREENTIC_AW_REDIS_URL` is set, else `memory` (ephemeral, in-process). `memory`/`disk` give single-process locking only — multi-instance HA needs `redis`. |
| `GREENTIC_AW_STATE_PATH` | On-disk (redb) file path when `GREENTIC_AW_STATE_BACKEND=disk` (default `~/.greentic/aw-state.redb`, falling back to `/var/lib/greentic/aw-state.redb`). |
| `GREENTIC_AW_REDIS_URL` | Agentic-worker Redis state store. **Optional** — the worker defaults to an in-memory backend when unset; set this (or `GREENTIC_AW_STATE_BACKEND=disk`) for durable / multi-instance state. |
| `GREENTIC_PRESENT_PROVIDER_AS_USER` | Opt-in (default OFF): restore the legacy presentation in which a component invoked by a flow with no verified caller is told the PROVIDER id as its `user` / `user_id`. See "What a component is told about its caller" above. Truthy = `1`/`true`/`yes`/`on`. |

Provider secrets: `SLACK_SIGNING_SECRET`, `WEBEX_WEBHOOK_SECRET`, `WHATSAPP_VERIFY_TOKEN`, `WHATSAPP_APP_SECRET`, `TELEGRAM_BOT_TOKEN`.

## Workspace Features

- `verify` (default) — validate pack files before loading
- `telemetry` — explicit span annotation via `greentic-telemetry` (telemetry crate always linked; feature gates `annotate_span` calls)
- `fault-injection` — testing fault injection
- `session-redis` — Redis session storage backend
- `component-v0-6-introspection` — v0.6 component inspection
- `greentic-x-provider` — Greentic-X extension runtime integration (host crate; pulls `greentic-x-runtime` + `greentic-x-types`)
- `legacy-gen-bindings` — gates the `greentic-gen-bindings` binary (runner crate only)
- `operala-in-process` — in-process deep-worker (`operala.call`) runtime: enables `agentic-worker` plus the greentic-dw operala invoker/bridge and `greentic-llm`. Server-safe. `GREENTIC_OPERALA_DISPATCH=nats` keeps NATS dispatch at run time.
- `desktop-agent-ephemeral` — ephemeral in-memory desktop-agent mode: enables `agentic-worker`, `greentic-llm-backend`, `operala-in-process` and `greentic-aw-runtime`'s `test-mock` (not for production; `dev-allow-unsigned` must be named separately)
- `greentic-llm-backend` — in-process LLM backend for the agentic worker: enables `agentic-worker` plus `greentic-aw-runtime/greentic-llm-backend`

## Git Conventions

Do NOT add Claude co-author attribution to commits or PRs.
