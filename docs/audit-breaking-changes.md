# Audit branch: user-visible and breaking changes

Everything below changed between `main` (`16b0cbfd`) and this branch. Each entry says
what changed, who is affected, and how to get the old behaviour back (or that there is no
way to, because the old behaviour was the bug). Source of truth: `git diff 16b0cbfd HEAD`
and `git log --no-merges 16b0cbfd..HEAD`; commit subjects are quoted where a change is easier
to find that way.

Sections: [Upgrade checklist](#upgrade-checklist) | [Pack loading and signing](#pack-loading-and-signing) |
[Auth and routing](#auth-and-routing) | [Sandbox and outbound](#sandbox-and-outbound) |
[Storage](#storage) | [Agentic workers and dispatch](#agentic-workers-and-dispatch) |
[CLI](#cli) | [Desktop harness](#desktop-harness) | [HTTP surface and telemetry](#http-surface-and-telemetry) |
[Providers and SQL](#providers-and-sql) | [Round 13](#round-13) | [Round 16](#round-16) |
[Changes missing from earlier rounds](#changes-missing-from-earlier-rounds) | [Rust API changes](#rust-api-changes) |
[Tests and CI](#tests-and-ci)

## Upgrade checklist

Things most likely to break a running deployment, in the order to check them:

1. `TENANT_RESOLVER=jwt`: tokens now need `exp`, and the process refuses to start without
   `TENANT_JWT_SECRET` or `TENANT_JWT_TRUST_GATEWAY=1`.
2. Packs signed with the legacy digest-only message under `PACK_VERIFY_STRICT`.
3. `fs://` pack locators outside the working directory while strict (or `PACK_FS_ALLOWED_ROOTS` set).
4. Anything that read `/healthz` `last_error`.
5. Components that wrote under the old host-wide `/state`, `/cache`, `/logs` (not migrated).
6. Guests that ran longer than 300 s, used more than 1 GiB of memory, or fetched
   metadata/link-local addresses or bodies over 64 MB.
7. `credential_ref` and secret segments that are not `[A-Za-z0-9._-]`.
8. Wasm component cache (cold start after upgrade).
9. Operators that call `POST /operator/op/invoke` without a token while `ADMIN_TOKEN` is set.
10. `PORT=""`, `TENANT_RESOLVER=""`, `GREENTIC_ENV=""`, `DEFAULT_TENANT=""` now mean "unset" (a relaxation; for
    `SECRETS_BACKEND=env` a blank `GREENTIC_ENV` is therefore allowed like an unset one).
11. Pointer-mode templates (`vars_out`, provider `in_map`/`out_map`) that name a MISSING path under a context root
    now fail the node instead of passing the literal text on
    ([details](#changes-missing-from-earlier-rounds)).
12. Rolling upgrade of the agentic worker on Redis: the tool-call ledger key format changed, so a message redelivered
    across the upgrade can re-run a side-effecting tool ([details](#changes-missing-from-earlier-rounds)).
13. Tenant ids with a leading `.` or a `..` (valid `TenantId`s) now fail at config load ([Round 13](#round-13)).
14. Embedders: new/changed public Rust API, see [Rust API changes](#rust-api-changes) and its semver recommendation.

## Pack loading and signing

### Strict mode rejects legacy digest-only signatures
- **What:** a signature over just the digest does not bind pack name/version, so a valid
  signature could be replayed for another pack. Under `PACK_VERIFY_STRICT` (or
  `packs.trust.require_signatures`) only the canonical `greentic-pack-sig-v1` message
  (`name`, `version`, `digest`) is accepted. Outside strict mode legacy signatures are still
  accepted with a `warn`.
- **Affected:** deployments with strict verification and packs signed before the canonical message.
- **Restore:** re-sign the packs (preferred), or set `PACK_ALLOW_LEGACY_SIGNATURES=1`
  (`1`/`true`/`yes`/`on`) to accept them under strict.

### `fs://` pack locators are confined
- **What:** absolute `fs://` locators must resolve inside the working directory plus
  `PACK_FS_ALLOWED_ROOTS` (platform path list). Setting `PACK_FS_ALLOWED_ROOTS` turns confinement on;
  strict mode turns it on too. Relative traversal that escapes is refused.
- **Affected:** strict deployments, or anyone who sets the variable, with packs stored elsewhere.
- **Restore:** add the pack directories to `PACK_FS_ALLOWED_ROOTS`. With the variable unset and
  strict off, absolute paths stay unrestricted as before.

### Strict mode needs a pinned digest for remote packs
- **What:** under strict, a non-`fs://` pack entry must carry a digest (or a digest `pack_ref`).
  Strict also fails startup when no trust key is configured.
- **Affected:** strict deployments using semver-only remote index entries.
- **Restore:** pin digests in the index. There is no switch to skip the check.

### Directory packs are refused under the strict policy
- **What:** CLI/desktop runs of an unpacked pack directory fail with "directory packs are unsigned and
  cannot be run under the strict signing policy".
- **Affected:** `--policy strict` / `--strict-signing` users running a directory.
- **Restore:** run a signed `.gtpack`, or use `--policy devok`.

### Pack names with path separators
- **What:** earlier on this branch (`1938643b`) names containing `/` or `\` were rejected, which
  aborted the whole reload for every tenant. They are now accepted (empty, `.`, `..` and NUL are still
  rejected) and the cache directory is derived with `sanitize_segment`.
- **Affected:** namespaced pack names (`acme/pack`). Names that sanitisation changes (`/`, `\`, `:`)
  now get a short digest suffix in their cache directory (`a_b-1a2b3c4d`) so `a/b` and `a_b` no longer
  share one; their previous cache entries are orphaned and re-fetched once. Plain names keep their
  directory.
- **Restore:** none needed; delete the orphaned directories under the pack cache if you care about disk.

### gtbind `pack_locator` packs cannot satisfy a trust key (known limitation)
- **What:** the index generated from `*.gtbind` has no signature or digest, so with a trust key set the
  loader refuses those packs. Startup now logs a warning. Verification was not weakened.
- **Restore:** use a signed pack index, or leave the trust key unset for development.

### Component cache invalidated (`epoch-v1`)
- **What:** the engine profile fingerprint now includes `epoch-v1` (epoch interruption and the limits
  changed codegen), the cache layout is `v1/<engine profile id>`, and cache keys use the sha256 of the real
  wasm bytes rather than a declared digest.
- **Affected:** every deployment: the first start recompiles all components, and old cache entries are
  orphaned.
- **Restore:** none. Delete the old cache root (`GREENTIC_CACHE_DIR`, default
  `.greentic/cache/components`) to reclaim space.

### Hot reload behaviour
- **What:** the watcher skips rebuilding a tenant whose pack digests already failed and retries failed
  rebuilds with exponential backoff, instead of retrying every poll.
- **Affected:** operators watching for a retry every `PACK_REFRESH_INTERVAL` after a bad pack.
- **Restore:** none; fix the pack or push a new digest.

### Pack archives have size caps
- **What:** extracted assets are capped per entry and in total, and archive entries read into memory are
  capped (zip-bomb guard). Exceeding a cap is an error, not truncation. Path resolution inside a pack
  refuses `..`, absolute paths and escaping symlinks.
- **Affected:** unusually large pack assets.
- **Restore:** none (limits are constants in `pack.rs`).

### Pack-carried extension archives have size caps and stale staging is swept
- **What:** an inner `extensions/*.gtxpack` is unpacked BEFORE signature verification, so it is now bounded:
  64 MiB per entry (declared size checked, real bytes streamed through a cap), 256 MiB total, 4096 entries
  (`pack_extensions.rs`). An archive over a cap is reported as a staging failure, not truncated. Staging slots of a
  superseded digest of the same extension, and abandoned partial unpacks, are removed once unused for an hour.
- **Affected:** extension archives larger than the caps (none known).
- **Restore:** none (constants).

## Auth and routing

### Verified JWT mode, `exp` required, startup failure
- **What:** `TENANT_RESOLVER=jwt` verifies HS256 tokens when `TENANT_JWT_SECRET` is set (signature,
  `exp`, `nbf` with 60 s leeway, optional `TENANT_JWT_ISSUER`). Failures answer `401`. Only `alg: HS256`
  is accepted. A verified token **must** carry `exp`. With no key the process refuses to start.
- **Affected:** anyone on `jwt` mode: previously tokens were decoded without verification.
- **Restore:**
  - `TENANT_JWT_ALLOW_NO_EXP=1` accepts tokens without `exp` (a present `exp` is always enforced).
  - `TENANT_JWT_TRUST_GATEWAY=1` restores decode-only behaviour for deployments behind a gateway that
    already verified the token (startup warning). Without a key and without this, startup fails.

### Operator route auth
- **What:** `POST /operator/op/invoke` demands a bearer token: `OPERATOR_TOKEN`, else `ADMIN_TOKEN`
  (constant-time compare, 401 otherwise). The token may be sent as `Authorization: Bearer ...` or
  `X-Operator-Token: ...` (for lanes where `Authorization` carries the tenant JWT). The body is capped at 8 MiB.
- **Affected:** callers of the operator route when either token is set. With neither set the route stays open
  and startup logs a warning.
- **Restore:** unset both tokens (not recommended).

### Admin and chat route guards
- **What:** `/admin/*`, `POST /agent/chat` and `/agent/chat/stream` use the admin guard: `ADMIN_TOKEN`
  bearer when set, otherwise loopback peers only (403 otherwise). `/healthz` stays open.
- **Restore:** set `ADMIN_TOKEN` for remote callers.

### Invalid `TENANT_RESOLVER` fails startup
- **What:** an unsupported value (typo) used to degrade to `env` mode or reject requests; it now fails
  startup. The check runs from both `RunnerConfig::from_config` and `run()`, so embedders that hand-build a
  `RunnerConfig` are covered.
- **Restore:** fix the value.

### Empty `PORT`, `TENANT_RESOLVER`, `GREENTIC_ENV`, `DEFAULT_TENANT` are treated as unset
- **What:** a relaxation. `PORT=""` used to be a fatal parse error; `--port` now skips reading `PORT` entirely.
  `GREENTIC_ENV` and `DEFAULT_TENANT` are read through one helper (`config::greentic_env` / `non_blank`: trimmed,
  blank = unset) at every site; before, only tenant-context building did so, while other readers used `""`
  (ingress envelope, component ctx, default env) or failed with ``invalid env id ``` (state nodes), and the env
  secrets gate refused `""`. The gate now treats blank exactly like unset (`local`, with the existing warning):
  no more permissive than unset, but a deployment that set `GREENTIC_ENV=""` and relied on the refusal must set
  a real env.
- **Affected:** containers that template empty env vars. An invalid non-empty `PORT` is still an error.

### Invalid tenant or env identifiers fail the build
- **What:** `HostBuilder::build`, `run()` and gtbind loading validate tenant and `GREENTIC_ENV`
  identifiers instead of silently running as the `local` tenant. gtbind tenant keys are trimmed
  (`" demo "` and `demo` are one tenant).
- **Restore:** use valid ids (`[A-Za-z0-9._-]`, tenant ids up to 128 chars).

### Invalid webhook binding config is a hard error
- **What:** a `webhook` binding whose `config` does not parse (for example `deny_paths: not-a-list`) used
  to be ignored in favour of defaults; it now fails config loading with "failed to parse webhook binding
  config".
- **Restore:** fix the binding.

### Secret URIs and sidecars validated
- **What:** every segment of a `secrets://` URI built from config or pack input must be `[A-Za-z0-9._-]`
  with no leading dot and no `..`. This covers `AgentConfig.llm.credential_ref`, agent tool
  provider/key, `scoped_secret_path_for_pack`, and pack sidecars. Forms such as `vault://acme/x` or
  `../other` are rejected, not looked up. The broker client also validates and percent-encodes each segment.
- **Affected:** `credential_ref` values that used other URI shapes.
- **Restore:** none (path-traversal fix). Rename the secret to a safe segment.

### Secrets backend selection
- **What:** `SECRETS_BACKEND` is `env` (default; allowed only when `GREENTIC_ENV` is `local`/`dev`/`test`
  or unset/blank) or `broker`; anything else errors. Pack secret lookups return `NotFound` only if every scope
  missed (other failures are typed errors).
- **Restore:** n/a.

### OAuth broker and email send
- **What:** the email OAuth token request now authenticates with `GREENTIC_OAUTH_BROKER_SHARED_SECRET`
  (or `oauth.shared_secret`) and uses the guarded HTTP client; the broker base URL keeps its last path
  segment when it has no trailing slash.
- **Affected:** deployments whose broker lacks the shared secret on the email path.

## Sandbox and outbound

### Per-tenant WASI directories (no migration)
- **What:** `/state`, `/cache`, `/logs` are backed by `<host dir>/tenants/<tenant>` per tenant. Tenant ids must
  match `[A-Za-z0-9._-]{1,128}` and not be `.`/`..`. The pack-artifact cache is never visible to guests.
- **Affected:** components that stored files under the old host-wide directories: that data is not visible
  to them any more and is **not migrated**.
- **Restore:** copy the old files into `<host dir>/tenants/<tenant>/...` by hand.

### Env allowlist per tenant
- **What:** a guest sees only its own tenant's `env_passthrough`, not the union across tenants.
- **Affected:** a tenant that relied on another tenant's passthrough variables.
- **Restore:** list the variables in that tenant's `env_passthrough`.

### Wasm execution and memory limits
- **What:** `GREENTIC_WASM_MAX_EXEC_SECS` (default `300`, `0` disables, clamped to avoid epoch overflow; 1 s
  granularity) and `GREENTIC_WASM_MAX_MEMORY_MB` (default `1024`, `0` disables). Tables and instance counts
  have fixed caps. A deadline trap surfaces as an ordinary invocation error. A grow past a memory or table cap is
  denied, not trapped: the guest sees `-1` (round 13 logs it).
- **Restore:** raise the value or set `0`.

### Outbound HTTP guard
- **What:** host `http-client` and `wasi:http` block link-local (`169.254/16`, `fe80::/10`) and cloud metadata
  addresses by default, cap response bodies at 64 MB, and cap each `wasi:http` phase timeout at 30 s.
- **Restore:**
  - `GREENTIC_HTTP_ALLOW_METADATA=1` re-allows metadata/link-local.
  - `GREENTIC_HTTP_MAX_RESPONSE_MB=<n>` (`0` disables the cap).
  - `GREENTIC_HTTP_OUTBOUND_TIMEOUT_SECS=<n>`.
  - New opt-ins: `GREENTIC_HTTP_DENY_PRIVATE=1` (also block loopback/RFC1918/CGNAT/ULA) and
    `GREENTIC_WASI_HTTP_ENFORCE_POLICY=1` (make `wasi:http` honour `http_enabled = false`).

### Outbound guard on MCP, A2A and knowledge-index URLs
- **What:** the host `http-client` policy now also applies to URLs a pack or tenant config names for the
  agentic lane: MCP `transport_url` (resolved and checked before connecting), A2A routes, and the
  chronicle-index knowledge endpoint. Link-local and cloud-metadata destinations are refused with
  "blocked by outbound http policy"; loopback and private ranges stay allowed. MCP redirects and a DNS
  answer that changes between the pre-flight and the connect are not covered (no hook in `greentic-mcp-client`).
- **Restore:** `GREENTIC_HTTP_ALLOW_METADATA=1`; `GREENTIC_HTTP_DENY_PRIVATE=1` is the stricter opt-in.

### Fault injection needs the feature
- **What:** the `GREENTIC_FAIL_*` variables only take effect in builds with the `fault-injection` feature.
  Production builds ignore them.
- **Restore:** build with `--features fault-injection`.

## Storage

### Redis session and state are opt-in via env
- **What:** `GREENTIC_SESSION_REDIS_URL` and `GREENTIC_STATE_REDIS_URL` select Redis stores (defaults stay
  in-memory). The session store needs a keyspace (`GREENTIC_RUNNER_SESSION_NAMESPACE` or `GREENTIC_ENV`);
  wait TTL is `GREENTIC_RUNNER_SESSION_WAIT_TTL_SECS` (default 24 h, `0` = none). A connect failure or missing
  keyspace is a hard startup error, never a silent memory fallback. `HostBuilder::with_storage` always beats env.
- **Affected:** multi-replica deployments, which need both for `session.wait` to resume on another pod.

### Session and state key changes
- **What:** state keys escape `%` and `/` in each segment and use `%2D` for a literal `-` hint, so distinct
  `(pack, flow, hint)` triples can no longer alias. Keys without those characters are unchanged, and
  `get_json` falls back to the historical key for ids that do contain them and migrates from it, without
  reading or deleting another session's state. Session `put`/`update_cas`/`touch`/`delete` are serialised by a
  per-session in-process lock, and the legacy hint-only session slot is removed when found. State values that
  are neither JSON nor UTF-8 are stored in a base64 envelope instead of being lossy.
- **Affected:** ids containing `%` or `/`, and hint `-`.
- **Restore:** none (aliasing was the bug).

### `GREENTIC_FLOW_STATE_TTL_SECS`
- **What:** optional expiry for flow-state entries. Unset/`0` keeps the old never-expire behaviour. A value above
  `u32::MAX` is clamped with a warning (it used to disable the TTL).

### Agent session lock refresh (Redis, memory, disk)
- **What:** refreshing the session lock no longer recreates a key that already expired. A lapse is `LockLost`
  and the turn aborts WITHOUT saving: an absent key does not mean nobody ran in the gap (another turn may have
  taken the lock, saved newer state and released it), and the old "re-acquire a lapsed unclaimed lock" let the
  stale holder overwrite that state. LLM calls, `flow:` tools and every other tool dispatch keep the lock alive
  (`limits.timeout` may exceed the 90 s TTL). The post-LLM-error save is also skipped when the lock is gone.
- **Affected:** a turn whose lock lapsed (stalled backend, process pause longer than 90 s) now fails with
  `LockLost` and is not persisted; before, it sometimes "recovered" and clobbered a newer turn.

## Agentic workers and dispatch

### Session id on the agentic NATS path
- **What:** the runner's `::cnv64=`, `::thr64=`, `::rpl64=` markers lengthen the correlation id, which the bridge
  uses as the session id. `validate_session_id` caps it at 512 bytes, so long Teams/email conversations were
  rejected with "session id is too long" on every turn. Over-long ids are now replaced by `h:` + sha256 hex (66
  chars, deterministic). Ids with a control character are still rejected.
- **Affected:** ids at or under 512 bytes are unchanged, so existing agent memory is kept. Conversations whose id
  was over the cap had no working memory (every turn failed) and now start one.
- **Restore:** n/a.

### Agent error replies
- **What:** an `ok:false` agent response with a non-empty partial `output.reply` shows that reply; with an empty
  one the user now sees the localized `runner.flow.execution_failed` line (en/id/es/ja/zh) instead of the machine
  string (`agent step ended without a reply: timeout`). `error.code` and `error.message` stay on the payload
  for operators. The turn re-parks without consuming the `MAX_PARK_TURNS` budget.
- **Affected:** UIs or tests matching the old machine string in the reply.

### Dispatch ledger key and `Greentic-Dispatch-Id`
- **What:** redelivery dedup is keyed `{tenant}:{env}:{dispatch_id}`, where `dispatch_id` is a random
  per-publish `Greentic-Dispatch-Id` header. Before, the key was the conversation hint, which replayed turn 1's
  reply as turn 2's for up to an hour. A request with no (or a malformed) dispatch id (an older runner) is not
  deduplicated at all.
- **Affected:** mixed-version fleets (new bridge, old runner): no dedup until the runner is upgraded.

### Deadline watchdog cancellation
- **What:** the replica that admits a response announces the correlation id on
  `greentic.dispatch.watchdog-cancel.v1`; every replica cancels the matching watchdog (the watchdog map is
  process-local). Response listeners join a per-tenant queue group `runner-{tenant}-{runtime}`.
- **Affected:** infra that filters NATS subjects or ACLs: allow the new subject. A notice arriving after
  another turn re-armed a watchdog on the receiving replica still retires it (documented residual window).

### JetStream consumer
- **What:** `MAX_DELIVER` is now 6 (was 5) for handler ERRORS, with scaled busy-redelivery (5 s growing to 60 s),
  2 s error-nak delay, handler timeout derived from the agent timeout (240 s default, 30 min ceiling) and an
  `ack_wait` of 300 s. A stale consumer config is updated on start. Handler errors are nak'd; rejection responses
  are scoped. (Round 13 changed how a BUSY session is bounded and what the consumer's own `max_deliver` is, see
  [Round 13](#round-13).)
- **Affected:** poison messages are retried once more before being dropped.

### Redaction (NATS URLs, audit events, secrets in logs)
- **What:** `GREENTIC_EVENTS_NATS_URL` credentials are masked in logs and error contexts (fail-closed for raw
  delimiters in userinfo). Agent audit events no longer carry raw tool arguments/results: they carry a keyed
  content hash (random per process, comparable only within one run), the byte size and a short key-redacted
  preview. The free-text redactor masks URL userinfo, `Bearer`/`Basic` tokens and the whole value of
  sensitive `key=value` pairs.
- **Affected:** log/audit consumers that read raw payloads or compared hashes across restarts.
- **Restore:** none. Raw payloads live in the run trace (`GREENTIC_TRACE_OUT`).

### State load failure no longer starts an empty conversation
- **What:** any session-state load error used to be logged and the turn ran on an EMPTY state, so the
  end-of-turn save (and the opening-message short-circuit) overwrote the stored conversation. Only a stored
  state or a genuinely missing key may start a turn now. A backend error (Redis blip) returns the new
  `AgentError::StateUnavailable` (`"agent state temporarily unavailable"`), which both event bridges treat like a
  busy session: JetStream Naks with backoff, core NATS answers `session_busy`. Undecodable or too-new state
  (`Decode`, `SchemaIncompatible`) returns `AgentError::StateLoad` and is NOT retried; nothing is saved.
- **Affected:** callers matching `AgentError` exhaustively (new variant). Operators see failed turns during a
  Redis outage instead of silent conversation resets.

### Guardrail evaluation runs on the blocking pool
- **What:** the inbound/outbound guardrail chains (`run_chain_blocking`) and the attachment text guard no longer
  run the synchronous WASM evaluator on an async worker. A panicking evaluator fails closed (`internal`).

### LLM error classes
- **What:** the greentic-llm backend maps 5xx/408 to `ServiceUnavailable` and transport errors to `Transport`
  (both retryable by `RetryingLlmBackend`); only 4xx/auth/config stay `BadRequest`. The OpenAI backend no longer
  has a whole-request timeout: the blocking path has a 45 s per-request timeout, the streaming path bounds the
  response head (45 s) and each chunk gap (60 s), so long streams are no longer cut. A reply without a `usage`
  object counts 0 tokens instead of failing to decode. (`llm_extension` is unchanged: its bridge returns only a
  string error, so there is no status to classify.)

### Registry URL paths and dispatch targets
- **What:** agent and graph ids are percent-encoded into admin-registry URLs, `.`/`..` ids are refused, and the
  dispatch `target` is validated (slash, backslash, `?`, `#`, `%`, `..`, whitespace, control characters, over
  256 bytes are rejected with a non-retryable error) before any config lookup.
- **Affected:** agent ids containing those characters (none are valid registry ids).

### Bounded in-memory state
- **What:** the in-process short-term memory provider caps keys (256 B), values (16 KiB), entries (100) and bytes
  (256 KiB) per session and keeps 10 000 sessions (LRU eviction); beyond a cap `remember` returns a
  `LimitExceeded` tool error. The memory and redb KV backends sweep expired entries on a write cadence (memory:
  every max(256, len) writes; redb: every 1024 writes), so finished sessions no longer accumulate. The redb file
  does not shrink (freed pages are reused).
- **Affected:** agents that store more than the caps in short-term memory.

### A2A, MCP store pull, parallel graph resume
- **What:** an A2A rejection (JSON-RPC error or HTTP 4xx other than 401/403/429) of a message that named a task
  retries once with the context only; the SendMessage reply body is capped at 4 MiB. MCP store pulls are
  serialised per `component_ref`, the artifact download has connect/read/total timeouts (10 s/30 s/300 s) and a
  first local-wasm pull has its own 300 s budget instead of the 5 s probe. Parallel-region checkpoints persist
  per-branch visit counts (`BranchCursor.visits`, serde default, no schema bump) so a looping branch resumes at
  the next attempt. A flow tool parked on the last iteration gets one iteration after resume. History truncation
  keeps `System` messages. Known, not done: store-pull cache paths are still keyed by `component_ref` only (two
  tenants pinning different digests of one ref re-pull each time and can run the other's verified version).

### Event bridge back-pressure
- **What:** core-NATS `run_bridge` (agentic and telco-x) allows at most 256 requests in flight (beyond that the
  caller gets an `overloaded` error response) and bounds each handler (the agent's step timeout plus margin for
  agentic, 240 s for telco-x) with a `timeout` response. Billing emits and long-term ingest are detached under a
  64-slot, 30 s gate; work that finds no free slot is dropped with a warning.

## CLI

### Trace default is Off; flag and env precedence
- **What:** trace capture is opt-in. `--trace` now has no default (it used to default to `on`) and
  `GREENTIC_TRACE_OUT` set to any value turns it on. `--trace-out` alone also turns it on. Precedence is now the
  **flag over the environment** (the env var used to beat `--trace-out`). `--trace-capture-inputs` has no default
  and falls back to `GREENTIC_TRACE_CAPTURE_INPUTS=1`. A stock deployment no longer writes `./trace.json`.
- **Restore:** pass `--trace on`, or set `GREENTIC_TRACE_OUT=<path>`.

### `--port` and `PORT`
- **What:** `--port` is optional (it used to default to `8080` and always override `PORT`). Order is `--port`,
  then `PORT`, then `8080`. An invalid non-empty `PORT` is an error.

### `--strict-signing` and `--policy`
- **What:** `--policy` wins when both are given; `--strict-signing` alone selects the strict policy.
  Before, `--strict-signing` did not influence the policy that was chosen. Combine with the directory-pack change above.

### Proxy environment
- **What:** the CLI no longer overwrites `NO_PROXY` and friends. It sets proxy defaults only if none of
  `NO_PROXY`, `HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY` (either case) is already set.
- **Restore:** n/a (set the variables explicitly).

### Logging and home
- **What:** "Run logs: <path>" goes to stderr (it was stdout). A log file that cannot be opened logs a warning and
  discards logs instead of panicking. `HOME` and `WASMTIME_CACHE_DIR` are defaulted independently (each only when
  missing).

### `replay`
- **What:** replay exits non-zero when any step's output hash diverges from the trace ("replay diverged ... at N
  step(s)"). An unknown hash algorithm is an error (it used to fall back to blake3). `--until-failure` and
  `--compare-hashes` now take a value and can be turned off: `--until-failure=false`, `--compare-hashes=false`.
- **Affected:** scripts that treated replay as always successful.

### `conformance`
- **What:** the run fails when no pack ran ("conformance ran no packs") or any pack failed; the report is still
  written first. The host is always stopped even on early errors. Levels are lowercase: `--level l2`.
- **Affected:** CI that ignored the exit status.

### `cache warmup` / `doctor` / `prune`
- **What:** they use the runtime's engine profile (`runtime_cache_engine_and_profile`), the same one the host
  uses, instead of a default-engine profile. Entries written by the old CLI under the old profile are orphaned.
  Lock handling is stricter: conflicting `path`/`wasm_digest` aliases in `pack.lock` are errors, bundled bytes are
  verified against the declared digest, and unknown digest algorithms are rejected.
- **Restore:** re-run `cache warmup`; delete orphaned profile directories to free space.

### `gen-bindings`
- **What:** a pack directory may contain `..` (it is canonicalized, not root-confined); flows are sorted for
  deterministic output; `{{ env.X }}` / `{{ secrets.X }}` placeholders are collected only inside `{{ ... }}`
  (plain text such as "see env.docs" no longer counts); `--complete` no longer adds a wildcard
  `https://*` network allow.
- **Affected:** generated `bindings.yaml` files differ; `--complete` output has no wildcard egress.
- **Restore:** add the network allow entry by hand if needed.

### `info` feature report
- **What:** features are reported from the compiled configuration (`telemetry` was always reported enabled), and
  `agentic-worker`, `desktop-agent-ephemeral`, `dev-allow-unsigned`, `greentic-llm-backend` are listed.

## Desktop harness

### Session snapshots
- **What:** snapshots are stored as `<escaped id>.snapshot.json`, where unsafe bytes are `%xx`-escaped (the old name
  mapped every unsafe byte to `_`, so `a/b` and `a_b` collided), and the file wraps the snapshot with the owning
  `session_id`. A snapshot recording another session id is refused. A corrupt snapshot is now an error that names
  the file and says to delete it (it used to silently restart at the flow entry). Writes are atomic (a unique `<name>.tmp-<pid>-<n>`
  temp file, best-effort fsync, then rename; concurrent saves of one session no longer share a temp name, and temp
  leftovers older than an hour are removed on save/load).
- **Backward compatibility:** bare legacy snapshots are still read for ids with unsafe characters, with a
  documented residual risk (a bare legacy file is adopted by whichever session maps to its name). A legacy file a
  session adopted is deleted once its park is re-saved under the current name, so it cannot be resurrected by a
  later conversation with the same id.
- **Restore:** n/a. To unwedge a session with a corrupt file, delete the file named in the error.

## HTTP surface and telemetry

### `/healthz`
- **What:** `last_error` (raw reload error text, which can leak paths and URLs) is replaced by the boolean
  `last_reload_failed`. Detail is available behind auth at `/admin/packs/status`.
- **Affected:** probes and dashboards reading `last_error`.

### `/agent/chat` errors
- **What:** failures return generic bodies (`{"error":"agent_chat_failed","message":"agent chat failed"}`, or
  `tenant_not_loaded` with 404, decided by a typed pre-check instead of matching error text). The full error chain
  goes to the log only. The stream endpoint behaves the same.

### Metrics
- **What:** duration histograms use explicit millisecond buckets (1 ms to 30 s); `tenant`, `flow_id`, `provider`
  and `op` label values are sanitised to `[A-Za-z0-9._:-]`, capped at 64 chars and limited to 256 distinct values
  each (the overflow is `other`, empty is `unknown`).
- **Affected:** dashboards and alerts on high-cardinality labels or on the old default bucket boundaries.

## Providers and SQL

### Gmail send
- **What:** the raw message is base64url (the Gmail API requires it; standard base64 was wrong). CR or LF in
  `from`/`to`/`cc`/`bcc`/`subject` is rejected (header injection). Non-ASCII subjects are RFC 2047 encoded,
  `MIME-Version: 1.0` and `Content-Transfer-Encoding: 8bit` are set.
- **Affected:** callers passing newlines in header fields now get an error.

### SQL gateway
- **What:** a gateway with an empty auth token (the placeholder when no SQL connections are configured) rejects
  every request, even an empty bearer. The compare is over fixed-size digests. Cells are decoded by column type
  (a strict-typed mismatch used to become `NULL`); an undecodable type is the string `"<unsupported type NAME>"`
  instead of `NULL`; results stop reading at `max_rows + 1`; Postgres/MySQL queries always run in a read-only
  transaction that is rolled back. MySQL pools set `max_execution_time` and fall back to MariaDB's
  `max_statement_time`.
- **Affected:** clients that treated `NULL` as "could not decode".

## Guest import hardening (round 9)

- **Outbound `http-client` timeout:** a guest `timeout-ms` is clamped to the host ceiling
  (`GREENTIC_HTTP_OUTBOUND_TIMEOUT_SECS`, default 30s), as on the `wasi:http` path. Zero/absent keeps the default.
- **Memory:** `GREENTIC_WASM_MAX_MEMORY_MB` now also bounds the TOTAL linear memory per store, and a store may hold at
  most 16 memories (was 10000). **Affected:** components declaring more than 16 memories, or whose combined memories
  exceed the cap.
- **`secrets-store.put`:** a refused write (empty key, provider-core-only, policy, backend failure) no longer
  panics the host; the guest traps with the error message (an ordinary invocation error). The WIT `put` has no result
  type, so a trap is the only way to surface it. Calling the trait method directly logs and drops the write; use
  `HostState::try_put_secret` for the `Result`. The runner registers `secrets-store` itself instead of the library's
  `add_secrets_store_compat_to_linker`.
- **Size caps:** `GREENTIC_STATE_MAX_VALUE_MB` (16) and `GREENTIC_HTTP_MAX_REQUEST_MB` (64); `0` disables.
- **Request timeout:** `GREENTIC_HTTP_REQUEST_TIMEOUT_SECS` (60, `0` disables) -> 408 on `/admin/*`, `/healthz`, `/sql/*`. Exempt (`LONG_RUNNING_PATHS`, own deadlines): `/agent/chat`,
  `/agent/chat/stream`, `/operator/op/invoke`.
- **Logs:** guest- or peer-supplied text (secret keys, URLs, telemetry fields, correlation ids) is control-char
  sanitised (`is_unsafe_log_char`: also U+2028/2029, bidi controls U+202A-202E / U+2066-2069, zero-width U+200B-200F and U+FEFF) and length-capped (`log_safe`); URLs have userinfo/query redacted. Guest telemetry logs carry the
  host-bound tenant as `host_tenant` and the guest claim as `guest_tenant`.
- **stdin:** guests no longer inherit the host's stdin when stdio is inherited.

- **Literal-IP SSRF:** A2A calls and knowledge-index embedding/search calls now refuse IP-literal link-local /
  metadata hosts before sending (the resolver guard is skipped for IP literals).

### Known, not fixed
- `greentic-a2a`'s `CardCache` agent-card GET uses its own unguarded HTTP client.
- A guest can override the `Host` header on outbound requests. It does not bypass the SSRF guard (the connect
  address is checked), but the virtual host seen by the upstream is guest-chosen.
- No disk quota for the guest `/state`, `/cache`, `/logs` mounts.
- The extension runtime (`greentic-ext-runtime`) wasmtime engine configuration has not been audited.

## Pack loading, caches and downloads (round 11)

- **Pack cache layout:** `runner-core` now stores `<cache>/<name>/<version>/<sha256_hex>/pack.gtpack`; the content
  digest is always part of the key, so two tenants with the same `name@semver` but different bytes no longer
  overwrite each other. Entries under the old `<name>/<version>/pack.gtpack` layout are ignored and re-fetched (and
  can be deleted by hand).
- **No refetch of cached pinned packs:** a pack with a pinned digest whose cached file still hashes to it is not
  downloaded again on every index refresh. The signature is still verified.
- **Download limits:** `GREENTIC_PACK_MAX_DOWNLOAD_MB` (1024) and `GREENTIC_PACK_INDEX_MAX_MB` (32) cap pack and
  remote index downloads; `0` disables. `network.read_timeout_ms` no longer acts as the whole-request deadline
  (`reqwest::blocking` has no per-read timeout); the total deadline is `GREENTIC_PACK_DOWNLOAD_TIMEOUT_SECS` (900,
  `0` disables), and `connect_timeout_ms` still applies.
- **`pack.lock` discovery:** only the archive's own directory is searched (a lock in a parent or grandparent
  directory is no longer found; keep `pack.lock` next to the archive), and an unparsable lock is skipped with a
  warning. (Commit `c48cc590`; this entry used to be repeated, without a matching commit, under Round 12.)
- **Materialized pack dirs:** a corrupt `manifest.cbor` with no archive to fall back to now fails the load instead
  of yielding an empty pack. `read_pack_file`, inline component paths and `legacy_path` are confined to the pack
  root (absolute paths, `..` and escaping symlinks are refused: a pack whose `components/*.wasm` entries are symlinks
  pointing outside the pack root fails to load, so ship component bytes inside the pack).
- **Nested flow loads** (`dw.agent` `flow:` tools) keep the parent pack's unit id, non-secret runtime config and
  runtime refs.
- **`cache warmup`** keys compiled artifacts by the sha256 of the component bytes, not the digest in `pack.lock`.
- **Disk compile cache:** readers no longer delete a half-written entry (60s grace); `prune` also sweeps orphan
  `.cwasm`/`.json` files and stale `tmp/` files, even without a disk limit.
- **Guest stdio:** guest lines that are not telemetry are control-character sanitised, capped at 8192 chars, and
  written with their newline in a single call.

## Lifecycle, secrets and auth (round 11)

- **Graceful shutdown:** on SIGTERM/ctrl-c the HTTP server stops accepting connections and drains in-flight requests
  for `GREENTIC_SHUTDOWN_GRACE_SECS` (default `30`; `0` aborts immediately) before they are cancelled. `host.stop()`
  (timers, listeners, watcher) now runs on every exit path, including a failed `serve`. **Affected:** supervisors whose
  kill timeout is shorter than the grace period (raise `terminationGracePeriodSeconds` or lower the variable).
- **`/agent/chat` and `/operator/op/invoke`** run the turn on a detached task: a client disconnect no longer cancels
  it half-way (state save and lock release now always happen). A panic becomes a 500 / SSE `error` frame.
- **Per-unit pack secret address (BREAKING for some pack ids):** `unit_pack_segment` appends a 12-hex `sha256` of the
  raw pack id whenever the pack slug is lossy (upper case, any non `[a-z0-9_]` character such as `-` or `.`, collapsed
  `_`, or longer than 40 bytes). Pack ids that are already their own slug keep their address. **Affected:** unit-scoped
  secrets of packs such as `supplier-sor-0.1.0` or `pack.dw.support` must be re-staged under the new segment (designer /
  greentic-setup call the same function). There is deliberately NO read fallback to the old address: it is exactly the
  address colliding pack ids (`my.pack` / `my_pack`) shared. The env-wide (non-unit) address is unchanged.
- **Hot reload:** a rebuilt tenant's cron timers now start after the runtime swap, and the superseded runtime's timers
  are stopped first, so a tick no longer fires on both. An invalid cron still rejects the reload before the swap.
- **Session store:** `put`/`update_cas`/`delete`/`touch` run as one non-cancellable unit, so a timed-out caller can no
  longer release the session lock while its write is still in flight.
- **Trace file:** a node that fails and then succeeds on retry now ends with the successful attempt in `trace.json`
  (the error-time flush no longer freezes it).
- **Tokens:** `ADMIN_TOKEN`, `OPERATOR_TOKEN` and the config admin token are trimmed and blank ones count as unset
  (the presented bearer was already trimmed). The secrets broker sends no `Authorization` header for an empty token.

## Engine and agent hardening (round 11)

- **`approval.call` gate fails closed:** `mode: above_risk` / `above_confidence` now require a human unless BOTH
  the value (`risk` / `confidence`) and its threshold are finite JSON numbers. A missing, `null`, string or
  non-finite value, or a missing threshold, used to read as `0.0` / `1.0` and auto-approve. **Affected:** flows
  whose templates render `risk` / `confidence` as a numeric STRING (`"0.4"`): they now always go to a human.
  **Restore:** render the value as a number.
- **Tool-extension HTTP client:** no longer follows redirects (the ext-runtime contract), and its DNS answers go
  through the outbound address guard (`GREENTIC_HTTP_ALLOW_METADATA` / `GREENTIC_HTTP_DENY_PRIVATE`).
- **Pack JSON Schemas cannot fetch:** operator input validation compiles a pack's schema with a retriever that
  refuses every external `$ref` (`http(s)://`, `file://`). In-document `#/...` refs are unaffected. **Affected:**
  packs whose schemas `$ref` a remote URL: the schema fails to compile and the input is reported invalid.
- **Playbook (skill) turns:** the nested runtime now has the same mandatory guardrail policy, billing meter and
  MCP tool source as the calling worker. **API:** `PlaybookTurnHost` gained a public `billing_meter` field
  (`Option<Arc<dyn BillingMeter>>`); struct-literal constructors must add it (`None` keeps the old no-op meter).
- **`flow.goto` redirect budget is per turn:** `redirect_count` resets when a parked run resumes, so the 4th goto
  across several parked turns no longer fails `redirect_limit`. The limit still trips inside one turn.
- **Engine-level session host stores nothing:** `RunnerHost` installs a discard `SessionHost` (`get` is `None`,
  writes are dropped). The store-backed one never read back what it wrote and, with no TTL, leaked one row per
  turn. Parked-flow snapshots (`FlowResumeStore`) are unaffected.
- **No re-run after a persistence failure:** `resume.clear` / `resume.save_with_run` after a flow ran are retried
  in place (the write only) and then fail with the new `RunnerError::PostEffectPersistence`, which
  `retry_with_jitter` never retries. Previously the whole adapter call (approval dispatch, SoR writes, artifact
  uploads, emits) was re-run up to 5 times. **API:** `RunnerError` gained that variant.
- **Approval HTTP poller:** each resume runs on its own task. A panicking or slow resume no longer kills or
  stalls the poller (and every other approval's polls and deadlines); `poller_started` is cleared when the loop
  exits.
- **Side-turn card re-offered after a guardrail denial:** with `on_text_while_parked: side_turn`, a guardrail
  denial while a `flow:` tool is parked re-offers the card (`awaiting_tool_input`) like any other failed turn.
- **Card navigation:** a blank `routeToCardId` no longer hides a valid `toCardId` / `nextCardId` after it.
- **Condition operators:** `evaluate_simple_condition` splits at the FIRST operator outside double quotes, so a
  value containing another operator (`x > "a==b"`) no longer mis-splits.
- **Step cap:** one turn may execute at most 10,000 nodes (`flow_step_limit_exceeded`), so a routing cycle that
  never waits fails instead of spinning.
- **MS Graph send:** the sender must be a single clean path segment (`\`, `%`, whitespace, control characters,
  `.` and `..` are refused) and is percent-encoded into the URL.
- **Knowledge index:** embedding and search responses are capped at 16 MiB.
- **Welcome marker TTL:** the welcome-seen marker is written with an explicit 365-day TTL, so the Redis wait TTL
  (24h default) no longer re-fires the welcome flow after a day.
- **SoR discovery errors:** `sorla.call` over HTTP reports `sorla_unauthorized` / `sorla_unreachable` /
  `sorla_discovery_failed` instead of "no capability for sorla action ..." when discovery itself failed.
- **Operator introspection cache:** `describe()` results are cached per `(digest, component, operation)`; an
  `unknown` digest is never cached.
- **Tool-secret env fallback deny-list:** `secret://a/b` -> `A_B` no longer resolves host configuration:
  `GREENTIC_*`, `TENANT_JWT_*`, `ADMIN_TOKEN`, `OPERATOR_TOKEN`, `PACK_PUBLIC_KEY` and the provider webhook
  secrets are reported as not found.
- **Discovery dirs:** with no `HOME`, extensions / agent manifests are read from `/var/lib/greentic/...`, not a
  predictable path under the temp dir.
- **Deterministic in-process LLM key:** with several agents naming a `credential_ref`, the lexicographically first
  agent id wins (was `HashMap` order).
- **Graph agent guardrails run once:** the graph turn no longer also installs the agent's own guardrails as the
  mandatory fallback policy (they ran twice per turn).
- **MCP sidecar duplicates:** a duplicate `server_id` in `assets/mcp-routes.json` keeps the FIRST record, like the
  a2a and sorla sidecars (was last).

### Known, not changed (round 11)
- Inbound `metadata.nextCardId` / `routeToCardId` / `toCardId` still select the entry node when nothing is parked,
  whether or not the provider stamped `greentic_submit`. The target must be a node of the routed flow, and
  non-webchat providers stamp neither marker; requiring the marker is a cross-repo decision.
- The welcome marker is written before the welcome turn succeeds (a failed welcome is not retried).
- Graph runs: terminal run-id slots (`__2..__100`) are never reclaimed and a resumed run drops the new user message;
  both need `greentic-aw-runtime` graph-executor / checkpoint-store API changes.

### Per-session turn lock (round 11)

Concurrent turns for one parked session are now serialised in-process
(`engine/turn_lock.rs`), including a late dispatch response racing a user
message. The second turn observes the first's cleared/re-saved park instead of
resuming the same snapshot, so a duplicated message no longer repeats
`approval.call`, `sorla` writes, artifact uploads or emits. Observable change:
turns on one session queue rather than interleave. Not fixed: the lock is
in-process only; multi-replica deployments on a shared Redis session store still
need a store-level compare-and-set/lease.

## Round 12

- **Hot reload / cron timers:** the superseded runtime's timers are now stopped cooperatively (a per-runtime
  `CancellationToken` observed only while a timer is sleeping) instead of `JoinHandle::abort`. A tick already inside
  its flow now runs to completion and saves its snapshot/state; only the NEXT tick is suppressed. A reload can
  therefore briefly overlap the old runtime's in-flight cron flow with the new runtime's (never a doubled tick).
- **Whole-turn retry:** deterministic or post-effect flow failures (`flow_step_limit_exceeded`, `redirect_limit`,
  unknown node/flow, missing required variable, `flow.call` nesting/pause, snapshot/pack mismatch) are tagged
  `TerminalFlowError`, reach the state machine as the new `RunnerError::FlowTerminal` and are no longer re-run by
  `retry_with_jitter` (a step-limit runaway used to execute up to 5 x 10 000 nodes with effects). Transient
  infrastructure errors are still retried. **Affected:** embedders matching `RunnerError` exhaustively (new variant).
- **Tool secrets from env:** the `secret://a/b` -> `A_B` env fallback now also refuses `SECRETS_*` (so
  `secret://secrets/broker/token` can no longer read `SECRETS_BROKER_TOKEN`), `PACK_*`, `REDIS_URL`,
  `OTLP_ENDPOINT` and `OTLP_HEADERS`. It stays a deny-list on purpose: an allow-list would break the documented
  fallback for ordinary tool keys (`secret://openai/api_key` -> `OPENAI_API_KEY` still resolves, even though the
  runner reads that variable itself). **Affected:** a tool that read one of those names from the env must be given
  the value through the secrets store instead.
- **Tool extension HTTP client:** requests to a forbidden IP literal (`http://169.254.169.254/`,
  `http://[::ffff:a9fe:a9fe]/`, any spelling the URL parser normalises to one) are refused; before, only names that
  RESOLVED to such an address were. `greentic_ext_runtime` offers no pre-request hook, so the client routes a
  forbidden literal to a local connection-dropping listener (via its proxy selector) and the request fails with a
  connection error. Same policy as the rest of the host (`GREENTIC_HTTP_ALLOW_METADATA`, `GREENTIC_HTTP_DENY_PRIVATE`).
- **Pack cache:** a pinned digest written `SHA256:<UPPER>` no longer breaks signature verification on a warm cache,
  and `ResolvedPack.digest` / the watcher's applied digest are always the canonical lowercase `sha256:<hex>`
  computed from the file (`PackCache::lookup` now returns `CachedPack { path, digest }`). A watcher that had stored
  the raw uppercase spelling does one extra rebuild after upgrading.
- **Pack cache pruning:** after storing a new archive, sibling `sha256_*` directories beyond the newest 2 that have
  been unused for over an hour, the legacy flat `<name>/<version>/pack.gtpack` and `pack.tmp-*` leftovers older
  than an hour are deleted. Cache hits refresh the archive's mtime, so a pack a tenant still resolves is never
  pruned (a loaded runtime keeps its archive PATH and re-opens it lazily, so deleting an in-use one would break it).
  Disk use per name/version is now bounded instead of growing by up to 1 GiB per republish. (Round 13: the one-hour
  gate became `max(1h, 2 x PACK_REFRESH_INTERVAL)`, see [Round 13](#round-13).)
- **Shutdown:** a second SIGINT/SIGTERM during the drain aborts the in-flight requests immediately instead of waiting
  out `GREENTIC_SHUTDOWN_GRACE_SECS`; `host.stop()` still runs.
- **Agentic worker:** the per-step tool-catalog resolve (and user-ledger read) now refreshes the session lock while it
  runs, so a cold MCP probe/pull can no longer outlast the 90 s lock TTL and kill the turn as `LockLost`
  (acked as `invoke_failed`). MCP servers are probed concurrently (at most 8 at once, results ingested in server
  order) instead of one after another.
- **Event bridges:** the core-NATS `overloaded` reply at the in-flight cap is kept and documented: core-NATS gives
  no server-side backpressure, so not reading the next message would only queue requests on a full replica.

## Round 13

Follow-ups to the round-12 audit. Each entry is a behaviour change; none bumps a version.

- **Whole-turn retry holds the session turn lock (`engine/state_machine.rs`).** The per-session lock used to be
  taken per attempt inside `call_traced`, so it was released between retry attempts: another message could consume the
  parked snapshot in the gap and the retry then ran out of order. The state machine now takes the same lock around the
  whole retry loop. Observable: a second message for a session whose first turn is backing off waits for the
  first to finish (or fail for good) instead of overtaking it.
- **No whole-turn retry after an effect (`runner/engine.rs`).** If a node that performs an external effect
  (`provider.invoke`, `flow.call`, `dw.agent`, `dw.agent_graph`, `sorla.call`, `operala.call`, `agentic.call`,
  `telco-x.call`, `approval.call`, `mcp`, `a2a`, `media.create`) has succeeded in a run, any LATER failure of that run
  is terminal (`RunnerError::FlowTerminal`): neither the engine retry loop nor the state machine re-runs the turn, so a
  second SoR write / approval request / upload no longer follows a transient failure at a later node. A failure before
  any such node still retries. **Affected:** flows that relied on a retry re-running an already-succeeded effectful
  node. **Known limitations (documented, not fixed):** generic component nodes are opaque and not counted; a
  `state.set` read-modify-write re-applies; the failing node's own partial effects are not tracked.
- **Deterministic jump failures are terminal.** `unknown_flow` (flow not registered), `unknown_node`, "no start
  node" and a malformed `greentic_control` block used to be plain errors and retried up to 5 times; they now fail
  at once. A `load_flow` I/O failure stays retryable.
- **Event bridges: blank `Greentic-Env` and `..` ids.** A blank `Greentic-Env` is treated as absent (defaults to
  `default`) in `aw-event-bridge` and `telco-x-event-bridge`, and rejection replies echo `Greentic-Tenant` /
  `Greentic-Env` whenever the ids fit the runner's `[A-Za-z0-9._-]` charset (including `..` inside a name such as
  `a..b`). Before, such a request was rejected with a reply that carried no tenant/env headers, the runner's
  tenant-scoped response listener dropped it, and the caller waited out its watchdog (forever with no
  `deadline_ms`). Key-building validation (`validate_scope_id`) still rejects `..` on purpose.
- **JetStream busy budget.** A busy session (lock contention or `StateUnavailable`) is redelivered while the message
  is younger than `busy_budget(step_timeout)` = the handler time bound + 60 s (about 5 minutes by default, at most 31),
  measured from the publish time; then it is answered `session_busy`. Before, 6 deliveries (about 160 s) was the whole
  budget, so a second message to a session whose healthy turn ran for minutes got `session_busy`. Every Nak counts
  against the server's `max_deliver`, so the consumer's `max_deliver` is now `CONSUMER_MAX_DELIVER` = 100 (a finite
  backstop; `update_consumer` converges an existing consumer on start) and `MAX_DELIVER` = 6 remains the
  application-level cap for handler errors. **Affected:** monitoring that alerts on `max_deliver`; the
  poison-message bound is unchanged for errors.
- **Cron timers of replaced runtimes.** `ActivePacks::insert_pack` (and so `RunnerHost::load_pack`),
  `insert_revision` and `remove_revision` now cancel the timers of the runtime they replace or remove. Only the
  watcher did before, so a runtime an embedder still referenced kept firing next to its replacement. Re-inserting
  the same `Arc` cancels nothing.
- **Tenant ids are checked against the secrets rule at load.** A tenant such as `.hidden` or `a..b` passed
  `TenantId` and the WASI directory check but every secret read failed (`is_safe_secret_segment`). It now fails
  `HostConfig::try_tenant_ctx` (bindings load, host registration) with a message naming the tenant. Tenant ids are
  now `[A-Za-z0-9._-]{1,128}` with no leading `.` and no `..`. **Restore:** rename the tenant.
- **Pack cache: read-only archives and the prune gate.** The mtime refresh opens the archive read-only (it failed with
  `EACCES` on 0444 archives, so the pack looked idle and could be pruned while in use) and warns once if it cannot
  refresh; the idle gate is `max(1h, 2 x PACK_REFRESH_INTERVAL)`; the watcher touches the archives of a runtime it
  keeps because a republish failed to build.
- **Routing truthy env vars are case-insensitive.** `TENANT_JWT_TRUST_GATEWAY=TRUE` / `On` (and
  `TENANT_JWT_ALLOW_NO_EXP`) were rejected although the docs said `1/true/yes/on` and every other parser
  lowercases. **Affected:** a deployment that set `TRUE` and relied on it NOT enabling gateway trust now has it enabled;
  grep your manifests.
- **`GREENTIC_OAUTH_BROKER_SHARED_SECRET=` (blank) no longer overrides the YAML `shared_secret`.** A blank env value is
  unset; a blank YAML value is unset too (the broker gets no secret rather than an empty one).
- **Logging added where data was dropped silently:**
  - tool results over 32 KiB are cut to a preview for the history (`warn!` with tool name and size; the cap itself is
    unchanged);
  - a dispatch response with no `Greentic-Tenant` header on a tenant-scoped listener is dropped as before, now with a
    `warn!` naming the (log-safe) correlation id. The awaiting flow stays parked until its watchdog fires, indefinitely
    when the dispatch carried no deadline;
  - a denied `memory.grow` / `table.grow` (past `GREENTIC_WASM_MAX_MEMORY_MB` or the fixed table caps) is NOT a trap:
    the guest sees `-1`, and a Rust guest usually aborts on the failed allocation, which does surface as an
    invocation error. The host now logs one `warn!` per store (the earlier wording above said "trap");
  - if the tool-extension HTTP client cannot be built (the loopback blackhole listener cannot bind, builder failure)
    it is cached as unavailable for the life of the process, so every tool HTTP call fails; each site now logs an
    `error!` with the reason;
  - files created beyond `MAX_CREATED_PER_TURN` (20) in one turn are stored but not listed in the reply, so not
    delivered; now a `warn!` with the dropped count, and the cap is documented in `CLAUDE.md`;
  - an unsafe or unbuildable unit secret scope falls back to the team/tenant scope, which can serve a broader
    credential; the first skip per process logs a `warn!`.

## Round 14

- **A failed effectful node now makes a transient failure terminal.** `drive_flow` marked "effects performed" only after
  an effectful node (`dw.agent`, `mcp`, `media.create`, `sorla.call`, `a2a`, ...; `performs_external_effect`) had
  SUCCEEDED, so one that failed transiently after a partial effect (a tool call that ran, a request that was
  published) was retried as a whole turn and the effect doubled. The flag is now also set when such a node fails
  (`may_have_partial_effect_on_failure`). Conservative on purpose: a transient failure BEFORE the node did anything
  (for example a broker connect error) is no longer retried either; non-retryable errors (validation, config) were
  never retried, so they are unaffected. `flow.call` is exempt on failure because its nested `drive_flow` already
  classifies its own failure. Accepted over-conservatism: a successful `flow.call` into a pure sub-flow, or a
  read-only `dw.agent`, still counts as effectful, because the host cannot cheaply tell. **Affected:** a flow whose
  first effectful node fails transiently now fails the turn instead of retrying it.
- **`FlowTerminal.reason` keeps the root cause.** `terminal_after_effects` rebuilt the error from `err.to_string()`
  (outermost context only), so the root cause was lost. (Round 14 first claimed a `{err:#}` reason; round 15 reverted
  that and round 16 settled it: the cause chain is kept as the error's `source()` while `Display` stays the outermost
  message, see [Round 16](#round-16).)
- **JetStream handler-error budget no longer counts busy Naks (`aw-event-bridge`).** `settlement` held `delivered >=
  MAX_DELIVER` against handler errors, but a busy Nak is also a delivery, so a request that waited out a long turn
  had no retries left for its first handler error. Handler errors are now counted separately
  (`jetstream::ErrorAttempts`, keyed by stream sequence, process-local) and `MAX_DELIVER` (6) is held against that
  count; `CONSUMER_MAX_DELIVER` still terminates. A redelivery landing on another replica restarts the error count
  (extra retries, never more than the backstop). `settlement` takes a third argument, `error_attempts`.
  `telco-x-event-bridge` has no JetStream consumer, so it has no analogue.
- **JetStream `info()` failure.** Without delivery metadata neither backstop could count, so a Nak could redeliver
  forever. The delivery is now settled as the last one (a busy session is answered `session_busy`, a handler error is
  terminated and answered) with a `warn`. Backlog age: the busy budget is still measured from the PUBLISH time, so
  time in the stream backlog counts; the caller's own wait started at publish, so answering sooner is correct.
- **Tenant ids (round 13 rule) unchanged, rationale recorded.** One invalid tenant fails startup for all tenants on
  purpose (fail fast beats serving a subset silently); the errors already name the tenant (`invalid host config for
  tenant ...`, `invalid tenant configuration for ...`, and `validate_tenant_segment` quotes the id).
  **Migration note:** renaming a tenant re-keys everything derived from the id: greentic-state FQNs, session and
  parked-flow keys (`env:tenant:...`), the WASI per-tenant directory (`<host dir>/tenants/<tenant>`, not migrated)
  and the secrets paths. Parked flows under the old name are not resumed. `IngressEnvelope::tenant_ctx()` keeps only
  the `TenantId` rule: its tenants come from the already-validated host configs, and rejecting there would turn a
  startup check into a per-request failure.
- **Log volume.** The oversize tool-result `warn` (`cap_tool_result_for`) is emitted at most once a minute per process
  (others at `debug`) and its tool name is stripped of control and bidi characters; the guest-memory-cap denial
  (`wasm_limits`) is a `warn` once per process instead of once per invocation (later ones at `debug`).
- Comment fixes only: `routing.rs::is_truthy_value` no longer claims to match every boolean env parser;
  `CONSUMER_MAX_DELIVER` documents the 31 minute budget (about 40 deliveries).

## Changes missing from earlier rounds

Found by review; each was already in the tree and absent from this document.

- **Template pointer mode errors on a missing path under a context root (`runner/templating.rs`).** With
  `allow_pointer` (`vars_out` bindings, provider `in_map` / `out_map`), a string such as `/node/x/output/typo` used to
  fall back to the literal text whenever no such pointer existed. It still does when the FIRST segment is not a context
  root (a URL path like `/health`), but a missing path under a root (`entry`, `in`, `prev`, `node`, `state`, `vars`,
  `now`, `input`, `result`, `response`, or a top-level key of the context) now fails with "template pointer ... not
  found in the render context", so the typo is not sent to a provider as text. **Affected:** flows whose pointer
  bindings named a path that is sometimes absent and relied on the literal fallback. **Restore:** write the binding as
  `{{ ... }}` (an exact-path expression resolves to an empty string when absent).
- **Redis tool-call ledger key format (`greentic-aw-runtime/src/tools.rs`).** The key is now
  `aw:{tenant}:{env}:{session_len}.{session}:tool_calls:{call_len}.{call_id}` (length-prefixed, since either id may
  contain `:`), and the ledger call id also carries a digest of the call's extension, tool and arguments. **Affected:**
  a rolling upgrade: a message redelivered across the upgrade misses the old entry and can re-run a side-effecting tool
  once (entries live 7 days). Drain in-flight turns before upgrading when tools write.
- **Per-agent `limits.llm_retry_attempts` / `llm_retry_backoff` are ignored behind a host-wrapped backend
  (`greentic-aw-runtime/src/llm.rs`).** When the shared backend already retries (`LlmBackend::is_retrying()`, true for
  every backend the host wraps), only that backend's policy applies, so attempts are never multiplied N x N. The agent
  limits apply only to a bare backend (tests, unwrapped embedders). **Affected:** anyone tuning those two limits on a
  deployed worker; they no longer have an effect there.
- **Desktop harness:** `max_run_wall_time_ms` (default `600000`, `0` disables) is now enforced: hitting it drops the
  run with "run exceeded max_run_wall_time_ms (N ms) and was aborted". `max_node_wall_time_ms` is still not enforced.
  The user's `--pack` path is canonicalised (so `.`, `..` and symlinked packs now work; the old parent-root check
  rejected them).
- **`conformance`:** an L1/L2 case whose flow returns the session-flow failure envelope (`metadata.error_kind` /
  `error_message` in an `Ok` reply) now FAILS with code `flow_error_envelope` instead of passing.
- **`replay`:** `--step` is validated (1-based; `0` and values past the trace length are errors; it used to
  saturate). Host-managed steps (no component to re-run) are skipped for the output-hash comparison with a note
  instead of being compared against an empty output.
- **`flow.call` nesting is capped at 16 levels** (`MAX_FLOW_CALL_DEPTH`); deeper calls fail terminally ("flow.call
  nesting exceeds 16 levels"), and a nested flow that pauses is an error.
- **v0.6 components receive non-JSON input as a JSON string** (`parse_v06_input`) instead of failing or being nulled.
- **`gen-bindings`:** `--component` can now be combined with a pack or `--pack-dir` (before they conflicted); alone it
  only prints detected capabilities, with a pack it enables the `--strict` HTTP/secrets capability checks. `--pretty`
  is accepted but a no-op (the YAML is always block-style) and `--complete` now only ensures the base env passthrough.
- **`greentic-runner --config-explain`** needs no `--bindings`; it prints the resolved configuration and exits before
  the bindings requirement is checked.
- **`RunnerConfig::from_config` errors on a malformed non-empty `PORT`** (it used to ignore it and use 8080). The new
  additive `RunnerConfig::from_config_with_port(.., Some(port))` skips `PORT` entirely, which is what `--port` uses.

## Rust API changes

Public-API changes an embedder (greentic-start, greentic-designer, desktop shims) can hit at compile time. Each item
was checked against the tree (`git diff 16b0cbfd HEAD`).

| Item | Change | Break |
|---|---|---|
| `aw_event_bridge::InvokeOutcome` | gains the required field `error: Option<DispatchError>` | struct literals and exhaustive destructuring |
| `greentic_runner_host::runtime::block_on` | now `F: Future<Output = R> + Send, R: Send` (it degrades to a runtime-less executor instead of panicking) | callers passing non-`Send` futures |
| `engine::runtime::IngressEnvelope::tenant_ctx()` | returns `GResult<TenantCtx>` (was `TenantCtx`) | every caller |
| `cache::ArtifactMetadata` | gains the public field `artifact_sha256: Option<String>` (+ `with_artifact_sha256`) | struct literals |
| `fault::FaultConfig`, `fault::FaultRate` | exist only with the `fault-injection` feature (`fault` exposes inert entry points otherwise) | code that named them without the feature |
| `routing::TenantResolver` | new variants `JwtVerified { .. }`, `JwtUnconfigured(String)`, `Invalid(String)` | exhaustive `match` |
| `greentic_aw_runtime::LlmError` | new variant `RateLimited(String)` (+ `is_retryable()`) | exhaustive `match` |
| `greentic_aw_runtime::StateError` | new variant `LockLost` | exhaustive `match` |
| `greentic_aw_runtime::AgentError` | new variants `StateUnavailable(StateError)`, `LockLost` | exhaustive `match` |
| `greentic_aw_runtime::MemoryError` | new variant `LimitExceeded(String)` | exhaustive `match` |
| `engine::error::RunnerError` | new variants `PostEffectPersistence { reason }`, `FlowTerminal { reason }` (+ `is_terminal()`) | exhaustive `match` |
| `runner::playbook_turn::PlaybookTurnHost` | gains the field `billing_meter: Option<Arc<dyn BillingMeter>>` | struct literals |
| `runner_core::PackCache::lookup` | new since the baseline; returns `Result<Option<CachedPack { path, digest }>>` (`CachedPack` is re-exported), and `ResolvedPack.digest` is always the canonical lowercase digest | callers of `lookup`, code comparing the raw pinned spelling |
| `RunnerConfig` | additive `from_config_with_port`; `from_config` now errors on a malformed `PORT` | behaviour only | behaviour only |
| Round 13 additions | `PackManager::with_refresh_interval`, `PackManager::touch_cached_archive`, `PackRuntime::archive_path`, `aw_event_bridge::{is_echoable_scope_id, jetstream::{busy_budget, CONSUMER_MAX_DELIVER, BUSY_BUDGET_MARGIN}}`, `state::cap_tool_result_for` | additive, except `jetstream::busy_policy` which now takes `(delivered, age, budget)` |

No new `#[non_exhaustive]` was added: marking the enums above `#[non_exhaustive]` would stop the NEXT variant from being
a break, but it is itself a break for exhaustive matchers, so it is a release decision left to the maintainer.

### Semver recommendation

The workspace is `1.2.1` at the baseline. Several of the changes above are breaking for downstream Rust code (new
variants on public enums, a required struct field, a changed return type and a tightened bound), which is
semver-major in the strict reading. Recommendation: release as **at least `1.3.0`** if the ecosystem treats these
crates as application-internal (all known consumers are in-tree siblings pinned together), or **`2.0.0`** if strict
semver is wanted for third-party embedders. Before bumping, check the sibling constraints in the root `Cargo.toml`
(about lines 128-186: the `[workspace.dependencies.*]` entries for `greentic-runner-host`, `greentic-runner-desktop`
and `runner-core`, plus the greentic-* lane ranges) and the per-crate path dependencies
(`crates/greentic-runner/Cargo.toml`, `crates/greentic-runner-desktop/Cargo.toml`, `crates/tests/Cargo.toml`,
`crates/greentic-runner-host/Cargo.toml`, `crates/greentic-aw-runtime/Cargo.toml`): every one currently says
`>=1.2.0-0, <1.3.0-0`, so a `1.3.0` workspace version would not satisfy its own siblings until they are widened, and
`2.0.0` needs the same. The external `greentic-*` lane ranges are a separate, cross-repo decision. **Whether and when
to release is the maintainer's decision; no version was bumped, no release workflow was touched, and nothing was
marked `#[non_exhaustive]`.**

## Tests and CI

- Tests that used to print "skipping" and pass (offline wasm fixture build, fixture build failure) now fail. Set
  the documented inputs, or run the suites that do not need them. The live LLM e2e tests (`tavily_research_e2e`,
  `weather_smoke`) are `#[ignore]`d with a reason; they run only via `-- --ignored` and then fail, not skip, when
  their keys or inputs are missing.
- The fault-injection integration test is gated behind its feature, and `GREENTIC_FAIL_*` env vars only work in
  builds that enable it.
- `ci/local_check.sh` gained a `wit_sync` step (soft-skips offline outside CI) and a `host_smoke` run against a
  throwaway root and a free port.
- Docs corrected: `PACK_CACHE_DIR`, `PACK_SOURCE`, `PACK_INDEX_URL` and `PACK_PUBLIC_KEY` are **not read from the
  environment** (use `packs.cache_dir`, `packs.source` and `packs.trust.public_keys` in greentic-config);
  `PACK_VERIFY_STRICT=0` does not disable verification when a trust key is configured;
  `GREENTIC_HEAVY_WASM=1` does not run `#[ignore]`d tests on a bare `cargo test --workspace`.

## Round 15

- **Round 14 note refined: a pre-effect failure of an effectful node retries again, when it is PROVEN pre-effect.**
  Round 14 made every `Err` of the 11 effect kinds set the effects flag, so even a provider 503 before the request
  was sent became terminal. Failures are now marked with `engine::pre_effect(err)` (a `PreEffectError` wrapper: same
  `Display`, the wrapped error stays visible to `should_retry` / `is_terminal_flow_error`) where nothing can have left
  the host yet, and `drive_flow_tracked` leaves the flag alone for them. The default is unchanged: an unmarked error
  sets the flag. Marked: `provider.invoke` everything before `invoke_provider` (payload, `in_map`, validation,
  binding, `BeforeToolCall` fault); no `AgentNodeHandler` / `GraphNodeHandler` / `RemoteDispatchHandler` configured
  and the `dw.agent` session derivation; `NatsDispatcher::dispatch` request-build and publish failures (nothing was
  sent); the `a2a` pack lookup; the `aw` agent handler's audit-context build before `runtime.step`. Not marked
  (cannot be proven cheaply): the provider call itself, a custom `RemoteDispatchHandler` (the approval inbox), a
  `dw.agent` handler that fails after it ran. The shipped `aw` handler turns an LLM 429 / agent error into an `Ok`
  node output (`terminated_by: "error"`), so it never reaches this path as an `Err`. `mcp`, `media.create` and
  `sorla` HTTP report failure as values, not errors. An earlier completed effect still makes any later failure of the
  turn terminal, marked or not.
- **`FlowTerminal.reason` carried the outermost message in round 15 and carries the full chain again since round 16** (reverts the round 14 `{err:#}` change). That string reaches
  the user-facing failure envelope (`metadata.error_message`), and a cause chain can carry a URL or host. The full chain
  is logged at `warn` in `terminal_after_effects`. (Round 16: the chain is also kept as `source()`, see
  [Round 16](#round-16); the user-facing text is unchanged.)
- **JetStream `ErrorAttempts` evicts the oldest-inserted sequence** (round 16: now least recently recorded) when its 4096-entry table is full, instead of clearing every
  count (which reset a poison pill's budget to zero). `settle_delivery` holds the record -> settle -> clear wiring
  and is unit-tested without NATS.
- **Test-only:** the oversize-warning limiter (`greentic-aw-runtime::state`) is an instance (`WarnLimiter`), so its test
  no longer races other tests over the process-wide state.

## Round 16

- **`terminal_after_effects` no longer drops the root cause.** It rebuilt the error as a bare `TerminalFlowError(String)`,
  so the `Err` path (`RunOutcomeBuilder::failed` via `anyhow_chain_text`, and `classify_flow_error`'s `{err:#}`) saw only
  the outermost message. `TerminalFlowError` now carries the original `anyhow::Error` (Send + Sync + 'static) and exposes
  its causes through `source()`. `Display` is still ONLY the outermost message, so `to_string()` (the user-facing
  `metadata.error_message`) cannot leak a host or URL, while `{err:#}`, `anyhow_chain_text` and the redacted run error now
  read `outer: cause: root cause`. **Affected:** `FlowTerminal.reason` and operator-side run errors for failures after an
  effect are longer (they gain the causes). `TerminalFlowError`'s field is no longer public; use `terminal_flow_error` /
  `terminal_flow_error_from`.
- **`jetstream::ErrorAttempts` evicts by recency.** `record` on an already-tracked sequence moves it to the back of the
  eviction order (no duplicate in the deque), so an actively failing poison pill is not evicted before an idle one.
- **Invalid tenant/env id in the agent audit context is terminal.** `tenant_ctx_for_audit` failures (the message echoes
  the id) are now `TerminalFlowError`s, so the text heuristic in `should_retry` ("internal", "timeout", ...) can never
  retry them. They stay `pre_effect` at the `dw.agent` / `dw.agent_graph` call sites (nothing ran).
- **Known limitation:** `should_retry`'s text heuristic can still retry any OTHER deterministic pre-effect error whose
  message happens to contain "internal", "unavailable", "timeout" or "transient" (for example a pack or flow id that
  contains one). Mark such errors with `terminal_flow_error` where they are raised.
- **Tests only:** the pre-effect marking points (provider.invoke payload / op / binding, `a2a` pack lookup, missing
  `RemoteDispatchHandler`, `NatsDispatcher` publish failure, invalid audit ids) are pinned. A missing
  `RemoteDispatchHandler` is a deterministic, pre-effect configuration error: not retried, and it does not set the
  effects flag.

## Round 18

- **Host `http-client` no longer logs or returns the request URL (F1).** reqwest 0.13's `Display` appends
  ` for url (<full url>)`, which carried query strings, userinfo and path tokens (`api.telegram.org/bot<TOKEN>/...`,
  Slack webhook paths) into the `warn!` lines and the guest-facing `unavailable` message. Errors now go through
  `without_url()`, and the log field is `origin` (scheme + host + port only, `log_safe::log_safe_origin`) instead of `url`
  (the removed `log_safe_url` kept the path). The response-body read error is scrubbed the same way (`http_guard::body_read_reason`,
  the `io::Error` wraps a reqwest error). Also scrubbed: the secrets-broker transport errors, the A2A agent call/card
  fetch errors and the pack index fetch. **Affected:** log queries on the `url` field of `http client request failed`;
  a guest that parsed the URL out of the `unavailable` message.
- **Azure WireServer `168.63.129.16` and Oracle IMDS `192.0.0.192` are `Metadata` (F3).** Blocked by default like
  `169.254.169.254`; `GREENTIC_HTTP_ALLOW_METADATA=1` still lifts the block. **Affected:** a pack that deliberately
  called either address.
- **Run-outcome redaction matches credential key names by suffix segment (F4).** `redact_sensitive_pairs` no longer
  needs an exact name: the LAST `-`/`_` segment of `key=`/`key:`/`"key":` is checked (substring for `token`, `secret`,
  `password`, `passwd`, `authorization`, `signature`, `credential`, `apikey`, `accesskey`, ...; whole segment for `key`,
  `sig`, `pwd`, `auth`, `session`, `cookie`, `bearer`, `jwt`). `x-amz-security-token`, `bot_token`, `x-goog-api-key`,
  `Ocp-Apim-Subscription-Key` and a bare `key=` are now redacted; `secret_missing`, `max_tokens`, `design`, `signal`,
  `keyboard` are not. Not covered: the `dst.` prefix of `agent_audit` (a `.` is not a key byte here).
- **Tool-secret env deny-list grows (F7).** `secret://aws/...`, `azure`, `google`, `gcp`, `database`, `nats`, `postgres`,
  `mysql`, `kubernetes` (prefixes `AWS_`, `AZURE_`, `GOOGLE_`, `GCP_`, `DATABASE_`, `NATS_`, `POSTGRES_`, `MYSQL_`,
  `KUBERNETES_`) and the exact names `PGPASSWORD`, `KUBECONFIG`, `GITHUB_TOKEN`, `GH_TOKEN`, `NPM_TOKEN`,
  `CARGO_REGISTRY_TOKEN`, `SSH_AUTH_SOCK` now resolve as not found. **Affected:** an extension tool that read its key from
  such an env var; seal it as a real secret instead.
- **Pack locators are redacted in errors, logs and `/admin/packs/status` (F8).** A locator may be a presigned S3 / Azure
  SAS URL. `runner_core::packs::resolver::redact_locator` (userinfo -> `[redacted]`, query/fragment -> `?[redacted]`)
  is used for the HTTP resolver, the resolver wrapper and the index fetch contexts; the reqwest error uses
  `without_url()`. The watcher's failure list, its logs, `HealthState::last_error` (so `last_error` in
  `/admin/packs/status`) and the `POST /admin/packs/reload` error body go through `watcher::redact_chain`
  (`run_outcome::error::redact`). **Affected:** the query part of a locator no longer appears in those messages.
- **SQLite SQL gateway accepts one `SELECT`/`WITH`/`VALUES` statement (F2).** `read_only(true)` did not stop
  `VACUUM INTO` (writes a new file), `ATTACH` or stacked statements. `execute::check_sqlite_read_only_sql` now refuses
  them before execution (comments, quotes and a trailing `;` are handled) and the read-only pool also sets
  `PRAGMA query_only = ON`. The SQL gateway is fail-closed/unused in the shipped hosts, so this is defence in depth.
  `PRAGMA`/`EXPLAIN` queries are no longer accepted on SQLite.

## Round 18b

- **JetStream agentic consumer is backpressured (`aw-event-bridge`).** `run_bridge_jetstream` no longer
  spawns a handler for every message it reads. It takes a slot from a `MAX_INFLIGHT` (256) semaphore BEFORE
  each pull (`fetch().max_messages(<slots>)`, batches of at most 16), so unread work stays in the stream for
  other replicas and nothing sits unacked without progress acks while waiting for a slot. The durable's
  `max_ack_pending` is now `MAX_INFLIGHT` (was the server default, 1000); `ensure_consumer` converges an
  existing durable whose value differs. Behaviour change: a saturated replica pulls less instead of running
  more; the core-NATS path still answers `overloaded`.
- **Known limitation (`RuntimeSessionResumer::resume_checked`).** A response is admitted, its deadline
  watchdog retired, and only then does `handle` run. If `handle` fails with an infrastructure error (resume-store
  read/save, `PostEffectPersistence`) the response is spent and the watchdog is gone, so a wait that is still
  parked has no timeout and the user gets no reply until they message again. Not fixed: the original deadline
  is not recorded, and a synthetic `timeout` would resume the park, repeating effects when the failure was
  post-effect, and would need the approval nonce/`decision_token`. The failure is logged at `error` with that
  consequence spelled out.
- **Pack-carried extensions are staged one at a time and bounded.** `stage_from_packs` reads, stages and
  releases each `extensions/*.gtxpack` in turn (resident memory: one archive) and refuses, with a visible
  `StagingFailure`, entries beyond 64 per pass or beyond 512 MiB of archive bytes in total.
- **Store pull body is capped while streaming (`mcp_store_pull`).** `download_artifact` reads the body chunk by
  chunk and fails with `Integrity` once `MAX_ARTIFACT_BYTES` is passed, instead of buffering a chunked body
  with no `Content-Length` first.

## Round 19

- **JetStream agentic consumer: `max_ack_pending` is `2 * MAX_INFLIGHT` (512) and is fleet-wide (`aw-event-bridge`).**
  The limit is a property of the durable consumer, shared by EVERY replica, not a per-replica cap, and a message
  holds ack-pending from delivery until its ack, including the delay after a busy/error Nak when it has no permit.
  Equal to `MAX_INFLIGHT` there was no headroom for those waiting redeliveries. `ensure_consumer` converges an
  existing durable (was 256). The earlier Round 18b wording ("per replica") was wrong. A pull answered `409`
  (`Exceeded MaxAckPending`; async-nats 0.47 yields it as a stream error item) is now retried after a fixed 500 ms at
  `debug`, without escalating the 0.5s..30s backoff. A pull that returns no message (`404`/`408`) pauses
  50-100 ms (`empty_batch_delay`) instead of re-fetching at once.
- **SQLite SQL gateway: `query_only` is on for every pool (M3).** The statement-shape guard reads only the first keyword,
  so `WITH t AS (SELECT 1) DELETE FROM x` passed on a `read_only: false` pool. `PRAGMA query_only = ON` is now set
  whatever `read_only` says. **Affected:** a `read_only: false` SQLite connection can no longer write through the gateway
  (the gateway only reads).
- **Tool-secret env deny-list is narrowed (M4).** `AWS_`, `AZURE_`, `GOOGLE_` and the exact `GITHUB_TOKEN` / `GH_TOKEN`
  blocked natural names for a tool's own key. Denied now: exact `AWS_ACCESS_KEY_ID`, `AZURE_CLIENT_SECRET`,
  `AZURE_USERNAME`, `AZURE_PASSWORD`, `GOOGLE_APPLICATION_CREDENTIALS` and prefixes `AWS_SECRET_`, `AWS_SESSION_`,
  `AWS_CONTAINER_`, `AWS_WEB_IDENTITY_`, `AWS_SHARED_`, `AZURE_FEDERATED_`, `AZURE_CLIENT_CERTIFICATE_`,
  `AZURE_STORAGE_`, `GOOGLE_CLOUD_`, `GCP_`. **Affected (loosened):** `GOOGLE_API_KEY`, `AZURE_OPENAI_API_KEY`,
  `GITHUB_TOKEN`, `GH_TOKEN`, `AWS_REGION`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` resolve again. Note `GITHUB_TOKEN` in
  a GitHub Actions runner is ambient: do not run the runner with a repo token in its env.
- **Error-text redaction (L1, L3).** `run_outcome::error` also redacts `accountkey`, `signingkey`, `encryptionkey`,
  `sessionkey` and camelCase `...Key` keys (`storageKey`); `session_id` is not. Pack index oversize errors and a failed
  pack/index body read no longer carry the request URL (`copy_capped` flattens the `io::Error` chain without
  `" for url (...)"`); more reqwest transport errors in aw-runtime and the host now use `without_url()`
  (MCP store pull, MCP admin source, OpenAI backend, graph registry, guardrail policy, sorx invoke).
- **Pack-carried extension staging reads under the remaining budget (L5/L6).** `stage_bounded` hands the reader the
  bytes left in the 512 MiB aggregate (`PackRuntime::read_pack_file_capped`), so an over-budget entry is refused while it
  is read, not after it was loaded whole. The tests drive `stage_bounded` (the production path) instead of a
  separate `cfg(test)` copy.

## Round 20

- **Tool-secret env deny-list covers the credentials the Round 19 narrowing dropped (F1).** Additionally denied:
  prefixes `AWS_BEARER_` (`AWS_BEARER_TOKEN_BEDROCK`), `AWS_SECURITY_` (`AWS_SECURITY_TOKEN`), `AWS_ACCESS_KEY`
  (legacy `AWS_ACCESS_KEY` and `AWS_ACCESS_KEY_ID`), `ARM_` (Terraform/azurerm service principal), `CLOUDSDK_`,
  `GOOGLE_OAUTH_`; exact `AZURE_CREDENTIALS`, `AZURE_ACCESS_TOKEN`, `AZURE_DEVOPS_EXT_PAT`; and any
  `AZURE_*_CONNECTION_STRING`. Still allowed: `AWS_REGION`, `AWS_PROFILE`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
  third-party `*_API_KEY`. **Affected:** a tool that read one of the newly denied names through `secret://` now gets
  "not found"; supply it through the secrets backend instead.
- **JetStream pull `409` is no longer always "max ack pending" (F2, `aw-event-bridge`).** Only a description naming
  `MaxAckPending` / `max_ack_pending` / `max ack pending` takes the quiet 500 ms retry. Any other `409` (`Exceeded
  MaxWaiting`, `Exceeded MaxRequestBatch/Expires/MaxBytes`, `Consumer is push based`, a bare `409`) warns and
  escalates the backoff. `Consumer Deleted` / `consumer not found` re-runs `ensure_consumer` to re-create the durable.
- **MCP store pull and decode errors no longer carry the URL (F3, `greentic-aw-runtime`).** Messages from
  `download_artifact` / `collect_capped` name `scheme://host/path` only (new `url_redact::redact_http_url`: userinfo,
  query and fragment dropped), and body/decode errors in the MCP admin source, graph registry, OpenAI backend and
  guardrail policy go through `without_url()` first. The Round 19 claim that the store pull was covered was
  inaccurate. Known gap unchanged: `greentic-a2a` `CardCache`.
- **`max_ack_pending` is only ever raised (F4).** `ensure_consumer` converges an existing durable only when its finite
  `max_ack_pending` is BELOW 512 (and keeps a larger value when it updates for `ack_wait`/`max_deliver`), so a rolling
  deploy no longer flaps 512 <-> 256 from the new build's side. An older build still lowers it to 256 on its own start.
  The warning names `max_ack_pending`.
- **Pack download error scrubbing balances parentheses (F5a).** `strip_url_suffix` removes a `" for url (...)"` suffix
  whose URL contains balanced `( )`. A lone literal `)` in the URL still ends the suffix early.
- **Directory packs: `read_pack_file` is capped at 64 MiB per file (F5b).** `PackRuntime::read_pack_file` for a
  materialised pack DIRECTORY uses the same `MAX_ARCHIVE_ENTRY_READ_BYTES` (64 MiB) bound as an archive entry.
  **Affected:** a directory pack with a sidecar or asset over 64 MiB now reads as missing (`None`, with a warning).
