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
[Providers and SQL](#providers-and-sql) | [Tests and CI](#tests-and-ci)

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
  have fixed caps. A trap surfaces as an ordinary invocation error.
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

### Redis agent lock refresh
- **What:** refreshing a Redis lock no longer recreates a key that already expired, and a lapsed unclaimed
  lock is re-acquired. LLM calls keep the session lock alive.

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
- **What:** `MAX_DELIVER` is now 6 (was 5), with scaled busy-redelivery (5 s growing to 60 s), 2 s error-nak
  delay, handler timeout derived from the agent timeout (240 s default, 30 min ceiling) and an `ack_wait` of
  300 s. A stale consumer config is updated on start. Handler errors are nak'd; rejection responses are scoped.
- **Affected:** poison messages are retried once more before being dropped.

### Redaction (NATS URLs, audit events, secrets in logs)
- **What:** `GREENTIC_EVENTS_NATS_URL` credentials are masked in logs and error contexts (fail-closed for raw
  delimiters in userinfo). Agent audit events no longer carry raw tool arguments/results: they carry a keyed
  content hash (random per process, comparable only within one run), the byte size and a short key-redacted
  preview. The free-text redactor masks URL userinfo, `Bearer`/`Basic` tokens and the whole value of
  sensitive `key=value` pairs.
- **Affected:** log/audit consumers that read raw payloads or compared hashes across restarts.
- **Restore:** none. Raw payloads live in the run trace (`GREENTIC_TRACE_OUT`).

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
- **Request timeout:** `GREENTIC_HTTP_REQUEST_TIMEOUT_SECS` (60, `0` disables) -> 408, except `/agent/chat/stream`.
- **Logs:** guest- or peer-supplied text (secret keys, URLs, telemetry fields, correlation ids) is control-char
  sanitised and length-capped (`log_safe`); URLs have userinfo/query redacted. Guest telemetry logs carry the
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

## Tests and CI

- Tests that used to print "skipping" and pass (offline wasm fixture build, missing `WEATHER_PACK_PATH`, fixture
  build failure) now fail. Set the documented inputs, or run the suites that do not need them.
- The fault-injection integration test is gated behind its feature, and `GREENTIC_FAIL_*` env vars only work in
  builds that enable it.
- `ci/local_check.sh` gained a `wit_sync` step (soft-skips offline outside CI) and a `host_smoke` run against a
  throwaway root and a free port.
- Docs corrected: `PACK_CACHE_DIR`, `PACK_SOURCE`, `PACK_INDEX_URL` and `PACK_PUBLIC_KEY` are **not read from the
  environment** (use `packs.cache_dir`, `packs.source` and `packs.trust.public_keys` in greentic-config);
  `PACK_VERIFY_STRICT=0` does not disable verification when a trust key is configured;
  `GREENTIC_HEAVY_WASM=1` does not run `#[ignore]`d tests on a bare `cargo test --workspace`.
