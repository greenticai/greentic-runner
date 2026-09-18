# MCP flow-node — deploy / merge checklist

Operator checklist for shipping the **MCP flow node** (LOCKED ENCODING v2:
`component == "mcp"`, payload `{ server, tool, arguments, output }`). Grounded in
the runner code on branch `feat/mcp-flow-node-exec` (greentic-runner PR #449).

All code citations are `file:line` against this repository unless another repo is
named. Anything not verifiable against code is tagged **VERIFY**.

---

## 1. The five PRs and their merge order

This epic spans five repos. The PRs are **independent** at runtime — there is no
hard build/runtime dependency chain between them. In particular:

> The runner does **not** depend on greentic-flow PR #235 (`NodeKind::Mcp`) to
> execute MCP nodes.

Proof in this repo: `flow_doc_to_ir` lowers the designer's YGTC op-key
`{ mcp: { server, tool, arguments, output }, routing }` into a FlowIR node whose
`component == "mcp"` purely by **structural op-key extraction** — it reads the
single flattened `raw` key off `NodeDoc` and carries it through verbatim,
independent of greentic-flow's own `classify_node_type` / `NodeKind` lowering.
The currently pinned greentic-flow is `1.1.0-dev.27132529466` (a `1.1.x-dev` line
*without* `NodeKind::Mcp`) and the node still lowers correctly.

- Adapter: `crates/greentic-runner-host/src/runner/flow_adapter.rs:77` (`flow_doc_to_ir`)
- Op-key passthrough set: `crates/greentic-runner-host/src/runner/flow_adapter.rs:53` (`NATIVE_OP_KEYS`, includes `"mcp"`) and `:73` (`is_native_op_key`)
- Test proving it: `crates/greentic-runner-host/tests/mcp_flow_node.rs:433`
  (`flow_doc_to_ir_preserves_mcp_op_key_verbatim`)

Recommended merge order (parallel-safe; ordered only for least operator friction):

| # | Repo | PR | What it ships | Runtime-blocking? |
|---|------|----|---------------|-------------------|
| 1 | mcp-client | #1 | stdio + HTTP MCP client (`greentic-mcp-client`) | Dependency of the runner (already published — see §2) |
| 2 | greentic-admin | #202 | per-team MCP storage + admin tab + designer read endpoint | Required to **register** servers; runner reads from it |
| 3 | greentic-runner | #449 | flow-node executor + e2e + legacy-path fix | The runtime feature itself |
| 4 | greentic-designer | #621 | flow-builder right-click MCP node + packc injection | Authoring only; packs can be hand-authored without it |
| 5 | greentic-flow | #235 | `NodeKind::Mcp` (tooling) | **No** — tooling-only; runner ignores it at runtime |

Rationale: 1→2 first so the runtime has a client crate and a server source to read.
3 ships the runtime. 4 + 5 are convenience/tooling and can land any time.

---

## 2. Per-repo: merge + publish

### mcp-client (#1)
- Crate `greentic-mcp-client`. The runner pins it via the workspace dep in
  `crates/greentic-aw-runtime/Cargo.toml:39`:
  `greentic-mcp-client = { version = "=1.2.0-research", features = ["native"] }`.
- `Cargo.lock` currently resolves **`1.2.0-research`** (checksum present), source
  `registry+https://github.com/rust-lang/crates.io-index`.
- **VERIFY before merge:** the epic note says mcp-client was bumped to
  `1.2.1-research`. This repo's pin and lock are still `=1.2.0-research`. If the
  runtime must consume `1.2.1-research`, bump the `=` pin in
  `crates/greentic-aw-runtime/Cargo.toml:39` and re-run `cargo update -p
  greentic-mcp-client --precise 1.2.1-research` so `Cargo.lock` moves in the same
  PR (CI runs `--locked`). If `1.2.0-research` is the intended runtime version,
  no change — but the checklist owner should pick one explicitly.
- `auth_token` plumbing: the admin seals tokens; mcp-client receives plaintext via
  `McpAuth { header_name, token }`
  (`crates/greentic-aw-runtime/src/mcp_source.rs:398`, `build_auth`). No secret in
  any repo file.

### greentic-admin (#202)
- Ships the storage + admin UI + the designer read endpoint the runner consumes.
- Routes (admin-mcp, branch `feat/mcp-per-team-and-ui`):
  - `POST/GET /api/admin/tenants/{id}/mcp-servers` — `src/routes/admin/tenant_mcp.rs:27`
  - `PUT/DELETE /api/admin/tenants/{id}/mcp-servers/{server_id}` — `:31`
  - `PUT /api/admin/tenants/{id}/mcp-servers/{server_id}/roles` — `:35`
  - `POST /api/admin/tenants/{id}/mcp-servers/{server_id}/test` — `:39`
  - `GET /api/v1/designer/tenant/me/mcp-servers` — `src/routes/designer/mcp.rs:22`
- Role constants: `flow_editor`, `agentic_worker` — admin-mcp
  `src/domain/mcp_server.rs:5`.
- The runner reads `GET /api/v1/designer/tenant/me/mcp-servers` and filters to the
  `flow_editor` role for flow nodes
  (`crates/greentic-aw-runtime/src/mcp_source.rs:302`, `:53` `MCP_ROLE_FLOW_EDITOR`).

### greentic-runner (#449) — this repo
- Merge the branch. New runtime code:
  - MCP flow-node helper + env builder: `crates/greentic-runner-host/src/runner/mcp_node.rs`
  - Engine dispatch arm `NodeKind::Mcp` + classifier: `crates/greentic-runner-host/src/runner/engine.rs` (`execute_mcp`, `mcp_node_kind`)
  - `McpToolSource::catalog_for_role` (flow_editor): `crates/greentic-aw-runtime/src/mcp_source.rs:255`
  - Legacy-path fix (op-key passthrough): `crates/greentic-runner-host/src/pack.rs` + `flow_adapter.rs` (commit `181121bc`)
- Feature gate: the executor lives behind the `agentic-worker` workspace feature
  (default-on). With the feature OFF, the node degrades to a clear error value and
  the build still succeeds
  (`crates/greentic-runner-host/src/runner/engine.rs`, `#[cfg(not(feature = "agentic-worker"))] execute_mcp`).
- `cargo update --verbose` + re-run `bash ci/local_check.sh` before declaring done.

### greentic-designer (#621)
- Authoring surface (right-click MCP node + packc injection). Not required for the
  runtime — packs authored elsewhere with the v2 encoding run identically.

### greentic-flow (#235)
- Tooling-only `NodeKind::Mcp`. Publish on the **`1.1.x-dev`** line if/when other
  tooling consumes it. The runner stays on its current `1.1.0-dev.*` pin and does
  not need this PR (see §1).

---

## 3. Runner deploy — required env vars

The MCP flow node is enabled entirely by environment. The builder is
`mcp_node::source_from_env` (`crates/greentic-runner-host/src/runner/mcp_node.rs:36`),
constructed once per `FlowEngine` (`engine.rs`, `mcp_tool_source` field, primed in
`FlowEngine::new`).

| Env var | Purpose | Failure mode if unset / wrong |
|---------|---------|-------------------------------|
| `GREENTIC_AW_ADMIN_ENDPOINT` | Admin origin the runner GETs `/api/v1/designer/tenant/me/mcp-servers` from. Trailing slash trimmed. | **MCP disabled (fail-open).** `source_from_env` returns `None`; every MCP node binds `{"error":"MCP is not configured on this runner (set GREENTIC_AW_ADMIN_ENDPOINT + GREENTIC_AW_ADMIN_TOKEN)"}` and the flow still completes. `mcp_node.rs:41`, `:65`. |
| `GREENTIC_AW_ADMIN_TOKEN` | Tenant `gtc_live_*` bearer presented to the designer read endpoint. | Same as above — `None` source, graceful per-node error. `mcp_node.rs:44`. |
| `GREENTIC_AW_MCP` | Opt-out switch. **MCP is ON by default** whenever the two admin vars are present. Set `GREENTIC_AW_MCP=0` to force-disable. | `=0` → source `None`, MCP disabled (logged `info`). Any other value / unset → enabled. `mcp_node.rs:37`. |

Notes:
- These are the **same** three vars the agent-loop MCP path uses
  (`crates/greentic-runner-host/src/runner/agent_node.rs:252`, `:256`, `:259`) —
  the flow node only swaps the role filter to `flow_editor`. One admin
  endpoint+token configures both surfaces.
- The admin token is tenant-scoped (`gtc_live_*`); the tenant is implied by the
  token, and `TenantContext` is used only as the per-tenant cache key
  (`mcp_source.rs:201` doc, `:231` `cache_key`).
- Resilience is total: a dead admin → empty catalog + `warn`; a dead/slow MCP
  server → that server skipped + `warn`; a bad tool call → `{"error": ...}`. MCP
  never aborts a flow run (`mcp_source.rs:11` resilience contract;
  `mcp_node.rs:16` node contract).

### Admin-side prerequisite (not a runner env var)

To register a server, an operator hits the **admin** mutating routes, which are
gated by an **operator session cookie (`gtcadmin_session`) + CSRF token**, *not* a
plain bearer (admin-mcp `src/routes/admin/tenant_mcp.rs:8` `OperatorCtx`;
`src/auth/session.rs`). The runner-facing read endpoint is the bearer-token one.
Assign the server the **`flow_editor`** role (admin-mcp
`src/routes/admin/tenant_mcp.rs:35`) or the runner's flow-node catalog filter will
exclude it.

---

## 4. Post-deploy verification

1. **Verify the deployed binary is the new one** (team habit: verify before
   blaming code). The runner exposes a version/info path:
   ```bash
   greentic-runner info        # or: greentic-runner --version
   ```
   Confirm the build embeds PR #449 (compare to the commit you deployed). Do NOT
   assume the image rolled — a stale image is the usual culprit.

2. **Health:** `GET /healthz` returns `200` with `active_packs`
   (`crates/greentic-runner-host/src/http/health.rs:77`, route
   `crates/greentic-runner-host/src/runner/mod.rs:77`).

3. **MCP source constructed:** with the env set, the runner logs at `info`
   `flow MCP node source constructed` with the endpoint
   (`mcp_node.rs:47`). Its absence (or `GREENTIC_AW_MCP=0; flow MCP node source
   disabled`) means MCP is off.

4. **Smoke:** run `scripts/mcp_smoke.sh` (see §6). It registers (or expects) a
   `flow_editor` MCP server, asserts the admin `/test` probe, and drives a
   single-MCP-node pack through the runner's real flow entrypoint
   (`greentic-runner-cli`, real-runtime mode) against a mock MCP server.

### There is NO direct "run a flow" HTTP API

The `HostServer` router exposes exactly:
`/operator/op/invoke`, `/healthz`, `/admin/packs/status`, `/admin/packs/reload`,
`/admin/capabilities`, `/sql/{conn}/schema`, `/sql/{conn}/query`
(`crates/greentic-runner-host/src/runner/mod.rs:138-159`, the `router()` function).

`/operator/op/invoke` runs a single **operator/component** (CBOR in/out,
`operator::invoke`), **not** a flow, and the MCP node is a *native flow node*, so
it is **not reachable** through that route. In production a flow runs only via a
provider **ingress adapter** (Telegram/Slack/WebChat/webhook) or the embedded
`RunnerHost::handle_activity` API (`crates/greentic-runner-host/src/host.rs:172`),
neither of which is a generic "POST input, get flow output" endpoint.

The real, deterministic, shipped entrypoint that runs **one flow with a JSON input
and returns the output** is the **`greentic-runner-cli`** binary in real-runtime
mode. It loads a `.gtpack`, builds a real `FlowEngine`
(greentic-runner-desktop `src/lib.rs:434`) — the **same** `FlowEngine::new` path
that primes `source_from_env` — and executes the entry flow. This is exactly the
engine path the e2e test exercises
(`crates/greentic-runner-host/tests/mcp_flow_node.rs:472`). The smoke uses it.

> Operator manual run (what the smoke automates):
> ```bash
> GREENTIC_AW_ADMIN_ENDPOINT=https://admin.example \
> GREENTIC_AW_ADMIN_TOKEN=gtc_live_xxx \
> greentic-runner-cli --pack one-mcp-node.gtpack --flow mcp.flow \
>   --tenant <tenant> --mocks off --json --input '{"issue_id":"42"}'
> ```
> `--mocks off` is **required**: the default `--mocks on` installs a short-circuit
> `mcp_tools` mock (greentic-runner-cli `src/bin/greentic-runner-cli.rs:448`,
> `short_circuit: true`) that would intercept the call instead of letting it reach
> the real MCP server.

---

## 5. Rollback + known follow-ups

**Rollback.** The feature is fail-open and env-gated:
- Fast disable without redeploy: set `GREENTIC_AW_MCP=0` (or unset
  `GREENTIC_AW_ADMIN_ENDPOINT` / `GREENTIC_AW_ADMIN_TOKEN`) and restart the runner.
  MCP nodes then bind a graceful error and flows keep running.
- Full rollback: redeploy the previous runner image. Packs containing `mcp` nodes
  still **load** on an older runner only if that runner recognizes the `mcp`
  op-key; pre-#449 runners would misclassify a raw-YGTC `mcp` node via the legacy
  path (the bug commit `181121bc` fixed) — so prefer the env kill-switch over a
  binary rollback when MCP packs are already deployed.

**Known follow-ups.**
- **Per-team secret storage is still tenant-scoped at the seam the runner sees.**
  Admin stores team-scoped overrides (admin-mcp `McpServerSummary.team_id`), but
  the runner authenticates with a single tenant `gtc_live_*` token and the
  designer read endpoint resolves team override only when the caller carries a
  `team_slug` (admin-mcp `src/repo/mcp_servers.rs` designer query). A runner using
  a tenant-level token sees tenant-default servers. **VERIFY** whether the runner
  must present a team-scoped token to get per-team MCP servers, or whether
  tenant-default is the intended runtime scope.
- mcp-client version pin (§2): reconcile `=1.2.0-research` vs the `1.2.1-research`
  the epic note mentions.
- greentic-flow #235 publish line (`1.1.x-dev`) is tooling-only; no runtime gate.

---

## 6. Smoke harness (`scripts/mcp_smoke.sh`)

Parameterized bash + curl + jq harness; bundles `scripts/mock_mcp_server.py` (a
stdlib-only mock MCP server speaking the runner's JSON-RPC contract). See
`scripts/mcp_smoke.sh --help` for all flags.

| Step | What it does | Automated? |
|------|--------------|------------|
| 0 | Start the bundled mock MCP server | Yes |
| 1 | Register a `flow_editor` MCP server in admin (POST + roles PUT) | Best-effort — needs `--admin-cookie` + `--csrf-token` (admin routes are session+CSRF gated); else SKIP + manual note |
| 2 | Probe the mock's `initialize` + `tools/list` directly (the runner's exact sequence) | Yes — the deterministic equivalent of admin `/test`, no admin session needed |
| 2b | Call admin `/test`, assert `ok=true` + tools | Best-effort — same session creds as step 1 |
| 3 | GET `/api/v1/designer/tenant/me/mcp-servers`, assert a `flow_editor` server appears (exactly what the runner consumes) | Yes — plain `gtc_live_*` bearer (`--designer-token`) |
| 4 | Run ONE `mcp` flow node through the real engine | Real runner via `greentic-runner-cli --mocks off` when `--pack` is supplied; otherwise falls back to the in-repo e2e test (same `FlowEngine` + `source_from_env` path). bash cannot synthesize a CBOR `.gtpack`, so a real-runner step 4 needs a pack built by `gtc`/`packc`. |
| 4b | Assert the mock MCP recorded a `get_issue` tools/call (real-pack path only) | Yes when `--pack` is used |

Exits non-zero on any failed assertion; SKIPs (missing optional creds) print the
manual step and do not fail the run.
