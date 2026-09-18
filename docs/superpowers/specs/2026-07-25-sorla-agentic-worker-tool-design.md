# SoRLa SoR as DW agentic-worker tool — design (thin vertical, BusinessAction-based)

_Date: 2026-07-25 · Epic: SoRLa/SoRX productionization (#6 agentic surface) · Sub-project 1_
_Primary repo: `greentic-runner` (aw-runtime + runner-host) · Follow-on repo: `greentic-designer` (surfacing)_

## Context

The SoRLa/SoRX productionization epic's #6 "agentic" requirement is the last of the eight
requirements without a working agent-facing surface. A grounded re-audit (2026-07-25, 5 parallel
Explore agents) established the real state:

- **`AgentEndpointDecl` is invisible to the SoRX runtime.** SoRX's capability discovery
  (`GET /admin/v1/capabilities`) and invoke (`POST /admin/v1/capabilities/invoke`) address only
  compiled **`BusinessAction`s** (from the SoRLa DSL `action {}`) plus business-event topics —
  never `agent_endpoints`. `ir.agent_endpoints` is never read by SoRX's router-building code.
- The `agentic_worker_metadata` field on the SoRLa design extension's WIT `tool-definition` is a
  **non-gap** (dead code on a v2 describe.json extension; documented separately). Populating it
  changes nothing observable.
- The DW-Composer already surfaces agentic-worker tools from **two** sources (WASM design
  extensions + admin-registered `component:` tools) and dispatches tool calls by a **string prefix
  on `extension_id`** resolved in `greentic-aw-runtime::tools::dispatch_tool_call`
  (`mcp:` / `component:` / `flow:` arms).
- The **BusinessAction invoke path is SHIPPED and tested**: `POST /admin/v1/capabilities/invoke`
  takes a `cap://greentic/business-functions/<pack>/<action_id>/v<version>` capability + input +
  context, enforces approval/risk gating (202 `approval_required` / 403 `Denied`), and is reachable
  from the designer through the existing per-deployment reverse-proxy (child SoRX runs
  `auth.mode = none`).

So the honest, shippable thin vertical is **BusinessAction-based**: surface a deployed SoR's
BusinessActions as DW agentic-worker tools and dispatch tool calls to the shipped capability-invoke
endpoint. This reuses working machinery end-to-end and needs **zero** change to `greentic-sorx`.
(Chosen with the user over: routing per-agent-endpoint to its `backing.actions[0]`, and making
agent-endpoints first-class in SoRX — both deferred.)

## Goal

A `dw.agent` can attach a SoRX **BusinessAction** (from a deployed SoRLa SoR) as an agentic-worker
tool in the DW-Composer, and — when the agent selects it at runtime — the tool call is dispatched to
the SoR's `POST /admin/v1/capabilities/invoke`, returning the action's result (honoring
approval/risk gating), on the **deployed / sidecar** execution path.

## Non-goals (SP1)

- Enriching tool metadata from the pack's `agent-endpoint-action-catalog.json`
  (`usage_hint`/`examples` from `AgentEndpointDecl.intent`/`examples`) — fast-follow SP2.
- In-process designer test-chat parity (the designer's own in-process `AgentRuntime` wires neither
  `component:` nor the new `sorla:` source today) — deferred; execution is via the sidecar (real
  `greentic-runner`) and deployed workers. Documented limitation, not a regression.
- Endpoints backed by flows or multiple actions; agent-endpoints first-class in SoRX (option C).
- Any change to `greentic-sorx`.
- Dynamic SoRX-address resolution via a registry (SP1 carries the address on the binding + env
  fallback; see Addressing).

## Architecture

Three mechanisms, each mirroring an existing precedent. The invoke target is always a
`cap://greentic/business-functions/...` BusinessAction.

### 1. Execution — `greentic-runner` (this repo, PR first)

**`crates/greentic-aw-runtime`** — mirror the `component:` family; the crate stays free of any
HTTP/SoRX dependency (it sees only a trait + JSON, per `component_source.rs`'s stated contract):

- `tools::dispatch_tool_call` — add a fourth prefix arm `call.extension_id.strip_prefix("sorla:")`
  beside the `mcp:` / `component:` / `flow:` arms, plus a new
  `sorla: Option<Arc<SorlaToolCatalog>>` parameter. On missing catalog/route it returns
  `Ok(json!({"error": ...}))` — never `Err`, never panic (matches the sibling arms).
- `SorxInvoker` trait — mirror `ComponentInvoker`:
  ```rust
  pub trait SorxInvoker: Send + Sync {
      fn list_operations(&self) -> Vec<SorxOperation>;   // total; may be empty
      fn invoke<'a>(&'a self, sor_id: &'a str, action: &'a str, args_json: &'a str)
          -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;
  }
  ```
  where `SorxOperation` mirrors `ComponentOperation` minimally: `{ pack, action, description,
  parameters, cap_uri }` (`extension_id = "sorla:<pack>"`, `tool_name = <action>`). The richer
  `agentic_worker_metadata` mapping is a designer-surfacing concern (PR-2), not the runtime catalog.
- `SorlaToolSource` / `SorlaToolCatalog` — mirror `ComponentToolSource` / `ComponentToolCatalog`
  (per-tenant TTL cache over `list_operations`; infallible `catalog()`).
- `AgentRuntime` — add `sorla: Option<Arc<SorlaToolSource>>` field + `#[must_use]
  with_sorla_source(mut self, sorla: Option<Arc<SorlaToolSource>>) -> Self`, and thread `sorla`
  into the `dispatch_tool_call(...)` call.

**`crates/greentic-runner-host`** — mirror `component_source_from_packs`:

- `sorla_source_from_env()` (gated by an env flag, e.g. `GREENTIC_AW_SORLA_TOOLS != "0"`) constructs
  the real HTTP `SorxInvoker` impl (`SorxHttpInvoker`) — this is where the HTTP client lives, so
  aw-runtime stays client-free.
- Wire `.with_sorla_source(sorla_source_from_env())` onto the `AgentRuntime` builder in
  `build_runtime_with_stores` (`runner/agent_node.rs`, beside the existing `.with_component_source`
  / `.with_flow_source` chain). This wires `sorla:` **exactly where `component:`/`flow:` are wired**
  — the in-process `dw.agent` path (incl. the sidecar, which is a real runner-host). The separate
  NATS `agentic.call` serve constructor (`build_agent_runtime`) wires none of `component`/`flow`/
  `sorla`; serve-path parity for all three is a shared known limitation, out of SP1 (the serve path
  still degrades safely — the tool is reported missing and dispatch returns an error value).

**`SorxHttpInvoker`** (runner-host): for each `invoke(sor_id, action, args_json)` it POSTs to
`{sorx_base_url}/admin/v1/capabilities/invoke` with body:
```json
{ "capability": "<cap:// resolved from (sor_id, action)>",
  "input": <args_json parsed>,
  "context": { "tenant_id": "<tenant>", "caller_id": "dw-agent", "roles": <agent's configured roles, else []> },
  "idempotency_key": "<optional>", "dry_run": false }
```
mapping the response: `ok:true` → `result`; `status:"approval_required"` (202) → a structured tool
result `{"status":"approval_required","approval":{...}}` (NOT an error — the agent must see it);
403 `Denied` → `{"error":"denied", ...}`; 404 → `{"error":"capability_not_found", ...}`. Auth: send
`X-Greentic-Tenant-Id` / `X-Greentic-Caller-Id` / `X-Greentic-Caller-Role` headers (mirror operax
`OperaxContext::sorx_headers`); `Authorization: Bearer` only if a token is configured (child SoRX is
`auth.mode = none`).

### 2. Surfacing — `greentic-designer` (follow-on PR, after aw-runtime rev-bump)

Mirror the admin `component:` tools feed end-to-end (`src/ui/component_catalog/build.rs` +
`src/ui/routes/extensions.rs` merge):

- **Fetcher**: read a deployment's BusinessActions from `GET /admin/v1/capabilities` through the
  existing reverse-proxy target (`sorla_deployments` row → `127.0.0.1:{port}`; poll sibling to the
  `/healthz` poll in `orchestrate/sorx_deploy/process.rs`). Keep only offers whose `contracts`
  contain `greentic.sorx.business-action.invoke.v1`.
- **Snapshot + cache**: `SorlaToolSnapshot` on `AppState`, same TTL/stale-on-error discipline as
  `ComponentToolsSnapshot`.
- **Mapper** `sorla_action_to_dto()` next to `component_tool_to_dto()`: one `ExtensionToolDto` per
  offer — `extension_id = "sorla:<pack>"`, `tool_name = <action_id>`
  (**MUST be `sorla:<pack>` where `<pack>` is the pack name from the offer's `cap://greentic/
  business-functions/<pack>/<action>/...` URI — this is the exact key PR-1's runtime catalog uses;
  a `sorla:<deployment_id>` binding with `deployment_id != pack` would be reported missing and never
  dispatch**),
  `capabilities = ["agentic_worker"]`, `input_schema_json` from the offer, and
  `agentic_worker_metadata` mapped from the offer metadata:
  `action.risk` → `cost` (low/medium/high) + `side_effects` (read/write/external heuristic),
  `action.approval == Required` → `confirmation_required: true`.
- **Merge point**: one more `tools.extend(...)` block in `list_tools_by_capability`
  (`src/ui/routes/extensions.rs`), gated on `q.capability == "agentic_worker"` and the tenant/
  deployment scope already in scope. Fetch failure → skip that source (warn), never fail the picker.

### 3. Packaging — no change

A `sorla:<pack>` binding travels through `dw_authoring_adapter::worker_spec_from_form` →
`WorkerSpec.extension_tools` → `.gtpack` → runner `AgentConfig.tools` **unchanged** (the mapper is
opaque to the prefix, exactly as for `component:`).

## Addressing (design decision, flagged for review)

`ToolRef` (`greentic-aw-runtime/src/config.rs:16`) carries only
`{extension_id, tool_name, description?, input_schema?, usage_note?}` — no slot for a SoRX address
or `cap://`, and widening it would cascade into `greentic-dw-authoring`'s `ExtensionToolBinding`.
The trait's `list_operations()` is also **sync** (called inside `ComponentToolCatalog::from_invoker`).

SP1 therefore mirrors the `component:` precedent exactly — the invoker enumerates the universe, and
listing filters by the agent's `allowed` bindings — with a **single SoR per worker**:

- `sorla_source_from_env()` (runner-host, async) reads `GREENTIC_AW_SORX_URL`; if set, it performs
  the one-time `GET {base}/admin/v1/capabilities`, keeps business-action offers, and constructs
  `SorxHttpInvoker { base_url, ops: Vec<SorxOperation>, cap_map: (pack, action) → cap:// }`. If unset
  → `None` (no `sorla:` tools).
- `SorxInvoker::list_operations()` returns the captured `ops` synchronously (no re-fetch — the SoR's
  action set is stable per deployment; TTL re-list just returns the same cached ops).
  Each `SorxOperation` → `extension_id = "sorla:<pack>"`, `action = <action_id>`, plus
  `description`/`parameters` from the offer.
- `SorxInvoker::invoke(pack, action, args)` (async) resolves `cap://` from `cap_map`, POSTs to
  `{base_url}/admin/v1/capabilities/invoke`.

**Limitations (documented, follow-up):** one SoR per worker (env-scoped); capability set captured at
worker start (no live TTL re-fetch). Multi-SoR + per-binding addressing (which requires widening
`ToolRef`/`ExtensionToolBinding` to carry `{base_url, cap://}`) and dynamic deployment-registry
resolution are deferred out of SP1.

## Sequencing (release-train coupling)

1. **PR-1 → `greentic-runner` `research`**: aw-runtime (`sorla:` dispatch + `SorxInvoker` trait +
   `SorlaToolSource` + `with_sorla_source`) + runner-host (`sorla_source_from_env` + `SorxHttpInvoker`
   + builder wiring). Runner's own crates consume aw-runtime via **path dep** → live immediately.
2. **Rev-bump**: after PR-1 lands, bump the aw-runtime `rev =` in `greentic-designer/Cargo.toml`
   (byte-match the coupled `greentic-ext-runtime` pin per the comment there; and `greentic-dw-authoring`
   must resolve against the same instances). `cargo update -p greentic-aw-runtime`.
3. **PR-2 → `greentic-designer` `research`**: the surfacing source + mapper + merge.

SP1 is complete when PR-1 + PR-2 are merged and a deployed/sidecar `dw.agent` can attach and invoke
a SoR BusinessAction. PR-1 is independently valuable and testable (unit + mock-HTTP) before PR-2.

## Testing

- **aw-runtime `sorla:` dispatch** (unit, fake `SorxInvoker`): unknown catalog → `{"error":...}` not
  `Err`; known `(sor_id, action)` → invoker called with the right args; result passthrough.
- **`SorxHttpInvoker`** (unit, mock HTTP server): happy `ok:true` → `result`; `202 approval_required`
  → structured non-error result; `403` → `{"error":"denied"}`; `404` → `{"error":"capability_not_found"}`;
  header presence (`X-Greentic-Tenant-Id`/`-Caller-Id`/`-Caller-Role`).
- **`SorlaToolSource` catalog** (unit): per-tenant cache; empty invoker → empty catalog, no panic.
- **runner-host wiring** (unit/integration): `sorla_source_from_env` returns `None` when
  `GREENTIC_AW_SORLA_TOOLS=0`; builder includes the source otherwise (mirror the component-source test).
- **designer mapper** (unit): offer → `ExtensionToolDto` risk/approval mapping; non-business-action
  offers filtered out; fetch failure → source skipped, picker still returns other sources.
- **designer packaging** (unit): `sorla:<id>` binding survives `worker_spec_from_form`.

## Files touched

- `greentic-runner/crates/greentic-aw-runtime/src/{tools.rs,lib.rs}` + new `sorla_source.rs`
  (mirror `component_source.rs`).
- `greentic-runner/crates/greentic-runner-host/src/runner/agent_node.rs` + new `SorxHttpInvoker`
  module (runner-host).
- `greentic-designer/src/ui/{component_catalog/,routes/extensions.rs,state/mod.rs}` + new
  `sorla_action_catalog` fetcher (PR-2).
- No `greentic-sorx` change.

## Global constraints (both repos)

- Rust edition/toolchain per each repo's `rust-toolchain.toml`; `#![forbid(unsafe_code)]` norm.
- **No `unwrap()`/`panic!()` in production paths** — `anyhow`/`thiserror`; the dispatch/invoke paths
  must never crash the agent loop (return structured `{"error":...}` tool results).
- English only; Conventional Commits. **NO Claude co-author attribution** on commits/PRs in
  `greentic-runner` (CLAUDE.md:382) and none in `greentic-designer` either (re-check each repo's
  CLAUDE.md before committing).
- `bash ci/local_check.sh` green before done in each repo (fmt + clippy `-D warnings` + tests).
- aw-runtime must NOT gain an HTTP/SoRX-client dependency — the client lives in runner-host.
- Default build unchanged when `GREENTIC_AW_SORLA_TOOLS=0` / no `sorla:` bindings.
- Respect the release-train: designer only sees `sorla:` after the rev-bump (step 2).

See the epic memory `sorla-sorx-productionization-epic` for the full audit trail and the reasons
options B/C were deferred.
