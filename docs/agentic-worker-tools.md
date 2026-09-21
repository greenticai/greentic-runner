# Enabling extension tools for a Digital Worker (agentic worker)

An agentic-worker agent (`DwAgent` flow node) calls extension tools through the
`greentic-ext-runtime`. Each tool the agent may use is declared as a
`ToolRef { extension_id, tool_name }` in the agent's `AgentConfig`.

## Method 1 — declare tools in operator YAML (always available)

The runner-host config (`HostConfig`) carries an `agents:` map. Each entry is a
full `AgentConfig`: the system prompt, the LLM provider/model, limits, and the
tool list.

```yaml
agents:
  research-bot:
    agent_id: research-bot
    system_prompt: "You are a research assistant. Use tools when helpful."
    llm:
      provider: openai
      model: gpt-4o-mini
    tools:
      - extension_id: greentic.tavily
        tool_name: web_search
      - extension_id: greentic.sql
        tool_name: sql_ask
    limits:
      max_iter: 8
      timeout: 60
      max_history_turns: 20
      llm_retry_attempts: 3
      llm_retry_backoff: 250
```

At runtime the loop resolves each `ToolRef` against the loaded extension via
`ExtensionRuntime::list_tools` — a tool whose extension is not installed is
logged and silently skipped (the LLM simply never sees it).

Prerequisites:
- The extension is installed in the extension discovery dir
  (`GREENTIC_EXTENSIONS_DIR`, else `~/.greentic/extensions`).
- `GREENTIC_AW_REDIS_URL` is set (the agent loop persists session state in Redis).

## Method 2 — manifest overlay

Instead of hand-listing `tools:` in YAML, drop the Digital Worker's manifest
JSON into the manifests dir and the runner overlays its `agentic_worker`-capable
tools onto the agent's `tools` list automatically.

- **Where:** `GREENTIC_AGENT_MANIFESTS_DIR` (else `~/.greentic/agents/`), file
  named `<agent_id>.json`.
- **What it is:** the `DigitalWorkerManifest` JSON the DW compose wizard emits
  (`greentic-dw` wizard stdout — capture it to a file).
- **What it overrides:** only `AgentConfig.tools`. The operator YAML `agents:`
  entry still supplies `system_prompt`, `llm`, and `limits` — those are NOT in
  the manifest.
- **Fail-soft:** a missing, malformed, invalid, or id-mismatched manifest is
  logged and ignored; the agent falls back to the YAML `tools:` list. A valid
  manifest that declares *no* agentic-worker tools is treated as "no tool
  opinion" and likewise leaves the YAML list intact (it never silently wipes it).
  A broken manifest never takes the agent offline.

Example: with `~/.greentic/agents/research-bot.json` present, the YAML
`agents.research-bot` needs only `system_prompt` + `llm` (+ optional `limits`);
its `tools:` may be empty and will be replaced by the manifest's tool set at
load time.

> The DW wizard writes this file for you: `gtc wizard … --emit-manifest <DIR>`
> emits `<DIR>/<manifest_id>.json` — the exact loose file this overlay reads.
> (The manifest is not stored inside the composed `.gtpack`, so this loose file
> is the supported delivery path.)

## Method 3 — MCP server tools (on by default)

Tenant-registered MCP servers (designer-admin → MCP Servers, role
`agentic_worker`) can be offered to an agent alongside extension tools. An MCP
tool is declared with the `mcp:` extension-id form — no schema change:

```yaml
    tools:
      - extension_id: mcp:<server_id>   # admin's server id, e.g. mcp:gh-issues
        tool_name: get_issue            # raw tool name on that server
```

MCP is active whenever the admin connection is configured — the real
authorization is upstream (the tenant must register the server with the
`agentic_worker` role, and the agent's allowlist must explicitly reference
`mcp:<server_id>`):

- `GREENTIC_AW_ADMIN_ENDPOINT` + `GREENTIC_AW_ADMIN_TOKEN` — the same admin
  endpoint/token the agent registry uses; the tenant's MCP servers are pulled
  from `/api/v1/designer/tenant/me/mcp-servers` and cached per tenant for
  5 minutes.
- `GREENTIC_AW_MCP=0` — operator opt-out: disables MCP tools for the whole
  runner (every `mcp:` ref becomes inert) even when the admin connection is
  configured. Use for environments where outbound calls to tenant-registered
  MCP servers must stay off.

Fail-soft, same spirit as the manifest overlay: an unreachable admin or MCP
server degrades to "tool not offered" (warn-logged); a tool call that fails at
runtime returns an in-band `{"error": ...}` value to the LLM. MCP can never
take an agent step down. Full design:
`docs/2026-06-07-aw-runtime-mcp-tools-design.md`.

## A2A agents (`a2a:` tools)

An agentic worker can call an external agent that speaks A2A (Agent2Agent
v1.0), bound as one tool per agent:

```yaml
tools:
  - extension_id: a2a:<agent_id>   # the admin registry's agent id
    tool_name: ask
    input_schema: { type: object, properties: { message: { type: string } }, required: [message] }
```

The `input_schema` is required: an A2A agent card carries no input schema, so
a ref without one is not offered to the model. The description shown to the
model is the agent card's own, falling back to the ref's.

**Where agents come from.** Only from the pack: `assets/a2a-routes.json`,
written by the designer (one record per agent: `agent_id`, `base_url`,
`auth_header_name`, `auth_team`, `requires_auth` — never a token). There is no
admin-backed A2A source. The source is built for the in-pack `dw.agent`
runtime (deployed units, the desktop runner and the designer's test-chat
sidecar); the process-level serve path and graph turns have none.

**Credentials.** A route with `requires_auth` has its token read at **call
time** (so rotation needs no restart), from the first of:

1. `secrets://default/<tenant>/<auth_team|_>/a2a/<agent_id>.unit-<segment>` — when running as a unit
2. `secrets://default/<tenant>/<auth_team>/a2a/<agent_id>` — when `auth_team` is set
3. `secrets://default/<tenant>/_/a2a/<agent_id>`

It is sent as `Authorization: Bearer <token>` (no header name, or any
spelling of `Authorization`), or raw under the configured header name. It goes
only on the `SendMessage` POST, never on the public agent-card fetch, and
**only when the card's interface URL has the same host and port as the
configured `base_url`** — otherwise the call is refused and nothing is sent.
A missing credential refuses the call; there is no unauthenticated retry.

**Switch:** `GREENTIC_AW_A2A=0` disables A2A tools for the whole runner.

**What an operator sees on failure.** A failed call returns an in-band
`{"error": ...}` value to the model naming the agent and the cause (every
secret URI tried, or both hosts on a mismatch), and logs it at `warn`
(`a2a tool call failed`, `a2a agent card unavailable`). At construction an
`info` line per credentialed agent (`a2a credential destination`) records
which host each credential will go to.

**Trust.** The sidecar is trusted input: a pack that pairs a real `agent_id`
with a different `base_url` would send that agent's token there. Only install
packs you trust. Ids and team segments outside `[A-Za-z0-9._-]` are refused.

## Agent graphs

Multi-agent orchestration is supported via the `dw.agent_graph` flow-node kind,
backed by `greentic_aw_runtime::graph::GraphExecutor`. Graphs are defined in an
`agent-graph.json` sidecar co-located with the pack. For the full design and
sidecar schema see
`docs/superpowers/specs/2026-06-06-runtime-agent-graph-execution-design.md`.
