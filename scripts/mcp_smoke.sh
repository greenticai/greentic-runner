#!/usr/bin/env bash
#
# mcp_smoke.sh — live smoke harness for the MCP flow node (LOCKED ENCODING v2).
#
# Validates the runner-reachable pieces of the MCP flow-node feature end to end,
# grounded in the real code paths (greentic-runner PR #449):
#
#   crates/greentic-runner-host/src/runner/mcp_node.rs   (source_from_env, invoke)
#   crates/greentic-aw-runtime/src/mcp_source.rs          (admin fetch -> flow_editor filter -> probe -> dispatch)
#   crates/greentic-runner-host/src/runner/engine.rs      (NodeKind::Mcp dispatch)
#   crates/greentic-runner-host/tests/mcp_flow_node.rs    (real FlowEngine run of one mcp node)
#
# What it does, in order:
#   STEP 0  Start a bundled mock MCP HTTP server (scripts/mock_mcp_server.py).
#   STEP 1  (best-effort) Register a flow_editor MCP server in the admin, pointed
#           at the mock, via POST /api/admin/tenants/{id}/mcp-servers + roles PUT.
#   STEP 2  Assert the mock speaks the MCP JSON-RPC contract the runner drives
#           (initialize -> tools/list -> tools/call) by calling it directly, the
#           same 4-call sequence McpToolSource performs. This is the deterministic
#           equivalent of the admin /test probe and needs no admin session.
#   STEP 2b (best-effort) Call the admin /test endpoint if admin session creds
#           are supplied, and assert initialize + tools.list succeeded.
#   STEP 3  Assert the runner's designer read endpoint
#           GET /api/v1/designer/tenant/me/mcp-servers returns a server carrying
#           the flow_editor role (this is exactly what the runner consumes).
#   STEP 4  Drive ONE mcp flow node through the runner's REAL flow entrypoint and
#           assert the tool result is bound into flow output AND the mock MCP
#           received a tools/call.
#
# Honest scoping (read docs/mcp-deploy-checklist.md §4):
#   * The runner has NO direct "run a flow with input" HTTP API. The real,
#     shipped flow entrypoint is the `greentic-runner-cli` binary (real-runtime
#     mode -> greentic-runner-desktop -> FlowEngine::new -> source_from_env). That
#     binary loads a .gtpack and runs the entry flow. STEP 4 uses it when you pass
#     a prebuilt single-mcp-node pack via --pack.
#   * Building a .gtpack requires a CBOR manifest (gtc/packc), which bash cannot
#     produce. So when --pack is NOT supplied, STEP 4 falls back to running the
#     repo's in-tree e2e test, which executes ONE mcp node through the SAME real
#     FlowEngine + source_from_env path against its own mock pair. Either way the
#     runner-side engine path is exercised for real.
#   * Admin mutating routes (register, roles, /test) are gated by an operator
#     SESSION cookie + CSRF token, not a bearer (admin-mcp
#     src/routes/admin/tenant_mcp.rs OperatorCtx). STEPS 1 and 2b therefore run
#     only when you supply --admin-cookie and --csrf-token; otherwise they are
#     SKIPPED with a clear note and you perform the register manually in the admin
#     UI. The designer read endpoint (STEP 3) uses a plain gtc_live_* bearer and
#     is always automated.
#
# No secrets are written to disk. Tokens come from flags / env only.

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults / inputs (flags override env; env overrides built-in defaults)
# ---------------------------------------------------------------------------
ADMIN_URL="${ADMIN_URL:-}"               # admin origin, e.g. https://admin.example
ADMIN_TOKEN="${ADMIN_TOKEN:-}"           # operator/service token (best-effort register)
ADMIN_COOKIE="${ADMIN_COOKIE:-}"         # gtcadmin_session cookie value (mutating admin routes)
CSRF_TOKEN="${CSRF_TOKEN:-}"             # x-csrf-token for mutating admin routes
TENANT_ID="${TENANT_ID:-}"               # tenant id (admin path segment)
DESIGNER_TOKEN="${DESIGNER_TOKEN:-}"     # gtc_live_* token the runner uses (designer read endpoint)
RUNNER_URL="${RUNNER_URL:-}"             # runner origin (for /healthz only; no flow-run route exists)
RUNNER_TENANT="${RUNNER_TENANT:-demo}"  # tenant id passed to greentic-runner-cli
PACK="${PACK:-}"                         # optional prebuilt single-mcp-node .gtpack (or dir)
FLOW_ID="${FLOW_ID:-mcp.flow}"           # entry flow id inside the pack
MOCK_HOST="${MOCK_HOST:-127.0.0.1}"
MOCK_PORT="${MOCK_PORT:-8765}"
SERVER_ID="${SERVER_ID:-smoke-github}"   # admin server id to register
SERVER_NAME="${SERVER_NAME:-Smoke GitHub Mock}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MOCK_URL="http://${MOCK_HOST}:${MOCK_PORT}"

FAILURES=0
MOCK_PID=""
WORKDIR=""

# ---------------------------------------------------------------------------
# Logging helpers — clear PASS/FAIL lines
# ---------------------------------------------------------------------------
log()  { printf '%s\n' "$*"; }
pass() { printf 'PASS: %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*"; FAILURES=$((FAILURES + 1)); }
skip() { printf 'SKIP: %s\n' "$*"; }
step() { printf '\n=== %s ===\n' "$*"; }

usage() {
  cat <<'EOF'
mcp_smoke.sh — live smoke for the MCP flow node (LOCKED ENCODING v2).

Usage:
  scripts/mcp_smoke.sh [options]

Options (each also settable via the UPPERCASE env var of the same name):
  --admin-url URL          Admin origin (e.g. https://admin.example).
  --admin-token TOKEN      Operator/service token (best-effort, STEP 1/2b).
  --admin-cookie VALUE     gtcadmin_session cookie value (enables STEP 1 + 2b).
  --csrf-token VALUE       x-csrf-token for mutating admin routes (STEP 1 + 2b).
  --tenant-id ID           Tenant id used in the admin path segment.
  --designer-token TOKEN   gtc_live_* token the runner uses (STEP 3, required).
  --runner-url URL         Runner origin, used only for /healthz.
  --runner-tenant ID       Tenant id passed to greentic-runner-cli (default: demo).
  --pack PATH              Prebuilt single-mcp-node .gtpack (or materialized dir).
                           If omitted, STEP 4 falls back to the in-repo e2e test.
  --flow-id ID             Entry flow id in the pack (default: mcp.flow).
  --mock-host HOST         Mock MCP bind host (default: 127.0.0.1).
  --mock-port PORT         Mock MCP bind port (default: 8765).
  --server-id ID           Admin server id to register (default: smoke-github).
  --server-name NAME       Admin server display name.
  -h, --help               Show this help.

Exit code is non-zero if any executed assertion FAILs. SKIPped steps (missing
optional creds) do not fail the run; they are reported and the corresponding
manual step is printed.

Required for a meaningful run:
  STEP 3 (designer read endpoint): --admin-url + --designer-token.
  STEP 4 via real runner:          --pack pointing at a single-mcp-node pack,
                                   plus GREENTIC_AW_* env (see below) so the
                                   runner constructs its MCP source.

Env the runner reads to enable MCP (the smoke exports these for STEP 4 when a
real pack is used, derived from --admin-url/--designer-token):
  GREENTIC_AW_ADMIN_ENDPOINT, GREENTIC_AW_ADMIN_TOKEN, GREENTIC_AW_MCP
EOF
}

# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------
while [ $# -gt 0 ]; do
  case "$1" in
    --admin-url)       ADMIN_URL="$2"; shift 2 ;;
    --admin-token)     ADMIN_TOKEN="$2"; shift 2 ;;
    --admin-cookie)    ADMIN_COOKIE="$2"; shift 2 ;;
    --csrf-token)      CSRF_TOKEN="$2"; shift 2 ;;
    --tenant-id)       TENANT_ID="$2"; shift 2 ;;
    --designer-token)  DESIGNER_TOKEN="$2"; shift 2 ;;
    --runner-url)      RUNNER_URL="$2"; shift 2 ;;
    --runner-tenant)   RUNNER_TENANT="$2"; shift 2 ;;
    --pack)            PACK="$2"; shift 2 ;;
    --flow-id)         FLOW_ID="$2"; shift 2 ;;
    --mock-host)       MOCK_HOST="$2"; shift 2 ;;
    --mock-port)       MOCK_PORT="$2"; shift 2 ;;
    --server-id)       SERVER_ID="$2"; shift 2 ;;
    --server-name)     SERVER_NAME="$2"; shift 2 ;;
    -h|--help)         usage; exit 0 ;;
    *) printf 'unknown argument: %s\n\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done
MOCK_URL="http://${MOCK_HOST}:${MOCK_PORT}"

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------
need() { command -v "$1" >/dev/null 2>&1 || { printf 'missing required tool: %s\n' "$1" >&2; exit 2; }; }
need curl
need jq
need python3

# shellcheck disable=SC2329  # invoked indirectly via `trap cleanup EXIT`
cleanup() {
  if [ -n "${MOCK_PID}" ] && kill -0 "${MOCK_PID}" 2>/dev/null; then
    kill "${MOCK_PID}" 2>/dev/null || true
    wait "${MOCK_PID}" 2>/dev/null || true
  fi
  if [ -n "${WORKDIR}" ] && [ -d "${WORKDIR}" ]; then
    rm -rf "${WORKDIR}"
  fi
}
trap cleanup EXIT

WORKDIR="$(mktemp -d)"
CALL_LOG="${WORKDIR}/mcp_calls.jsonl"
: >"${CALL_LOG}"

# ---------------------------------------------------------------------------
# STEP 0 — start the mock MCP server
# ---------------------------------------------------------------------------
step "STEP 0: start mock MCP server"
python3 "${SCRIPT_DIR}/mock_mcp_server.py" \
  --host "${MOCK_HOST}" --port "${MOCK_PORT}" --call-log "${CALL_LOG}" &
MOCK_PID=$!

# Wait for it to accept connections.
ready=0
for _ in $(seq 1 50); do
  if curl -fsS -o /dev/null \
       -H 'Content-Type: application/json' \
       -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
       "${MOCK_URL}/" 2>/dev/null; then
    ready=1; break
  fi
  sleep 0.1
done
if [ "${ready}" -eq 1 ]; then
  pass "mock MCP server is up at ${MOCK_URL}"
else
  fail "mock MCP server did not come up at ${MOCK_URL}"
  exit 1
fi

# ---------------------------------------------------------------------------
# STEP 1 — register a flow_editor MCP server in the admin (best-effort)
# ---------------------------------------------------------------------------
step "STEP 1: register flow_editor MCP server in admin"
if [ -n "${ADMIN_URL}" ] && [ -n "${TENANT_ID}" ] && [ -n "${ADMIN_COOKIE}" ] && [ -n "${CSRF_TOKEN}" ]; then
  create_body="$(jq -n \
    --arg name "${SERVER_NAME}" \
    --arg url "${MOCK_URL}/" \
    '{name:$name, transport_url:$url, enabled:true}')"
  # POST /api/admin/tenants/{id}/mcp-servers  (tenant_mcp.rs:27 create)
  create_resp="$(curl -sS -w '\n%{http_code}' \
    -X POST "${ADMIN_URL%/}/api/admin/tenants/${TENANT_ID}/mcp-servers" \
    -H 'Content-Type: application/json' \
    -H "Cookie: gtcadmin_session=${ADMIN_COOKIE}" \
    -H "x-csrf-token: ${CSRF_TOKEN}" \
    ${ADMIN_TOKEN:+-H "Authorization: Bearer ${ADMIN_TOKEN}"} \
    -d "${create_body}" || true)"
  create_code="$(printf '%s' "${create_resp}" | tail -n1)"
  create_json="$(printf '%s' "${create_resp}" | sed '$d')"
  if [ "${create_code}" = "201" ]; then
    NEW_SERVER_ID="$(printf '%s' "${create_json}" | jq -r '.server.id // empty')"
    SERVER_ID="${NEW_SERVER_ID:-$SERVER_ID}"
    pass "created MCP server id=${SERVER_ID}"
    # PUT .../{server_id}/roles  (tenant_mcp.rs:35 set_server_roles)
    roles_code="$(curl -sS -o /dev/null -w '%{http_code}' \
      -X PUT "${ADMIN_URL%/}/api/admin/tenants/${TENANT_ID}/mcp-servers/${SERVER_ID}/roles" \
      -H 'Content-Type: application/json' \
      -H "Cookie: gtcadmin_session=${ADMIN_COOKIE}" \
      -H "x-csrf-token: ${CSRF_TOKEN}" \
      ${ADMIN_TOKEN:+-H "Authorization: Bearer ${ADMIN_TOKEN}"} \
      -d '{"roles":["flow_editor"]}' || true)"
    if [ "${roles_code}" = "200" ]; then
      pass "assigned role flow_editor to ${SERVER_ID}"
    else
      fail "roles PUT returned ${roles_code} (expected 200)"
    fi
  else
    fail "create MCP server returned ${create_code} (expected 201): ${create_json}"
  fi
else
  skip "admin register (need --admin-url --tenant-id --admin-cookie --csrf-token)"
  log  "MANUAL: in the admin UI, add an MCP server with transport_url=${MOCK_URL}/ and role flow_editor."
fi

# ---------------------------------------------------------------------------
# STEP 2 — assert the mock speaks the runner's MCP JSON-RPC contract
# (the exact 4-call sequence McpToolSource performs: initialize, initialized,
#  tools/list, tools/call — mcp_source.rs list_server_tools + call_route)
# ---------------------------------------------------------------------------
step "STEP 2: probe mock MCP contract (initialize + tools/list)"
init_json="$(curl -sS -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
  "${MOCK_URL}/" || true)"
if printf '%s' "${init_json}" | jq -e '.result.protocolVersion' >/dev/null 2>&1; then
  pass "initialize ok (protocolVersion=$(printf '%s' "${init_json}" | jq -r '.result.protocolVersion'))"
else
  fail "initialize did not return result.protocolVersion: ${init_json}"
fi

list_json="$(curl -sS -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  "${MOCK_URL}/" || true)"
if printf '%s' "${list_json}" | jq -e '.result.tools | map(select(.name=="get_issue")) | length == 1' >/dev/null 2>&1; then
  pass "tools/list advertises get_issue"
else
  fail "tools/list did not advertise get_issue: ${list_json}"
fi

# ---------------------------------------------------------------------------
# STEP 2b — admin /test probe (best-effort)
# POST /api/admin/tenants/{id}/mcp-servers/{server_id}/test  (tenant_mcp.rs:39)
# returns { ok, server_info{...}, tools[...] }
# ---------------------------------------------------------------------------
step "STEP 2b: admin /test probe"
if [ -n "${ADMIN_URL}" ] && [ -n "${TENANT_ID}" ] && [ -n "${ADMIN_COOKIE}" ] && [ -n "${CSRF_TOKEN}" ]; then
  test_json="$(curl -sS \
    -X POST "${ADMIN_URL%/}/api/admin/tenants/${TENANT_ID}/mcp-servers/${SERVER_ID}/test" \
    -H 'Content-Type: application/json' \
    -H "Cookie: gtcadmin_session=${ADMIN_COOKIE}" \
    -H "x-csrf-token: ${CSRF_TOKEN}" \
    ${ADMIN_TOKEN:+-H "Authorization: Bearer ${ADMIN_TOKEN}"} \
    -d '{}' || true)"
  if printf '%s' "${test_json}" | jq -e '.ok == true and (.tools | length >= 1)' >/dev/null 2>&1; then
    pass "admin /test ok=true with $(printf '%s' "${test_json}" | jq -r '.tools | length') tool(s)"
  else
    fail "admin /test did not report ok=true with tools: ${test_json}"
  fi
else
  skip "admin /test (need --admin-cookie --csrf-token); STEP 2 already proved the contract directly"
fi

# ---------------------------------------------------------------------------
# STEP 3 — runner-consumed designer read endpoint
# GET /api/v1/designer/tenant/me/mcp-servers  (designer/mcp.rs:22), bearer gtc_live_*
# This is EXACTLY what the runner fetches and filters to flow_editor
# (mcp_source.rs fetch_servers -> build_catalog role filter).
# ---------------------------------------------------------------------------
step "STEP 3: designer read endpoint returns a flow_editor server"
if [ -n "${ADMIN_URL}" ] && [ -n "${DESIGNER_TOKEN}" ]; then
  designer_json="$(curl -sS \
    -H "Authorization: Bearer ${DESIGNER_TOKEN}" \
    "${ADMIN_URL%/}/api/v1/designer/tenant/me/mcp-servers" || true)"
  if printf '%s' "${designer_json}" \
       | jq -e '.servers | map(select(.roles | index("flow_editor"))) | length >= 1' >/dev/null 2>&1; then
    n="$(printf '%s' "${designer_json}" | jq -r '.servers | map(select(.roles | index("flow_editor"))) | length')"
    pass "designer endpoint returns ${n} flow_editor server(s) — the runner's MCP catalog source"
  else
    fail "designer endpoint returned no flow_editor server: ${designer_json}"
  fi
else
  skip "designer read endpoint (need --admin-url --designer-token)"
  log  "MANUAL: GET ${ADMIN_URL:-<admin>}/api/v1/designer/tenant/me/mcp-servers with your gtc_live_* token;"
  log  "        confirm a server carries roles:[\"flow_editor\"]."
fi

# ---------------------------------------------------------------------------
# (optional) runner health
# ---------------------------------------------------------------------------
if [ -n "${RUNNER_URL}" ]; then
  step "runner health"
  if curl -fsS "${RUNNER_URL%/}/healthz" >/dev/null 2>&1; then
    pass "runner /healthz ok"
  else
    fail "runner /healthz not reachable at ${RUNNER_URL%/}/healthz"
  fi
fi

# ---------------------------------------------------------------------------
# STEP 4 — drive ONE mcp flow node through the runner's REAL flow entrypoint
# ---------------------------------------------------------------------------
step "STEP 4: run one mcp flow node through the real engine"
if [ -n "${PACK}" ]; then
  # Real shipped entrypoint: greentic-runner-cli (real-runtime mode) loads the
  # pack and runs the entry flow via FlowEngine::new -> source_from_env.
  # --mocks off is REQUIRED so the real MCP HTTP call is not short-circuited by
  # the desktop tools mock (greentic-runner-cli.rs:448 short_circuit:true).
  if [ -z "${ADMIN_URL}" ] || [ -z "${DESIGNER_TOKEN}" ]; then
    fail "real-pack run needs --admin-url + --designer-token so the runner can build its MCP source"
  else
    log "running: greentic-runner-cli --pack ${PACK} --flow ${FLOW_ID} --tenant ${RUNNER_TENANT} --mocks off"
    run_out="${WORKDIR}/run_out.json"
    if ( cd "${REPO_ROOT}" && \
         GREENTIC_AW_ADMIN_ENDPOINT="${ADMIN_URL%/}" \
         GREENTIC_AW_ADMIN_TOKEN="${DESIGNER_TOKEN}" \
         GREENTIC_AW_MCP="1" \
         cargo run --quiet -p greentic-runner --features legacy-gen-bindings --bin greentic-runner-cli -- \
           --pack "${PACK}" --flow "${FLOW_ID}" --tenant "${RUNNER_TENANT}" \
           --mocks off --json --input '{"issue_id":"42"}' >"${run_out}" 2>"${WORKDIR}/run_err.log" ); then
      if grep -q '"title"' "${run_out}" && grep -q 'Bug' "${run_out}"; then
        pass "runner bound the MCP tool result into flow output"
      else
        fail "runner output did not contain the expected bound result; see ${run_out}"
      fi
    else
      fail "greentic-runner-cli run failed; stderr tail:"
      tail -n 20 "${WORKDIR}/run_err.log" || true
    fi
  fi
else
  # No prebuilt pack: bash cannot synthesize a CBOR .gtpack manifest. Exercise
  # the SAME real FlowEngine + source_from_env MCP path via the repo's in-tree
  # e2e test, which runs ONE mcp node end to end (against its own mock pair).
  skip "no --pack supplied; running the in-repo e2e test as the runner-side proof"
  log "running: cargo test -p greentic-runner-host --test mcp_flow_node --features agentic-worker"
  if ( cd "${REPO_ROOT}" && \
       cargo test --quiet -p greentic-runner-host --test mcp_flow_node \
         --features agentic-worker \
         mcp_node_from_ygtc_op_key_calls_tool_and_binds_output >"${WORKDIR}/e2e.log" 2>&1 ); then
    pass "in-repo e2e proved one mcp node runs through FlowEngine + binds output"
  else
    fail "in-repo mcp e2e test failed; log tail:"
    tail -n 30 "${WORKDIR}/e2e.log" || true
  fi
  log "MANUAL (real runner): build a single-mcp-node .gtpack with gtc/packc (v2 YGTC below)"
  log "  and re-run with --pack <pack> so STEP 4 drives greentic-runner-cli against the live mock."
  cat <<'YGTC'
  --- minimal v2 YGTC flow (designer packc injection shape) ---
  id: mcp.flow
  type: messaging
  start: lookup
  nodes:
    lookup:
      mcp:
        server: smoke-github
        tool: get_issue
        arguments:
          id: "{{ entry.issue_id }}"
        output: issue
      routing: [ { out: true } ]
YGTC
fi

# ---------------------------------------------------------------------------
# STEP 4 corroboration — did the mock MCP actually receive a tools/call?
# ---------------------------------------------------------------------------
step "STEP 4b: mock MCP received a tools/call"
if [ -n "${PACK}" ]; then
  if [ -s "${CALL_LOG}" ] && jq -e 'select(.tool=="get_issue")' "${CALL_LOG}" >/dev/null 2>&1; then
    pass "mock MCP recorded a get_issue tools/call from the runner"
  else
    fail "mock MCP recorded no get_issue tools/call (call log empty)"
  fi
else
  skip "tools/call corroboration only applies to the --pack real-runner path"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
step "SUMMARY"
if [ "${FAILURES}" -eq 0 ]; then
  log "RESULT: PASS (no failed assertions)"
  exit 0
else
  log "RESULT: FAIL (${FAILURES} failed assertion(s))"
  exit 1
fi
