//! Per-tenant agentic-worker MCP tool catalog.
//!
//! Builds a per-tenant snapshot of MCP tools by pulling the tenant's enabled
//! MCP servers from the admin (`/api/v1/designer/tenant/me/mcp-servers`),
//! keeping only the servers that carry the `agentic_worker` role, then probing
//! each (connect → `initialize` → `tools/list`) and recording per-tool schemas
//! plus the dispatch route. The snapshot is cached per tenant behind a short
//! TTL so a stable admin config avoids re-hitting the remote servers on every
//! step.
//!
//! Resilience contract (MCP must never break an agent step):
//! - A network or non-200 admin response degrades to an EMPTY (cached) catalog
//!   with a `warn` — [`McpToolSource::catalog`] is infallible by contract and
//!   never returns or propagates an error.
//! - A server that times out or errors during the probe is skipped with a
//!   `warn`; the catalog still returns with the servers that worked.
//! - [`dispatch_route`] always returns a JSON [`serde_json::Value`] and never
//!   panics — on any failure it returns `{"error": "..."}`.
//!
//! Unlike the designer (which namespaces tools as `mcp__server__tool`
//! strings), the runner keys every tool by a `(server_id, raw_tool_name)`
//! tuple — no string mangling. Task 2 consumes [`McpToolEntry`] to build an
//! `LlmToolSchema`, so each entry stores the raw `description` + JSON-Schema
//! `parameters`.
//!
//! # Module layout
//!
//! - `types` — wire types, domain types, [`McpToolCatalog`]
//! - `source` — [`McpToolSource`], transport helpers, [`dispatch_route`]
//! - `tests` — unit + integration tests (cfg(test) only)

mod source;
mod types;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;

// --- Public re-exports (stable API surface) ---

pub use source::{McpToolSource, dispatch_route};
pub use types::{
    MCP_ROLE_AGENTIC_WORKER, MCP_ROLE_FLOW_EDITOR, McpCallerIdentity, McpPackRoute, McpRoute,
    McpToolCatalog, McpToolEntry, Transport,
};

#[cfg(test)]
pub(crate) use types::route_for_tests;
