//! Tests for the per-tenant agentic-worker MCP tool catalog.
//!
//! Split by concern:
//! - `catalog` — catalog-building, role filtering, TTL cache tests
//! - `dispatch` — dispatch, parse_rows, and local-wasm integration tests
//! - `pack_routes` — the pack-backed source (no admin fetch, no probe)

mod catalog;
mod dispatch;
mod pack_routes;
