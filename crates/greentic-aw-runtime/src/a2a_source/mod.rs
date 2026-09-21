//! A2A (Agent2Agent) tool source: lets an agentic worker call an external A2A
//! agent as a tool.
//!
//! # The shape decision
//!
//! This follows [`crate::mcp_source`], not [`crate::flow_source`]: an A2A
//! agent is plain HTTP(S), reachable directly from this crate (which already
//! depends on `reqwest`), so this module owns its own transport rather than
//! taking an injected invoker. `flow_source` injects because a flow can only
//! be run by the pack runtime in `runner-host` — there is no such split here.
//!
//! # The precedence decision
//!
//! [`A2aToolEntry`] deliberately carries no `parameters` field. An A2A
//! `AgentSkill` has no input schema at all, so unlike the `mcp:` catalogue
//! (which resolves both description AND schema from a live probe), a schema
//! for an `a2a:` binding can only come from the author's own contract. The
//! description still prefers the live card — it is the agent's own current
//! statement about itself — with the author contract as the fallback that
//! keeps a worker running while an agent is temporarily unreachable.
//!
//! # Module layout
//!
//! - `types` — [`A2aToolEntry`], [`A2aToolCatalog`]
//! - `source` — [`A2aToolSource`]: card fetch, catalog build, `SendMessage`
//!   dispatch
//! - `tests` — unit + integration tests (cfg(test) only)

mod source;
mod types;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;

// --- Public re-exports (stable API surface) ---

pub use source::A2aToolSource;
pub use types::{A2aToolCatalog, A2aToolEntry};
