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
//! - `types` — [`A2aRoute`], [`A2aToolEntry`], [`A2aToolCatalog`]
//! - `auth` — the per-call credential: §5 URI candidates via
//!   [`crate::scoped_secrets`], header rule, same-host rule, missing-credential refusal
//! - `continuation` — [`A2aContinuation`]: the remote `contextId`/`taskId` a
//!   Greentic conversation holds, so a follow-up resumes the same remote task
//! - `outcome` — how a remote task's state is rendered for the worker's own
//!   model, including the rule that only an answer carries a `reply`
//! - `source` — [`A2aToolSource`]: card fetch, catalog build, `SendMessage`
//!   dispatch
//! - `tests` — unit + integration tests (cfg(test) only)

mod auth;
mod continuation;
mod outcome;
mod source;
mod types;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;

// --- Public re-exports (stable API surface) ---

pub use continuation::{A2aContinuation, A2aContinuations, CONTINUATION_IDLE_TTL_SECS};
pub use outcome::call_error_value;
pub use source::A2aToolSource;
pub use types::{A2aRoute, A2aToolCatalog, A2aToolEntry};
