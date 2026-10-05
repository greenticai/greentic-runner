//! What a `flow:` tool's per-call flow engine borrows from the host.
//!
//! `PackRuntime::run_flow_for_tool*` / `resume_flow_for_tool` build a FRESH
//! `FlowEngine` per call (`PackRuntime::load_flow_engine`), and
//! `FlowEngine::new` wires no node handler. Only the top-level engine is wired
//! (`runtime.rs`, the desktop host), so before this module a `dw.agent` node
//! inside a flow tool failed with "no AgentNodeHandler configured".
//!
//! The host registers its handler on every `PackRuntime` it built the agent
//! runtime over. The handler is held WEAKLY: the handler owns the agent
//! runtime, whose flow tool source owns the invoker, which owns these same
//! `Arc<PackRuntime>`s — a strong reference here would be a cycle. When the
//! host has dropped its engine (a revision swap) the upgrade fails and the
//! nested node fails loudly, exactly as before.

use std::sync::{Arc, Weak};

use crate::runner::agent_node::AgentNodeHandler;
use crate::runner::engine::FlowEngine;

/// Handlers a `flow:` tool's engine borrows from the host that wired the
/// top-level engine. Only the `dw.agent` handler today; the graph and
/// operala handlers are deliberate follow-ups.
#[derive(Clone, Default)]
pub struct NestedFlowHandlers {
    #[cfg_attr(not(feature = "agentic-worker"), allow(dead_code))]
    agent: Option<Weak<dyn AgentNodeHandler>>,
}

impl NestedFlowHandlers {
    /// Lend `handler` to flow-tool engines, without keeping it alive.
    #[must_use]
    pub fn with_agent(mut self, handler: &Arc<dyn AgentNodeHandler>) -> Self {
        self.agent = Some(Arc::downgrade(handler));
        self
    }

    /// Install every handler that is still alive on `engine`.
    pub(crate) fn install_on(&self, engine: &mut FlowEngine) {
        #[cfg(feature = "agentic-worker")]
        if let Some(handler) = self.agent.as_ref().and_then(Weak::upgrade) {
            engine.set_agent_node_handler(handler);
        }
        #[cfg(not(feature = "agentic-worker"))]
        let _ = engine;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    struct Noop;
    #[async_trait::async_trait]
    impl AgentNodeHandler for Noop {
        async fn execute(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: &str,
            _: &serde_json::Value,
            _: bool,
            _: Option<&serde_json::Value>,
        ) -> anyhow::Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
    }

    /// The cycle guard: lending a handler adds no strong reference.
    #[test]
    fn a_lent_handler_is_held_weakly() {
        let handler: Arc<dyn AgentNodeHandler> = Arc::new(Noop);
        let lent = NestedFlowHandlers::default().with_agent(&handler);
        assert_eq!(Arc::strong_count(&handler), 1);
        assert_eq!(Arc::weak_count(&handler), 1);
        drop(handler);
        assert!(lent.agent.as_ref().and_then(Weak::upgrade).is_none());
    }
}
