//! What the agent runtime keeps about the A2A agents a worker may call.

use std::collections::HashMap;

/// One agent, as the LLM will see it.
///
/// There is no `parameters` here, unlike the flow and MCP entries: an A2A
/// `AgentSkill` carries no input schema, so a schema can only come from the
/// binding's own author contract. This type holds what the CARD can tell us,
/// which is the description.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aToolEntry {
    pub description: String,
}

/// The agents reachable this run, and why the others are not.
#[derive(Clone, Debug, Default)]
pub struct A2aToolCatalog {
    pub(super) tools: HashMap<String, A2aToolEntry>,
    /// `agent_id` → why it has no entry, so a dispatch error can name the real
    /// cause rather than "unknown a2a agent".
    pub(super) errors: HashMap<String, String>,
}

impl A2aToolCatalog {
    pub fn tool_entry(&self, agent_id: &str) -> Option<&A2aToolEntry> {
        self.tools.get(agent_id)
    }

    pub fn error_for(&self, agent_id: &str) -> Option<&str> {
        self.errors.get(agent_id).map(String::as_str)
    }
}
