//! Per-binding sharing modes for the run context (shared context, spec §4.4).
//!
//! A [`SharePolicy`] says, for each agent (keyed by the id `run_step`
//! receives, i.e. the key of the pack's agent map) and each of its tool
//! bindings (keyed by `ToolRef::extension_id`, e.g. `flow:refund`), how much of
//! the caller's run context a nested call may see. Anything absent is
//! [`ShareMode::None`]: sharing is off unless a pack configures it.

use std::collections::HashMap;
use std::sync::Arc;

/// How much of the caller's run context a nested call receives. Ordered from
/// least to most, so `a.min(b)` is the stricter of two modes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ShareMode {
    /// Nothing: the nested call runs with a detached, empty context.
    #[default]
    None,
    /// The nested agent sees the trace view but records nothing.
    Read,
    /// The nested agent sees the view and records its own steps.
    ReadWrite,
}

impl ShareMode {
    /// The wire spelling used by `assets/run-context.json`.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "none" => Some(Self::None),
            "read" => Some(Self::Read),
            "read_write" => Some(Self::ReadWrite),
            _ => None,
        }
    }

    /// The mode a binding may run under, given what was configured for it.
    ///
    /// Only `flow:` and `playbook:` run an agent turn in this process. `a2a:`
    /// is another party (spec §4.1) and every other prefix is a leaf, so they
    /// are `None` whatever the configuration says.
    pub fn for_binding(binding: &str, configured: ShareMode) -> ShareMode {
        if binding.starts_with("flow:") || binding.starts_with("playbook:") {
            configured
        } else {
            ShareMode::None
        }
    }
}

/// One agent's bindings: `extension_id` → mode.
pub type BindingModes = HashMap<String, ShareMode>;

/// Every agent's bindings, as a pack configured them.
#[derive(Clone, Debug, Default)]
pub struct SharePolicy {
    agents: HashMap<String, Arc<BindingModes>>,
}

impl SharePolicy {
    pub fn new(agents: HashMap<String, BindingModes>) -> Self {
        Self {
            agents: agents
                .into_iter()
                .map(|(agent, modes)| (agent, Arc::new(modes)))
                .collect(),
        }
    }

    /// The bindings of `agent_id`, or `None` when the policy names no such agent.
    pub fn for_agent(&self, agent_id: &str) -> Option<Arc<BindingModes>> {
        self.agents.get(agent_id).cloned()
    }

    /// True when at least one of `agent_id`'s bindings can actually share.
    pub fn has_sharing_binding(&self, agent_id: &str) -> bool {
        self.agents.get(agent_id).is_some_and(|modes| {
            modes
                .iter()
                .any(|(binding, mode)| ShareMode::for_binding(binding, *mode) != ShareMode::None)
        })
    }

    pub fn agent_ids(&self) -> impl Iterator<Item = &str> {
        self.agents.keys().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.agents.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn policy(agent: &str, binding: &str, mode: ShareMode) -> SharePolicy {
        let mut modes = BindingModes::new();
        modes.insert(binding.to_string(), mode);
        let mut agents = HashMap::new();
        agents.insert(agent.to_string(), modes);
        SharePolicy::new(agents)
    }

    #[test]
    fn modes_parse_from_their_wire_spelling_only() {
        assert_eq!(ShareMode::parse("none"), Some(ShareMode::None));
        assert_eq!(ShareMode::parse("read"), Some(ShareMode::Read));
        assert_eq!(ShareMode::parse("read_write"), Some(ShareMode::ReadWrite));
        assert_eq!(ShareMode::parse("READ"), None);
        assert_eq!(ShareMode::parse("write"), None);
        assert_eq!(ShareMode::default(), ShareMode::None);
    }

    #[test]
    fn min_is_the_stricter_mode() {
        assert_eq!(ShareMode::ReadWrite.min(ShareMode::Read), ShareMode::Read);
        assert_eq!(ShareMode::Read.min(ShareMode::ReadWrite), ShareMode::Read);
        assert_eq!(ShareMode::Read.min(ShareMode::None), ShareMode::None);
    }

    #[test]
    fn only_flow_and_playbook_bindings_can_share() {
        let rw = ShareMode::ReadWrite;
        assert_eq!(ShareMode::for_binding("flow:refund", rw), rw);
        assert_eq!(ShareMode::for_binding("playbook:p", rw), rw);
        assert_eq!(ShareMode::for_binding("a2a:recipe", rw), ShareMode::None);
        assert_eq!(ShareMode::for_binding("mcp:crm", rw), ShareMode::None);
        assert_eq!(ShareMode::for_binding("component:x", rw), ShareMode::None);
        assert_eq!(
            ShareMode::for_binding("greentic.billing", rw),
            ShareMode::None
        );
    }

    #[test]
    fn has_sharing_binding_ignores_none_and_a2a() {
        assert!(policy("a", "flow:x", ShareMode::Read).has_sharing_binding("a"));
        assert!(!policy("a", "flow:x", ShareMode::None).has_sharing_binding("a"));
        assert!(!policy("a", "a2a:r", ShareMode::ReadWrite).has_sharing_binding("a"));
        assert!(!policy("a", "flow:x", ShareMode::Read).has_sharing_binding("other"));
    }

    #[test]
    fn for_agent_returns_that_agents_bindings_only() {
        let p = policy("a", "flow:x", ShareMode::Read);
        assert_eq!(
            p.for_agent("a").unwrap().get("flow:x"),
            Some(&ShareMode::Read)
        );
        assert!(p.for_agent("b").is_none());
        assert_eq!(p.agent_ids().collect::<Vec<_>>(), vec!["a"]);
        assert!(!p.is_empty());
        assert!(SharePolicy::default().is_empty());
    }
}
