//! Build the agent runtime's [`SharePolicy`] from the packs'
//! `assets/run-context.json` sidecars (shared context, Phase A2). Twin of
//! `a2a_pack_source`, in its own file because `agent_node.rs` is already over
//! 4000 lines.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use greentic_aw_runtime::{BindingModes, ShareMode, SharePolicy};

/// The sharing policy for one runtime, or `None` when no pack carries a
/// sidecar. `packs` is one revision's pack list; the first pack naming an
/// agent wins, as for A2A routes. An agent key outside `known_agents` is
/// warned about: it is almost always a key written under the wrong id (the
/// display name instead of the agent-map key), which shares nothing silently.
pub(crate) fn share_policy_from_packs<'a>(
    packs: &[Arc<crate::pack::PackRuntime>],
    known_agents: impl IntoIterator<Item = &'a str>,
) -> Option<Arc<SharePolicy>> {
    let known: HashSet<&str> = known_agents.into_iter().collect();
    let mut agents: HashMap<String, BindingModes> = HashMap::new();
    for pack in packs {
        let Some(declared) = pack.run_context() else {
            continue;
        };
        for (agent_id, bindings) in declared.agents() {
            if agents.contains_key(agent_id) {
                continue;
            }
            if !known.contains(agent_id.as_str()) {
                tracing::warn!(
                    agent = %agent_id,
                    "run-context: sidecar names an agent this runtime does not carry; \
                     its bindings will never match a step"
                );
            }
            let modes: BindingModes = bindings
                .iter()
                .filter_map(|(binding, mode)| ShareMode::parse(mode).map(|m| (binding.clone(), m)))
                .collect();
            agents.insert(agent_id.clone(), modes);
        }
    }
    if agents.is_empty() {
        return None;
    }
    tracing::info!(
        agents = agents.len(),
        "run-context sharing policy constructed"
    );
    Some(Arc::new(SharePolicy::new(agents)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use greentic_aw_runtime::ShareMode;

    fn pack_with(sidecar: Option<&str>) -> (tempfile::TempDir, Arc<crate::pack::PackRuntime>) {
        let dir = tempfile::tempdir().unwrap();
        if let Some(body) = sidecar {
            std::fs::create_dir_all(dir.path().join("assets")).unwrap();
            std::fs::write(dir.path().join("assets/run-context.json"), body).unwrap();
        }
        let pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(dir.path()));
        (dir, pack)
    }

    #[test]
    fn no_sidecar_means_no_policy() {
        let (_d, pack) = pack_with(None);
        assert!(share_policy_from_packs(&[pack], ["a"]).is_none());
    }

    #[test]
    fn the_first_pack_naming_an_agent_wins() {
        let (_d1, p1) = pack_with(Some(r#"{"a":{"flow:x":"read"}}"#));
        let (_d2, p2) = pack_with(Some(r#"{"a":{"flow:x":"read_write"}}"#));
        let policy = share_policy_from_packs(&[p1, p2], ["a"]).unwrap();
        assert_eq!(
            policy.for_agent("a").unwrap().get("flow:x"),
            Some(&ShareMode::Read)
        );
        assert!(policy.has_sharing_binding("a"));
    }

    #[test]
    fn an_unknown_agent_key_is_warned_and_kept_inert() {
        let (_d, pack) = pack_with(Some(r#"{"Support Bot":{"flow:x":"read"}}"#));
        let policy = share_policy_from_packs(&[pack], ["support-bot"]).unwrap();
        assert!(
            !policy.has_sharing_binding("support-bot"),
            "a mismatched key shares nothing"
        );
    }
}
