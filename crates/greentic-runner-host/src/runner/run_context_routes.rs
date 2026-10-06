//! Per-binding sharing modes carried by a `.gtpack` (shared context, Phase A2).
//!
//! The designer will write `assets/run-context.json` (Phase D):
//!
//! ```json
//! { "<agent_id>": { "<extension_id>": "none" | "read" | "read_write" } }
//! ```
//!
//! `<agent_id>` is the key of the pack's agent map, the id `run_step`
//! receives (for a `dw.agent` node, its operation). Twin of
//! [`super::a2a_pack_routes`] and as lenient: malformed JSON reads as ABSENT
//! with a warning, a non-object agent entry or an unknown mode drops that
//! entry with a warning and keeps the rest. Only `flow:<id>` and
//! `playbook:<id>` bindings (non-empty id) are kept: an `a2a:` binding is
//! ignored (another party never receives the run context) and so is any other
//! prefix. Absent means every binding is `none`. This module is ungated; it
//! carries the wire spelling only, and `run_context_policy` turns it into the
//! agent runtime's types.

use std::collections::BTreeMap;

/// The pack entry name the designer's writer and this reader agree on.
pub const RUN_CONTEXT_ENTRY: &str = "assets/run-context.json";

const MODES: [&str; 3] = ["none", "read", "read_write"];

/// True for `flow:<id>` / `playbook:<id>` with a non-empty id.
fn is_shareable_binding(binding: &str) -> bool {
    ["flow:", "playbook:"]
        .iter()
        .any(|p| binding.strip_prefix(p).is_some_and(|id| !id.is_empty()))
}

/// Every agent's bindings, with mode values already validated.
#[derive(Debug, Clone, Default)]
pub struct PackRunContext {
    agents: BTreeMap<String, BTreeMap<String, String>>,
}

impl PackRunContext {
    /// Parse the sidecar from its own bytes (extracted by
    /// `PackRuntime::read_pack_file`). `None` when it is not a JSON object.
    pub fn from_sidecar_bytes(bytes: &[u8]) -> Option<Self> {
        let raw: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(bytes)
            .inspect_err(|e| tracing::warn!(error = %e, "run-context: malformed; ignoring sidecar"))
            .ok()?;
        let mut agents = BTreeMap::new();
        for (agent_id, bindings) in raw {
            let Some(bindings) = bindings.as_object() else {
                tracing::warn!(agent = %agent_id, "run-context: agent entry is not an object; dropped");
                continue;
            };
            let mut modes = BTreeMap::new();
            for (binding, mode) in bindings {
                if binding.starts_with("a2a:") {
                    tracing::warn!(agent = %agent_id, binding = %binding, "run-context: a2a bindings never share; ignored");
                    continue;
                }
                if !is_shareable_binding(binding) {
                    tracing::warn!(
                        agent = %agent_id,
                        binding = %binding,
                        "run-context: only flow:<id> and playbook:<id> bindings can share; ignored"
                    );
                    continue;
                }
                match mode.as_str().filter(|m| MODES.contains(m)) {
                    Some(m) => {
                        modes.insert(binding.clone(), m.to_string());
                    }
                    None => tracing::warn!(
                        agent = %agent_id,
                        binding = %binding,
                        "run-context: unknown mode (expected none, read or read_write); binding dropped"
                    ),
                }
            }
            agents.insert(agent_id, modes);
        }
        Some(Self { agents })
    }

    /// Every agent and its validated `binding -> mode` map, sorted by agent id.
    pub fn agents(&self) -> impl Iterator<Item = (&String, &BTreeMap<String, String>)> {
        self.agents.iter()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn a_valid_sidecar_reads_every_binding() {
        let rc = PackRunContext::from_sidecar_bytes(
            br#"{"a":{"flow:x":"read","playbook:p":"read_write","flow:y":"none"}}"#,
        )
        .unwrap();
        let (agent, bindings) = rc.agents().next().unwrap();
        assert_eq!(agent, "a");
        assert_eq!(bindings.get("flow:x").map(String::as_str), Some("read"));
        assert_eq!(
            bindings.get("playbook:p").map(String::as_str),
            Some("read_write")
        );
        assert_eq!(bindings.get("flow:y").map(String::as_str), Some("none"));
    }

    #[test]
    fn a_bad_mode_drops_only_that_binding() {
        let rc = PackRunContext::from_sidecar_bytes(
            br#"{"a":{"flow:x":"READ","flow:y":"read","flow:z":3}}"#,
        )
        .unwrap();
        let (_, bindings) = rc.agents().next().unwrap();
        assert_eq!(bindings.len(), 1);
        assert!(bindings.contains_key("flow:y"));
    }

    #[test]
    fn a2a_bindings_are_ignored() {
        let rc = PackRunContext::from_sidecar_bytes(br#"{"a":{"a2a:r":"read_write"}}"#).unwrap();
        let (_, bindings) = rc.agents().next().unwrap();
        assert!(bindings.is_empty());
    }

    #[test]
    fn only_flow_and_playbook_bindings_with_an_id_are_kept() {
        let rc = PackRunContext::from_sidecar_bytes(
            br#"{"a":{"flow:":"read","playbook:":"read","mcp:s":"read","bare":"read","flow:ok":"read"}}"#,
        )
        .unwrap();
        let (_, bindings) = rc.agents().next().unwrap();
        assert_eq!(bindings.len(), 1);
        assert!(bindings.contains_key("flow:ok"));
    }

    #[test]
    fn a_malformed_sidecar_reads_as_absent() {
        assert!(PackRunContext::from_sidecar_bytes(b"[1,2]").is_none());
        assert!(PackRunContext::from_sidecar_bytes(b"not json").is_none());
        let rc =
            PackRunContext::from_sidecar_bytes(br#"{"a":"read","b":{"flow:x":"read"}}"#).unwrap();
        assert_eq!(
            rc.agents().count(),
            1,
            "a non-object agent entry is dropped"
        );
    }
}
