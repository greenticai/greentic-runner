//! A2A (Agent2Agent) v1.0 wire types and agent-card retrieval.
//!
//! The canonical specification is proto-first — `specification/a2a.proto` in
//! `github.com/a2aproject/A2A`, release v1.0.1. The generated `a2a.json` is a
//! non-normative build artifact and is not committed there; where the prose
//! and the proto disagree, the proto is authoritative.
//!
//! **The proto is snake_case and the wire form is camelCase.** Every type here
//! carries `#[serde(rename_all = "camelCase")]` for that reason.
//!
//! This crate is deliberately types plus one HTTP call: no client loop, no
//! server, no projection of skills into LLM tools. Those belong to the
//! consumers, so that a worker calling an agent and a worker exposed as one
//! share these definitions rather than growing two.

pub mod card;
pub mod message;

#[cfg(test)]
pub(crate) mod testutil {
    /// Return the first OBJECT KEY in `value` that is not camelCase.
    ///
    /// Walks keys only. A2A **values** legitimately contain underscores —
    /// `ROLE_USER`, `TASK_STATE_SUBMITTED` — so asserting over the raw
    /// serialised string would fail on correct output.
    pub(crate) fn first_snake_case_key(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    if key.contains('_') {
                        return Some(key.clone());
                    }
                    if let Some(found) = first_snake_case_key(child) {
                        return Some(found);
                    }
                }
                None
            }
            serde_json::Value::Array(items) => items.iter().find_map(first_snake_case_key),
            _ => None,
        }
    }
}
