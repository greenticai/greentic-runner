//! Which flows of a `.gtpack` keep a conversation history (opt-in per flow).
//!
//! The designer writes `assets/conversation-history.json`:
//!
//! ```json
//! { "<flow_id>": { "turns": 10 } }
//! ```
//!
//! `true` is accepted as shorthand for the default (the cap). A flow that is
//! absent, or whose value is `false` or unreadable, keeps no history: the
//! feature is OFF unless a pack asks for it, because a transcript is end-user
//! text and a flow must choose to hold it.
//!
//! Lenient like [`super::run_context_routes`]: malformed JSON reads as ABSENT
//! with a warning, and one bad entry is dropped without costing the rest.

use std::collections::BTreeMap;

use serde_json::Value;

/// The pack entry name the designer's writer and this reader agree on.
pub const HISTORY_ENTRY: &str = "assets/conversation-history.json";

/// Most turns a flow may keep. A turn is one user message and the reply to it.
pub const MAX_TURNS: usize = 10;

/// The flows of one pack that keep a history, with how many turns each keeps.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackConversationHistory {
    flows: BTreeMap<String, usize>,
}

impl PackConversationHistory {
    /// Parse the sidecar from its own bytes. `None` when it is not a JSON
    /// object.
    pub fn from_sidecar_bytes(bytes: &[u8]) -> Option<Self> {
        let raw: serde_json::Map<String, Value> = serde_json::from_slice(bytes)
            .inspect_err(
                |e| tracing::warn!(error = %e, "conversation-history: malformed; ignoring sidecar"),
            )
            .ok()?;
        let mut flows = BTreeMap::new();
        for (flow_id, setting) in raw {
            if flow_id.trim().is_empty() {
                tracing::warn!("conversation-history: blank flow id; dropped");
                continue;
            }
            match turns_from(&setting) {
                Some(turns) => {
                    flows.insert(flow_id, turns);
                }
                None => tracing::warn!(
                    flow = %flow_id,
                    "conversation-history: expected true or {{\"turns\": n}}; flow keeps no history"
                ),
            }
        }
        Some(Self { flows })
    }

    /// How many turns `flow_id` keeps, or `None` when it keeps no history.
    pub fn turns_for(&self, flow_id: &str) -> Option<usize> {
        self.flows.get(flow_id).copied()
    }
}

/// `true` is the cap; `{"turns": n}` is `n` clamped to `1..=MAX_TURNS`
/// (a missing `turns` is the cap); anything else, `false` included, is off.
fn turns_from(setting: &Value) -> Option<usize> {
    match setting {
        Value::Bool(true) => Some(MAX_TURNS),
        Value::Object(map) => match map.get("turns") {
            None => Some(MAX_TURNS),
            Some(Value::Number(n)) => n
                .as_u64()
                .map(|n| usize::try_from(n).unwrap_or(MAX_TURNS).clamp(1, MAX_TURNS)),
            Some(_) => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> PackConversationHistory {
        PackConversationHistory::from_sidecar_bytes(json.as_bytes()).expect("object")
    }

    #[test]
    fn a_flow_is_off_unless_the_pack_names_it() {
        let history = parse(r#"{"support": true}"#);
        assert_eq!(history.turns_for("support"), Some(MAX_TURNS));
        assert_eq!(history.turns_for("other"), None);
    }

    #[test]
    fn turns_are_clamped_to_the_cap_and_at_least_one() {
        let history =
            parse(r#"{"a": {"turns": 3}, "b": {"turns": 99}, "c": {"turns": 0}, "d": {}}"#);
        assert_eq!(history.turns_for("a"), Some(3));
        assert_eq!(history.turns_for("b"), Some(MAX_TURNS));
        assert_eq!(history.turns_for("c"), Some(1));
        assert_eq!(history.turns_for("d"), Some(MAX_TURNS));
    }

    #[test]
    fn false_and_unreadable_settings_keep_no_history_and_cost_nothing_else() {
        let history = parse(r#"{"a": false, "b": "yes", "c": {"turns": "ten"}, "ok": true}"#);
        assert_eq!(history.turns_for("a"), None);
        assert_eq!(history.turns_for("b"), None);
        assert_eq!(history.turns_for("c"), None);
        assert_eq!(history.turns_for("ok"), Some(MAX_TURNS));
    }

    #[test]
    fn malformed_json_reads_as_absent() {
        assert!(PackConversationHistory::from_sidecar_bytes(b"not json").is_none());
        assert!(PackConversationHistory::from_sidecar_bytes(b"[1]").is_none());
    }
}
