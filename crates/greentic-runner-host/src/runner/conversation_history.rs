//! A flow's conversation history, exposed to its templates as `{{history}}`.
//!
//! Every new message enters a flow at its entry, so a deterministic step sees
//! only the current turn. A flow that opts in (see
//! [`super::conversation_history_routes`]) gets the last few turns of its
//! session as a list of `{role, text}` entries, oldest first, where `role` is
//! `user` or `assistant`.
//!
//! The history lives in the pack's state store under its own prefix, scoped by
//! tenant, env, flow and session, and idle-expires. It is NOT under the prefix
//! the `state.get` / `state.set` nodes use, for the reason the A2A continuation
//! gives: a flow author owns every key in that namespace.
//!
//! Every failure degrades to "no history". The cost of a lost read or write is
//! a turn without context; failing the turn would punish the person for a
//! housekeeping fault.

use greentic_types::{StateKey, TenantCtx};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::runner::conversation_history_routes::MAX_TURNS;
use crate::storage::DynStateStore;

/// Namespace of the stored history, apart from the flow-state nodes'.
pub const HISTORY_PREFIX: &str = "runner-history";

/// Longest text kept per entry, in characters. Replies can be long and the
/// whole list is rendered into whatever template reads it.
pub const MAX_TEXT_CHARS: usize = 2000;

/// How long an idle session's history is kept.
pub const IDLE_TTL_SECS: u32 = 24 * 60 * 60;

/// One message of the conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub role: String,
    pub text: String,
}

fn key(flow_id: &str, session_id: &str) -> StateKey {
    StateKey::new(format!("history/{flow_id}/{session_id}"))
}

/// Read a session's history. Empty on every failure.
pub fn load(
    store: &DynStateStore,
    ctx: &TenantCtx,
    flow_id: &str,
    session_id: &str,
) -> Vec<HistoryEntry> {
    match store.get_json(ctx, HISTORY_PREFIX, &key(flow_id, session_id), None) {
        Ok(Some(value)) => serde_json::from_value(value).unwrap_or_else(|error| {
            tracing::warn!(%error, "conversation history could not be decoded; starting empty");
            Vec::new()
        }),
        Ok(None) => Vec::new(),
        Err(error) => {
            tracing::warn!(%error, "conversation history could not be read; starting empty");
            Vec::new()
        }
    }
}

/// Write a session's history back. A failure is a warning and nothing else.
pub fn save(
    store: &DynStateStore,
    ctx: &TenantCtx,
    flow_id: &str,
    session_id: &str,
    entries: &[HistoryEntry],
) {
    let value = match serde_json::to_value(entries) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, "conversation history could not be serialised");
            return;
        }
    };
    if let Err(error) = store.set_json(
        ctx,
        HISTORY_PREFIX,
        &key(flow_id, session_id),
        None,
        &value,
        Some(IDLE_TTL_SECS),
    ) {
        tracing::warn!(%error, "conversation history could not be persisted; the next turn has less context");
    }
}

/// The history as the template value: a list of `{role, text}`.
pub fn as_value(entries: &[HistoryEntry]) -> Value {
    Value::Array(
        entries
            .iter()
            .map(|e| json!({ "role": e.role, "text": e.text }))
            .collect(),
    )
}

fn clean(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.chars().take(MAX_TEXT_CHARS).collect())
}

/// Add one turn to `prior` and keep the newest `turns` turns (two entries
/// each). A side with no text adds no entry, so a card submit that carried no
/// sentence still records the reply.
pub fn with_turn(
    mut prior: Vec<HistoryEntry>,
    user: Option<&str>,
    assistant: Option<&str>,
    turns: usize,
) -> Vec<HistoryEntry> {
    for (role, text) in [("user", user), ("assistant", assistant)] {
        if let Some(text) = text.and_then(clean) {
            prior.push(HistoryEntry {
                role: role.to_string(),
                text,
            });
        }
    }
    let keep = turns.clamp(1, MAX_TURNS) * 2;
    if prior.len() > keep {
        prior.drain(..prior.len() - keep);
    }
    prior
}

/// What the person typed this turn, read from the activity the same way the
/// routing context reads it.
pub fn user_text(input: &Value) -> Option<String> {
    input
        .pointer("/input/text")
        .or_else(|| input.pointer("/text"))
        .and_then(Value::as_str)
        .and_then(clean)
}

/// The reply text of one turn, from the flow's output (one payload, or the
/// list a turn that emitted several things produces). Best effort: an item
/// with no recognisable text, a card included, contributes nothing.
pub fn assistant_text(output: &Value) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    match output {
        Value::Array(items) => items.iter().for_each(|i| collect(i, &mut parts)),
        other => collect(other, &mut parts),
    }
    clean(&parts.join("\n"))
}

fn collect(item: &Value, parts: &mut Vec<String>) {
    let text = ["/text", "/reply", "/response/reply", "/payload/text"]
        .iter()
        .find_map(|p| item.pointer(p).and_then(Value::as_str))
        .and_then(clean);
    if let Some(text) = text {
        parts.push(text);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_state::inmemory::InMemoryStateStore;
    use greentic_types::{EnvId, TenantId};
    use std::str::FromStr;
    use std::sync::Arc;

    fn entry(role: &str, text: &str) -> HistoryEntry {
        HistoryEntry {
            role: role.into(),
            text: text.into(),
        }
    }

    #[test]
    fn a_turn_adds_the_user_then_the_assistant() {
        let h = with_turn(Vec::new(), Some("hi"), Some("hello"), 10);
        assert_eq!(h, vec![entry("user", "hi"), entry("assistant", "hello")]);
    }

    #[test]
    fn a_side_with_no_text_adds_no_entry() {
        let h = with_turn(Vec::new(), None, Some("a reply"), 10);
        assert_eq!(h, vec![entry("assistant", "a reply")]);
        assert!(with_turn(Vec::new(), Some("  "), Some(""), 10).is_empty());
    }

    #[test]
    fn only_the_newest_turns_are_kept() {
        let mut h = Vec::new();
        for n in 0..14 {
            h = with_turn(h, Some(&format!("q{n}")), Some(&format!("a{n}")), 10);
        }
        assert_eq!(h.len(), 20);
        assert_eq!(h.first().unwrap().text, "q4");
        assert_eq!(h.last().unwrap().text, "a13");
        let three = with_turn(h, Some("q14"), Some("a14"), 3);
        assert_eq!(three.len(), 6);
        assert_eq!(three.first().unwrap().text, "q12");
    }

    #[test]
    fn a_long_text_is_cut_to_the_cap() {
        let long = "x".repeat(MAX_TEXT_CHARS + 500);
        let h = with_turn(Vec::new(), Some(&long), None, 10);
        assert_eq!(h[0].text.chars().count(), MAX_TEXT_CHARS);
    }

    #[test]
    fn the_user_text_is_read_from_either_delivery_shape() {
        assert_eq!(user_text(&json!({"text": "a"})), Some("a".into()));
        assert_eq!(
            user_text(&json!({"input": {"text": "b"}})),
            Some("b".into())
        );
        assert_eq!(user_text(&json!({"input": {"metadata": {}}})), None);
    }

    #[test]
    fn the_assistant_text_is_joined_from_every_item_that_has_one() {
        let output = json!([{"text": "one"}, {"card": {}}, {"reply": "two"}]);
        assert_eq!(assistant_text(&output), Some("one\ntwo".into()));
        assert_eq!(
            assistant_text(&json!({"response": {"reply": "r"}})),
            Some("r".into())
        );
        assert_eq!(assistant_text(&Value::Null), None);
    }

    #[test]
    fn history_round_trips_through_the_store_and_is_scoped_by_flow_and_session() {
        let store: DynStateStore = Arc::new(InMemoryStateStore::new());
        let ctx = TenantCtx::new(
            EnvId::from_str("local").unwrap(),
            TenantId::from_str("acme").unwrap(),
        );
        let h = with_turn(Vec::new(), Some("hi"), Some("hello"), 10);
        save(&store, &ctx, "support", "s1", &h);
        assert_eq!(load(&store, &ctx, "support", "s1"), h);
        assert!(load(&store, &ctx, "support", "s2").is_empty());
        assert!(load(&store, &ctx, "other", "s1").is_empty());
    }

    #[test]
    fn the_value_is_a_list_of_role_and_text() {
        let v = as_value(&[entry("user", "hi")]);
        assert_eq!(v, json!([{"role": "user", "text": "hi"}]));
    }
}
