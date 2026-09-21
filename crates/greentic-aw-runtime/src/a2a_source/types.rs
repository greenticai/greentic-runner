//! What the agent runtime keeps about the A2A agents a worker may call.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Value, json};

use super::source::Transport;

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
    /// The transport that built this catalogue, so it can dispatch a call.
    /// `None` only for a catalogue built without one (tests).
    pub(super) caller: Option<Arc<Transport>>,
}

impl A2aToolCatalog {
    pub fn tool_entry(&self, agent_id: &str) -> Option<&A2aToolEntry> {
        self.tools.get(agent_id)
    }

    pub fn error_for(&self, agent_id: &str) -> Option<&str> {
        self.errors.get(agent_id).map(String::as_str)
    }

    /// Call one agent, always returning a JSON value: `{"reply": <text>}`, or
    /// `{"error": <reason>}` so the LLM observes a failure as a normal tool
    /// result, as the flow and MCP arms do.
    ///
    /// The call is attempted even when this catalogue has no entry for the
    /// agent. The listing advertises such a tool from its author contract, so
    /// that a worker keeps it while an agent is briefly down; refusing it here
    /// would make that fallback useless. Only an agent that was never
    /// configured is refused outright.
    pub async fn dispatch(&self, agent_id: &str, args: &Value) -> Value {
        let Some(caller) = self.caller.as_ref().filter(|c| c.knows(agent_id)) else {
            let reason = self
                .error_for(agent_id)
                .map(|cause| format!("a2a agent {agent_id} is unavailable: {cause}"))
                .unwrap_or_else(|| format!("unknown a2a agent '{agent_id}'"));
            return json!({ "error": reason });
        };
        match caller.call(agent_id, &args_to_text(args)).await {
            Ok(reply) => json!({ "reply": reply }),
            Err(reason) => json!({ "error": reason }),
        }
    }
}

/// The text an A2A agent receives for a tool call's `args`.
///
/// A2A carries a message, while the model sends JSON shaped by the binding's
/// author contract. A bare string is sent as is; an object with a string
/// `message` field sends that field, which is the natural shape for a
/// conversational agent; anything else is sent as its JSON text, so no
/// argument is ever silently dropped.
fn args_to_text(args: &Value) -> String {
    match args {
        Value::String(text) => text.clone(),
        Value::Object(map) => match map.get("message") {
            Some(Value::String(text)) => text.clone(),
            _ => args.to_string(),
        },
        _ => args.to_string(),
    }
}

#[cfg(test)]
mod args_tests {
    use super::args_to_text;
    use serde_json::json;

    #[test]
    fn a_bare_string_is_sent_as_is() {
        assert_eq!(args_to_text(&json!("hello")), "hello");
    }

    #[test]
    fn an_object_with_a_message_field_sends_that_field() {
        assert_eq!(
            args_to_text(&json!({ "message": "hello", "x": 1 })),
            "hello"
        );
    }

    #[test]
    fn anything_else_is_sent_as_json_so_no_argument_is_dropped() {
        assert_eq!(
            args_to_text(&json!({ "city": "Jakarta" })),
            r#"{"city":"Jakarta"}"#
        );
        assert_eq!(args_to_text(&json!({ "message": 42 })), r#"{"message":42}"#);
    }
}
