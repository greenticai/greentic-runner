//! What the agent runtime keeps about the A2A agents a worker may call.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Value, json};

use super::source::Transport;

/// One A2A agent a worker may call, as the pack sidecar
/// (`assets/a2a-routes.json`, cross-repo contract 2026-09-21 §4) describes it.
///
/// Carries NO credential. `requires_auth` only says one is stored; the token
/// itself is read from the secrets store at call time (§5), so a rotation
/// takes effect without a restart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct A2aRoute {
    /// The admin row id; the `a2a:<agent_id>` tool ref and the secret name.
    pub agent_id: String,
    /// Where the agent card lives (`<base_url>/.well-known/agent-card.json`).
    pub base_url: String,
    /// Header the token travels in. `None` means `Authorization: Bearer`.
    pub auth_header_name: Option<String>,
    /// Team slug the credential is sealed under. `None` means `_`.
    pub auth_team: Option<String>,
    /// A token is stored and must be sent. A route with this set and no
    /// resolvable token is refused, never called unauthenticated.
    pub requires_auth: bool,
}

impl A2aRoute {
    /// A route with no credential, which is what [`super::A2aToolSource::new`]
    /// builds from a bare `(agent_id, base_url)` pair.
    pub fn unauthenticated(agent_id: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
            base_url: base_url.into(),
            auth_header_name: None,
            auth_team: None,
            requires_auth: false,
        }
    }
}

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
    /// A catalogue with no transport, for tests of code that only reads it:
    /// `entries` are configured agents whose card was fetched, `down` are
    /// configured agents whose card fetch failed.
    #[cfg(test)]
    pub(crate) fn for_tests(entries: &[(&str, &str)], down: &[&str]) -> Self {
        Self {
            tools: entries
                .iter()
                .map(|(id, description)| {
                    (
                        (*id).to_string(),
                        A2aToolEntry {
                            description: (*description).to_string(),
                        },
                    )
                })
                .collect(),
            errors: down
                .iter()
                .map(|id| ((*id).to_string(), "card unreachable (test)".to_string()))
                .collect(),
            caller: None,
        }
    }

    /// Whether `agent_id` is one of the agents this catalogue was built for.
    ///
    /// Every configured agent lands in exactly one of `tools` (card fetched)
    /// or `errors` (fetch failed), so this needs no field of its own and
    /// cannot drift from them. The listing, the preflight check and dispatch
    /// all gate on it, so they agree on which `a2a:` refs exist at all.
    pub fn is_configured(&self, agent_id: &str) -> bool {
        self.tools.contains_key(agent_id) || self.errors.contains_key(agent_id)
    }

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
    /// The call is attempted for any configured agent, including one whose
    /// card could not be fetched when this catalogue was built — the listing
    /// advertises such a tool from its author contract. The call re-fetches
    /// the card, so it succeeds only if the agent has recovered since; while
    /// the card is still down, the call fails with that cause. An agent that
    /// was never configured is refused outright.
    pub async fn dispatch(&self, agent_id: &str, args: &Value) -> Value {
        let caller = self
            .caller
            .as_ref()
            .filter(|_| self.is_configured(agent_id));
        let Some(caller) = caller else {
            let reason = self
                .error_for(agent_id)
                .map(|cause| format!("a2a agent {agent_id} is unavailable: {cause}"))
                .unwrap_or_else(|| format!("unknown a2a agent '{agent_id}'"));
            return json!({ "error": reason });
        };
        match caller.call(agent_id, &args_to_text(args)).await {
            Ok(reply) => json!({ "reply": reply }),
            Err(reason) => {
                // The model is the only reader of the `{"error"}` value; this
                // line is what lets an operator see the failure at all. The
                // reason never carries the token (see `auth`).
                tracing::warn!(agent = %agent_id, error = %reason, "a2a tool call failed");
                json!({ "error": reason })
            }
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
