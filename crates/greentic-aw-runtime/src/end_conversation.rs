//! Host built-in `end_conversation` tool for conversational agents (SP1).
//!
//! Mirrors the `short_term`/`long_term` host-tool pattern: a reserved `"host"`
//! extension id + an LLM-facing schema, advertised only when the agent is
//! `conversational`. When the model calls it, the Plan-Act-Observe loop
//! (`crate::r#loop`) terminates the turn with
//! `TerminationReason::ConversationEnded`.

use crate::config::AgentConfig;
use crate::llm::LlmToolSchema;

/// Reserved extension id for host-provided built-in tools (shared with memory).
pub(crate) const HOST_EXTENSION_ID: &str = "host";
/// Host built-in tool: end the current conversation segment.
pub(crate) const END_CONVERSATION_TOOL: &str = "end_conversation";

/// The tool is offered only to conversational agents.
pub(crate) fn conversational_active(config: &AgentConfig) -> bool {
    config.conversational
}

/// LLM-facing schema for the host built-in `end_conversation` tool.
pub(crate) fn end_conversation_tool_schema() -> LlmToolSchema {
    LlmToolSchema {
        extension_id: HOST_EXTENSION_ID.to_string(),
        tool_name: END_CONVERSATION_TOOL.to_string(),
        description: "End the current conversation when it has reached a natural \
            conclusion — the user's goal is met, they say goodbye, or there is \
            nothing left to do. Optionally include a brief closing message."
            .to_string(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "final_message": {
                    "type": "string",
                    "description": "Optional short closing message shown to the user."
                }
            }
        }),
    }
}

/// Append the conversational system-prompt note so the model knows the tool
/// exists. Applied only for conversational agents.
pub(crate) fn augment_system_prompt(base: &str) -> String {
    format!(
        "{base}\n\nWhen the conversation has reached a natural end — the user's goal \
         is met, they say goodbye, or there is nothing left to do — call the \
         `end_conversation` tool with a brief `final_message`. Do not call it while \
         the user still needs help. If a tool or backend system fails or is \
         unavailable, do NOT end the conversation: clearly tell the user which \
         capability failed and that it looks like a configuration or availability \
         problem, then stay available so they can respond or retry. A tool failure \
         is a blocker to surface, not a reason to say goodbye."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(conversational: bool) -> AgentConfig {
        AgentConfig {
            agent_id: "a".into(),
            system_prompt: "sys".into(),
            tools: vec![],
            guardrails: vec![],
            llm: crate::config::LlmProviderRef {
                provider: "openai".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: crate::config::AgentLimits::default(),
            memory: None,
            knowledge: None,
            conversational,
            opening_message: None,
        }
    }

    #[test]
    fn active_only_when_conversational() {
        assert!(conversational_active(&cfg(true)));
        assert!(!conversational_active(&cfg(false)));
    }

    #[test]
    fn schema_shape() {
        let s = end_conversation_tool_schema();
        assert_eq!(s.extension_id, HOST_EXTENSION_ID);
        assert_eq!(s.tool_name, END_CONVERSATION_TOOL);
        // final_message is optional: no "required" array (or it omits final_message).
        let required = s
            .parameters
            .get("required")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
            .unwrap_or_default();
        assert!(!required.contains(&"final_message"));
        assert!(s.parameters["properties"].get("final_message").is_some());
    }

    #[test]
    fn augment_appends_note() {
        let out = augment_system_prompt("BASE");
        assert!(out.starts_with("BASE"));
        assert!(out.contains("end_conversation"));
    }

    #[test]
    fn augment_warns_against_ending_on_tool_failure() {
        let out = augment_system_prompt("BASE");
        // The note must steer the model to keep the conversation open (surface
        // the blocker) rather than say goodbye when a tool/backend fails.
        assert!(out.contains("do NOT end the conversation"));
        assert!(out.contains("blocker"));
    }
}
