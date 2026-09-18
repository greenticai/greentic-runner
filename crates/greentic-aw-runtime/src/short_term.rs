//! Short-term ("working") memory tool surface for the agentic-worker loop.
//!
//! Mirrors [`crate::long_term`]'s host-tool pattern: a reserved `"host"`
//! extension id with built-in `remember`/`recall` tools, advertised + dispatched
//! only when the short-term tier is active. The backend is a
//! [`crate::memory::MemoryProvider`] wired via
//! [`crate::AgentRuntime::with_short_term_memory`].

use crate::config::AgentConfig;
use crate::llm::LlmToolSchema;

/// Reserved extension id for host-provided built-in tools (shared with long-term).
pub(crate) const SHORT_TERM_EXTENSION_ID: &str = "host";
/// Host built-in tool: store a key/value into short-term memory.
pub(crate) const REMEMBER_TOOL: &str = "remember";
/// Host built-in tool: read a value back from short-term memory by key.
pub(crate) const RECALL_TOOL: &str = "recall";

/// The short-term tier is active when a provider is attached AND the agent's
/// config opts in via `memory.short_term`. Mirrors [`crate::long_term::long_term_active`].
pub(crate) fn short_term_active(has_provider: bool, config: &AgentConfig) -> bool {
    has_provider
        && config
            .memory
            .as_ref()
            .and_then(|m| m.short_term.as_ref())
            .is_some()
}

/// LLM-facing schema for the host built-in `remember` tool.
pub(crate) fn remember_tool_schema() -> LlmToolSchema {
    LlmToolSchema {
        extension_id: SHORT_TERM_EXTENSION_ID.to_string(),
        tool_name: REMEMBER_TOOL.to_string(),
        description: "Store a short-term (working-memory) value under a key for this \
             conversation. Use to remember things the user tells you within this session."
            .to_string(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "key": { "type": "string", "description": "Identifier to store the value under." },
                "value": { "type": "string", "description": "The value to remember." }
            },
            "required": ["key", "value"]
        }),
    }
}

/// LLM-facing schema for the host built-in `recall` tool.
pub(crate) fn recall_tool_schema() -> LlmToolSchema {
    LlmToolSchema {
        extension_id: SHORT_TERM_EXTENSION_ID.to_string(),
        tool_name: RECALL_TOOL.to_string(),
        description: "Read a short-term (working-memory) value back by the key it was \
             stored under in this conversation."
            .to_string(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "key": { "type": "string", "description": "Identifier the value was stored under." }
            },
            "required": ["key"]
        }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::{MemoryProviderRef, MemorySettings};

    fn cfg_with_short_term(present: bool) -> AgentConfig {
        let mut c = AgentConfig {
            agent_id: "test-agent".into(),
            system_prompt: "You are helpful.".into(),
            tools: vec![],
            llm: crate::LlmProviderRef {
                provider: "openai".into(),
                model: "gpt-4o-mini".into(),
                credential_ref: None,
            },
            limits: crate::AgentLimits::default(),
            memory: None,
            knowledge: None,
            guardrails: vec![],
            conversational: false,
            opening_message: None,
        };
        if present {
            c.memory = Some(MemorySettings {
                short_term: Some(MemoryProviderRef {
                    provider: "in-memory".into(),
                    capability: "cap://memory/short-term".into(),
                    params: serde_json::Map::new(),
                    credential_ref: None,
                }),
                long_term: None,
            });
        }
        c
    }

    #[test]
    fn active_only_when_provider_and_config_present() {
        assert!(short_term_active(true, &cfg_with_short_term(true)));
        assert!(!short_term_active(false, &cfg_with_short_term(true)));
        assert!(!short_term_active(true, &cfg_with_short_term(false)));
    }

    #[test]
    fn remember_schema_shape() {
        let s = remember_tool_schema();
        assert_eq!(s.tool_name, REMEMBER_TOOL);
        assert_eq!(s.extension_id, SHORT_TERM_EXTENSION_ID);
        let req = s
            .parameters
            .get("required")
            .and_then(|v| v.as_array())
            .unwrap();
        let names: Vec<&str> = req.iter().filter_map(|v| v.as_str()).collect();
        assert!(names.contains(&"key") && names.contains(&"value"));
    }

    #[test]
    fn recall_schema_shape() {
        let s = recall_tool_schema();
        assert_eq!(s.tool_name, RECALL_TOOL);
        assert_eq!(s.extension_id, SHORT_TERM_EXTENSION_ID);
        let req = s
            .parameters
            .get("required")
            .and_then(|v| v.as_array())
            .unwrap();
        let names: Vec<&str> = req.iter().filter_map(|v| v.as_str()).collect();
        assert!(names.contains(&"key"));
    }
}
