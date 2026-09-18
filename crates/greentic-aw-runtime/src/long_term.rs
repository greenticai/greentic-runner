//! Long-term (episodic) memory wiring for the agentic-worker runtime.
//!
//! The long-term tier uses the episodic [`LongTermMemory`] contract from
//! `greentic-dw-memory-long-term` (ingest episodes, semantic recall of facts) —
//! a deliberately different shape from the key-value
//! [`crate::memory::MemoryProvider`] seam, which suits the short-term/working
//! tier. The concrete backend (Chronicle / graphiti over a graph store) is
//! injected at the runner-host edge; this crate depends only on the lightweight
//! trait so it stays buildable without graph infrastructure.

use crate::AgentConfig;
use crate::tenant::TenantContext;
use greentic_types::{EnvId, TenantCtx, TenantId};

pub use greentic_dw_memory_long_term::{
    EpisodeIngest, EpisodeSource, IngestOutcome, LongTermMemory, LongTermMemoryError, RecallQuery,
    RecalledFact,
};

/// Convert the runtime's [`TenantContext`] into the `greentic-types`
/// [`TenantCtx`] expected by [`LongTermMemory`]. Validation failures (an id that
/// doesn't satisfy the shared tenant/env format) map to
/// [`LongTermMemoryError::InvalidTenant`] rather than panicking.
pub(crate) fn to_types_tenant(ctx: &TenantContext) -> Result<TenantCtx, LongTermMemoryError> {
    let env = EnvId::try_from(ctx.env_id.as_str())
        .map_err(|e| LongTermMemoryError::InvalidTenant(format!("env_id '{}': {e}", ctx.env_id)))?;
    let tenant = TenantId::try_from(ctx.tenant_id.as_str()).map_err(|e| {
        LongTermMemoryError::InvalidTenant(format!("tenant_id '{}': {e}", ctx.tenant_id))
    })?;
    Ok(TenantCtx::new(env, tenant))
}

/// Default number of facts auto-injected into the prompt each turn.
pub(crate) const AUTO_INJECT_K: usize = 5;

/// Whether the long-term tier is active for this turn: a backend is wired AND
/// the agent's config enables the long-term memory binding.
pub(crate) fn long_term_active(has_provider: bool, config: &AgentConfig) -> bool {
    has_provider
        && config
            .memory
            .as_ref()
            .and_then(|m| m.long_term.as_ref())
            .is_some()
}

/// Build the system prompt for a turn: the base prompt followed by a delimited
/// `<long_term_memory>` block listing the recalled facts. Returns the base
/// prompt unchanged when there are no facts (no empty block).
pub(crate) fn augment_system_prompt(base: &str, facts: &[RecalledFact]) -> String {
    if facts.is_empty() {
        return base.to_string();
    }
    let mut out = String::with_capacity(base.len() + 64 * facts.len());
    out.push_str(base);
    out.push_str("\n\n<long_term_memory>\nRelevant facts recalled from earlier interactions:\n");
    for f in facts {
        out.push_str("- ");
        out.push_str(&f.fact);
        if !f.relation.is_empty() {
            out.push_str(" (");
            out.push_str(&f.relation);
            out.push(')');
        }
        if let Some(valid) = f.valid_at {
            out.push_str(" [valid ");
            out.push_str(&valid.to_rfc3339());
            out.push(']');
        }
        out.push('\n');
    }
    out.push_str("</long_term_memory>");
    out
}

/// Reserved extension id for the host-provided built-in tools (distinct from
/// any WASM extension id).
pub(crate) const RECALL_MEMORY_EXTENSION_ID: &str = "host";
/// Reserved tool name for the host built-in long-term recall tool.
pub(crate) const RECALL_MEMORY_TOOL: &str = "recall_memory";
/// Max facts returned by an agentic `recall_memory` tool call.
pub(crate) const TOOL_LIMIT: usize = 10;

/// LLM-facing schema for the host built-in `recall_memory` tool. Advertised
/// only when the long-term tier is active.
pub(crate) fn recall_memory_tool_schema() -> crate::llm::LlmToolSchema {
    crate::llm::LlmToolSchema {
        extension_id: RECALL_MEMORY_EXTENSION_ID.to_string(),
        tool_name: RECALL_MEMORY_TOOL.to_string(),
        description: "Search the agent's long-term memory for facts relevant to a query. \
             Use when you need to recall something from earlier interactions."
            .to_string(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "query": { "type": "string", "description": "Natural-language search query." },
                "limit": { "type": "integer", "description": "Max facts to return (default 10)." }
            },
            "required": ["query"]
        }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn to_types_tenant_maps_valid_ids() {
        let ctx = TenantContext::new("acme", "dev");
        let resolved = to_types_tenant(&ctx).expect("valid tenant converts");
        // Round-trips back to the same string ids.
        assert_eq!(resolved.tenant.as_str(), "acme");
        assert_eq!(resolved.env.as_str(), "dev");
    }

    #[test]
    fn to_types_tenant_rejects_empty_tenant() {
        let ctx = TenantContext::new("", "dev");
        let err = to_types_tenant(&ctx).expect_err("empty tenant id is invalid");
        assert!(matches!(err, LongTermMemoryError::InvalidTenant(_)));
    }

    // A minimal in-crate `LongTermMemory` to prove the trait + tenant conversion
    // line up end-to-end without a graph backend (the real backend is Chronicle,
    // injected at the runner-host edge and exercised in integration tests).
    struct CountingMemory;

    #[async_trait::async_trait]
    impl LongTermMemory for CountingMemory {
        async fn ingest_episode(
            &self,
            _tenant: &TenantCtx,
            episode: EpisodeIngest,
        ) -> Result<IngestOutcome, LongTermMemoryError> {
            Ok(IngestOutcome {
                episode_id: format!("ep-{}", episode.name),
                fact_count: 1,
                entity_count: 1,
            })
        }

        async fn recall(
            &self,
            _tenant: &TenantCtx,
            query: RecallQuery,
        ) -> Result<Vec<RecalledFact>, LongTermMemoryError> {
            Ok(vec![RecalledFact {
                fact: format!("recalled for: {}", query.query),
                relation: "about".into(),
                valid_at: None,
                invalid_at: None,
                source_episode_ids: vec![],
            }])
        }
    }

    #[tokio::test]
    async fn trait_object_drives_ingest_and_recall_through_converted_tenant() {
        let memory: Arc<dyn LongTermMemory> = Arc::new(CountingMemory);
        let ctx = to_types_tenant(&TenantContext::new("acme", "dev")).unwrap();

        let outcome = memory
            .ingest_episode(
                &ctx,
                EpisodeIngest {
                    name: "turn-1".into(),
                    body: "Alice prefers dark mode".into(),
                    source: EpisodeSource::Message,
                    source_description: None,
                    reference_time: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome.episode_id, "ep-turn-1");

        let facts = memory
            .recall(
                &ctx,
                RecallQuery {
                    query: "preferences".into(),
                    limit: Some(3),
                },
            )
            .await
            .unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].fact, "recalled for: preferences");
    }

    fn fact(text: &str, relation: &str) -> RecalledFact {
        RecalledFact {
            fact: text.into(),
            relation: relation.into(),
            valid_at: None,
            invalid_at: None,
            source_episode_ids: vec![],
        }
    }

    #[test]
    fn augment_with_facts_wraps_a_block() {
        let facts = vec![
            fact("Alice prefers dark mode", "prefers"),
            fact("Bob works at Acme", "works_at"),
        ];
        let out = augment_system_prompt("base prompt", &facts);
        assert!(out.starts_with("base prompt"));
        assert!(out.contains("<long_term_memory>"));
        assert!(out.contains("</long_term_memory>"));
        assert!(out.contains("Alice prefers dark mode"));
        assert!(out.contains("Bob works at Acme"));
    }

    #[test]
    fn augment_with_no_facts_returns_base_unchanged() {
        let out = augment_system_prompt("base prompt", &[]);
        assert_eq!(out, "base prompt");
    }

    #[test]
    fn long_term_active_requires_provider_and_enabled_binding() {
        use crate::config::{MemoryProviderRef, MemorySettings};
        let mut cfg = AgentConfig {
            agent_id: "a".into(),
            system_prompt: "s".into(),
            tools: vec![],
            llm: crate::LlmProviderRef {
                provider: "m".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: crate::AgentLimits::default(),
            memory: None,
            knowledge: None,
            guardrails: vec![],
            conversational: false,
            opening_message: None,
        };
        assert!(!long_term_active(false, &cfg));
        assert!(!long_term_active(true, &cfg));
        cfg.memory = Some(MemorySettings {
            short_term: None,
            long_term: Some(MemoryProviderRef {
                provider: "provider.memory.chronicle".into(),
                capability: "cap://memory/long-term".into(),
                params: serde_json::Map::new(),
                credential_ref: None,
            }),
        });
        assert!(long_term_active(true, &cfg));
        assert!(!long_term_active(false, &cfg));
    }

    #[test]
    fn recall_memory_schema_shape() {
        let s = recall_memory_tool_schema();
        assert_eq!(s.tool_name, RECALL_MEMORY_TOOL);
        assert_eq!(s.extension_id, RECALL_MEMORY_EXTENSION_ID);
        assert_eq!(s.parameters["required"][0], "query");
        assert!(s.parameters["properties"]["query"].is_object());
    }
}
