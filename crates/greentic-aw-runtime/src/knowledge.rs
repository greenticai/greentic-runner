//! Knowledge / RAG (document-corpus) wiring for the agentic-worker runtime.
//!
//! Defines a local [`Knowledge`] seam — ingest pre-chunked document text,
//! hybrid-retrieve ranked chunks — kept deliberately distinct from
//! [`crate::long_term`] (D4): a read-mostly document corpus with auto
//! pre-retrieval, not evolving conversational memory, and from the key-value
//! [`crate::memory::MemoryProvider`] short-term tier.
//!
//! The trait + DTOs mirror the W3 `greentic-dw-knowledge` contract shape exactly,
//! but are defined locally rather than re-exported: the W3 trait crate pulls
//! `greentic-dw-providers-common` (catalog/pack machinery), which would drag a
//! conflicting `greentic-types` and the whole provider stack into this runtime
//! crate. Keeping a thin local seam lets the runtime stay free of graph/provider
//! weight; the concrete Chronicle-backed provider is adapted to this trait at the
//! runner-host edge (W4 4d). When W3 ships a lightweight trait-only crate (as the
//! memory tier does via `greentic-dw-memory-long-term`), this can re-export it.

use std::sync::Arc;

use crate::AgentConfig;
use crate::tenant::TenantContext;
use greentic_types::{EnvId, TenantCtx, TenantId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A pre-chunked unit of document text to ingest into the knowledge corpus.
/// Mirrors `greentic_dw_knowledge::KnowledgeChunk`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeChunk {
    pub doc_id: String,
    pub chunk_index: usize,
    pub text: String,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
    /// Optional pre-computed embedding vector for this chunk. When present,
    /// backends should use this vector directly instead of embedding `text`;
    /// `None` preserves the existing backend-computed-embedding behavior.
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
}

/// Outcome of an ingest: the backend-assigned id for each stored chunk.
/// Mirrors `greentic_dw_knowledge::IngestOutcome`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct IngestOutcome {
    pub chunk_ids: Vec<String>,
}

/// A retrieval query over the tenant's knowledge corpus.
/// Mirrors `greentic_dw_knowledge::KnowledgeQuery`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeQuery {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// A ranked chunk returned from retrieval.
/// Mirrors `greentic_dw_knowledge::RetrievedChunk`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RetrievedChunk {
    pub text: String,
    pub score: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_index: Option<usize>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

/// Errors returned by knowledge operations.
/// Mirrors `greentic_dw_knowledge::KnowledgeError`.
#[derive(Debug, Error)]
pub enum KnowledgeError {
    /// The underlying storage or RAG backend returned an error.
    #[error("knowledge backend error: {0}")]
    Backend(String),
    /// The tenant identifier is invalid or unknown.
    #[error("invalid tenant: {0}")]
    InvalidTenant(String),
    /// No knowledge backend has been configured.
    #[error("knowledge provider not configured")]
    NotConfigured,
}

/// Convenience result alias for knowledge operations.
pub type KnowledgeResult<T> = Result<T, KnowledgeError>;

/// Contract implemented by knowledge (document-RAG) backends. Object-safe so the
/// concrete backend can be injected as `Arc<dyn Knowledge>` at the runner-host
/// edge. Mirrors `greentic_dw_knowledge::Knowledge`.
#[async_trait::async_trait]
pub trait Knowledge: Send + Sync {
    /// Ingest a batch of pre-chunked document text into the tenant's corpus.
    async fn ingest(
        &self,
        tenant: &TenantCtx,
        chunks: Vec<KnowledgeChunk>,
    ) -> KnowledgeResult<IngestOutcome>;

    /// Retrieve chunks relevant to `query`, ranked by relevance descending and
    /// bounded by `query.limit`.
    async fn search(
        &self,
        tenant: &TenantCtx,
        query: KnowledgeQuery,
    ) -> KnowledgeResult<Vec<RetrievedChunk>>;

    /// Retrieval with the agent's per-turn knowledge binding in hand.
    ///
    /// [`Self::search`] mirrors the `greentic-dw-knowledge` contract and must
    /// keep mirroring it, so the provider binding cannot be carried inside
    /// [`KnowledgeQuery`]. But a backend whose target is chosen PER WORKER —
    /// "delegate retrieval to extension X's tool Y" — cannot read its own ids
    /// from a runtime-level field: [`crate::AgentRuntime::knowledge`] is set
    /// once when the runtime is built, while the binding arrives per turn in
    /// [`AgentConfig`]. The out-of-process serve path serves many agents from
    /// one runtime, so mounting per-runtime from config would hand one worker
    /// another worker's provider.
    ///
    /// The default delegates to [`Self::search`], so a backend that ignores the
    /// binding (an env-configured Chronicle corpus, say) needs no change.
    async fn search_bound(
        &self,
        tenant: &TenantCtx,
        query: KnowledgeQuery,
        _binding: Option<&crate::config::MemoryProviderRef>,
    ) -> KnowledgeResult<Vec<RetrievedChunk>> {
        self.search(tenant, query).await
    }

    /// The backend this one delegates to, when it is a WRAPPER rather than a
    /// leaf. `None` for every backend that retrieves by itself.
    ///
    /// [`crate::AgentRuntime::with_knowledge`] replaces the mounted backend, so
    /// a host that mounts two must have the second wrap the first. Once one of
    /// those mounts is unconditional — as the runner-host's extension adapter
    /// is, because which extension to invoke is a per-turn decision it cannot
    /// make at build time — [`crate::AgentRuntime::has_knowledge`] is true on
    /// that path whether or not a corpus backend was ever mounted. It therefore
    /// stops being able to answer "did the corpus mount run", which is a
    /// question a host regression guard has to keep asking: a corpus mount that
    /// silently stopped running is what made the model hallucinate instead of
    /// retrieving. This answers it.
    fn wrapped_backend(&self) -> Option<Arc<dyn Knowledge>> {
        None
    }

    /// A stable identifier for this backend IMPLEMENTATION, so a host walking a
    /// wrapper chain can tell one layer from another.
    ///
    /// Never for dispatch — nothing in this crate reads it. It exists because
    /// [`Self::wrapped_backend`] returning `Some` says only "something is
    /// underneath", which a second delegating wrapper satisfies exactly as well
    /// as a corpus backend does; a host asking "did my corpus mount actually
    /// run" needs to tell those apart.
    fn backend_id(&self) -> &'static str {
        "unidentified"
    }
}

/// Convert the runtime's [`TenantContext`] into the `greentic-types`
/// [`TenantCtx`] expected by [`Knowledge`]. Validation failures (an id that
/// doesn't satisfy the shared tenant/env format) map to
/// [`KnowledgeError::InvalidTenant`] rather than panicking.
pub(crate) fn to_types_tenant(ctx: &TenantContext) -> Result<TenantCtx, KnowledgeError> {
    let env = EnvId::try_from(ctx.env_id.as_str())
        .map_err(|e| KnowledgeError::InvalidTenant(format!("env_id '{}': {e}", ctx.env_id)))?;
    let tenant = TenantId::try_from(ctx.tenant_id.as_str()).map_err(|e| {
        KnowledgeError::InvalidTenant(format!("tenant_id '{}': {e}", ctx.tenant_id))
    })?;
    Ok(TenantCtx::new(env, tenant))
}

/// Number of chunks auto-retrieved and injected each turn, read from the agent's
/// knowledge binding (falls back to the default, clamped to at least 1 so a stray
/// `0` never disables retrieval silently).
pub(crate) fn auto_top_k(config: &AgentConfig) -> usize {
    config
        .knowledge
        .as_ref()
        .map(|k| k.top_k)
        .unwrap_or_else(crate::config::default_knowledge_top_k)
        .max(1)
}

/// Whether the knowledge tier is active for this turn: a backend is wired AND the
/// agent's config carries an enabled knowledge provider binding.
pub(crate) fn knowledge_active(has_provider: bool, config: &AgentConfig) -> bool {
    has_provider
        && config
            .knowledge
            .as_ref()
            .and_then(|k| k.knowledge.as_ref())
            .is_some()
}

/// Build the system prompt for a turn: the base prompt followed by a delimited
/// `<knowledge>` block listing the retrieved chunks. Returns the base prompt
/// unchanged when there are no chunks (no empty block).
pub(crate) fn augment_system_prompt(base: &str, chunks: &[RetrievedChunk]) -> String {
    if chunks.is_empty() {
        return base.to_string();
    }
    let mut out = String::with_capacity(base.len() + 128 * chunks.len());
    out.push_str(base);
    out.push_str("\n\n<knowledge>\nRelevant passages retrieved from the agent's knowledge base:\n");
    for c in chunks {
        out.push_str("- ");
        out.push_str(c.text.trim());
        out.push('\n');
    }
    out.push_str("</knowledge>");
    out
}

/// Surface an auto knowledge retrieval as a trace step, so the UI can show which
/// corpus chunks were pulled into context — doc id, chunk index, score, and text.
///
/// Emitted through the tool-call observer seam ONLY, and deliberately NOT pushed
/// onto `AgentOutput.trail`. Retrieval is automatic pre-context, not a
/// model-invoked tool: it belongs in the live trace the test-chat UI streams, but
/// must stay out of the flow trail / metering, whose consumers treat each entry
/// as a real agent action. A no-op when nothing was retrieved (no empty step).
///
/// The synthetic `call_id` is per-call so the UI pairs this result with its own
/// call and never a neighbouring tool's. `doc`/`index` ride through as JSON `null`
/// when the backend did not supply them (`RetrievedChunk`'s `Option` fields).
pub(crate) fn emit_retrieval_trace(
    observer: &dyn crate::StepObserver,
    query: &str,
    chunks: &[RetrievedChunk],
) {
    if chunks.is_empty() {
        return;
    }
    const NAME: &str = "search_knowledge";
    let call_id = uuid::Uuid::new_v4().to_string();
    observer.on_tool_call(NAME, &call_id, &serde_json::json!({ "query": query }));

    let hits: Vec<serde_json::Value> = chunks
        .iter()
        .map(|c| {
            serde_json::json!({
                "doc": c.doc_id,
                "index": c.chunk_index,
                "score": c.score,
                "text": c.text,
            })
        })
        .collect();
    observer.on_tool_result(
        NAME,
        &call_id,
        &serde_json::json!({ "chunks": hits, "count": chunks.len() }),
    );
}

/// Surface a FAILED auto knowledge retrieval as a trace step.
///
/// The counterpart to [`emit_retrieval_trace`], and the reason it exists: a
/// retrieval that errors degrades to no injection, and until this existed that
/// degrade was indistinguishable from "the corpus held nothing relevant" —
/// same empty prompt, same confident answer, no trace step either way. The
/// operator watching test-chat could not tell a dead backend from an empty one.
///
/// Emitted through the same observer seam and with the same synthetic
/// `call_id` discipline. The result payload carries `"error"` rather than
/// `"chunks"`, which is what makes the two cases distinguishable downstream.
pub(crate) fn emit_retrieval_failure_trace(
    observer: &dyn crate::StepObserver,
    query: &str,
    error: &KnowledgeError,
) {
    const NAME: &str = "search_knowledge";
    let call_id = uuid::Uuid::new_v4().to_string();
    observer.on_tool_call(NAME, &call_id, &serde_json::json!({ "query": query }));
    observer.on_tool_result(
        NAME,
        &call_id,
        &serde_json::json!({ "error": error.to_string(), "count": 0 }),
    );
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
        assert_eq!(resolved.tenant.as_str(), "acme");
        assert_eq!(resolved.env.as_str(), "dev");
    }

    #[test]
    fn to_types_tenant_rejects_empty_tenant() {
        let ctx = TenantContext::new("", "dev");
        let err = to_types_tenant(&ctx).expect_err("empty tenant id is invalid");
        assert!(matches!(err, KnowledgeError::InvalidTenant(_)));
    }

    fn cfg_with_knowledge(top_k: Option<usize>, with_binding: bool) -> AgentConfig {
        use crate::config::{KnowledgeSettings, MemoryProviderRef};
        let binding = with_binding.then(|| MemoryProviderRef {
            provider: "provider.knowledge.chronicle".into(),
            capability: "cap://dw.knowledge".into(),
            params: serde_json::Map::new(),
            credential_ref: None,
        });
        AgentConfig {
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
            knowledge: Some(KnowledgeSettings {
                knowledge: binding,
                embedding: None,
                top_k: top_k.unwrap_or_else(crate::config::default_knowledge_top_k),
            }),
            guardrails: vec![],
            conversational: false,
            opening_message: None,
        }
    }

    #[test]
    fn knowledge_active_requires_provider_and_enabled_binding() {
        let cfg = cfg_with_knowledge(None, true);
        assert!(knowledge_active(true, &cfg));
        assert!(!knowledge_active(false, &cfg));

        // Binding present but no knowledge provider → inactive.
        let cfg_no_binding = cfg_with_knowledge(None, false);
        assert!(!knowledge_active(true, &cfg_no_binding));

        // No knowledge settings at all → inactive.
        let mut bare = cfg_with_knowledge(None, true);
        bare.knowledge = None;
        assert!(!knowledge_active(true, &bare));
    }

    #[test]
    fn auto_top_k_uses_config_then_default_clamped() {
        assert_eq!(auto_top_k(&cfg_with_knowledge(Some(3), true)), 3);
        assert_eq!(auto_top_k(&cfg_with_knowledge(None, true)), 5);
        // A stray 0 clamps up to 1 rather than disabling retrieval.
        assert_eq!(auto_top_k(&cfg_with_knowledge(Some(0), true)), 1);
        let mut bare = cfg_with_knowledge(None, true);
        bare.knowledge = None;
        assert_eq!(auto_top_k(&bare), 5);
    }

    fn chunk(text: &str, score: f64) -> RetrievedChunk {
        RetrievedChunk {
            text: text.into(),
            score,
            doc_id: None,
            chunk_index: None,
            metadata: serde_json::Map::new(),
        }
    }

    #[test]
    fn augment_with_chunks_wraps_a_block() {
        let chunks = vec![
            chunk("Refunds are processed within 5 business days.", 0.9),
            chunk("Premium plans include priority support.", 0.7),
        ];
        let out = augment_system_prompt("base prompt", &chunks);
        assert!(out.starts_with("base prompt"));
        assert!(out.contains("<knowledge>"));
        assert!(out.contains("</knowledge>"));
        assert!(out.contains("Refunds are processed within 5 business days."));
        assert!(out.contains("Premium plans include priority support."));
    }

    #[test]
    fn augment_with_no_chunks_returns_base_unchanged() {
        let out = augment_system_prompt("base prompt", &[]);
        assert_eq!(out, "base prompt");
    }

    // A minimal in-crate `Knowledge` to prove the trait + tenant conversion line
    // up end-to-end without a graph backend (the real backend is Chronicle doc-RAG,
    // injected at the runner-host edge and exercised in integration tests).
    struct StubKnowledge;

    #[async_trait::async_trait]
    impl Knowledge for StubKnowledge {
        async fn ingest(
            &self,
            _tenant: &TenantCtx,
            chunks: Vec<KnowledgeChunk>,
        ) -> KnowledgeResult<IngestOutcome> {
            Ok(IngestOutcome {
                chunk_ids: chunks
                    .iter()
                    .map(|c| format!("{}#{}", c.doc_id, c.chunk_index))
                    .collect(),
            })
        }

        async fn search(
            &self,
            _tenant: &TenantCtx,
            query: KnowledgeQuery,
        ) -> KnowledgeResult<Vec<RetrievedChunk>> {
            Ok(vec![chunk(&format!("retrieved for: {}", query.query), 1.0)])
        }
    }

    #[tokio::test]
    async fn trait_object_drives_ingest_and_search_through_converted_tenant() {
        let kb: Arc<dyn Knowledge> = Arc::new(StubKnowledge);
        let ctx = to_types_tenant(&TenantContext::new("acme", "dev")).unwrap();

        let outcome = kb
            .ingest(
                &ctx,
                vec![KnowledgeChunk {
                    doc_id: "faq".into(),
                    chunk_index: 0,
                    text: "Refunds within 5 days.".into(),
                    metadata: serde_json::Map::new(),
                    embedding: None,
                }],
            )
            .await
            .unwrap();
        assert_eq!(outcome.chunk_ids, vec!["faq#0".to_string()]);

        let hits = kb
            .search(
                &ctx,
                KnowledgeQuery {
                    query: "refund policy".into(),
                    limit: Some(3),
                },
            )
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].text, "retrieved for: refund policy");
    }

    #[derive(Default)]
    struct RecordingObserver {
        // (event, tool_name, payload) per observer call.
        calls: std::sync::Mutex<Vec<(&'static str, String, serde_json::Value)>>,
    }
    impl crate::StepObserver for RecordingObserver {
        fn on_tool_call(&self, name: &str, _call_id: &str, args: &serde_json::Value) {
            self.calls
                .lock()
                .unwrap()
                .push(("call", name.to_string(), args.clone()));
        }
        fn on_tool_result(&self, name: &str, _call_id: &str, result: &serde_json::Value) {
            self.calls
                .lock()
                .unwrap()
                .push(("result", name.to_string(), result.clone()));
        }
    }

    #[test]
    fn emit_retrieval_trace_reports_chunk_fields_as_a_tool_step() {
        let rec = RecordingObserver::default();
        let chunks = vec![RetrievedChunk {
            text: "Agentic worker adalah komponen...".into(),
            score: 0.89,
            doc_id: Some("greentic.pdf".into()),
            chunk_index: Some(3),
            metadata: serde_json::Map::new(),
        }];
        emit_retrieval_trace(&rec, "Apa itu agentic?", &chunks);

        let calls = rec.calls.lock().unwrap();
        assert_eq!(calls.len(), 2, "one on_tool_call + one on_tool_result");
        assert_eq!(calls[0].0, "call");
        assert_eq!(calls[0].1, "search_knowledge");
        assert_eq!(calls[0].2["query"], "Apa itu agentic?");

        assert_eq!(calls[1].0, "result");
        assert_eq!(calls[1].2["count"], 1);
        let hit = &calls[1].2["chunks"][0];
        assert_eq!(hit["doc"], "greentic.pdf");
        assert_eq!(hit["index"], 3);
        assert_eq!(hit["score"], 0.89);
        assert_eq!(hit["text"], "Agentic worker adalah komponen...");
    }

    /// Backends that leave `doc_id`/`chunk_index` unset must not break the step —
    /// they ride through as JSON `null`, which the UI tolerates.
    #[test]
    fn emit_retrieval_trace_carries_null_for_missing_doc_and_index() {
        let rec = RecordingObserver::default();
        emit_retrieval_trace(&rec, "q", &[chunk("no metadata", 0.5)]);

        let calls = rec.calls.lock().unwrap();
        let hit = &calls[1].2["chunks"][0];
        assert!(hit["doc"].is_null());
        assert!(hit["index"].is_null());
        assert_eq!(hit["score"], 0.5);
    }

    #[test]
    fn emit_retrieval_trace_is_noop_without_chunks() {
        let rec = RecordingObserver::default();
        emit_retrieval_trace(&rec, "q", &[]);
        assert!(
            rec.calls.lock().unwrap().is_empty(),
            "no chunks retrieved => no trace step at all"
        );
    }

    /// The whole point of the failure trace: an operator watching the live
    /// trace must be able to tell "the corpus held nothing relevant" from "the
    /// backend did not answer". Those were the same empty prompt before.
    #[test]
    fn a_failed_retrieval_is_distinguishable_from_an_empty_one() {
        let empty = RecordingObserver::default();
        emit_retrieval_trace(&empty, "q", &[]);
        assert!(empty.calls.lock().unwrap().is_empty());

        let failed = RecordingObserver::default();
        emit_retrieval_failure_trace(
            &failed,
            "q",
            &KnowledgeError::Backend("service refused".into()),
        );
        let calls = failed.calls.lock().unwrap();
        assert_eq!(calls.len(), 2, "one on_tool_call + one on_tool_result");
        assert_eq!(calls[0].1, "search_knowledge");
        assert_eq!(calls[0].2["query"], "q");
        assert_eq!(calls[1].2["count"], 0);
        assert!(
            calls[1].2["error"]
                .as_str()
                .is_some_and(|e| e.contains("service refused")),
            "the result must name the failure, not merely report zero chunks: {:?}",
            calls[1].2
        );
        assert!(
            calls[1].2.get("chunks").is_none(),
            "a failure must not look like a successful retrieval of nothing"
        );
    }

    /// A backend that ignores the binding — every backend that existed before
    /// this seam — must keep working through the default method.
    #[tokio::test]
    async fn search_bound_defaults_to_search_for_a_binding_agnostic_backend() {
        let kb: Arc<dyn Knowledge> = Arc::new(StubKnowledge);
        let ctx = to_types_tenant(&TenantContext::new("acme", "dev")).unwrap();
        let binding = crate::config::MemoryProviderRef {
            provider: "provider.knowledge.chronicle".into(),
            capability: "cap://dw.knowledge".into(),
            params: serde_json::Map::new(),
            credential_ref: None,
        };
        let hits = kb
            .search_bound(
                &ctx,
                KnowledgeQuery {
                    query: "refund policy".into(),
                    limit: Some(3),
                },
                Some(&binding),
            )
            .await
            .unwrap();
        assert_eq!(hits[0].text, "retrieved for: refund policy");
    }
}
