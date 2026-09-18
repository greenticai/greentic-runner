//! Knowledge retrieval delegated to a design-extension tool.
//!
//! The runner-host edge of a knowledge provider whose corpus lives OUTSIDE the
//! platform: the operator binds a provider that names an installed `.gtxpack`
//! extension and one of its tools, and every turn's auto-retrieval calls that
//! tool instead of a corpus this process holds. What the tool then does — which
//! service it talks to, over which protocol, with which credential — is the
//! extension's business and is deliberately invisible here. That is what makes
//! this a one-time change rather than one adapter per customer, so nothing in
//! this file may grow a mention of a specific transport or service.
//!
//! ## Why the ids are read per call and not held as fields
//!
//! [`greentic_aw_runtime::AgentRuntime::knowledge`] is a RUNTIME-level field,
//! set once when the runtime is built, while the provider choice arrives PER
//! TURN in `AgentConfig`. The out-of-process serve path serves many agents from
//! one runtime, so a runtime built around one worker's extension id would hand
//! every other worker that same extension. The binding therefore travels down
//! through [`greentic_aw_runtime::knowledge::Knowledge::search_bound`] and the
//! ids are read from its `params` at call time.
//!
//! ## Why this wraps rather than replaces
//!
//! `with_knowledge` overwrites the runtime's backend. A Chronicle mount
//! (`knowledge_mount`, behind `knowledge-chronicle`) and this one would
//! otherwise each disable the other depending on call order, silently. So
//! [`attach`] takes whatever is already mounted and delegates to it for every
//! binding that is not this provider — including `ingest`, which is how a
//! boot-ingested corpus keeps working.
//!
//! ## No cargo feature
//!
//! Unlike `knowledge_mount`, this is not feature-gated. It drags nothing: the
//! `ExtensionRuntime` it invokes is already mandatory on this path
//! (`agent_node::build_ext_runtime` returning `None` disables `dw.agent` nodes
//! outright). Gating it would force an operator who wants a customer's own
//! retrieval service to rebuild the runner with an embedded database they never
//! use.

use std::sync::Arc;

use greentic_aw_runtime::AgentRuntime;
use greentic_aw_runtime::config::MemoryProviderRef;
use greentic_aw_runtime::knowledge::{
    IngestOutcome, Knowledge, KnowledgeChunk, KnowledgeError, KnowledgeQuery, KnowledgeResult,
    RetrievedChunk,
};
use greentic_ext_runtime::ExtensionRuntime;
use greentic_ext_runtime::host_ports::HostCallContext;
use greentic_types::TenantCtx;

/// The provider id a knowledge binding carries when retrieval is delegated to a
/// design-extension tool. Written by the designer's composer; matched here.
pub const EXTENSION_PROVIDER_ID: &str = "provider.knowledge.extension";

/// The `params` key naming the extension to delegate retrieval to.
///
/// A contract with the designer's provider catalog (`block_id` in
/// `assets/dw-providers-catalog.json`, mirrored by `EXTENSION_ID_PARAM` in its
/// `agent_tool_deps`). A variant spelling on either side makes the feature
/// inert while every layer still reports success, so do not invent one.
pub const EXTENSION_ID_PARAM: &str = "provider.knowledge.extension.extension_id";

/// The `params` key naming which of that extension's tools performs retrieval.
/// Same contract and same discipline as [`EXTENSION_ID_PARAM`].
pub const TOOL_NAME_PARAM: &str = "provider.knowledge.extension.tool_name";

/// Chunks accepted when the query names no limit. The loop always sets one
/// (`auto_top_k`), so this only bounds a direct caller.
const DEFAULT_LIMIT: usize = 5;

/// Characters kept from any single chunk.
const MAX_CHUNK_CHARS: usize = 4_000;

/// Characters kept across all chunks of one retrieval.
///
/// `augment_system_prompt` bullets everything it is handed straight into the
/// system prompt, so an unbounded response is our token bill, not the
/// customer's. The extension is expected to cap too; this is defence in depth at
/// the boundary that is always crossed.
///
/// **This is a bound on PROMPT size, not on memory.** By the time either cap
/// applies the response has already been materialised twice: the host's
/// `http::fetch` reads the body with `resp.bytes()` and applies no size cap at
/// the pinned ext-runtime rev, and the tool result reaches this module as an
/// owned `String`. A multi-gigabyte response is therefore a host memory problem
/// these constants do not touch. Neither introduced here nor fixable here — it
/// needs a cap on the host's HTTP port — but the distinction has to be written
/// down, because a reader who takes these for a memory guard will not go looking.
const MAX_TOTAL_CHARS: usize = 24_000;

/// Identifies this backend within a wrapper chain.
///
/// [`Knowledge::wrapped_backend`] unwraps one layer and cannot tell a corpus
/// backend from a second delegating wrapper, so a host asking "is a corpus
/// mounted" has to walk the chain past every layer that is this adapter. See
/// [`corpus_backend`].
const BACKEND_ID: &str = "runner-host.knowledge_ext";

/// Mount extension-delegated knowledge retrieval, wrapping whatever backend is
/// already mounted (see the module docs — this must not replace it).
///
/// Infallible and unconditional: the adapter only acts on a binding that names
/// [`EXTENSION_PROVIDER_ID`], so mounting it on a runtime no worker binds it on
/// changes nothing.
#[must_use]
pub fn attach(base: AgentRuntime, ext: Arc<ExtensionRuntime>) -> AgentRuntime {
    let inner = base.knowledge_backend();
    base.with_knowledge(Arc::new(ExtensionKnowledge::new(ext, inner)))
}

/// What one agent's knowledge binding means to this adapter.
///
/// The third arm is the reason this is an enum rather than an `Option`. A
/// binding that NAMES [`EXTENSION_PROVIDER_ID`] but whose delegation target does
/// not read — key absent, blank, or misspelled — must not be treated the same as
/// a binding for some other provider. Falling through was quiet in two
/// directions, and the second is the worse one:
///
/// - default build (nothing wrapped) it reached `NotConfigured`, logged at
///   `debug`, and the worker answered ungrounded;
/// - with `knowledge-chronicle` on it reached `KnowledgeBridge`, which ignores
///   the binding entirely and searches its own env-configured corpus — so the
///   worker retrieved from **the wrong source** and reported success.
///
/// The provider id is an explicit operator declaration, so an unreadable target
/// under it is an error, not a normal state. [`Claim::Malformed`] carries the
/// offending key and becomes a `Backend` error, which the loop already turns
/// into a warning plus a distinguishable trace step.
enum Claim {
    /// Some other provider — the wrapped backend's business, not ours.
    NotOurs,
    /// This provider, with a usable delegation target.
    Delegate {
        extension_id: String,
        tool_name: String,
    },
    /// This provider, but the named `params` key is missing or blank.
    Malformed(&'static str),
}

/// Retrieval performed by a design-extension tool named in the agent's binding.
pub struct ExtensionKnowledge {
    ext: Arc<ExtensionRuntime>,
    /// The backend mounted before this one, if any. Every binding that is not
    /// [`EXTENSION_PROVIDER_ID`] — and every `ingest` — goes here.
    inner: Option<Arc<dyn Knowledge>>,
}

impl ExtensionKnowledge {
    #[must_use]
    pub fn new(ext: Arc<ExtensionRuntime>, inner: Option<Arc<dyn Knowledge>>) -> Self {
        Self { ext, inner }
    }

    /// What a binding says about this adapter.
    fn claim_of(binding: &MemoryProviderRef) -> Claim {
        if binding.provider != EXTENSION_PROVIDER_ID {
            return Claim::NotOurs;
        }
        let read = |key: &str| -> Option<String> {
            let value = binding.params.get(key)?.as_str()?.trim();
            (!value.is_empty()).then(|| value.to_string())
        };
        let Some(extension_id) = read(EXTENSION_ID_PARAM) else {
            return Claim::Malformed(EXTENSION_ID_PARAM);
        };
        let Some(tool_name) = read(TOOL_NAME_PARAM) else {
            return Claim::Malformed(TOOL_NAME_PARAM);
        };
        Claim::Delegate {
            extension_id,
            tool_name,
        }
    }

    async fn delegate_search(
        &self,
        tenant: &TenantCtx,
        query: KnowledgeQuery,
        binding: Option<&MemoryProviderRef>,
    ) -> KnowledgeResult<Vec<RetrievedChunk>> {
        match self.inner.as_ref() {
            Some(inner) => inner.search_bound(tenant, query, binding).await,
            None => Err(KnowledgeError::NotConfigured),
        }
    }
}

#[async_trait::async_trait]
impl Knowledge for ExtensionKnowledge {
    /// A no-op for the extension path: the corpus belongs to the customer's own
    /// service and this runtime has nothing to write it to.
    ///
    /// Success rather than an error, deliberately — a pack that happens to
    /// carry a corpus (an operator who bound documents and then switched
    /// provider, say) must not fail boot ingestion over a corpus the retrieval
    /// side simply will not read.
    ///
    /// A wrapped inner backend still receives it, so a Chronicle mount under
    /// this one keeps ingesting exactly as before.
    async fn ingest(
        &self,
        tenant: &TenantCtx,
        chunks: Vec<KnowledgeChunk>,
    ) -> KnowledgeResult<IngestOutcome> {
        match self.inner.as_ref() {
            Some(inner) => inner.ingest(tenant, chunks).await,
            None => Ok(IngestOutcome::default()),
        }
    }

    /// Retrieval with no binding in hand cannot know which extension to invoke,
    /// so it can only be the wrapped backend's — or nothing. The agentic loop
    /// always goes through [`Self::search_bound`].
    async fn search(
        &self,
        tenant: &TenantCtx,
        query: KnowledgeQuery,
    ) -> KnowledgeResult<Vec<RetrievedChunk>> {
        self.delegate_search(tenant, query, None).await
    }

    async fn search_bound(
        &self,
        tenant: &TenantCtx,
        query: KnowledgeQuery,
        binding: Option<&MemoryProviderRef>,
    ) -> KnowledgeResult<Vec<RetrievedChunk>> {
        let (extension_id, tool_name) = match binding.map_or(Claim::NotOurs, Self::claim_of) {
            Claim::NotOurs => return self.delegate_search(tenant, query, binding).await,
            Claim::Malformed(key) => {
                return Err(KnowledgeError::Backend(format!(
                    "knowledge binding names `{EXTENSION_PROVIDER_ID}` but its `{key}` param is \
                     missing or blank, so there is no retrieval target to invoke"
                )));
            }
            Claim::Delegate {
                extension_id,
                tool_name,
            } => (extension_id, tool_name),
        };

        let limit = query.limit.unwrap_or(DEFAULT_LIMIT).max(1);
        let args = serde_json::json!({
            "query": query.query,
            "top_k": limit,
            "tenant": tenant.tenant.as_str(),
            "env": tenant.env.as_str(),
        })
        .to_string();

        // The caller's tenant travels with the call rather than the process
        // default, so a tenant-aware host port sees it.
        //
        // Do NOT read that as "the extension's credential resolves per tenant".
        // At the pinned ext-runtime rev (29b9cc5) `HostCallContext` is consulted
        // at exactly ONE site — the LLM port, `host_state.rs:417`.
        // `secrets::Host::get` forwards to `SecretsBackend::get(&uri)`, which
        // takes no tenant at all, and the OAuth port reads its tenant from
        // runtime-level config. So on the out-of-process serve path — the very
        // path that serves many agents from one runtime, and the reason the
        // binding has to travel per turn — the runtime is built with
        // `build_ext_runtime(Arc::new(EnvSecretsBackend), None)`
        // (`agent_node.rs:1736`) and EVERY tenant's retrieval resolves the same
        // env credential. Passing the tenant is correct and free once the port
        // grows tenant awareness; today it is not a multi-tenancy guarantee, and
        // a comment claiming otherwise is what would stop the next reader
        // checking.
        let ctx = HostCallContext {
            tenant: Some(tenant.tenant.as_str().to_string()),
            user_email: None,
        };

        // `invoke_tool_ctx` is synchronous (wasmtime sync) and instantiates a
        // component, so it must never run on the async executor.
        //
        // The cost of that, spelled out because §6.2 records only its cause: the
        // guest cannot bound its own call (`extension-host/http@0.1.0` carries
        // no timeout field), so the ceiling is the host's 30 s. A hung customer
        // service therefore pins one blocking-pool thread AND one turn for up to
        // 30 s per retrieval — and `spawn_blocking` is not cancellable, so an
        // abandoned turn does not release the thread any sooner.
        let ext = Arc::clone(&self.ext);
        let called = {
            let extension_id = extension_id.clone();
            let tool_name = tool_name.clone();
            tokio::task::spawn_blocking(move || {
                ext.invoke_tool_ctx(&extension_id, &tool_name, &args, &ctx)
            })
            .await
        };

        let raw = called
            .map_err(|e| {
                KnowledgeError::Backend(format!(
                    "knowledge retrieval task for '{extension_id}/{tool_name}' did not \
                     complete: {e}"
                ))
            })?
            .map_err(|e| {
                KnowledgeError::Backend(format!(
                    "knowledge retrieval tool '{extension_id}/{tool_name}' failed: {e}"
                ))
            })?;

        parse_chunks(&raw, limit)
    }

    /// [`attach`] is unconditional, so `AgentRuntime::has_knowledge()` is true on
    /// every path that mounts this adapter — whether or not a corpus backend was
    /// mounted underneath it. That makes `has_knowledge()` unable to answer "did
    /// the corpus mount run", which is exactly the question the in-process
    /// `dw.agent` regression guard exists to ask. This answers it instead.
    fn wrapped_backend(&self) -> Option<Arc<dyn Knowledge>> {
        self.inner.clone()
    }

    fn backend_id(&self) -> &'static str {
        BACKEND_ID
    }
}

/// The first backend in the runtime's wrapper chain that is NOT this adapter —
/// i.e. the corpus backend, if one was mounted underneath.
///
/// Walks the whole chain rather than unwrapping one layer:
/// [`Knowledge::wrapped_backend`] returning `Some` only says "something is
/// underneath", which a second delegating wrapper would satisfy just as well as
/// a corpus. Since [`attach`] is unconditional, `AgentRuntime::has_knowledge()`
/// cannot answer this question at all any more — every runner-host path has a
/// backend mounted whether or not a corpus was.
#[must_use]
pub fn corpus_backend(runtime: &AgentRuntime) -> Option<Arc<dyn Knowledge>> {
    corpus_in_chain(runtime.knowledge_backend())
}

/// The walk itself, split from [`corpus_backend`] so it can be exercised over a
/// hand-built chain without standing up a whole `AgentRuntime`.
fn corpus_in_chain(top: Option<Arc<dyn Knowledge>>) -> Option<Arc<dyn Knowledge>> {
    let mut current = top;
    while let Some(backend) = current {
        if backend.backend_id() != BACKEND_ID {
            return Some(backend);
        }
        current = backend.wrapped_backend();
    }
    None
}

/// Parse a retrieval tool's result into ranked chunks.
///
/// The ONE place third-party data enters the runtime, so every decision here is
/// deliberate:
///
/// - **Response order is the ranking.** The chunks are not re-sorted; a service
///   that returns its best hit first must see it stay first, and an absent
///   `score` becomes `0.0` rather than dropping the chunk or inventing a rank.
/// - **A body this function cannot read is an error, never an empty result.**
///   Returning `Ok(vec![])` for a malformed or unexpected body is exactly the
///   silent degrade this whole line of work exists to remove: the worker would
///   answer confidently from nothing and no layer would say so.
/// - **Both caps are applied here as well as in the extension**, because the
///   extension is one artifact among many and this is the boundary that is
///   always crossed.
fn parse_chunks(raw: &str, limit: usize) -> KnowledgeResult<Vec<RetrievedChunk>> {
    let body: serde_json::Value = serde_json::from_str(raw).map_err(|e| {
        KnowledgeError::Backend(format!(
            "knowledge retrieval returned a body that is not JSON: {e}"
        ))
    })?;

    let items = body
        .get("chunks")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            KnowledgeError::Backend(
                "knowledge retrieval returned no `chunks` array; expected \
                 {\"chunks\":[{\"text\":…}]}"
                    .to_string(),
            )
        })?;

    let mut out = Vec::with_capacity(items.len().min(limit));
    let mut budget = MAX_TOTAL_CHARS;

    for (index, item) in items.iter().take(limit).enumerate() {
        let text = item
            .get("text")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                KnowledgeError::Backend(format!(
                    "knowledge retrieval chunk {index} carries no `text` string"
                ))
            })?;

        if budget == 0 {
            break;
        }
        let text = truncate_chars(text, MAX_CHUNK_CHARS.min(budget));
        budget = budget.saturating_sub(text.chars().count());

        // A missing — or non-numeric — score is 0.0, while a missing or
        // non-string `text` above is a hard `Backend` error. The asymmetry is
        // chosen, not an oversight: response ORDER is the ranking, so a score
        // this function cannot read costs nothing but a trace field, whereas a
        // chunk with no readable text is an empty bullet in the system prompt
        // and there is no honest value to substitute for it.
        let score = item
            .get("score")
            .and_then(serde_json::Value::as_f64)
            .filter(|s| s.is_finite())
            .unwrap_or(0.0);

        out.push(RetrievedChunk {
            text,
            score,
            doc_id: item
                .get("doc_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            chunk_index: None,
            metadata: item
                .get("metadata")
                .and_then(serde_json::Value::as_object)
                .cloned()
                .unwrap_or_default(),
        });
    }

    Ok(out)
}

/// Truncate on a character boundary. Byte slicing would panic mid-codepoint on
/// any non-ASCII corpus, which is most of them.
fn truncate_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    text.chars().take(max).collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn body(chunks: serde_json::Value) -> String {
        serde_json::json!({ "chunks": chunks }).to_string()
    }

    #[test]
    fn a_missing_score_is_zero_and_the_service_order_is_kept() {
        let raw = body(serde_json::json!([
            { "text": "first" },
            { "text": "second", "score": 0.9 },
            { "text": "third", "score": 0.1 },
        ]));
        let chunks = parse_chunks(&raw, 10).expect("a well-formed body parses");

        assert_eq!(
            chunks.iter().map(|c| c.text.as_str()).collect::<Vec<_>>(),
            ["first", "second", "third"],
            "response order IS the ranking; re-sorting by score would promote \
             the second hit over the service's own first choice"
        );
        assert_eq!(chunks[0].score, 0.0);
        assert_eq!(chunks[1].score, 0.9);
    }

    #[test]
    fn more_chunks_than_the_limit_are_truncated() {
        let raw = body(serde_json::json!([
            { "text": "a" }, { "text": "b" }, { "text": "c" }, { "text": "d" },
        ]));
        let chunks = parse_chunks(&raw, 2).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "a");
        assert_eq!(chunks[1].text, "b");
    }

    #[test]
    fn an_oversized_chunk_is_capped() {
        let long = "x".repeat(MAX_CHUNK_CHARS + 500);
        let raw = body(serde_json::json!([{ "text": long }]));
        let chunks = parse_chunks(&raw, 5).unwrap();
        assert_eq!(chunks[0].text.chars().count(), MAX_CHUNK_CHARS);
    }

    #[test]
    fn the_total_character_budget_bounds_the_whole_retrieval() {
        let long = "y".repeat(MAX_CHUNK_CHARS);
        let items: Vec<serde_json::Value> = (0..20)
            .map(|_| serde_json::json!({ "text": long }))
            .collect();
        let raw = body(serde_json::json!(items));
        let chunks = parse_chunks(&raw, 20).unwrap();
        let total: usize = chunks.iter().map(|c| c.text.chars().count()).sum();
        assert!(
            total <= MAX_TOTAL_CHARS,
            "{total} characters exceeds the {MAX_TOTAL_CHARS}-character budget"
        );
        assert!(
            !chunks.is_empty(),
            "the budget must not empty the retrieval"
        );
    }

    #[test]
    fn a_multibyte_chunk_is_capped_without_panicking() {
        let long = "é".repeat(MAX_CHUNK_CHARS + 10);
        let raw = body(serde_json::json!([{ "text": long }]));
        let chunks = parse_chunks(&raw, 5).unwrap();
        assert_eq!(chunks[0].text.chars().count(), MAX_CHUNK_CHARS);
    }

    #[test]
    fn a_body_that_is_not_json_is_a_backend_error_not_an_empty_result() {
        let err = parse_chunks("not json at all", 5)
            .expect_err("an unreadable body must not read as an empty corpus");
        assert!(matches!(err, KnowledgeError::Backend(_)), "got {err:?}");
    }

    #[test]
    fn a_body_without_a_chunks_array_is_a_backend_error() {
        let err = parse_chunks(r#"{"results":[]}"#, 5)
            .expect_err("a differently-shaped body must not read as an empty corpus");
        assert!(matches!(err, KnowledgeError::Backend(_)), "got {err:?}");
    }

    #[test]
    fn an_empty_chunks_array_is_an_honest_empty_result() {
        let chunks = parse_chunks(r#"{"chunks":[]}"#, 5).unwrap();
        assert!(chunks.is_empty());
    }

    #[test]
    fn a_chunk_without_text_is_a_backend_error() {
        let raw = body(serde_json::json!([{ "score": 0.5 }]));
        let err = parse_chunks(&raw, 5).expect_err("a chunk with no text is unusable");
        assert!(matches!(err, KnowledgeError::Backend(_)), "got {err:?}");
    }

    #[test]
    fn doc_id_and_metadata_ride_through_when_present() {
        let raw = body(serde_json::json!([{
            "text": "t", "score": 0.5, "doc_id": "kb/faq#12", "metadata": { "lang": "en" }
        }]));
        let chunks = parse_chunks(&raw, 5).unwrap();
        assert_eq!(chunks[0].doc_id.as_deref(), Some("kb/faq#12"));
        assert_eq!(chunks[0].metadata["lang"], "en");
    }

    fn binding(provider: &str, params: serde_json::Value) -> MemoryProviderRef {
        MemoryProviderRef {
            provider: provider.to_string(),
            capability: "cap://dw.knowledge".to_string(),
            params: params.as_object().cloned().unwrap_or_default(),
            credential_ref: None,
        }
    }

    #[test]
    fn ids_are_read_from_the_bindings_params() {
        let b = binding(
            EXTENSION_PROVIDER_ID,
            serde_json::json!({
                EXTENSION_ID_PARAM: "greentic.rag-http",
                TOOL_NAME_PARAM: "search_knowledge",
            }),
        );
        assert!(matches!(
            ExtensionKnowledge::claim_of(&b),
            Claim::Delegate { extension_id, tool_name }
                if extension_id == "greentic.rag-http" && tool_name == "search_knowledge"
        ));
    }

    #[test]
    fn another_provider_is_not_claimed() {
        let b = binding(
            "provider.knowledge.chronicle",
            serde_json::json!({
                EXTENSION_ID_PARAM: "greentic.rag-http",
                TOOL_NAME_PARAM: "search_knowledge",
            }),
        );
        assert!(
            matches!(ExtensionKnowledge::claim_of(&b), Claim::NotOurs),
            "a Chronicle binding must fall through to the wrapped backend"
        );
    }

    #[test]
    fn missing_or_blank_ids_do_not_claim_the_binding() {
        let no_tool = binding(
            EXTENSION_PROVIDER_ID,
            serde_json::json!({ EXTENSION_ID_PARAM: "greentic.rag-http" }),
        );
        assert!(matches!(
            ExtensionKnowledge::claim_of(&no_tool),
            Claim::Malformed(TOOL_NAME_PARAM)
        ));

        let blank = binding(
            EXTENSION_PROVIDER_ID,
            serde_json::json!({ EXTENSION_ID_PARAM: "  ", TOOL_NAME_PARAM: "search_knowledge" }),
        );
        assert!(matches!(
            ExtensionKnowledge::claim_of(&blank),
            Claim::Malformed(EXTENSION_ID_PARAM)
        ));
    }

    /// The companion to the test above, and the more important half: NOT
    /// invoking is right, but doing it quietly is what made a drifted param key
    /// dangerous. With a corpus backend wrapped, the old fall-through reached
    /// `KnowledgeBridge`, which ignores the binding and searches its own
    /// env-configured corpus — so the worker retrieved from the WRONG source and
    /// reported success. A `Backend` error is what routes this through the
    /// loop's warning and its distinguishable trace step.
    #[tokio::test]
    async fn a_binding_whose_target_does_not_read_is_reported_not_fallen_through() {
        let inner = Arc::new(RecordingInner {
            ingested: std::sync::Mutex::new(Vec::new()),
        });
        let broken = binding(
            EXTENSION_PROVIDER_ID,
            serde_json::json!({ EXTENSION_ID_PARAM: "greentic.rag-http" }),
        );
        let err = adapter(Some(inner))
            .search_bound(
                &tenant(),
                KnowledgeQuery {
                    query: "q".into(),
                    limit: Some(3),
                },
                Some(&broken),
            )
            .await
            .expect_err("a declared provider with no readable target is an error");

        let KnowledgeError::Backend(message) = &err else {
            panic!("expected a Backend error the loop will warn about, got {err:?}");
        };
        assert!(
            message.contains(TOOL_NAME_PARAM),
            "the diagnostic must name the key that did not read: {message}"
        );
    }

    struct RecordingInner {
        ingested: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Knowledge for RecordingInner {
        async fn ingest(
            &self,
            _tenant: &TenantCtx,
            chunks: Vec<KnowledgeChunk>,
        ) -> KnowledgeResult<IngestOutcome> {
            let ids: Vec<String> = chunks.iter().map(|c| c.doc_id.clone()).collect();
            self.ingested.lock().unwrap().extend(ids.clone());
            Ok(IngestOutcome { chunk_ids: ids })
        }

        async fn search(
            &self,
            _tenant: &TenantCtx,
            _query: KnowledgeQuery,
        ) -> KnowledgeResult<Vec<RetrievedChunk>> {
            Ok(vec![RetrievedChunk {
                text: "from the wrapped backend".to_string(),
                score: 1.0,
                doc_id: None,
                chunk_index: None,
                metadata: serde_json::Map::new(),
            }])
        }
    }

    fn tenant() -> TenantCtx {
        TenantCtx::new(
            greentic_types::EnvId::try_from("dev").unwrap(),
            greentic_types::TenantId::try_from("acme").unwrap(),
        )
    }

    fn adapter(inner: Option<Arc<dyn Knowledge>>) -> ExtensionKnowledge {
        ExtensionKnowledge {
            ext: Arc::new(ExtensionRuntime::for_test().unwrap()),
            inner,
        }
    }

    #[tokio::test]
    async fn ingest_is_a_no_op_with_nothing_wrapped() {
        let outcome = adapter(None).ingest(&tenant(), vec![]).await.unwrap();
        assert!(
            outcome.chunk_ids.is_empty(),
            "the corpus belongs to the customer's service; there is nothing to write"
        );
    }

    #[tokio::test]
    async fn ingest_with_a_corpus_still_succeeds_rather_than_failing_boot() {
        let outcome = adapter(None)
            .ingest(
                &tenant(),
                vec![KnowledgeChunk {
                    doc_id: "faq".into(),
                    chunk_index: 0,
                    text: "anything".into(),
                    metadata: serde_json::Map::new(),
                    embedding: None,
                }],
            )
            .await
            .expect("a pack carrying a corpus must not fail boot ingestion");
        assert!(outcome.chunk_ids.is_empty());
    }

    #[tokio::test]
    async fn a_wrapped_backend_still_receives_ingest_and_non_extension_searches() {
        let inner = Arc::new(RecordingInner {
            ingested: std::sync::Mutex::new(Vec::new()),
        });
        let wrapper = adapter(Some(inner.clone()));

        wrapper
            .ingest(
                &tenant(),
                vec![KnowledgeChunk {
                    doc_id: "faq".into(),
                    chunk_index: 0,
                    text: "t".into(),
                    metadata: serde_json::Map::new(),
                    embedding: None,
                }],
            )
            .await
            .unwrap();
        assert_eq!(inner.ingested.lock().unwrap().as_slice(), ["faq"]);

        let chronicle = binding("provider.knowledge.chronicle", serde_json::json!({}));
        let hits = wrapper
            .search_bound(
                &tenant(),
                KnowledgeQuery {
                    query: "q".into(),
                    limit: Some(3),
                },
                Some(&chronicle),
            )
            .await
            .unwrap();
        assert_eq!(hits[0].text, "from the wrapped backend");
    }

    /// `attach` is unconditional, so `has_knowledge()` can no longer tell a host
    /// whether a CORPUS backend was mounted. [`corpus_backend`] is what answers
    /// that now, and the in-process `dw.agent` regression guard depends on it —
    /// so it has to report both cases, not merely be present.
    ///
    /// The doubled case is the point: `wrapped_backend()` alone says only
    /// "something is underneath", which a SECOND copy of this adapter satisfies
    /// exactly as well as a corpus does. Mounting twice is not hypothetical —
    /// [`attach`] is called once per construction path and a fourth site could
    /// stack one — and the guard must not read that as a corpus.
    #[test]
    fn corpus_backend_walks_past_every_layer_of_this_adapter() {
        fn corpus_recorder() -> Arc<dyn Knowledge> {
            Arc::new(RecordingInner {
                ingested: std::sync::Mutex::new(Vec::new()),
            })
        }
        fn layer(inner: Option<Arc<dyn Knowledge>>) -> Option<Arc<dyn Knowledge>> {
            Some(Arc::new(adapter(inner)))
        }

        assert!(
            corpus_in_chain(None).is_none(),
            "nothing mounted at all => no corpus backend"
        );
        assert!(
            corpus_in_chain(layer(None)).is_none(),
            "this adapter alone is not a corpus"
        );
        assert!(
            corpus_in_chain(layer(layer(None))).is_none(),
            "two stacked adapters must not read as a mounted corpus — this is \
             exactly what a one-level `wrapped_backend()` check got wrong"
        );
        assert!(
            corpus_in_chain(layer(Some(corpus_recorder()))).is_some(),
            "a corpus directly under the adapter must stay visible to its host"
        );
        assert!(
            corpus_in_chain(layer(layer(Some(corpus_recorder())))).is_some(),
            "and must stay visible however many adapter layers sit above it"
        );
    }

    #[tokio::test]
    async fn with_nothing_wrapped_a_foreign_binding_is_not_configured() {
        let chronicle = binding("provider.knowledge.chronicle", serde_json::json!({}));
        let err = adapter(None)
            .search_bound(
                &tenant(),
                KnowledgeQuery {
                    query: "q".into(),
                    limit: Some(3),
                },
                Some(&chronicle),
            )
            .await
            .expect_err("no backend can serve a Chronicle binding here");
        assert!(matches!(err, KnowledgeError::NotConfigured), "got {err:?}");
    }
}

/// Ratchet: no `AgentRuntime` may be constructed without a decision about
/// extension-delegated retrieval.
///
/// The mount is a bare statement whose omission compiles, boots, and serves —
/// the runtime simply has no extension-delegated retrieval, `knowledge_active`
/// stays false, and the model answers from nothing. That has already happened
/// once on this exact seam: the comment above the in-process `dw.agent` mount
/// records that a missed call site left `runtime.knowledge` at `None` and *"the
/// model hallucinated instead of retrieving from the corpus that had already
/// been ingested at boot."*
///
/// So this scans the source instead of trusting the call sites to stay in step,
/// and it does so through TWO anchors, because neither catches the other's
/// regression:
///
/// - [`constructor_anchor`] — every file constructing an `AgentRuntime` must
///   mount this adapter as many times, minus an explicit, reasoned
///   [`EXEMPT`] allowance. This is the one that catches the incident shape
///   quoted above: **a brand-new construction path that mounts nothing at
///   all**. Such a file names neither mount, so a mount-anchored scan never
///   even looks at it. Modelled on the designer's
///   `dw_test_chat/mcp_source.rs`, which anchors on the constructor with an
///   exemption list precisely so that each site is a decision rather than an
///   inference.
/// - [`mount_anchor`] — a file that mounts a Chronicle corpus must mount this
///   adapter too. This catches the likelier near-term regression: a knowledge
///   mount copied to a new site with only half the pair.
#[cfg(test)]
mod call_site_ratchet {
    use std::path::Path;

    /// The Chronicle mount, whose call sites are the corpus mount sites.
    const CHRONICLE_MOUNT: &str = "knowledge_mount::attach(";
    /// This module's mount.
    const EXTENSION_MOUNT: &str = "knowledge_ext::attach(";
    /// Every runtime construction, mounted or not.
    const CONSTRUCTOR: &str = "AgentRuntime::new(";

    /// Files that construct an `AgentRuntime` WITHOUT mounting this adapter, and
    /// how many such constructions each is allowed.
    ///
    /// Every entry is a decision, and adding one means writing down why that
    /// runtime must have no extension-delegated retrieval. A new constructor in
    /// an exempt file changes its count and fails this test, so the exemption
    /// covers the sites reasoned about here and never the next one.
    const EXEMPT: &[(&str, usize, &str)] = &[
        (
            "src/runner/graph_node.rs",
            2,
            "the supervisor ROUTING runtime, whose synthesised config is \
             `knowledge: None` with an empty tool list — it picks a branch and \
             never answers from a corpus — plus one test constructor. Note the \
             `agent_ref: None` inline path is NOT one of these: it shares the \
             graph-agent constructor and skips the mount at run time, so it \
             costs no allowance here",
        ),
        (
            "src/runner/agent_node.rs",
            3,
            "three test constructors; both production paths mount",
        ),
        (
            "src/runner/engine.rs",
            1,
            "one test constructor; engine.rs builds no production AW runtime",
        ),
    ];

    /// Count non-comment occurrences of `needle`, so a doc comment naming a
    /// mount or a constructor is never mistaken for one.
    fn count_calls(text: &str, needle: &str) -> usize {
        text.lines()
            .filter(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && !t.starts_with("*")
            })
            .map(|l| l.matches(needle).count())
            .sum()
    }

    struct Site {
        path: String,
        chronicle: usize,
        extension: usize,
        constructors: usize,
    }

    /// Every source file that mounts either backend or constructs a runtime.
    fn sites() -> Vec<Site> {
        fn walk(dir: &Path, out: &mut Vec<Site>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let Ok(text) = std::fs::read_to_string(&path) else {
                        continue;
                    };
                    let rel = path.to_string_lossy().replace('\\', "/");
                    if rel.ends_with("src/runner/knowledge_ext.rs") {
                        continue; // this file, which defines all three names
                    }
                    let site = Site {
                        path: rel,
                        chronicle: count_calls(&text, CHRONICLE_MOUNT),
                        extension: count_calls(&text, EXTENSION_MOUNT),
                        constructors: count_calls(&text, CONSTRUCTOR),
                    };
                    if site.chronicle > 0 || site.extension > 0 || site.constructors > 0 {
                        out.push(site);
                    }
                }
            }
        }
        let mut out = Vec::new();
        walk(Path::new("src"), &mut out);
        out.sort_by(|a, b| a.path.cmp(&b.path));
        out
    }

    fn exempted(path: &str) -> usize {
        EXEMPT
            .iter()
            .find(|(p, _, _)| *p == path)
            .map_or(0, |(_, n, _)| *n)
    }

    /// The anchor that catches a NEW construction path mounting nothing: such a
    /// file names neither mount, so [`mount_anchor`] never sees it.
    #[test]
    fn constructor_anchor() {
        let offenders: Vec<_> = sites()
            .into_iter()
            .filter(|s| s.constructors > 0)
            .filter(|s| s.constructors != s.extension + exempted(&s.path))
            .map(|s| {
                format!(
                    "{}: {} `AgentRuntime::new(`, {} `knowledge_ext::attach(`, {} exempted",
                    s.path,
                    s.constructors,
                    s.extension,
                    exempted(&s.path)
                )
            })
            .collect();
        assert!(
            offenders.is_empty(),
            "each of these builds an `AgentRuntime` without deciding about \
             extension-delegated retrieval. Mount `knowledge_ext::attach`, or add \
             the site to `EXEMPT` with a reason — a runtime that silently has no \
             retrieval is how the model came to answer from nothing before: {offenders:#?}"
        );
    }

    /// The anchor that catches half a copied pair: a corpus mount without this
    /// adapter beside it. One-directional on purpose — the reverse (this
    /// adapter alone) is the DEFAULT build's shape, since the Chronicle mount is
    /// feature-gated and this one is not.
    #[test]
    fn mount_anchor() {
        let offenders: Vec<_> = sites()
            .into_iter()
            .filter(|s| s.chronicle > s.extension)
            .map(|s| {
                format!(
                    "{}: {} chronicle, {} extension",
                    s.path, s.chronicle, s.extension
                )
            })
            .collect();
        assert!(
            offenders.is_empty(),
            "these mount a knowledge corpus without also mounting \
             `knowledge_ext::attach`, so a worker binding \
             `provider.knowledge.extension` silently retrieves nothing and the \
             model answers from nothing instead: {offenders:#?}"
        );
    }

    /// Guards the guards. Both scans are textual, so a rename would leave them
    /// passing vacuously over a file set of zero — and an `EXEMPT` entry for a
    /// path that no longer constructs anything is debt hiding the next
    /// regression behind it, exactly as `ci/file_size_ratchet.py` treats a
    /// baselined file that is no longer over its cap.
    #[test]
    fn the_scanned_anchors_still_exist() {
        let sites = sites();
        let mounts: usize = sites.iter().map(|s| s.extension).sum();
        let corpora: usize = sites.iter().map(|s| s.chronicle).sum();
        let constructors: usize = sites.iter().map(|s| s.constructors).sum();
        assert!(
            mounts >= 3,
            "expected the in-process `dw.agent`, out-of-process serve and \
             graph-node mounts; found {mounts} `knowledge_ext::attach` call sites"
        );
        assert!(
            corpora >= 3,
            "expected three Chronicle mounts beside them; found {corpora} — has \
             `knowledge_mount::attach` been renamed?"
        );
        assert!(
            constructors >= mounts,
            "found {constructors} `AgentRuntime::new(` against {mounts} mounts — \
             has the constructor been renamed?"
        );
        for (path, _, _) in EXEMPT {
            let found = sites.iter().find(|s| s.path == *path);
            assert!(
                found.is_some_and(|s| s.constructors > 0),
                "`EXEMPT` names {path}, which constructs no `AgentRuntime`. A \
                 stale allowance hides the next unmounted construction added to \
                 that file — remove it."
            );
        }
    }
}
