//! Flow-execution knowledge search node (LOCKED ENCODING v1:
//! `component == "knowledge.search"`).
//!
//! A plain flow can ask a knowledge base a question and get the best matching
//! passages back as data, through the SAME [`KnowledgePort`] an extension's
//! `host.knowledge.search` uses. The passages are then usable by later steps,
//! for example in an LLM prompt as `{{node.<id>.passages}}`.
//!
//! ```text
//! nodes:
//!   find_policy:
//!     knowledge.search:
//!       query: "{{entry.input.text}}"   # required, rendered by the runner
//!       knowledge_base: "Support policies"  # required: the base's NAME
//!       top_k: 5                        # optional, 1..=20 (default 5)
//!       output: policy                  # optional state key
//!     routing: end
//! ```
//!
//! `knowledge_base_id` is accepted as an alias of `knowledge_base` because the
//! designer's `rag_chat` capability spells it that way. The value is handed to
//! the port unchanged: the port resolves it (today by base name) inside the
//! scope of the call.
//!
//! The result is `{ok:true, passages:[{text, doc_id, name, score}], count}` or
//! `{ok:false, error:{code,message}}`. A failure is a VALUE; the engine marks
//! the node failed only when it has an `on_error` edge (see
//! `engine::knowledge_node_output`), exactly like `media.create` and `a2a`.
//! A run with no knowledge port installed answers `knowledge_not_configured`,
//! never an empty success: an empty `passages` list must mean "the base has
//! nothing relevant", not "nothing was asked".
//!
//! Scope. The call carries the flow's tenant and session only. The node never
//! reads a team (or any other scope) from its payload: a payload is authorable
//! by the flow, and a flow must not be able to widen the scope a port resolves
//! a base in. A port that scopes below the tenant resolves the team from the
//! session.
//!
//! Retrieved text is untrusted data. It is returned to the flow, never logged,
//! and a failure message is a fixed sentence that never carries a port detail.

use std::sync::Arc;

use greentic_ext_runtime::host_ports::{
    HostCallContext, KnowledgePort, KnowledgePortError, KnowledgeSearchRequest,
};
use serde_json::{Value, json};

/// Extension id reported to the port (host log only).
const SOURCE_ID: &str = "greentic.flow.knowledge";

/// Passages returned when `top_k` is not given.
pub const DEFAULT_TOP_K: u32 = 5;

/// The most passages one node asks for. Matches the port's own clamp
/// (`MAX_SEARCH_LIMIT`), so a larger request is not an error but is capped.
pub const MAX_TOP_K: u32 = 20;

/// Failure codes of this node's result value.
pub mod codes {
    /// The query or the base is missing or blank, or `top_k` is not a positive
    /// integer.
    pub const INVALID_INPUT: &str = "invalid_input";
    /// This runtime has no knowledge port.
    pub const KNOWLEDGE_NOT_CONFIGURED: &str = "knowledge_not_configured";
    /// The caller may not use the base, or it does not exist. The port makes
    /// the two read the same on purpose, so this node does too.
    pub const KNOWLEDGE_NOT_ALLOWED: &str = "knowledge_not_allowed";
    /// The store failed, is unreachable, or this host does not support search.
    pub const SEARCH_FAILED: &str = "search_failed";
}

fn failure(code: &str, message: &str) -> Value {
    json!({ "ok": false, "error": { "code": code, "message": message } })
}

fn non_blank_string<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

/// A validated `knowledge.search` request.
#[derive(Debug, PartialEq, Eq)]
struct SearchInput {
    query: String,
    knowledge_base: String,
    limit: u32,
}

fn parse_input(payload: &Value) -> Result<SearchInput, Value> {
    let Some(query) = non_blank_string(payload, "query") else {
        return Err(failure(
            codes::INVALID_INPUT,
            "knowledge.search needs a non-empty `query`",
        ));
    };
    let Some(knowledge_base) = non_blank_string(payload, "knowledge_base")
        .or_else(|| non_blank_string(payload, "knowledge_base_id"))
    else {
        return Err(failure(
            codes::INVALID_INPUT,
            "knowledge.search needs a `knowledge_base`",
        ));
    };
    let limit = match payload.get("top_k") {
        None | Some(Value::Null) => DEFAULT_TOP_K,
        Some(value) => match value.as_u64() {
            Some(requested) if requested >= 1 => requested.min(u64::from(MAX_TOP_K)) as u32,
            _ => {
                return Err(failure(
                    codes::INVALID_INPUT,
                    "`top_k` must be a whole number of at least 1",
                ));
            }
        },
    };
    Ok(SearchInput {
        query: query.to_string(),
        knowledge_base: knowledge_base.to_string(),
        limit,
    })
}

/// Search the base named by the rendered node `payload`. Always returns a
/// value; never panics.
pub async fn search(
    payload: &Value,
    tenant: &str,
    session_id: Option<&str>,
    port: Option<&Arc<dyn KnowledgePort>>,
) -> Value {
    let input = match parse_input(payload) {
        Ok(input) => input,
        Err(failure) => return failure,
    };
    let Some(port) = port else {
        return failure(
            codes::KNOWLEDGE_NOT_CONFIGURED,
            "this runtime has no knowledge search",
        );
    };
    let ctx = HostCallContext {
        tenant: Some(tenant.to_string()).filter(|tenant| !tenant.trim().is_empty()),
        session_id: session_id
            .map(str::to_string)
            .filter(|session| !session.trim().is_empty()),
        ..HostCallContext::default()
    };
    let request = KnowledgeSearchRequest {
        knowledge_base: input.knowledge_base,
        query: input.query,
        limit: input.limit,
    };
    let port = Arc::clone(port);
    // `KnowledgePort::search` is synchronous and may block on a store; run it on
    // the blocking pool so it never parks an async worker.
    let answered = tokio::task::spawn_blocking(move || port.search(SOURCE_ID, &ctx, request)).await;
    match answered {
        Ok(Ok(hits)) => {
            let passages: Vec<Value> = hits
                .into_iter()
                .take(MAX_TOP_K as usize)
                .map(|hit| {
                    json!({
                        "text": hit.passage,
                        "doc_id": hit.document_id,
                        "name": hit.name,
                        "score": hit.score,
                    })
                })
                .collect();
            let count = passages.len();
            json!({ "ok": true, "passages": passages, "count": count })
        }
        Ok(Err(KnowledgePortError::NotAllowed)) => failure(
            codes::KNOWLEDGE_NOT_ALLOWED,
            "the knowledge base is not available to this flow",
        ),
        Ok(Err(error)) => {
            // Only the error KIND is logged: a port detail can name a store or
            // a host, and passage text is never in it.
            tracing::warn!(code = "knowledge_search_failed", kind = ?std::mem::discriminant(&error), "knowledge.search failed");
            failure(codes::SEARCH_FAILED, "knowledge search is unavailable")
        }
        Err(_) => failure(codes::SEARCH_FAILED, "knowledge search is unavailable"),
    }
}

/// Whether `result` is a failure value.
pub fn is_failure(result: &Value) -> bool {
    result.get("ok").and_then(Value::as_bool) == Some(false)
}

/// The failure's `error.message`, for the node's `errors[0]`.
pub fn failure_message(result: &Value) -> String {
    result
        .pointer("/error/message")
        .and_then(Value::as_str)
        .unwrap_or("knowledge.search failed")
        .to_string()
}

/// The failure's `error.code`.
pub fn failure_code(result: &Value) -> String {
    result
        .pointer("/error/code")
        .and_then(Value::as_str)
        .unwrap_or(codes::SEARCH_FAILED)
        .to_string()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use greentic_ext_runtime::host_ports::KnowledgeSearchHit;

    use super::*;

    /// Records what the port was asked and answers a canned result.
    struct FakePort {
        seen: Mutex<Vec<(HostCallContext, KnowledgeSearchRequest)>>,
        answer: Result<Vec<KnowledgeSearchHit>, KnowledgePortError>,
    }

    impl FakePort {
        fn answering(answer: Result<Vec<KnowledgeSearchHit>, KnowledgePortError>) -> Arc<Self> {
            Arc::new(Self {
                seen: Mutex::new(Vec::new()),
                answer,
            })
        }
    }

    fn hit(name: &str, passage: &str, score: f32) -> KnowledgeSearchHit {
        KnowledgeSearchHit {
            document_id: format!("doc-{name}"),
            name: name.to_string(),
            passage: passage.to_string(),
            score,
        }
    }

    impl KnowledgePort for FakePort {
        fn create(
            &self,
            _: &str,
            _: &HostCallContext,
            _: greentic_ext_runtime::host_ports::KnowledgeCreateRequest,
        ) -> Result<greentic_ext_runtime::host_ports::KnowledgeCreated, KnowledgePortError>
        {
            unreachable!("knowledge.search never creates")
        }
        fn read(
            &self,
            _: &str,
            _: &HostCallContext,
            _: &str,
            _: &str,
        ) -> Result<greentic_ext_runtime::host_ports::KnowledgeDocument, KnowledgePortError>
        {
            unreachable!("knowledge.search never reads a document")
        }
        fn list(
            &self,
            _: &str,
            _: &HostCallContext,
            _: &str,
        ) -> Result<Vec<greentic_ext_runtime::host_ports::KnowledgeDocumentInfo>, KnowledgePortError>
        {
            unreachable!("knowledge.search never lists")
        }
        fn remove(
            &self,
            _: &str,
            _: &HostCallContext,
            _: &str,
            _: &str,
        ) -> Result<bool, KnowledgePortError> {
            unreachable!("knowledge.search never removes")
        }
        fn search(
            &self,
            _: &str,
            ctx: &HostCallContext,
            request: KnowledgeSearchRequest,
        ) -> Result<Vec<KnowledgeSearchHit>, KnowledgePortError> {
            self.seen.lock().unwrap().push((ctx.clone(), request));
            self.answer.clone()
        }
    }

    fn as_port(port: &Arc<FakePort>) -> Arc<dyn KnowledgePort> {
        Arc::clone(port) as Arc<dyn KnowledgePort>
    }

    #[tokio::test]
    async fn returns_the_passages_with_a_count_and_asks_the_port_in_the_flow_scope() {
        let port = FakePort::answering(Ok(vec![
            hit("Refunds", "Refunds take 5 days.", 0.91),
            hit("Shipping", "Shipping is free.", 0.4),
        ]));
        let result = search(
            &json!({"query": " refund time ", "knowledge_base": "Policies", "top_k": 3}),
            "acme",
            Some("sess-1"),
            Some(&as_port(&port)),
        )
        .await;

        assert_eq!(result["ok"], json!(true));
        assert_eq!(result["count"], json!(2));
        assert_eq!(result["passages"][0]["text"], json!("Refunds take 5 days."));
        assert_eq!(result["passages"][0]["doc_id"], json!("doc-Refunds"));
        assert_eq!(result["passages"][0]["name"], json!("Refunds"));
        let seen = port.seen.lock().unwrap();
        let (ctx, request) = &seen[0];
        assert_eq!(ctx.tenant.as_deref(), Some("acme"));
        assert_eq!(ctx.session_id.as_deref(), Some("sess-1"));
        assert_eq!(request.query, "refund time");
        assert_eq!(request.knowledge_base, "Policies");
        assert_eq!(request.limit, 3);
    }

    #[tokio::test]
    async fn no_hits_is_an_empty_success_not_a_failure() {
        let port = FakePort::answering(Ok(vec![]));
        let result = search(
            &json!({"query": "q", "knowledge_base": "kb"}),
            "acme",
            None,
            Some(&as_port(&port)),
        )
        .await;
        assert_eq!(result, json!({"ok": true, "passages": [], "count": 0}));
    }

    #[tokio::test]
    async fn without_a_port_the_answer_is_knowledge_not_configured_never_an_empty_success() {
        let result = search(
            &json!({"query": "q", "knowledge_base": "kb"}),
            "acme",
            None,
            None,
        )
        .await;
        assert!(is_failure(&result));
        assert_eq!(failure_code(&result), codes::KNOWLEDGE_NOT_CONFIGURED);
    }

    #[tokio::test]
    async fn invalid_input_is_refused_before_the_port_is_asked() {
        let port = FakePort::answering(Ok(vec![]));
        let bad_payloads = [
            json!({"knowledge_base": "kb"}),
            json!({"query": "   ", "knowledge_base": "kb"}),
            json!({"query": "q"}),
            json!({"query": "q", "knowledge_base": ""}),
            json!({"query": "q", "knowledge_base": "kb", "top_k": 0}),
            json!({"query": "q", "knowledge_base": "kb", "top_k": -2}),
            json!({"query": "q", "knowledge_base": "kb", "top_k": "five"}),
        ];
        for payload in bad_payloads {
            let result = search(&payload, "acme", None, Some(&as_port(&port))).await;
            assert_eq!(failure_code(&result), codes::INVALID_INPUT, "{payload}");
        }
        assert!(port.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn top_k_defaults_to_five_and_is_capped_at_twenty() {
        let port = FakePort::answering(Ok(vec![]));
        for (payload, expected) in [
            (json!({"query": "q", "knowledge_base": "kb"}), 5),
            (
                json!({"query": "q", "knowledge_base": "kb", "top_k": null}),
                5,
            ),
            (
                json!({"query": "q", "knowledge_base": "kb", "top_k": 500}),
                20,
            ),
        ] {
            search(&payload, "acme", None, Some(&as_port(&port))).await;
            assert_eq!(port.seen.lock().unwrap().last().unwrap().1.limit, expected);
        }
    }

    #[tokio::test]
    async fn knowledge_base_id_is_an_alias_and_the_primary_name_wins() {
        let port = FakePort::answering(Ok(vec![]));
        search(
            &json!({"query": "q", "knowledge_base_id": "from-alias"}),
            "acme",
            None,
            Some(&as_port(&port)),
        )
        .await;
        search(
            &json!({"query": "q", "knowledge_base": "primary", "knowledge_base_id": "alias"}),
            "acme",
            None,
            Some(&as_port(&port)),
        )
        .await;
        let seen = port.seen.lock().unwrap();
        assert_eq!(seen[0].1.knowledge_base, "from-alias");
        assert_eq!(seen[1].1.knowledge_base, "primary");
    }

    #[tokio::test]
    async fn a_team_named_in_the_payload_never_reaches_the_port() {
        let port = FakePort::answering(Ok(vec![]));
        search(
            &json!({"query": "q", "knowledge_base": "kb", "team": "finance", "tenant": "other"}),
            "acme",
            None,
            Some(&as_port(&port)),
        )
        .await;
        let seen = port.seen.lock().unwrap();
        assert_eq!(seen[0].0.team, None);
        assert_eq!(seen[0].0.tenant.as_deref(), Some("acme"));
    }

    #[tokio::test]
    async fn a_refused_base_and_a_store_failure_get_fixed_sentences_without_port_detail() {
        let refused = FakePort::answering(Err(KnowledgePortError::NotAllowed));
        let unavailable = FakePort::answering(Err(KnowledgePortError::Unavailable(
            "db at 10.0.0.7 password=hunter2".into(),
        )));
        let payload = json!({"query": "q", "knowledge_base": "kb"});

        let refused = search(&payload, "acme", None, Some(&as_port(&refused))).await;
        assert_eq!(failure_code(&refused), codes::KNOWLEDGE_NOT_ALLOWED);

        let unavailable = search(&payload, "acme", None, Some(&as_port(&unavailable))).await;
        assert_eq!(failure_code(&unavailable), codes::SEARCH_FAILED);
        assert!(!unavailable.to_string().contains("hunter2"));
        assert!(!unavailable.to_string().contains("10.0.0.7"));
    }

    #[tokio::test]
    async fn a_blank_tenant_or_session_is_passed_as_none() {
        let port = FakePort::answering(Ok(vec![]));
        search(
            &json!({"query": "q", "knowledge_base": "kb"}),
            "  ",
            Some(" "),
            Some(&as_port(&port)),
        )
        .await;
        let seen = port.seen.lock().unwrap();
        assert_eq!(seen[0].0.tenant, None);
        assert_eq!(seen[0].0.session_id, None);
    }
}
