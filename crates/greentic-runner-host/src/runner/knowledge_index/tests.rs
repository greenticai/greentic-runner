#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine as _;
use greentic_aw_runtime::config::MemoryProviderRef;
use greentic_aw_runtime::knowledge::{
    IngestOutcome, Knowledge, KnowledgeChunk, KnowledgeError, KnowledgeQuery, KnowledgeResult,
    RetrievedChunk,
};
use greentic_aw_runtime::scoped_secrets::secret_uri;
use greentic_secrets_lib::SecretsManager;
use greentic_types::TenantCtx;
use serde_json::json;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{ChronicleIndexKnowledge, PROVIDER_ID};

const INDEX_KEY: &str = "idx-key";
const EMBED_KEY: &str = "emb-key";

/// A secrets manager holding exactly the URIs it was given (shape of
/// `greentic-aw-runtime`'s own `MapSecrets` test impl).
struct MapSecrets {
    entries: HashMap<String, Vec<u8>>,
}

impl MapSecrets {
    fn with(pairs: &[(String, &str)]) -> Arc<Self> {
        Arc::new(Self {
            entries: pairs
                .iter()
                .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
                .collect(),
        })
    }
}

#[async_trait]
impl SecretsManager for MapSecrets {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        self.entries
            .get(path)
            .cloned()
            .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
    }
    async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
}

fn default_uri(key: &str) -> String {
    secret_uri("knowledge", "default", None, key).unwrap()
}

/// Both credentials at the tenant-default scope of the runtime tenant.
fn both_keys() -> Arc<MapSecrets> {
    MapSecrets::with(&[
        (default_uri("chronicle_index_key"), INDEX_KEY),
        (default_uri("embedding_key"), EMBED_KEY),
    ])
}

#[derive(Default)]
struct RecordingInner {
    searched: Mutex<Vec<String>>,
    ingested: Mutex<usize>,
}

#[async_trait]
impl Knowledge for RecordingInner {
    async fn ingest(
        &self,
        _tenant: &TenantCtx,
        chunks: Vec<KnowledgeChunk>,
    ) -> KnowledgeResult<IngestOutcome> {
        *self.ingested.lock().unwrap() += chunks.len();
        Ok(IngestOutcome::default())
    }

    async fn search(
        &self,
        _tenant: &TenantCtx,
        query: KnowledgeQuery,
    ) -> KnowledgeResult<Vec<RetrievedChunk>> {
        self.searched.lock().unwrap().push(query.query);
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
        greentic_types::TenantId::try_from("default").unwrap(),
    )
}

fn binding(provider: &str, params: serde_json::Value) -> MemoryProviderRef {
    MemoryProviderRef {
        provider: provider.to_string(),
        capability: "cap://dw.knowledge".to_string(),
        params: params.as_object().cloned().unwrap_or_default(),
        credential_ref: None,
    }
}

fn p(key: &str) -> String {
    format!("{PROVIDER_ID}.{key}")
}

/// A binding naming `index_ids` on `server`, embedding through the same server.
fn index_binding(server: &MockServer, index_ids: &[&str], team: Option<&str>) -> MemoryProviderRef {
    let mut params = serde_json::Map::new();
    params.insert(p("endpoint"), json!(server.uri()));
    params.insert(p("tenant"), json!("acme"));
    if let Some(team) = team {
        params.insert(p("team"), json!(team));
    }
    params.insert(p("index_ids"), json!(index_ids));
    params.insert(p("embedding_provider"), json!("openai"));
    params.insert(p("embedding_model"), json!("text-embedding-3-small"));
    params.insert(
        p("embedding_base_url"),
        json!(format!("{}/v1/", server.uri())),
    );
    binding(PROVIDER_ID, serde_json::Value::Object(params))
}

fn query(text: &str, limit: usize) -> KnowledgeQuery {
    KnowledgeQuery {
        query: text.to_string(),
        limit: Some(limit),
    }
}

fn vector_b64(vector: &[f32]) -> String {
    base64::engine::general_purpose::STANDARD.encode(
        vector
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect::<Vec<u8>>(),
    )
}

async fn mount_embeddings(server: &MockServer, vector: &[f32]) {
    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .and(header(
            "authorization",
            format!("Bearer {EMBED_KEY}").as_str(),
        ))
        .and(body_partial_json(
            json!({"model": "text-embedding-3-small"}),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"data": [{"embedding": vector, "index": 0}]})),
        )
        .mount(server)
        .await;
}

async fn mount_index(server: &MockServer, index_id: &str, response: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(path(format!("/v1/indexes/{index_id}/search")))
        .respond_with(response)
        .mount(server)
        .await;
}

fn chunks(items: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "chunks": items }))
}

fn backend(inner: Option<Arc<dyn Knowledge>>, secrets: Arc<MapSecrets>) -> ChronicleIndexKnowledge {
    ChronicleIndexKnowledge::new(inner, Some(secrets), None)
}

fn backend_message(err: KnowledgeError) -> String {
    match err {
        KnowledgeError::Backend(message) => message,
        other => panic!("expected a Backend error, got {other:?}"),
    }
}

#[tokio::test]
async fn other_providers_and_ingest_reach_the_wrapped_backend() {
    let server = MockServer::start().await;
    let inner = Arc::new(RecordingInner::default());
    let adapter = backend(Some(inner.clone()), both_keys());

    let other = binding(
        "provider.knowledge.chronicle",
        json!({ p("endpoint"): server.uri() }),
    );
    let got = adapter
        .search_bound(&tenant(), query("hello", 3), Some(&other))
        .await
        .unwrap();
    assert_eq!(got[0].text, "from the wrapped backend");

    let got = adapter
        .search_bound(&tenant(), query("unbound", 3), None)
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(*inner.searched.lock().unwrap(), vec!["hello", "unbound"]);

    adapter
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
        .unwrap();
    assert_eq!(*inner.ingested.lock().unwrap(), 1);

    let requests = server.received_requests().await.unwrap_or_default();
    assert!(requests.is_empty(), "no HTTP for another provider");

    let bare = backend(None, both_keys());
    assert!(matches!(
        bare.search_bound(&tenant(), query("q", 1), Some(&other))
            .await,
        Err(KnowledgeError::NotConfigured)
    ));
    assert!(bare.ingest(&tenant(), vec![]).await.is_ok());
}

#[tokio::test]
async fn searches_the_index_with_the_embedded_query() {
    let server = MockServer::start().await;
    mount_embeddings(&server, &[0.5, -1.0]).await;
    Mock::given(method("POST"))
        .and(path("/v1/indexes/kb1/search"))
        .and(header("authorization", format!("Bearer {INDEX_KEY}").as_str()))
        .and(header("x-greentic-tenant", "acme"))
        .and(header("x-greentic-team", "general"))
        .and(body_partial_json(json!({
            "query": "refund policy",
            "limit": 3,
            "vector_b64": vector_b64(&[0.5, -1.0]),
        })))
        .respond_with(chunks(json!([
            {"text": "refunds within 30 days", "score": 0.9, "document_id": "refunds", "chunk_index": 2},
            {"text": "no refunds on sale items", "score": 0.4, "document_id": "sale", "chunk_index": 0},
        ])))
        .expect(1)
        .mount(&server)
        .await;

    let got = backend(None, both_keys())
        .search_bound(
            &tenant(),
            query("refund policy", 3),
            Some(&index_binding(&server, &["kb1"], None)),
        )
        .await
        .unwrap();

    assert_eq!(got.len(), 2);
    assert_eq!(got[0].text, "refunds within 30 days");
    assert_eq!(got[0].doc_id.as_deref(), Some("refunds"));
    assert_eq!(got[0].chunk_index, Some(2));
    assert_eq!(got[0].metadata["index_id"], "kb1");
    assert_eq!(got[1].doc_id.as_deref(), Some("sale"));
    assert_eq!(got[1].chunk_index, Some(0));
}

#[tokio::test]
async fn merges_by_score_and_respects_limit_and_caps() {
    let server = MockServer::start().await;
    mount_embeddings(&server, &[1.0]).await;
    let long = "x".repeat(5_000);
    mount_index(
        &server,
        "a",
        chunks(json!([
            {"text": "a1", "score": 0.9, "document_id": "a", "chunk_index": 0},
            {"text": "a2", "score": 0.5, "document_id": "a", "chunk_index": 1},
        ])),
    )
    .await;
    mount_index(
        &server,
        "b",
        chunks(json!([
            {"text": long, "score": 0.7, "document_id": "b", "chunk_index": 0},
            {"text": "b2", "score": 0.1, "document_id": "b", "chunk_index": 1},
        ])),
    )
    .await;

    let got = backend(None, both_keys())
        .search_bound(
            &tenant(),
            query("q", 3),
            Some(&index_binding(&server, &["a", "b"], None)),
        )
        .await
        .unwrap();

    let scores: Vec<f64> = got.iter().map(|c| c.score).collect();
    assert_eq!(
        scores,
        vec![0.9, 0.7, 0.5],
        "top three by score across both"
    );
    assert_eq!(got[1].metadata["index_id"], "b");
    assert_eq!(
        got[1].text.chars().count(),
        crate::runner::knowledge_ext::MAX_CHUNK_CHARS
    );
}

#[tokio::test]
async fn one_failing_index_does_not_hide_the_others() {
    let server = MockServer::start().await;
    mount_embeddings(&server, &[1.0]).await;
    mount_index(&server, "bad", ResponseTemplate::new(500)).await;
    mount_index(
        &server,
        "good",
        chunks(json!([{"text": "kept", "score": 0.3, "document_id": "g", "chunk_index": 0}])),
    )
    .await;

    let got = backend(None, both_keys())
        .search_bound(
            &tenant(),
            query("q", 5),
            Some(&index_binding(&server, &["bad", "good"], None)),
        )
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].text, "kept");
}

#[tokio::test]
async fn every_index_failing_is_a_backend_error() {
    let server = MockServer::start().await;
    mount_embeddings(&server, &[1.0]).await;
    let not_found =
        ResponseTemplate::new(404).set_body_json(json!({"error": {"code": "index_not_found"}}));
    mount_index(&server, "a", not_found.clone()).await;
    mount_index(&server, "b", not_found).await;

    let err = backend(None, both_keys())
        .search_bound(
            &tenant(),
            query("q", 5),
            Some(&index_binding(&server, &["a", "b"], None)),
        )
        .await
        .expect_err("no index answered");
    let message = backend_message(err);
    assert!(
        message.contains("every knowledge index search failed"),
        "{message}"
    );
    assert!(!message.contains(INDEX_KEY) && !message.contains(EMBED_KEY));
}

#[tokio::test]
async fn missing_credential_is_a_backend_error_naming_the_uris() {
    let server = MockServer::start().await;
    let secrets = MapSecrets::with(&[(default_uri("chronicle_index_key"), INDEX_KEY)]);
    let err = backend(None, secrets)
        .search_bound(
            &tenant(),
            query("q", 5),
            Some(&index_binding(&server, &["kb1"], None)),
        )
        .await
        .expect_err("no embedding key");
    let message = backend_message(err);
    assert!(message.contains("secrets://"), "{message}");
    assert!(!message.contains(INDEX_KEY), "{message}");
    let requests = server.received_requests().await.unwrap_or_default();
    assert!(
        requests.is_empty(),
        "nothing is called without both credentials"
    );
}

#[tokio::test]
async fn malformed_binding_is_a_backend_error() {
    let server = MockServer::start().await;
    let adapter = backend(None, both_keys());

    let mut empty_ids = index_binding(&server, &["kb1"], None);
    empty_ids.params.insert(p("index_ids"), json!([]));
    let message = backend_message(
        adapter
            .search_bound(&tenant(), query("q", 1), Some(&empty_ids))
            .await
            .expect_err("no index ids"),
    );
    assert!(
        message.contains("provider.knowledge.chronicle-index.index_ids"),
        "{message}"
    );

    let mut cohere = index_binding(&server, &["kb1"], None);
    cohere
        .params
        .insert(p("embedding_provider"), json!("cohere"));
    let message = backend_message(
        adapter
            .search_bound(&tenant(), query("q", 1), Some(&cohere))
            .await
            .expect_err("unsupported provider"),
    );
    assert!(
        message.contains("unsupported embedding provider `cohere`"),
        "{message}"
    );
}

#[tokio::test]
async fn secrets_are_read_under_the_team_then_default() {
    let server = MockServer::start().await;
    mount_embeddings(&server, &[1.0]).await;
    Mock::given(method("POST"))
        .and(path("/v1/indexes/kb1/search"))
        .and(header(
            "authorization",
            format!("Bearer {INDEX_KEY}").as_str(),
        ))
        .and(header("x-greentic-team", "sales"))
        .respond_with(chunks(json!([
            {"text": "team hit", "score": 0.5, "document_id": "d", "chunk_index": 0},
        ])))
        .mount(&server)
        .await;

    let secrets = MapSecrets::with(&[
        (
            secret_uri("knowledge", "default", Some("sales"), "chronicle_index_key").unwrap(),
            INDEX_KEY,
        ),
        (default_uri("embedding_key"), EMBED_KEY),
    ]);
    let got = backend(None, secrets)
        .search_bound(
            &tenant(),
            query("q", 2),
            Some(&index_binding(&server, &["kb1"], Some("sales"))),
        )
        .await
        .unwrap();
    assert_eq!(got[0].text, "team hit");
}
