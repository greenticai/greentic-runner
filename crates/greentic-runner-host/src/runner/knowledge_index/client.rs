//! The two HTTP calls behind one retrieval — embed the query, search each
//! index — and the merge of their hits.
//!
//! Nothing here may put a credential, the query vector, or chunk text into an
//! error message or a log line.

use std::cmp::Ordering;

use base64::Engine as _;
use greentic_aw_runtime::knowledge::{KnowledgeError, KnowledgeResult, RetrievedChunk};
use serde::Deserialize;

use super::{DEFAULT_TEAM, IndexBinding};
use crate::runner::knowledge_ext::{MAX_CHUNK_CHARS, MAX_TOTAL_CHARS, truncate_chars};

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingItem>,
}

#[derive(Deserialize)]
struct EmbeddingItem {
    embedding: Vec<f32>,
}

#[derive(Deserialize)]
struct SearchResponse {
    chunks: Vec<Hit>,
}

/// One chunk as the index server returns it.
#[derive(Deserialize)]
struct Hit {
    text: String,
    score: f64,
    document_id: String,
    chunk_index: usize,
}

/// A hit tagged with the index that returned it.
pub(super) struct IndexedHit {
    index_id: String,
    hit: Hit,
}

fn embed_error(reason: impl std::fmt::Display) -> KnowledgeError {
    KnowledgeError::Backend(format!("embedding the query failed: {reason}"))
}

/// Embed `text` through the binding's OpenAI-compatible embeddings API.
pub(super) async fn embed(
    http: &reqwest::Client,
    binding: &IndexBinding,
    key: &str,
    text: &str,
) -> KnowledgeResult<Vec<f32>> {
    let url = format!(
        "{}/embeddings",
        binding.embedding_base_url.trim_end_matches('/')
    );
    let response = http
        .post(&url)
        .bearer_auth(key)
        .json(&serde_json::json!({ "model": binding.embedding_model, "input": [text] }))
        .send()
        .await
        .map_err(|e| embed_error(transport_reason(&e)))?;
    let status = response.status();
    if !status.is_success() {
        return Err(embed_error(format!("status {}", status.as_u16())));
    }
    let body: EmbeddingResponse = response
        .json()
        .await
        .map_err(|_| embed_error("the response body is not an embeddings list"))?;
    body.data
        .into_iter()
        .next()
        .map(|item| item.embedding)
        .filter(|vector| !vector.is_empty())
        .ok_or_else(|| embed_error("the response carries no embedding"))
}

/// Standard base64 of the vector's little-endian `f32` bytes.
pub(super) fn encode_vector(vector: &[f32]) -> String {
    base64::engine::general_purpose::STANDARD.encode(
        vector
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect::<Vec<u8>>(),
    )
}

/// Search every bound index in parallel. An index that fails is logged and
/// skipped; only when every index failed is the retrieval an error.
pub(super) async fn search_all(
    http: &reqwest::Client,
    binding: &IndexBinding,
    key: &str,
    query: &str,
    vector_b64: &str,
    limit: usize,
) -> KnowledgeResult<Vec<IndexedHit>> {
    let calls = binding
        .index_ids
        .iter()
        .map(|index_id| search_one(http, binding, key, index_id, query, vector_b64, limit));
    let results = futures::future::join_all(calls).await;

    let mut any_succeeded = false;
    let mut hits = Vec::new();
    for (index_id, result) in binding.index_ids.iter().zip(results) {
        match result {
            Ok(found) => {
                any_succeeded = true;
                hits.extend(found.into_iter().map(|hit| IndexedHit {
                    index_id: index_id.clone(),
                    hit,
                }));
            }
            Err(status) => {
                tracing::warn!(
                    index_id = %index_id,
                    status = %status,
                    "knowledge index search failed"
                );
            }
        }
    }
    if !any_succeeded {
        return Err(KnowledgeError::Backend(
            "every knowledge index search failed".to_string(),
        ));
    }
    Ok(hits)
}

/// One index's search. The error is a short status/reason for the log line.
async fn search_one(
    http: &reqwest::Client,
    binding: &IndexBinding,
    key: &str,
    index_id: &str,
    query: &str,
    vector_b64: &str,
    limit: usize,
) -> Result<Vec<Hit>, String> {
    let mut url = binding.endpoint.clone();
    url.set_query(None);
    url.set_fragment(None);
    url.path_segments_mut()
        .map_err(|()| "the endpoint cannot carry a path".to_string())?
        .clear()
        .extend(["v1", "indexes", index_id, "search"]);

    let response = http
        .post(url)
        .bearer_auth(key)
        .header("X-Greentic-Tenant", &binding.tenant)
        .header(
            "X-Greentic-Team",
            binding.team.as_deref().unwrap_or(DEFAULT_TEAM),
        )
        .json(&serde_json::json!({
            "query": query,
            "vector_b64": vector_b64,
            "limit": limit,
        }))
        .send()
        .await
        .map_err(|e| transport_reason(&e))?;
    let status = response.status();
    if !status.is_success() {
        return Err(status.as_u16().to_string());
    }
    let body: SearchResponse = response
        .json()
        .await
        .map_err(|_| "malformed response body".to_string())?;
    Ok(body.chunks)
}

/// Merge hits from every index: best score first (NaN last), `limit` of them,
/// then the same per-chunk and total character caps as `knowledge_ext`.
pub(super) fn merge(mut hits: Vec<IndexedHit>, limit: usize) -> Vec<RetrievedChunk> {
    hits.sort_by(|a, b| by_score_desc(a.hit.score, b.hit.score));

    let mut out = Vec::with_capacity(hits.len().min(limit));
    let mut budget = MAX_TOTAL_CHARS;
    for IndexedHit { index_id, hit } in hits.into_iter().take(limit) {
        if budget == 0 {
            break;
        }
        let text = truncate_chars(&hit.text, MAX_CHUNK_CHARS.min(budget));
        budget = budget.saturating_sub(text.chars().count());

        let mut metadata = serde_json::Map::new();
        metadata.insert("index_id".to_string(), serde_json::Value::String(index_id));
        out.push(RetrievedChunk {
            text,
            score: hit.score,
            doc_id: Some(hit.document_id),
            chunk_index: Some(hit.chunk_index),
            metadata,
        });
    }
    out
}

fn by_score_desc(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => b.partial_cmp(&a).unwrap_or(Ordering::Equal),
    }
}

/// A transport failure described without its URL (which is harmless) or any
/// header (which is not).
fn transport_reason(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timed out"
    } else if error.is_connect() {
        "could not connect"
    } else {
        "transport error"
    }
}
