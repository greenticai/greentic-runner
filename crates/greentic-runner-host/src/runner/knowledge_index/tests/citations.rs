#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

async fn search(hits: serde_json::Value) -> Vec<RetrievedChunk> {
    let server = MockServer::start().await;
    mount_embeddings(&server, &[0.5, -1.0]).await;
    mount_index(&server, "kb1", chunks(hits)).await;
    backend(None, both_keys())
        .search_bound(
            &tenant(),
            query("refund policy", 5),
            Some(&index_binding(&server, &["kb1"], None)),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn the_citation_fields_of_a_hit_reach_the_chunk_metadata() {
    let got = search(json!([{
        "text": "refunds within 30 days", "score": 0.9, "document_id": "refunds",
        "chunk_index": 2, "title": "Refund policy.pdf",
        "source_url": "https://drive.google.com/f/1", "section": "Policy > Refunds"
    }]))
    .await;
    let m = &got[0].metadata;
    assert_eq!(m["title"], "Refund policy.pdf");
    assert_eq!(m["source_url"], "https://drive.google.com/f/1");
    assert_eq!(m["section"], "Policy > Refunds");
    assert_eq!(m["index_id"], "kb1");
    // The text rule is unchanged: still the chunk's own text, nothing added.
    assert_eq!(got[0].text, "refunds within 30 days");
}

#[tokio::test]
async fn a_hit_from_an_older_server_has_no_citation_keys() {
    let got = search(json!([
        {"text": "t", "score": 0.5, "document_id": "d", "chunk_index": 0}
    ]))
    .await;
    let m = &got[0].metadata;
    assert!(
        !m.contains_key("title") && !m.contains_key("source_url") && !m.contains_key("section")
    );
}

#[tokio::test]
async fn a_link_that_is_not_https_is_dropped_but_the_rest_stays() {
    let got = search(json!([{
        "text": "t", "score": 0.5, "document_id": "d", "chunk_index": 0,
        "title": "T", "source_url": "javascript:alert(1)", "section": "S"
    }]))
    .await;
    let m = &got[0].metadata;
    assert_eq!(m["title"], "T");
    assert_eq!(m["section"], "S");
    assert!(!m.contains_key("source_url"));
}

#[tokio::test]
async fn the_recorded_trail_step_carries_the_fields_next_to_the_chunk() {
    let got = search(json!([{
        "text": "t", "score": 0.5, "document_id": "d", "chunk_index": 1,
        "title": "T", "source_url": "https://e.com/a", "section": "S"
    }]))
    .await;
    let step =
        serde_json::to_value(greentic_aw_runtime::AgentStep::KnowledgeRetrieval { chunks: got })
            .unwrap();
    let chunk = &step["chunks"][0];
    assert_eq!(chunk["doc_id"], "d");
    assert_eq!(chunk["metadata"]["title"], "T");
    assert_eq!(chunk["metadata"]["source_url"], "https://e.com/a");
    assert_eq!(chunk["metadata"]["section"], "S");
}
