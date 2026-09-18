//! Reads the knowledge corpus baked into a pack (W5) and turns it into
//! [`KnowledgeChunk`]s for first-boot ingest (W4 4c). Compiled only with the
//! `knowledge-chronicle` feature.
//!
//! The designer bakes a `knowledge_corpus.json` annotation at the pack root plus
//! the raw document text as `assets/knowledge/*.txt`. The annotation is NOT
//! pre-chunked — it lists the asset files — so this module reads each asset and
//! splits it into overlapping character windows. Chunk ingest downstream is
//! idempotent (Chronicle keys chunks by a deterministic content hash), so
//! re-reading the same corpus on every boot is safe.
//!
//! When the pack was built with precomputed embeddings, a corpus file entry also
//! carries `vectors_asset_path` pointing at a per-doc `assets/knowledge/<id>.vec.json`
//! that lists the already-chunked text paired with its embedding vector. For those
//! files this module emits the pre-chunked text + vector verbatim (skipping both
//! the character-window re-chunk and downstream re-embedding). If that asset is
//! missing or unparseable we log and fall back to the `.txt` re-chunk path so a
//! damaged vectors asset never aborts boot.
//!
//! Precomputed vectors are only usable when they match the embedder the runner is
//! actually configured with — see [`EmbeddingExpectation`]. A mismatch takes the
//! same `.txt` fallback: the text is re-embedded with the configured model, which
//! costs latency but is always correct.
//!
//! KNOWN LIMITATION (pre-existing, narrowed but not closed here). The two paths
//! chunk differently — the producer at 1000/100, this module's `.txt` window at
//! [`CHUNK_CHARS`]/[`CHUNK_OVERLAP_CHARS`] — so they yield different chunk
//! COUNTS for the same document. Chunk identity downstream is
//! `hash(group_id, doc_id, chunk_index)` with no content in the hash and no
//! delete path, so a document that switches paths after a successful ingest
//! leaves its surplus tail indices behind as orphans: stale vectors that still
//! match queries. Closing this needs a delete-by-`doc_id` sweep before ingest
//! (in the store layer, not here) or a chunker discriminator in the chunk id.

use std::sync::Arc;

use greentic_aw_runtime::knowledge::KnowledgeChunk;
use serde::Deserialize;

use crate::pack::PackRuntime;

const CORPUS_SIDECAR: &str = "knowledge_corpus.json";
/// Window size (in characters) for splitting a document into chunks.
const CHUNK_CHARS: usize = 1500;
/// Overlap (in characters) carried between consecutive chunks so a passage that
/// straddles a window boundary still appears intact in at least one chunk.
const CHUNK_OVERLAP_CHARS: usize = 200;

/// What the configured embedder will actually produce, and therefore what a
/// precomputed vector must match to be usable.
///
/// Both fields are load-bearing, and each guards a DIFFERENT silent failure:
/// - `model`: a vector from another model lives in an unrelated embedding space.
///   Cosine similarity against it still returns a ranked list, so the failure is
///   invisible — just quietly wrong neighbours. Dimension alone cannot catch this
///   (`text-embedding-3-small` and `-ada-002` are both 1536).
/// - `dim`: the backend REJECTS a precomputed vector whose length differs, and
///   that error aborts the entire ingest batch — so one stale vector costs the
///   worker its whole corpus, logged only at `warn`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EmbeddingExpectation<'a> {
    pub model: &'a str,
    pub dim: usize,
}

/// The subset of the `knowledge_corpus.json` annotation this reader needs. Other
/// fields (version, strategy, provider ids, top_k, counters) are ignored.
#[derive(Debug, Deserialize)]
struct CorpusAnnotation {
    #[serde(default)]
    files: Vec<CorpusFile>,
}

#[derive(Debug, Deserialize)]
struct CorpusFile {
    /// Archive-relative path of the baked text, e.g. `assets/knowledge/faq.txt`.
    asset_path: String,
    /// Human-facing source name, e.g. `faq.pdf`. Used as the chunk `doc_id`.
    #[serde(default)]
    original_name: String,
    /// Archive-relative path of the precomputed vectors asset, e.g.
    /// `assets/knowledge/faq.vec.json`. `None` (the default, backward-compatible)
    /// means the pack carries no precomputed vectors for this file and the runner
    /// re-chunks + re-embeds the `.txt` as before.
    #[serde(default)]
    vectors_asset_path: Option<String>,
}

/// The precomputed-vectors sidecar (`assets/knowledge/<id>.vec.json`) baked next
/// to a document's `.txt`. Holds the document pre-chunked at build time, each
/// chunk paired with its embedding vector, so the runner can emit chunk+vector
/// verbatim instead of re-chunking and re-embedding.
#[derive(Debug, Deserialize)]
struct VectorsAsset {
    /// Embedding model the vectors were produced with (e.g. `text-embedding-3-small`).
    ///
    /// LOAD-BEARING — do not drop or loosen: this is the only guard against
    /// serving vectors from a foreign embedding space, which fails silently
    /// (wrong neighbours, no error). The dimension cannot substitute for it.
    /// See [`precomputed_rejection`].
    #[serde(default)]
    embedding_model: String,
    /// Dimensionality of each `vector`. Informational: logged when the asset
    /// loads. Deliberately NOT trusted as the guard — it is the asset's own
    /// claim, whereas each vector's real length is what the backend measures.
    #[serde(default)]
    dims: usize,
    #[serde(default)]
    chunks: Vec<VectorChunk>,
}

#[derive(Debug, Deserialize)]
struct VectorChunk {
    /// Zero-based position of this chunk within the source document.
    #[serde(default)]
    chunk_index: usize,
    /// Verbatim chunk text as chunked at build time (do NOT re-window this).
    #[serde(default)]
    chunk_text: String,
    /// Precomputed embedding for `chunk_text`.
    #[serde(default)]
    vector: Vec<f32>,
}

/// Collect and chunk the knowledge corpus across every pack. Returns an empty
/// vec when no pack carries a corpus (the common case), so callers can skip ingest
/// cheaply. Malformed annotations / missing assets are logged and skipped rather
/// than aborting the whole runtime.
///
/// `expected` describes the embedder the caller will ingest with; precomputed
/// vectors that do not match it are discarded in favour of re-embedding the text.
/// `None` means "no embedder configured" — the caller will skip ingest entirely,
/// so nothing is validated.
pub(crate) fn collect(
    packs: &[Arc<PackRuntime>],
    expected: Option<EmbeddingExpectation<'_>>,
) -> Vec<KnowledgeChunk> {
    let mut chunks = Vec::new();
    for pack in packs {
        let Some(bytes) = pack.read_pack_file(CORPUS_SIDECAR) else {
            continue;
        };
        let pack_id = pack.metadata().pack_id.clone();
        let annotation: CorpusAnnotation = match serde_json::from_slice(&bytes) {
            Ok(annotation) => annotation,
            Err(error) => {
                tracing::warn!(pack_id = %pack_id, error = %error, "knowledge: malformed knowledge_corpus.json; skipping pack corpus");
                continue;
            }
        };
        for file in &annotation.files {
            collect_file(&mut chunks, &pack_id, file, expected, &|name| {
                pack.read_pack_file(name)
            });
        }
    }
    chunks
}

/// Why a loaded vectors asset cannot be used, as operator-facing text. `None`
/// means it is usable.
///
/// The per-chunk length check is deliberately not a shortcut past `asset.dims`:
/// `dims` is the asset's own CLAIM, while the length is what the backend will
/// actually measure. A wrong claim with correct vectors is harmless; correct
/// claim with wrong vectors is the case that drops the corpus.
fn precomputed_rejection(
    asset: &VectorsAsset,
    expected: EmbeddingExpectation<'_>,
) -> Option<String> {
    // A blank model is rejected rather than waved through. It cannot come from a
    // real pack — the producer's `PrecomputedVectors::embedding_model` is a
    // required `String` and the designer filters blanks before constructing it —
    // so the only assets an escape hatch here would admit are hand-crafted or
    // the output of a future bug. That is exactly the provenance to trust least,
    // and the model check has no backstop: the dimension cannot catch a
    // same-width foreign model.
    if asset.embedding_model != expected.model {
        return Some(format!(
            "built with model {:?} but the runner is configured for {}",
            asset.embedding_model, expected.model
        ));
    }
    let mismatched = asset
        .chunks
        .iter()
        .find(|chunk| chunk.vector.len() != expected.dim)?;
    Some(format!(
        "chunk_index {} has dimension {} but the runner's embedder produces {}",
        mismatched.chunk_index,
        mismatched.vector.len(),
        expected.dim
    ))
}

/// Emit the chunks for a single corpus file. When the file carries precomputed
/// vectors (`vectors_asset_path`) that load successfully, the pre-chunked
/// text+vector pairs are pushed verbatim. Otherwise — no vectors asset, or an
/// unreadable/unparseable/empty one — this falls back to reading the `.txt` and
/// re-chunking it (with `embedding: None`). `read` resolves an archive-relative
/// path to its bytes (backed by [`PackRuntime::read_pack_file`] in production).
fn collect_file<R: Fn(&str) -> Option<Vec<u8>>>(
    out: &mut Vec<KnowledgeChunk>,
    pack_id: &str,
    file: &CorpusFile,
    expected: Option<EmbeddingExpectation<'_>>,
    read: &R,
) {
    let doc_id = if file.original_name.is_empty() {
        file.asset_path.clone()
    } else {
        file.original_name.clone()
    };

    if let Some(vec_path) = file.vectors_asset_path.as_deref() {
        match load_vectors(read, pack_id, vec_path) {
            Some(asset) if !asset.chunks.is_empty() => {
                match expected.and_then(|expected| precomputed_rejection(&asset, expected)) {
                    None => {
                        append_vector_chunks(out, pack_id, &file.asset_path, &doc_id, asset);
                        return;
                    }
                    Some(reason) => tracing::warn!(
                        pack_id = %pack_id,
                        vectors_asset = %vec_path,
                        fallback_asset = %file.asset_path,
                        %reason,
                        "knowledge: precomputed vectors do not match the configured embedder; \
                         falling back to text re-chunk"
                    ),
                }
            }
            _ => {
                // Missing / unparseable / empty vectors asset: fall back to the
                // `.txt` re-chunk path rather than dropping the document or
                // aborting boot.
                tracing::warn!(
                    pack_id = %pack_id,
                    vectors_asset = %vec_path,
                    fallback_asset = %file.asset_path,
                    "knowledge: precomputed vectors asset unusable; falling back to text re-chunk"
                );
            }
        }
    }

    let Some(raw) = read(&file.asset_path) else {
        tracing::warn!(pack_id = %pack_id, asset = %file.asset_path, "knowledge: corpus asset missing from pack; skipping");
        return;
    };
    let text = match String::from_utf8(raw) {
        Ok(text) => text,
        Err(_) => {
            tracing::warn!(pack_id = %pack_id, asset = %file.asset_path, "knowledge: corpus asset is not UTF-8; skipping");
            return;
        }
    };
    append_chunks(out, pack_id, &file.asset_path, &doc_id, &text);
}

/// Read and parse a precomputed-vectors asset. Returns `None` (after logging) when
/// the asset is absent or does not parse, so the caller can fall back cleanly.
fn load_vectors<R: Fn(&str) -> Option<Vec<u8>>>(
    read: &R,
    pack_id: &str,
    vec_path: &str,
) -> Option<VectorsAsset> {
    let bytes = read(vec_path)?;
    match serde_json::from_slice::<VectorsAsset>(&bytes) {
        Ok(asset) => {
            tracing::debug!(
                pack_id = %pack_id,
                vectors_asset = %vec_path,
                embedding_model = %asset.embedding_model,
                dims = asset.dims,
                chunks = asset.chunks.len(),
                "knowledge: loaded precomputed vectors asset"
            );
            Some(asset)
        }
        Err(error) => {
            tracing::warn!(
                pack_id = %pack_id,
                vectors_asset = %vec_path,
                error = %error,
                "knowledge: failed to parse precomputed vectors asset"
            );
            None
        }
    }
}

/// Push one [`KnowledgeChunk`] per precomputed chunk, carrying its embedding
/// verbatim. Mirrors [`append_chunks`]'s `doc_id`/metadata shape so downstream
/// ingest treats vectored and re-chunked documents identically apart from the
/// pre-supplied vector.
fn append_vector_chunks(
    out: &mut Vec<KnowledgeChunk>,
    pack_id: &str,
    asset_path: &str,
    doc_id: &str,
    asset: VectorsAsset,
) {
    for chunk in asset.chunks {
        let mut metadata = serde_json::Map::new();
        metadata.insert("pack_id".into(), pack_id.into());
        metadata.insert("asset_path".into(), asset_path.into());
        out.push(KnowledgeChunk {
            doc_id: doc_id.to_string(),
            chunk_index: chunk.chunk_index,
            text: chunk.chunk_text,
            metadata,
            // Precomputed at build time; the backend uses this directly instead
            // of embedding `text` at ingest.
            embedding: Some(chunk.vector),
        });
    }
}

/// Split `text` into overlapping character windows and push one [`KnowledgeChunk`]
/// per window. Whitespace-only documents produce no chunks.
fn append_chunks(
    out: &mut Vec<KnowledgeChunk>,
    pack_id: &str,
    asset_path: &str,
    doc_id: &str,
    text: &str,
) {
    if text.trim().is_empty() {
        return;
    }
    // Character-based windowing keeps multi-byte UTF-8 boundaries valid (slicing
    // by byte offset could split a codepoint).
    let chars: Vec<char> = text.chars().collect();
    let step = CHUNK_CHARS.saturating_sub(CHUNK_OVERLAP_CHARS).max(1);
    let mut start = 0;
    let mut chunk_index = 0;
    while start < chars.len() {
        let end = (start + CHUNK_CHARS).min(chars.len());
        let window: String = chars[start..end].iter().collect();
        if !window.trim().is_empty() {
            let mut metadata = serde_json::Map::new();
            metadata.insert("pack_id".into(), pack_id.into());
            metadata.insert("asset_path".into(), asset_path.into());
            out.push(KnowledgeChunk {
                doc_id: doc_id.to_string(),
                chunk_index,
                text: window,
                metadata,
                // Pack-baked text carries no pre-computed vector; the backend
                // embeds it at ingest time.
                embedding: None,
            });
            chunk_index += 1;
        }
        if end == chars.len() {
            break;
        }
        start += step;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_is_a_single_chunk() {
        let mut out = Vec::new();
        append_chunks(
            &mut out,
            "p",
            "assets/knowledge/a.txt",
            "a.txt",
            "hello world",
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].doc_id, "a.txt");
        assert_eq!(out[0].chunk_index, 0);
        assert_eq!(out[0].text, "hello world");
        assert_eq!(
            out[0].metadata.get("asset_path").and_then(|v| v.as_str()),
            Some("assets/knowledge/a.txt")
        );
    }

    #[test]
    fn long_text_windows_with_overlap_and_sequential_indices() {
        let text = "x".repeat(CHUNK_CHARS * 2 + 100);
        let mut out = Vec::new();
        append_chunks(&mut out, "p", "a", "doc", &text);
        assert!(
            out.len() >= 3,
            "expected multiple windows, got {}",
            out.len()
        );
        for (i, c) in out.iter().enumerate() {
            assert_eq!(c.chunk_index, i, "chunk indices must be sequential");
            assert!(c.text.chars().count() <= CHUNK_CHARS);
        }
    }

    #[test]
    fn whitespace_only_text_produces_no_chunks() {
        let mut out = Vec::new();
        append_chunks(&mut out, "p", "a", "doc", "   \n\t  ");
        assert!(out.is_empty());
    }

    fn corpus_file(
        asset_path: &str,
        original_name: &str,
        vectors_asset_path: Option<&str>,
    ) -> CorpusFile {
        CorpusFile {
            asset_path: asset_path.to_string(),
            original_name: original_name.to_string(),
            vectors_asset_path: vectors_asset_path.map(str::to_string),
        }
    }

    /// Matches the vectors-asset fixtures below (`text-embedding-3-small`,
    /// 3-component vectors), i.e. a correctly-configured runner.
    fn matching_expectation() -> EmbeddingExpectation<'static> {
        EmbeddingExpectation {
            model: "text-embedding-3-small",
            dim: 3,
        }
    }

    /// Build a vectors asset whose chunks are `dim`-dimensional.
    fn vectors_asset_bytes(model: &str, dims: usize, vector: Vec<f32>) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "embedding_model": model,
            "dims": dims,
            "chunks": [
                { "chunk_index": 0, "chunk_text": "first chunk", "vector": vector }
            ]
        }))
        .unwrap()
    }

    fn read_pair(vec_bytes: Vec<u8>) -> impl Fn(&str) -> Option<Vec<u8>> {
        move |name: &str| match name {
            "assets/knowledge/a.vec.json" => Some(vec_bytes.clone()),
            "assets/knowledge/a.txt" => Some(b"hello world".to_vec()),
            _ => None,
        }
    }

    fn vectored_file() -> CorpusFile {
        corpus_file(
            "assets/knowledge/a.txt",
            "a.txt",
            Some("assets/knowledge/a.vec.json"),
        )
    }

    /// Vectors built with a DIFFERENT model must be discarded even though their
    /// dimension matches — the vectors live in an unrelated embedding space, and
    /// cosine similarity would silently return wrong neighbours rather than fail.
    #[test]
    fn model_mismatch_falls_back_to_text_rechunk() {
        let read = read_pair(vectors_asset_bytes(
            "text-embedding-ada-002",
            3,
            vec![0.5, 0.25, 0.125],
        ));
        let mut out = Vec::new();
        collect_file(
            &mut out,
            "p",
            &vectored_file(),
            Some(matching_expectation()),
            &read,
        );

        assert_eq!(out.len(), 1, "must re-chunk the text instead");
        assert_eq!(out[0].text, "hello world");
        assert_eq!(
            out[0].embedding, None,
            "the foreign-model vector must be dropped so the backend re-embeds"
        );
    }

    /// The headline regression: a dimension mismatch is REJECTED by the backend,
    /// and that error aborts the whole ingest batch — so one stale vector used to
    /// cost the worker its entire corpus. Falling back keeps the document.
    ///
    /// Shape of the real case (a 1536-wide asset against a narrower embedder),
    /// scaled to the fixture's 3-wide expectation.
    #[test]
    fn dim_mismatch_falls_back_to_text_rechunk() {
        let read = read_pair(vectors_asset_bytes(
            "text-embedding-3-small",
            1536,
            vec![0.5; 1536],
        ));
        let mut out = Vec::new();
        collect_file(
            &mut out,
            "p",
            &vectored_file(),
            Some(matching_expectation()),
            &read,
        );

        assert_eq!(out.len(), 1, "the document must survive as re-chunked text");
        assert_eq!(out[0].text, "hello world");
        assert_eq!(
            out[0].embedding, None,
            "the wrong-width vector must be dropped so the backend re-embeds"
        );
    }

    /// A blank `embedding_model` fails CLOSED. It cannot come from a real pack
    /// (the producer's field is a required String and the designer filters
    /// blanks), so waving it through would only admit hand-crafted or
    /// bug-produced assets — with no dimension backstop to catch a same-width
    /// foreign model.
    #[test]
    fn blank_model_falls_back_to_text_rechunk() {
        let read = read_pair(vectors_asset_bytes("", 3, vec![0.5, 0.25, 0.125]));
        let mut out = Vec::new();
        collect_file(
            &mut out,
            "p",
            &vectored_file(),
            Some(matching_expectation()),
            &read,
        );

        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].embedding, None,
            "unverifiable provenance must not be trusted"
        );
    }

    /// One bad chunk among good ones rejects the WHOLE asset — the backend
    /// aborts its batch on the first bad vector, so a partial accept would be a
    /// corpus-dropping trap. Exercises `find` across a heterogeneous list, which
    /// the single-chunk fixtures never do.
    #[test]
    fn one_wrong_width_chunk_rejects_the_whole_asset() {
        let vec_bytes = serde_json::to_vec(&serde_json::json!({
            "embedding_model": "text-embedding-3-small",
            "dims": 3,
            "chunks": [
                { "chunk_index": 0, "chunk_text": "good", "vector": [0.5, 0.25, 0.125] },
                { "chunk_index": 1, "chunk_text": "bad", "vector": [0.5, 0.25] }
            ]
        }))
        .unwrap();
        let read = read_pair(vec_bytes);
        let mut out = Vec::new();
        collect_file(
            &mut out,
            "p",
            &vectored_file(),
            Some(matching_expectation()),
            &read,
        );

        assert_eq!(
            out.len(),
            1,
            "must re-chunk the text, not emit the one good precomputed chunk"
        );
        assert_eq!(out[0].text, "hello world");
        assert_eq!(out[0].embedding, None);
    }

    /// A wrong `dims` CLAIM with correct vectors is harmless — the claim is not
    /// what the backend measures, the vector length is.
    #[test]
    fn wrong_dims_claim_with_correct_vectors_is_accepted() {
        let read = read_pair(vectors_asset_bytes(
            "text-embedding-3-small",
            999,
            vec![0.5, 0.25, 0.125],
        ));
        let mut out = Vec::new();
        collect_file(
            &mut out,
            "p",
            &vectored_file(),
            Some(matching_expectation()),
            &read,
        );

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].embedding, Some(vec![0.5, 0.25, 0.125]));
    }

    /// No embedder configured => the caller skips ingest entirely, so there is
    /// nothing to validate against and the asset is taken verbatim.
    #[test]
    fn no_expectation_accepts_precomputed_verbatim() {
        let read = read_pair(vectors_asset_bytes(
            "some-other-model",
            3,
            vec![0.5, 0.25, 0.125],
        ));
        let mut out = Vec::new();
        collect_file(&mut out, "p", &vectored_file(), None, &read);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].embedding, Some(vec![0.5, 0.25, 0.125]));
    }

    #[test]
    fn vectors_asset_emits_precomputed_chunks_without_rechunking() {
        // Vectors chosen to be exactly representable in f32 so JSON round-trip
        // equality is stable.
        let vec_json = serde_json::json!({
            "embedding_model": "text-embedding-3-small",
            "dims": 3,
            "chunks": [
                { "chunk_index": 0, "chunk_text": "first chunk", "vector": [0.5, 0.25, 0.125] },
                { "chunk_index": 1, "chunk_text": "second chunk", "vector": [1.0, -2.0, 0.75] }
            ]
        });
        let vec_bytes = serde_json::to_vec(&vec_json).unwrap();
        // A `.txt` long enough to yield several windows *if* it were re-chunked —
        // proving the vectors path bypasses `append_chunks`.
        let long_txt = "z".repeat(CHUNK_CHARS * 3).into_bytes();
        let read = |name: &str| -> Option<Vec<u8>> {
            match name {
                "assets/knowledge/faq.vec.json" => Some(vec_bytes.clone()),
                "assets/knowledge/faq.txt" => Some(long_txt.clone()),
                _ => None,
            }
        };

        let file = corpus_file(
            "assets/knowledge/faq.txt",
            "faq.pdf",
            Some("assets/knowledge/faq.vec.json"),
        );
        let mut out = Vec::new();
        collect_file(
            &mut out,
            "pack-1",
            &file,
            Some(matching_expectation()),
            &read,
        );

        assert_eq!(
            out.len(),
            2,
            "must emit exactly the 2 precomputed chunks, not re-chunk the .txt"
        );
        assert_eq!(out[0].doc_id, "faq.pdf");
        assert_eq!(out[0].chunk_index, 0);
        assert_eq!(out[0].text, "first chunk");
        assert_eq!(out[0].embedding, Some(vec![0.5, 0.25, 0.125]));
        assert_eq!(out[1].chunk_index, 1);
        assert_eq!(out[1].text, "second chunk");
        assert_eq!(out[1].embedding, Some(vec![1.0, -2.0, 0.75]));
        assert_eq!(
            out[0].metadata.get("pack_id").and_then(|v| v.as_str()),
            Some("pack-1")
        );
        assert_eq!(
            out[0].metadata.get("asset_path").and_then(|v| v.as_str()),
            Some("assets/knowledge/faq.txt")
        );
    }

    #[test]
    fn no_vectors_asset_path_rechunks_with_no_embedding() {
        let read = |name: &str| -> Option<Vec<u8>> {
            (name == "assets/knowledge/a.txt").then(|| b"hello world".to_vec())
        };
        let file = corpus_file("assets/knowledge/a.txt", "a.txt", None);
        let mut out = Vec::new();
        collect_file(&mut out, "p", &file, Some(matching_expectation()), &read);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].doc_id, "a.txt");
        assert_eq!(out[0].text, "hello world");
        assert_eq!(
            out[0].embedding, None,
            "no vectors asset => backend embeds at ingest"
        );
    }

    #[test]
    fn missing_vectors_asset_falls_back_to_text_rechunk() {
        // vec.json absent; only the `.txt` resolves.
        let read = |name: &str| -> Option<Vec<u8>> {
            (name == "assets/knowledge/a.txt").then(|| b"hello world".to_vec())
        };
        let file = corpus_file(
            "assets/knowledge/a.txt",
            "a.txt",
            Some("assets/knowledge/a.vec.json"),
        );
        let mut out = Vec::new();
        collect_file(&mut out, "p", &file, Some(matching_expectation()), &read);

        assert_eq!(out.len(), 1, "must not panic; falls back to text re-chunk");
        assert_eq!(out[0].text, "hello world");
        assert_eq!(out[0].embedding, None);
    }

    #[test]
    fn corrupt_vectors_asset_falls_back_to_text_rechunk() {
        let read = |name: &str| -> Option<Vec<u8>> {
            match name {
                "assets/knowledge/a.vec.json" => Some(b"{ not valid json".to_vec()),
                "assets/knowledge/a.txt" => Some(b"hello world".to_vec()),
                _ => None,
            }
        };
        let file = corpus_file(
            "assets/knowledge/a.txt",
            "a.txt",
            Some("assets/knowledge/a.vec.json"),
        );
        let mut out = Vec::new();
        collect_file(&mut out, "p", &file, Some(matching_expectation()), &read);

        assert_eq!(
            out.len(),
            1,
            "corrupt vectors asset must fall back, not panic"
        );
        assert_eq!(out[0].text, "hello world");
        assert_eq!(out[0].embedding, None);
    }

    #[test]
    fn empty_vectors_asset_falls_back_to_text_rechunk() {
        let vec_json = serde_json::json!({
            "embedding_model": "text-embedding-3-small",
            "dims": 3,
            "chunks": []
        });
        let vec_bytes = serde_json::to_vec(&vec_json).unwrap();
        let read = |name: &str| -> Option<Vec<u8>> {
            match name {
                "assets/knowledge/a.vec.json" => Some(vec_bytes.clone()),
                "assets/knowledge/a.txt" => Some(b"hello world".to_vec()),
                _ => None,
            }
        };
        let file = corpus_file(
            "assets/knowledge/a.txt",
            "a.txt",
            Some("assets/knowledge/a.vec.json"),
        );
        let mut out = Vec::new();
        collect_file(&mut out, "p", &file, Some(matching_expectation()), &read);

        assert_eq!(
            out.len(),
            1,
            "empty chunk list must fall back to text re-chunk"
        );
        assert_eq!(out[0].embedding, None);
    }

    #[test]
    fn multibyte_text_does_not_panic_and_preserves_codepoints() {
        // Emoji + accented chars across a window boundary must stay valid UTF-8.
        let text = "café☕".repeat(CHUNK_CHARS);
        let mut out = Vec::new();
        append_chunks(&mut out, "p", "a", "doc", &text);
        assert!(!out.is_empty());
        let rejoined: String = out.iter().map(|c| c.text.as_str()).collect();
        assert!(rejoined.contains("café☕"));
    }
}
