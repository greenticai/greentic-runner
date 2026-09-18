//! Operator-configured Knowledge (document-RAG) wiring for the agentic worker.
//! Compiled only with the `knowledge-chronicle` feature.
//!
//! This is the runner-host edge of the W4 Knowledge tier: it builds a concrete
//! Chronicle-backed knowledge provider and adapts it to the runtime's local
//! [`greentic_aw_runtime::knowledge::Knowledge`] seam (the runtime deliberately
//! keeps a thin local trait rather than depending on the provider stack). The
//! agentic loop then auto-retrieves from it each turn (see W4 4a/4b).
//!
//! Knowledge is a SEPARATE tier from long-term memory ([D4]): its own capability,
//! its own embedded store, its own env namespace (`GREENTIC_KNOWLEDGE_*`). It must
//! NOT share the embedded-SurrealDB path with long-term memory — two RocksDB
//! handles on one directory in the same process collide — so it defaults to a
//! distinct path. The graph store is operator-selectable via
//! `GREENTIC_KNOWLEDGE_BACKEND`, defaulting to embedded SurrealDB so document RAG
//! works with no external graph server. The embedding endpoint
//! (`GREENTIC_KNOWLEDGE_EMBED_*`, any OpenAI-compatible API) is the opt-in signal —
//! specifically its **key + model**; the base URL is optional and defaults to the
//! embeddings client's provider default (a plain OpenAI provider has no explicit
//! endpoint). Absent the signal — or on a config / connection failure — the
//! knowledge tier simply stays disabled; it never fails runtime construction.

use std::sync::Arc;
use std::time::Duration;

use chronicle_core::driver::GraphDriver;
use chronicle_driver_falkor::FalkorDriver;
use chronicle_driver_neo4j::Neo4jDriver;
use chronicle_driver_surreal::SurrealDriver;
use greentic_aw_runtime::AgentRuntime;
use greentic_aw_runtime::knowledge::{
    IngestOutcome as AwIngestOutcome, Knowledge as AwKnowledge, KnowledgeChunk as AwChunk,
    KnowledgeError as AwKnowledgeError, KnowledgeQuery as AwQuery, KnowledgeResult as AwResult,
    RetrievedChunk as AwRetrievedChunk,
};
use greentic_dw_knowledge::{
    IngestOutcome as DwIngestOutcome, Knowledge as DwKnowledge, KnowledgeChunk as DwChunk,
    KnowledgeQuery as DwQuery, RetrievedChunk as DwRetrievedChunk,
};
use greentic_dw_knowledge_chronicle::{KnowledgeChronicle, KnowledgeConfig};
use greentic_types::TenantCtx;

// Graph-store backend selector + per-backend connection (own namespace, distinct
// from the long-term-memory tier — they must not share an embedded store path).
const ENV_BACKEND: &str = "GREENTIC_KNOWLEDGE_BACKEND";
const ENV_SURREAL_PATH: &str = "GREENTIC_KNOWLEDGE_SURREAL_PATH";
/// Graph store used when `GREENTIC_KNOWLEDGE_BACKEND` is unset: embedded SurrealDB
/// on disk — no external graph server required.
const DEFAULT_BACKEND: &str = "surreal-embedded";
/// Default on-disk location for the embedded SurrealDB knowledge store. Distinct
/// from the long-term-memory default so the two tiers never open the same store.
///
/// Prefers the per-user `~/.greentic/knowledge` and only falls back to the
/// system path — the same convention [`crate::runner::aw_backends`] uses for the
/// agent state store. A bare `/var/lib/greentic` default is unwritable for any
/// non-root process (e.g. the designer's Test-chat sidecar), and the mount
/// failure is GRACEFUL ("knowledge disabled"), so vector retrieval silently
/// yields nothing instead of erroring. Root/systemd deployments have no `HOME`
/// and keep the system path.
fn default_surreal_path() -> String {
    match std::env::var("HOME") {
        Ok(home) if !home.is_empty() => format!("{home}/.greentic/knowledge"),
        _ => "/var/lib/greentic/knowledge".to_string(),
    }
}
// Neo4j (backend=neo4j) — may point at the same server long-term memory uses; the
// `knowledge:<tenant>` group_id keeps corpora isolated from memory graph entities.
const ENV_NEO4J_URI: &str = "GREENTIC_KNOWLEDGE_NEO4J_URI";
const ENV_NEO4J_USER: &str = "GREENTIC_KNOWLEDGE_NEO4J_USER";
const ENV_NEO4J_PASSWORD: &str = "GREENTIC_KNOWLEDGE_NEO4J_PASSWORD";
const ENV_NEO4J_DATABASE: &str = "GREENTIC_KNOWLEDGE_NEO4J_DATABASE";
const DEFAULT_NEO4J_DATABASE: &str = "neo4j";
// FalkorDB (backend=falkor).
const ENV_FALKOR_URL: &str = "GREENTIC_KNOWLEDGE_FALKOR_URL";
const ENV_FALKOR_GRAPH: &str = "GREENTIC_KNOWLEDGE_FALKOR_GRAPH";
const DEFAULT_FALKOR_GRAPH: &str = "chronicle";
// Embeddings (vector recall) — any OpenAI-compatible endpoint.
const ENV_EMBED_BASE_URL: &str = "GREENTIC_KNOWLEDGE_EMBED_BASE_URL";
const ENV_EMBED_API_KEY: &str = "GREENTIC_KNOWLEDGE_EMBED_API_KEY";
const ENV_EMBED_MODEL: &str = "GREENTIC_KNOWLEDGE_EMBED_MODEL";
const ENV_EMBED_DIM: &str = "GREENTIC_KNOWLEDGE_EMBED_DIM";

const DEFAULT_EMBEDDING_DIM: usize = 1024;

/// Attach an operator-configured Chronicle knowledge (document-RAG) backend to
/// `runtime`. The embedding endpoint env is the opt-in signal (Chronicle needs an
/// embedder to retrieve); the graph backend is selected by
/// `GREENTIC_KNOWLEDGE_BACKEND`, defaulting to embedded SurrealDB so no external
/// graph server is required. Returns the runtime unchanged when the environment is
/// incomplete or the connection fails.
pub async fn attach(runtime: AgentRuntime) -> AgentRuntime {
    let Some(config) = build_config() else {
        tracing::debug!(
            "knowledge: embedding endpoint env unset/invalid; knowledge (RAG) disabled"
        );
        return runtime;
    };
    let Some(driver) = build_driver(config.embedding_dim).await else {
        // build_driver logs the specific reason (unknown kind / connection error).
        return runtime;
    };

    match KnowledgeChronicle::from_config(&config, driver).await {
        Ok(knowledge) => {
            tracing::info!(
                "knowledge: Chronicle document-RAG attached (provider-neutral embeddings)"
            );
            runtime.with_knowledge(Arc::new(KnowledgeBridge(knowledge)))
        }
        Err(err) => {
            tracing::warn!(error = %err, "knowledge: Chronicle connect failed; knowledge disabled");
            runtime
        }
    }
}

/// Build the knowledge embedding config from the environment. The **key + model**
/// are mandatory — their absence is how an operator opts out — so a missing value
/// returns `None` and leaves knowledge disabled.
///
/// The base URL is OPTIONAL: `KnowledgeConfig::openai_base_url` is an `Option`
/// whose `None` means "the embeddings client's provider default". Requiring it
/// here contradicted that and disabled knowledge for the most common setup — a
/// plain OpenAI embedding provider, which legitimately has no explicit endpoint.
fn build_config() -> Option<KnowledgeConfig> {
    let api_key = std::env::var(ENV_EMBED_API_KEY).ok()?;
    let model = std::env::var(ENV_EMBED_MODEL).ok()?;
    let base_url = std::env::var(ENV_EMBED_BASE_URL)
        .ok()
        .filter(|v| !v.trim().is_empty());
    let embedding_dim = std::env::var(ENV_EMBED_DIM)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_EMBEDDING_DIM);

    let mut config = KnowledgeConfig::new(embedding_dim);
    config.openai_base_url = base_url;
    config.openai_api_key = Some(api_key);
    config.embedding_model = Some(model);
    Some(config)
}

/// The model + dimension the configured embedder will produce, for validating
/// pack-supplied precomputed vectors before they reach ingest. `None` when no
/// embedder is configured — the caller skips ingest entirely in that case.
///
/// Goes through [`build_config`] rather than reading the env vars directly, so
/// the expectation applies the same defaulting the embedder does — notably
/// `DEFAULT_EMBEDDING_DIM` when `GREENTIC_KNOWLEDGE_EMBED_DIM` is unset or
/// unparseable. This is a SEPARATE `build_config` call from the one
/// [`ingest_corpus`] makes, not a shared value; they agree because nothing
/// mutates the environment between them, not because the types enforce it.
pub(crate) fn embedding_expectation() -> Option<(String, usize)> {
    let config = build_config()?;
    Some((config.embedding_model.clone()?, config.embedding_dim))
}

/// The graph-store backend resolved from the environment, before any connection
/// is attempted. Selection is kept separate from connection so it is unit-testable
/// without a live database.
#[derive(Debug, PartialEq)]
enum BackendChoice {
    SurrealEmbedded {
        path: String,
    },
    SurrealMemory,
    Neo4j {
        uri: String,
        user: String,
        password: String,
        database: String,
    },
    Falkor {
        connection: String,
        graph: String,
    },
}

/// Resolve the graph-store backend from `GREENTIC_KNOWLEDGE_BACKEND` (defaulting to
/// embedded SurrealDB). Returns `None` — with a logged reason — when the selector
/// value is unknown or an explicitly-chosen backend is missing its required
/// connection env. Mirrors the long-term-memory selector but in its own env
/// namespace and store.
fn resolve_backend() -> Option<BackendChoice> {
    let kind = std::env::var(ENV_BACKEND).unwrap_or_else(|_| DEFAULT_BACKEND.to_string());
    match kind.as_str() {
        "surreal-embedded" => {
            let path = std::env::var(ENV_SURREAL_PATH).unwrap_or_else(|_| default_surreal_path());
            Some(BackendChoice::SurrealEmbedded { path })
        }
        "surreal-memory" => Some(BackendChoice::SurrealMemory),
        "neo4j" => {
            let (Ok(uri), Ok(user), Ok(password)) = (
                std::env::var(ENV_NEO4J_URI),
                std::env::var(ENV_NEO4J_USER),
                std::env::var(ENV_NEO4J_PASSWORD),
            ) else {
                tracing::warn!(
                    "knowledge: backend=neo4j but GREENTIC_KNOWLEDGE_NEO4J_URI/USER/PASSWORD \
                     incomplete; knowledge disabled"
                );
                return None;
            };
            let database = std::env::var(ENV_NEO4J_DATABASE)
                .unwrap_or_else(|_| DEFAULT_NEO4J_DATABASE.to_string());
            Some(BackendChoice::Neo4j {
                uri,
                user,
                password,
                database,
            })
        }
        "falkor" => {
            let Ok(connection) = std::env::var(ENV_FALKOR_URL) else {
                tracing::warn!(
                    "knowledge: backend=falkor but GREENTIC_KNOWLEDGE_FALKOR_URL unset; \
                     knowledge disabled"
                );
                return None;
            };
            let graph = std::env::var(ENV_FALKOR_GRAPH)
                .unwrap_or_else(|_| DEFAULT_FALKOR_GRAPH.to_string());
            Some(BackendChoice::Falkor { connection, graph })
        }
        other => {
            tracing::warn!(
                backend = %other,
                "knowledge: GREENTIC_KNOWLEDGE_BACKEND='{other}' is unknown (supported: \
                 surreal-embedded|surreal-memory|neo4j|falkor); knowledge disabled",
            );
            None
        }
    }
}

/// Resolve and connect the graph-store driver. Returns `None` (with a logged
/// reason) when selection fails or the connection errors. `embedding_dim` sizes
/// the vector index on the backends that build one.
async fn build_driver(embedding_dim: usize) -> Option<Arc<dyn GraphDriver>> {
    match resolve_backend()? {
        BackendChoice::SurrealEmbedded { path } => {
            connect_embedded_retrying(&path, embedding_dim).await
        }
        BackendChoice::SurrealMemory => {
            tracing::warn!(
                "knowledge: backend=surreal-memory is EPHEMERAL — the ingested corpus is lost on \
                 restart and must be re-ingested"
            );
            match SurrealDriver::connect_memory(embedding_dim).await {
                Ok(driver) => Some(Arc::new(driver)),
                Err(err) => {
                    tracing::warn!(error = %err, "knowledge: in-memory SurrealDB connect failed; knowledge disabled");
                    None
                }
            }
        }
        BackendChoice::Neo4j {
            uri,
            user,
            password,
            database,
        } => match Neo4jDriver::connect(&uri, &user, &password, database).await {
            Ok(driver) => Some(Arc::new(driver)),
            Err(err) => {
                tracing::warn!(error = %err, "knowledge: Neo4j connect failed; knowledge disabled");
                None
            }
        },
        BackendChoice::Falkor { connection, graph } => {
            match FalkorDriver::connect(&connection, &graph, embedding_dim).await {
                Ok(driver) => Some(Arc::new(driver)),
                Err(err) => {
                    tracing::warn!(error = %err, "knowledge: FalkorDB connect failed; knowledge disabled");
                    None
                }
            }
        }
    }
}

/// Whether a connect error is the embedded store's directory-lock contention,
/// as opposed to a permanent failure (bad path, dim mismatch). RocksDB reports
/// it as `No locks available` / `lock hold by current process`; matched
/// case-insensitively so a wording change across versions still degrades to the
/// old fail-open behaviour rather than a false retry.
fn is_lock_contention(err_msg: &str) -> bool {
    let lower = err_msg.to_ascii_lowercase();
    lower.contains("no locks available") || lower.contains("lock hold")
}

/// Connect to the embedded store, retrying briefly while its directory lock is
/// still held by a just-closed handle.
///
/// `ingest_corpus` opens this same store, writes the corpus, and drops its
/// handle immediately before the serving mount ([`attach`]) opens its own — but
/// embedded SurrealDB (RocksDB) releases the directory lock *asynchronously* as
/// the previous handle finishes closing. So `attach`'s open can lose a short
/// race with the ingest handle's teardown and see `No locks available`, which
/// silently disabled the whole knowledge tier (RAG present in the store, but the
/// serving mount never came up). The window is brief and self-clears once the
/// close completes, so a bounded backoff turns that silent miss into a wait of a
/// few hundred milliseconds at worst.
///
/// Only lock contention is retried; a permanent error returns on the first
/// attempt so a genuine misconfiguration still fails fast into the same
/// knowledge-disabled fallback.
async fn connect_embedded_retrying(
    path: &str,
    embedding_dim: usize,
) -> Option<Arc<dyn GraphDriver>> {
    const MAX_ATTEMPTS: u32 = 8;
    const MAX_DELAY: Duration = Duration::from_millis(400);
    let mut delay = Duration::from_millis(25);

    for attempt in 1..=MAX_ATTEMPTS {
        let err = match SurrealDriver::connect_embedded(path, embedding_dim).await {
            Ok(driver) => return Some(Arc::new(driver)),
            Err(err) => err,
        };
        let msg = err.to_string();
        if !is_lock_contention(&msg) || attempt == MAX_ATTEMPTS {
            tracing::warn!(
                error = %err, path = %path, attempt,
                "knowledge: embedded SurrealDB connect failed; knowledge disabled"
            );
            return None;
        }
        tracing::debug!(
            path = %path, attempt,
            "knowledge: embedded store lock still held by the closing ingest handle; retrying"
        );
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(MAX_DELAY);
    }
    None
}

/// Adapts the provider-stack [`KnowledgeChronicle`] (which implements the
/// `greentic-dw-knowledge` trait over its own DTOs) to the runtime's local
/// [`AwKnowledge`] seam. The two DTO families mirror each other field-for-field;
/// this bridge does the (trivial) conversion so the runtime crate stays free of
/// the provider/catalog dependency weight.
struct KnowledgeBridge(KnowledgeChronicle);

#[async_trait::async_trait]
impl AwKnowledge for KnowledgeBridge {
    async fn ingest(&self, tenant: &TenantCtx, chunks: Vec<AwChunk>) -> AwResult<AwIngestOutcome> {
        let dw_chunks: Vec<DwChunk> = chunks.into_iter().map(to_dw_chunk).collect();
        let DwIngestOutcome { chunk_ids } =
            self.0.ingest(tenant, dw_chunks).await.map_err(to_aw_err)?;
        Ok(AwIngestOutcome { chunk_ids })
    }

    async fn search(&self, tenant: &TenantCtx, query: AwQuery) -> AwResult<Vec<AwRetrievedChunk>> {
        let hits = self
            .0
            .search(
                tenant,
                DwQuery {
                    query: query.query,
                    limit: query.limit,
                },
            )
            .await
            .map_err(to_aw_err)?;
        Ok(hits
            .into_iter()
            .map(
                |DwRetrievedChunk {
                     text,
                     score,
                     doc_id,
                     chunk_index,
                     metadata,
                 }| AwRetrievedChunk {
                    text,
                    score,
                    doc_id,
                    chunk_index,
                    metadata,
                },
            )
            .collect())
    }
}

/// Map a provider-stack knowledge error onto the runtime's local error. Tenant
/// validation and "not configured" both surface as backend errors here because
/// the runtime seam only needs the message for its fail-degrade logging.
fn to_aw_err(err: greentic_dw_knowledge::KnowledgeError) -> AwKnowledgeError {
    AwKnowledgeError::Backend(err.to_string())
}

/// Convert a runtime knowledge chunk to the provider-stack DTO (field-for-field).
fn to_dw_chunk(c: AwChunk) -> DwChunk {
    DwChunk {
        doc_id: c.doc_id,
        chunk_index: c.chunk_index,
        text: c.text,
        metadata: c.metadata,
        embedding: c.embedding,
    }
}

/// First-boot ingest of a pack-baked knowledge corpus (W4 4c). Opens a temporary
/// knowledge connection, ingests `chunks` for `tenant`, then drops it — releasing
/// the embedded-store lock before the serving mount ([`attach`]) opens its own
/// connection (embedded SurrealDB allows only one handle per store directory, so
/// the two must not overlap). Idempotent: Chronicle keys chunks by content hash,
/// so re-ingesting an unchanged corpus on every boot is a no-op. Fail-open: any
/// error is logged and skipped, never blocking startup.
pub(crate) async fn ingest_corpus(tenant: &TenantCtx, chunks: Vec<AwChunk>) {
    if chunks.is_empty() {
        return;
    }
    let Some(config) = build_config() else {
        tracing::debug!("knowledge: embedding env unset; skipping baked-corpus ingest");
        return;
    };
    // surreal-memory's ingest connection is a SEPARATE in-memory DB from the
    // serving mount's, so ingested chunks would be invisible at query time. Skip
    // with a clear note — durable RAG needs a persistent backend.
    if std::env::var(ENV_BACKEND).as_deref() == Ok("surreal-memory") {
        tracing::warn!(
            "knowledge: backend=surreal-memory does not retain a baked corpus across the \
             ingest/serve connection boundary; skipping corpus ingest (use surreal-embedded)"
        );
        return;
    }
    let Some(driver) = build_driver(config.embedding_dim).await else {
        return;
    };
    let knowledge = match KnowledgeChronicle::from_config(&config, driver).await {
        Ok(knowledge) => knowledge,
        Err(err) => {
            tracing::warn!(error = %err, "knowledge: baked-corpus ingest connect failed; skipping");
            return;
        }
    };
    let count = chunks.len();
    let dw_chunks: Vec<DwChunk> = chunks.into_iter().map(to_dw_chunk).collect();
    match knowledge.ingest(tenant, dw_chunks).await {
        Ok(outcome) => tracing::info!(
            chunks = count,
            stored = outcome.chunk_ids.len(),
            "knowledge: baked corpus ingested"
        ),
        Err(err) => tracing::warn!(error = %err, "knowledge: baked-corpus ingest failed"),
    }
    // `knowledge` (and its embedded-store handle) drops here. The directory lock
    // is released asynchronously as that handle finishes closing, so the serving
    // mount's open can briefly race this teardown — `connect_embedded_retrying`
    // absorbs that window.
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn is_lock_contention_matches_rocksdb_flock_messages() {
        // The verbatim RocksDB LOG line observed when the ingest handle had not
        // finished releasing the directory lock.
        assert!(is_lock_contention(
            "IO error: lock hold by current process, acquire time 1784187876 \
             acquiring thread 1908306: /home/u/.greentic/knowledge/LOCK: No locks available"
        ));
        assert!(is_lock_contention("No Locks Available")); // case-insensitive
    }

    #[test]
    fn is_lock_contention_ignores_permanent_errors() {
        // A misconfiguration must NOT be retried — it would only delay the
        // fail-open into the knowledge-disabled fallback.
        assert!(!is_lock_contention("No such file or directory"));
        assert!(!is_lock_contention(
            "embedding dimension mismatch: 1536 vs 1024"
        ));
        assert!(!is_lock_contention(""));
    }

    /// The real fix, against a real embedded store: while one handle holds the
    /// directory lock (the ingest handle mid-teardown), `connect_embedded_retrying`
    /// must keep trying and win once that handle drops — where a single open would
    /// have returned `No locks available` and disabled knowledge.
    ///
    /// Two embedded opens on one directory in the same process genuinely collide
    /// (RocksDB reports "lock hold by current process"), which is exactly the boot
    /// race this reproduces. Timing margin is wide (release at 100ms vs a ~1.5s
    /// retry budget) so it is not flaky.
    #[tokio::test]
    async fn embedded_connect_retries_past_a_lingering_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().into_owned();

        // A single second open, with the first still held, loses on lock
        // contention — the failure this fix absorbs.
        let first = SurrealDriver::connect_embedded(&path, 8)
            .await
            .expect("first open");
        let contended = SurrealDriver::connect_embedded(&path, 8)
            .await
            .err()
            .map(|e| e.to_string());
        assert!(
            contended.as_deref().is_some_and(is_lock_contention),
            "a plain second open must fail on lock contention; got {contended:?}"
        );

        // Release the holder shortly after the retry loop starts.
        let releaser = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(first);
        });

        let driver = connect_embedded_retrying(&path, 8).await;
        releaser.await.expect("releaser");

        assert!(
            driver.is_some(),
            "retry must win once the prior handle releases the lock"
        );
    }

    // `std::env::set_var`/`remove_var` are `unsafe` under edition 2024; confined to
    // tests, which run under `#[serial]` so no other thread observes the env mid-mutation.
    #[allow(unsafe_code)]
    fn set(key: &str, val: &str) {
        // SAFETY: env-touching tests are serialized via `#[serial]`.
        unsafe { std::env::set_var(key, val) };
    }

    #[allow(unsafe_code)]
    fn unset(key: &str) {
        // SAFETY: env-touching tests are serialized via `#[serial]`.
        unsafe { std::env::remove_var(key) };
    }

    fn clear_embed_env() {
        for key in [
            ENV_EMBED_BASE_URL,
            ENV_EMBED_API_KEY,
            ENV_EMBED_MODEL,
            ENV_EMBED_DIM,
        ] {
            unset(key);
        }
    }

    fn set_required_embed_env() {
        set(ENV_EMBED_BASE_URL, "https://api.example/v1");
        set(ENV_EMBED_API_KEY, "sk-test");
        set(ENV_EMBED_MODEL, "text-embedding-3-small");
    }

    fn clear_backend_env() {
        for key in [
            ENV_BACKEND,
            ENV_SURREAL_PATH,
            ENV_NEO4J_URI,
            ENV_NEO4J_USER,
            ENV_NEO4J_PASSWORD,
            ENV_NEO4J_DATABASE,
            ENV_FALKOR_URL,
            ENV_FALKOR_GRAPH,
        ] {
            unset(key);
        }
    }

    #[test]
    fn to_dw_chunk_carries_precomputed_embedding() {
        let vec = vec![0.1_f32, 0.2, 0.3];
        let aw = AwChunk {
            doc_id: "faq".into(),
            chunk_index: 2,
            text: "Refunds within 5 days.".into(),
            metadata: serde_json::Map::new(),
            embedding: Some(vec.clone()),
        };
        let dw = to_dw_chunk(aw);
        assert_eq!(dw.doc_id, "faq");
        assert_eq!(dw.chunk_index, 2);
        assert_eq!(dw.text, "Refunds within 5 days.");
        // The pre-computed vector must survive the AW→DW mapping unchanged so the
        // Chronicle backend can skip re-embedding.
        assert_eq!(dw.embedding, Some(vec));
    }

    #[test]
    fn to_dw_chunk_preserves_absent_embedding() {
        let aw = AwChunk {
            doc_id: "faq".into(),
            chunk_index: 0,
            text: "No vector here.".into(),
            metadata: serde_json::Map::new(),
            embedding: None,
        };
        assert_eq!(to_dw_chunk(aw).embedding, None);
    }

    #[test]
    #[serial]
    fn resolve_backend_defaults_to_embedded_surreal() {
        clear_backend_env();
        assert_eq!(
            resolve_backend(),
            Some(BackendChoice::SurrealEmbedded {
                path: default_surreal_path()
            })
        );
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn resolve_backend_surreal_embedded_honours_path() {
        clear_backend_env();
        set(ENV_BACKEND, "surreal-embedded");
        set(ENV_SURREAL_PATH, "/data/knowledge");
        assert_eq!(
            resolve_backend(),
            Some(BackendChoice::SurrealEmbedded {
                path: "/data/knowledge".to_string()
            })
        );
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn resolve_backend_neo4j_full_and_default_database() {
        clear_backend_env();
        set(ENV_BACKEND, "neo4j");
        set(ENV_NEO4J_URI, "bolt://db:7687");
        set(ENV_NEO4J_USER, "neo");
        set(ENV_NEO4J_PASSWORD, "secret");
        assert_eq!(
            resolve_backend(),
            Some(BackendChoice::Neo4j {
                uri: "bolt://db:7687".to_string(),
                user: "neo".to_string(),
                password: "secret".to_string(),
                database: DEFAULT_NEO4J_DATABASE.to_string(),
            })
        );
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn resolve_backend_neo4j_missing_credentials_disables() {
        clear_backend_env();
        set(ENV_BACKEND, "neo4j");
        set(ENV_NEO4J_URI, "bolt://db:7687");
        // user + password deliberately absent.
        assert_eq!(resolve_backend(), None);
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn resolve_backend_falkor_full_and_default_graph() {
        clear_backend_env();
        set(ENV_BACKEND, "falkor");
        set(ENV_FALKOR_URL, "redis://falkor:6379");
        assert_eq!(
            resolve_backend(),
            Some(BackendChoice::Falkor {
                connection: "redis://falkor:6379".to_string(),
                graph: DEFAULT_FALKOR_GRAPH.to_string(),
            })
        );
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn resolve_backend_falkor_missing_url_disables() {
        clear_backend_env();
        set(ENV_BACKEND, "falkor");
        assert_eq!(resolve_backend(), None);
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn resolve_backend_unknown_disables() {
        clear_backend_env();
        set(ENV_BACKEND, "cassandra");
        assert_eq!(resolve_backend(), None);
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn build_config_reads_embedding_env() {
        clear_embed_env();
        set_required_embed_env();
        set(ENV_EMBED_DIM, "1536");
        let config = build_config().expect("complete embedding env yields a config");
        assert_eq!(
            config.openai_base_url.as_deref(),
            Some("https://api.example/v1")
        );
        assert_eq!(config.openai_api_key.as_deref(), Some("sk-test"));
        assert_eq!(
            config.embedding_model.as_deref(),
            Some("text-embedding-3-small")
        );
        assert_eq!(config.embedding_dim, 1536);
        clear_embed_env();
    }

    #[test]
    #[serial]
    fn build_config_defaults_dim_when_unset_or_invalid() {
        clear_embed_env();
        set_required_embed_env();
        // Unset dim -> default.
        assert_eq!(
            build_config().expect("config").embedding_dim,
            DEFAULT_EMBEDDING_DIM
        );
        // Non-numeric dim -> default (never panics).
        set(ENV_EMBED_DIM, "not-a-number");
        assert_eq!(
            build_config().expect("config").embedding_dim,
            DEFAULT_EMBEDDING_DIM
        );
        clear_embed_env();
    }

    #[test]
    #[serial]
    fn build_config_disabled_when_embedding_env_incomplete() {
        clear_embed_env();
        // No embedding env at all -> opt-out.
        assert!(build_config().is_none());
        // Base URL + key but no model -> still incomplete.
        set(ENV_EMBED_BASE_URL, "https://api.example/v1");
        set(ENV_EMBED_API_KEY, "sk-test");
        assert!(build_config().is_none());
        clear_embed_env();
    }

    #[test]
    #[serial]
    fn build_config_enabled_without_base_url() {
        // The common setup — a plain OpenAI embedding provider — carries no
        // explicit endpoint. `openai_base_url: None` means "provider default", so
        // key + model alone must ENABLE knowledge; requiring a base URL here
        // silently disabled the whole tier (and the caller had already skipped the
        // static KB injection, losing the documents entirely).
        clear_embed_env();
        set(ENV_EMBED_API_KEY, "sk-test");
        set(ENV_EMBED_MODEL, "text-embedding-3-small");
        let config = build_config().expect("key + model alone must enable knowledge");
        assert!(
            config.openai_base_url.is_none(),
            "absent base URL must stay None (provider default), not be invented"
        );
        assert_eq!(config.openai_api_key.as_deref(), Some("sk-test"));
        assert_eq!(
            config.embedding_model.as_deref(),
            Some("text-embedding-3-small")
        );
        clear_embed_env();
    }
}
