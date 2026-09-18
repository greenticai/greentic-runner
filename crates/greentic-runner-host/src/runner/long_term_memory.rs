//! Operator-configured long-term (episodic) memory wiring for the agentic
//! worker. Compiled only with the `long-term-chronicle` feature.
//!
//! A single native Chronicle backend is built once from operator environment
//! variables and shared across every agent on the runtime; per-agent/tenant data
//! isolation is handled inside the runtime by scoping each call to the agent's
//! tenant (Chronicle `group_id`).
//!
//! The graph store is operator-selectable via `GREENTIC_CHRONICLE_BACKEND`
//! (`neo4j` | `falkor` | `surreal-embedded` | `surreal-memory`), defaulting to
//! **embedded SurrealDB on disk** so no external graph server is required.
//! Chronicle is driven **entirely through the provider-neutral DW LLM and
//! embedding families** — both wired to whatever OpenAI-compatible endpoint the
//! operator points at (`GREENTIC_CHRONICLE_{LLM,EMBED}_BASE_URL`). There is no
//! hard dependency on a single provider. When the required LLM/embedding
//! environment is unset — or a provider config / backend connection fails — the
//! long-term tier simply stays disabled; a missing backend never fails
//! construction.

use std::sync::Arc;

use greentic_aw_runtime::{AgentRuntime, LongTermMemory};
use greentic_dw_embedding::EmbeddingProvider;
use greentic_dw_embedding_openai_compatible::{
    OpenAiCompatibleEmbeddingConfig, OpenAiCompatibleEmbeddingProvider,
};
use greentic_dw_llm::LlmProvider;
use greentic_dw_llm_openai_compatible::{OpenAiCompatibleConfig, OpenAiCompatibleProvider};
use greentic_dw_memory_chronicle::{
    ChronicleBackend, ChronicleLongTermMemory, ChronicleMemoryConfig, DEFAULT_FALKOR_GRAPH,
    DEFAULT_NEO4J_DATABASE,
};
use greentic_types::{EnvId, TenantCtx, TenantId};

// Graph-store backend selector + per-backend connection.
const ENV_BACKEND: &str = "GREENTIC_CHRONICLE_BACKEND";
// Neo4j (backend=neo4j).
const ENV_NEO4J_URI: &str = "GREENTIC_CHRONICLE_NEO4J_URI";
const ENV_NEO4J_USER: &str = "GREENTIC_CHRONICLE_NEO4J_USER";
const ENV_NEO4J_PASSWORD: &str = "GREENTIC_CHRONICLE_NEO4J_PASSWORD";
const ENV_NEO4J_DATABASE: &str = "GREENTIC_CHRONICLE_NEO4J_DATABASE";
// FalkorDB (backend=falkor).
const ENV_FALKOR_URL: &str = "GREENTIC_CHRONICLE_FALKOR_URL";
const ENV_FALKOR_GRAPH: &str = "GREENTIC_CHRONICLE_FALKOR_GRAPH";
// Embedded SurrealDB (backend=surreal-embedded).
const ENV_SURREAL_PATH: &str = "GREENTIC_CHRONICLE_SURREAL_PATH";
/// Graph store used when `GREENTIC_CHRONICLE_BACKEND` is unset: embedded
/// SurrealDB on disk — no external graph server required.
const DEFAULT_BACKEND: &str = "surreal-embedded";
/// Default on-disk location for the embedded SurrealDB store.
///
/// Prefers the per-user `~/.greentic/chronicle` and only falls back to the system
/// path — the same convention [`crate::runner::aw_backends`] uses for the agent
/// state store. A bare `/var/lib/greentic` default is unwritable for any non-root
/// process (e.g. the designer's Test-chat sidecar) and the mount failure is
/// GRACEFUL, so the tier silently stays disabled. Root/systemd deployments have
/// no `HOME` and keep the system path. Kept distinct from the knowledge tier's
/// path — two embedded handles must never share one directory.
fn default_surreal_path() -> String {
    match std::env::var("HOME") {
        Ok(home) if !home.is_empty() => format!("{home}/.greentic/chronicle"),
        _ => "/var/lib/greentic/chronicle".to_string(),
    }
}
// LLM (entity/edge extraction) — any OpenAI-compatible endpoint.
const ENV_LLM_BASE_URL: &str = "GREENTIC_CHRONICLE_LLM_BASE_URL";
const ENV_LLM_API_KEY: &str = "GREENTIC_CHRONICLE_LLM_API_KEY";
const ENV_LLM_MODEL: &str = "GREENTIC_CHRONICLE_LLM_MODEL";
// Embeddings (vector recall) — any OpenAI-compatible endpoint.
const ENV_EMBED_BASE_URL: &str = "GREENTIC_CHRONICLE_EMBED_BASE_URL";
const ENV_EMBED_API_KEY: &str = "GREENTIC_CHRONICLE_EMBED_API_KEY";
const ENV_EMBED_MODEL: &str = "GREENTIC_CHRONICLE_EMBED_MODEL";
const ENV_EMBED_DIM: &str = "GREENTIC_CHRONICLE_EMBED_DIM";

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_EMBEDDING_DIM: usize = 1024;

/// Attach an operator-configured, provider-neutral Chronicle long-term backend
/// to `runtime`. The LLM and embedding endpoints are the opt-in signal (Chronicle
/// needs both regardless of graph store); the graph backend is selected by
/// `GREENTIC_CHRONICLE_BACKEND`, defaulting to embedded SurrealDB so no external
/// graph server is required. Returns the runtime unchanged when the environment
/// is incomplete or the connection fails.
pub async fn attach(runtime: AgentRuntime) -> AgentRuntime {
    let Some(llm) = build_llm() else {
        tracing::debug!("long-term memory: LLM endpoint env unset/invalid; long-term disabled");
        return runtime;
    };
    let Some((embedder, embedding_dim)) = build_embedder() else {
        tracing::debug!(
            "long-term memory: embedding endpoint env unset/invalid; long-term disabled"
        );
        return runtime;
    };
    let Some(backend) = select_backend() else {
        // select_backend logs the specific reason (unknown kind / missing conn env).
        return runtime;
    };

    let mut config = ChronicleMemoryConfig::with_backend(backend);
    // Chronicle's graph vector index dimension must match the embedder's output.
    config.embedding_dim = Some(embedding_dim);
    let backend_kind = config.backend.kind();

    match ChronicleLongTermMemory::connect_with_dw_providers(
        config,
        llm,
        embedder,
        operator_tenant(),
    )
    .await
    {
        Ok(memory) => {
            tracing::info!(
                backend = %backend_kind,
                "long-term memory: Chronicle attached (provider-neutral DW LLM + embeddings)"
            );
            runtime.with_long_term_memory(Arc::new(memory) as Arc<dyn LongTermMemory>)
        }
        Err(err) => {
            tracing::warn!(
                backend = %backend_kind,
                error = %err,
                "long-term memory: Chronicle connect failed; long-term disabled"
            );
            runtime
        }
    }
}

/// Resolve the graph-store backend from the environment. Defaults to embedded
/// SurrealDB. Returns `None` — with a logged reason — when an explicitly-chosen
/// backend is missing its required connection env, or the selector value is
/// unknown.
fn select_backend() -> Option<ChronicleBackend> {
    let kind = std::env::var(ENV_BACKEND).unwrap_or_else(|_| DEFAULT_BACKEND.to_string());
    match kind.as_str() {
        "neo4j" => {
            let (Ok(uri), Ok(user), Ok(password)) = (
                std::env::var(ENV_NEO4J_URI),
                std::env::var(ENV_NEO4J_USER),
                std::env::var(ENV_NEO4J_PASSWORD),
            ) else {
                tracing::warn!(
                    "long-term memory: backend=neo4j but GREENTIC_CHRONICLE_NEO4J_URI/USER/PASSWORD \
                     incomplete; long-term disabled"
                );
                return None;
            };
            let database = std::env::var(ENV_NEO4J_DATABASE)
                .unwrap_or_else(|_| DEFAULT_NEO4J_DATABASE.to_string());
            Some(ChronicleBackend::Neo4j {
                uri,
                user,
                password,
                database,
            })
        }
        "falkor" => {
            let Ok(connection) = std::env::var(ENV_FALKOR_URL) else {
                tracing::warn!(
                    "long-term memory: backend=falkor but GREENTIC_CHRONICLE_FALKOR_URL unset; \
                     long-term disabled"
                );
                return None;
            };
            let graph = std::env::var(ENV_FALKOR_GRAPH)
                .unwrap_or_else(|_| DEFAULT_FALKOR_GRAPH.to_string());
            Some(ChronicleBackend::Falkor { connection, graph })
        }
        "surreal-embedded" => {
            let path = std::env::var(ENV_SURREAL_PATH).unwrap_or_else(|_| default_surreal_path());
            Some(ChronicleBackend::surreal_embedded(path))
        }
        "surreal-memory" => {
            tracing::warn!(
                "long-term memory: backend=surreal-memory is EPHEMERAL — recalled facts are lost \
                 on restart"
            );
            Some(ChronicleBackend::surreal_memory())
        }
        other => {
            tracing::warn!(
                backend = %other,
                "long-term memory: unknown GREENTIC_CHRONICLE_BACKEND \
                 (expected neo4j|falkor|surreal-embedded|surreal-memory); long-term disabled"
            );
            None
        }
    }
}

/// Build the provider-neutral DW LLM backend (any OpenAI-compatible endpoint).
/// Returns `None` when the endpoint env is incomplete or the config is invalid.
fn build_llm() -> Option<Arc<dyn LlmProvider>> {
    let base_url = std::env::var(ENV_LLM_BASE_URL).ok()?;
    let api_key = std::env::var(ENV_LLM_API_KEY).ok()?;
    let model = std::env::var(ENV_LLM_MODEL).ok()?;
    let mut cfg = OpenAiCompatibleConfig::new(base_url, model, DEFAULT_TIMEOUT_MS);
    cfg.api_key_secret = Some(api_key);
    match OpenAiCompatibleProvider::new(cfg) {
        Ok(provider) => Some(Arc::new(provider)),
        Err(err) => {
            tracing::warn!(error = %err, "long-term memory: LLM provider config invalid");
            None
        }
    }
}

/// Build the provider-neutral DW embedding backend (any OpenAI-compatible
/// endpoint), returning it alongside the embedding dimension Chronicle indexes
/// at. Returns `None` when the endpoint env is incomplete or invalid.
fn build_embedder() -> Option<(Arc<dyn EmbeddingProvider>, usize)> {
    let base_url = std::env::var(ENV_EMBED_BASE_URL).ok()?;
    let api_key = std::env::var(ENV_EMBED_API_KEY).ok()?;
    let model = std::env::var(ENV_EMBED_MODEL).ok()?;
    let embedding_dim = std::env::var(ENV_EMBED_DIM)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_EMBEDDING_DIM);
    let mut cfg =
        OpenAiCompatibleEmbeddingConfig::new(api_key, base_url, model, DEFAULT_TIMEOUT_MS);
    cfg.embedding_dim = embedding_dim;
    match OpenAiCompatibleEmbeddingProvider::new(cfg) {
        Ok(provider) => Some((Arc::new(provider), embedding_dim)),
        Err(err) => {
            tracing::warn!(error = %err, "long-term memory: embedding provider config invalid");
            None
        }
    }
}

/// Operator/system tenant the Chronicle LLM + embedding calls run as. The
/// OpenAI-compatible providers ignore the tenant (the bridge merely carries it),
/// and end-tenant data isolation is via the per-call tenant -> Chronicle
/// `group_id`, so a fixed operator identity is correct here.
fn operator_tenant() -> TenantCtx {
    let env = EnvId::try_from("operator").unwrap_or_else(|_| {
        EnvId::try_from("dev").expect("the literal env id \"dev\" is always valid")
    });
    let tenant = TenantId::try_from("chronicle")
        .expect("the literal tenant id \"chronicle\" is always valid");
    TenantCtx::new(env, tenant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // The crate denies unsafe, but `std::env::set_var`/`remove_var` are `unsafe`
    // under edition 2024. These helpers are confined to tests, which run under
    // `#[serial]`, so no other thread observes the environment mid-mutation.
    #[allow(unsafe_code)]
    fn set(key: &str, val: &str) {
        // SAFETY: tests touching the environment are serialized via `#[serial]`.
        unsafe { std::env::set_var(key, val) };
    }

    #[allow(unsafe_code)]
    fn unset(key: &str) {
        // SAFETY: tests touching the environment are serialized via `#[serial]`.
        unsafe { std::env::remove_var(key) };
    }

    /// Clear every backend-selection variable so a test starts from a known,
    /// empty environment regardless of the host or sibling tests.
    fn clear_backend_env() {
        for key in [
            ENV_BACKEND,
            ENV_NEO4J_URI,
            ENV_NEO4J_USER,
            ENV_NEO4J_PASSWORD,
            ENV_NEO4J_DATABASE,
            ENV_FALKOR_URL,
            ENV_FALKOR_GRAPH,
            ENV_SURREAL_PATH,
        ] {
            unset(key);
        }
    }

    #[test]
    #[serial]
    fn defaults_to_embedded_surreal_on_disk() {
        clear_backend_env();
        match select_backend() {
            Some(ChronicleBackend::SurrealEmbedded { path }) => {
                // Per-user by default (the sidecar runs as a normal user); only a
                // HOME-less root/systemd process gets the system path.
                assert_eq!(path, default_surreal_path());
                match std::env::var("HOME") {
                    Ok(home) if !home.is_empty() => {
                        assert_eq!(path, format!("{home}/.greentic/chronicle"))
                    }
                    _ => assert_eq!(path, "/var/lib/greentic/chronicle"),
                }
            }
            other => panic!("expected default SurrealEmbedded, got {other:?}"),
        }
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn surreal_embedded_honours_custom_path() {
        clear_backend_env();
        set(ENV_BACKEND, "surreal-embedded");
        set(ENV_SURREAL_PATH, "/data/chronicle");
        match select_backend() {
            Some(ChronicleBackend::SurrealEmbedded { path }) => {
                assert_eq!(path, "/data/chronicle");
            }
            other => panic!("expected SurrealEmbedded, got {other:?}"),
        }
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn surreal_memory_selected() {
        clear_backend_env();
        set(ENV_BACKEND, "surreal-memory");
        assert_eq!(select_backend().map(|b| b.kind()), Some("surreal-memory"));
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn neo4j_reads_all_connection_fields() {
        clear_backend_env();
        set(ENV_BACKEND, "neo4j");
        set(ENV_NEO4J_URI, "bolt://db:7687");
        set(ENV_NEO4J_USER, "neo");
        set(ENV_NEO4J_PASSWORD, "secret");
        set(ENV_NEO4J_DATABASE, "graph-db");
        match select_backend() {
            Some(ChronicleBackend::Neo4j {
                uri,
                user,
                password,
                database,
            }) => {
                assert_eq!(uri, "bolt://db:7687");
                assert_eq!(user, "neo");
                assert_eq!(password, "secret");
                assert_eq!(database, "graph-db");
            }
            other => panic!("expected Neo4j, got {other:?}"),
        }
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn neo4j_defaults_database_when_unset() {
        clear_backend_env();
        set(ENV_BACKEND, "neo4j");
        set(ENV_NEO4J_URI, "bolt://db:7687");
        set(ENV_NEO4J_USER, "neo");
        set(ENV_NEO4J_PASSWORD, "secret");
        match select_backend() {
            Some(ChronicleBackend::Neo4j { database, .. }) => {
                assert_eq!(database, DEFAULT_NEO4J_DATABASE);
            }
            other => panic!("expected Neo4j, got {other:?}"),
        }
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn neo4j_missing_credentials_disables() {
        clear_backend_env();
        set(ENV_BACKEND, "neo4j");
        set(ENV_NEO4J_URI, "bolt://db:7687");
        // user + password deliberately absent.
        assert!(select_backend().is_none());
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn falkor_reads_connection_and_graph() {
        clear_backend_env();
        set(ENV_BACKEND, "falkor");
        set(ENV_FALKOR_URL, "redis://falkor:6379");
        set(ENV_FALKOR_GRAPH, "episodes");
        match select_backend() {
            Some(ChronicleBackend::Falkor { connection, graph }) => {
                assert_eq!(connection, "redis://falkor:6379");
                assert_eq!(graph, "episodes");
            }
            other => panic!("expected Falkor, got {other:?}"),
        }
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn falkor_defaults_graph_when_unset() {
        clear_backend_env();
        set(ENV_BACKEND, "falkor");
        set(ENV_FALKOR_URL, "redis://falkor:6379");
        match select_backend() {
            Some(ChronicleBackend::Falkor { graph, .. }) => {
                assert_eq!(graph, DEFAULT_FALKOR_GRAPH);
            }
            other => panic!("expected Falkor, got {other:?}"),
        }
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn falkor_missing_url_disables() {
        clear_backend_env();
        set(ENV_BACKEND, "falkor");
        assert!(select_backend().is_none());
        clear_backend_env();
    }

    #[test]
    #[serial]
    fn unknown_backend_disables() {
        clear_backend_env();
        set(ENV_BACKEND, "cassandra");
        assert!(select_backend().is_none());
        clear_backend_env();
    }
}
