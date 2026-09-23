//! Process-level extensions to every agentic-worker runtime this host builds.
//!
//! Long-term memory and knowledge (document RAG) backends used to be compiled
//! into this crate behind the `long-term-chronicle` / `knowledge-chronicle`
//! cargo features. Those backends live in a PRIVATE repository, and a crate that
//! names a git dependency — even an optional one — cannot be published to
//! crates.io, so the features made the whole crate unpublishable.
//!
//! The host never needed the concrete types: every mount installed a trait
//! object (`Arc<dyn LongTermMemory>` / `Arc<dyn Knowledge>`) on the
//! [`AgentRuntime`]. This module keeps that seam and moves the concrete code
//! out. A binary that wants those backends registers an
//! [`AgentRuntimeExtension`] with [`register_agent_runtime_extension`] BEFORE it
//! starts the host (for the stock CLI: before calling
//! `greentic_runner::cli_main()`), and the host calls every registered extension
//! at the three places the feature-gated mounts used to run:
//!
//! 1. boot, once, [`ingest_all_corpora`] — each extension is handed the
//!    pack-baked knowledge corpus. This runs BEFORE any runtime is built, and
//!    that order is load-bearing: an embedded SurrealDB store allows one handle
//!    per directory, so the ingest connection must close before the serving
//!    mount opens its own.
//! 2. every runtime construction, [`attach_all`] — each extension wraps the
//!    runtime in registration order. The callers run this BEFORE
//!    `knowledge_ext::attach`, which wraps whatever corpus backend an extension
//!    left in place (see that module).
//!
//! A binary that registers nothing gets exactly what the default feature set
//! produced before: no long-term memory and no corpus backend. The one thing
//! that changes is that it is no longer silent — see [`warn_unserved_env`].

use std::sync::{Arc, LazyLock, Once};

use async_trait::async_trait;
use greentic_aw_runtime::AgentRuntime;
use greentic_aw_runtime::knowledge::KnowledgeChunk;
use greentic_types::TenantCtx;
use parking_lot::RwLock;

use crate::pack::PackRuntime;
use crate::runner::knowledge_corpus;

/// A backend a host binary contributes to every agentic-worker runtime.
///
/// Implementations are expected to be env-driven and fail-open, as the
/// in-tree mounts were: an extension whose operator environment is absent or
/// whose connection fails returns the runtime unchanged and logs why. Nothing
/// here can recover from a panic inside an extension, so do not panic.
#[async_trait]
pub trait AgentRuntimeExtension: Send + Sync {
    /// Install this extension's backend on `rt` and return it. Called once per
    /// runtime construction, in registration order, before the host's own
    /// extension-delegated knowledge adapter wraps the result.
    async fn attach(&self, rt: AgentRuntime) -> AgentRuntime;

    /// The embedding `(model, dimension)` this extension will ingest with, so
    /// precomputed corpus vectors can be validated against the embedder that
    /// actually runs. `None` accepts precomputed vectors verbatim.
    fn embedding_expectation(&self) -> Option<(String, usize)> {
        None
    }

    /// First-boot ingest of the pack-baked knowledge corpus. Called once, before
    /// any runtime is built, and only when the packs carry a corpus. The default
    /// ignores it: an extension that holds no corpus has nothing to ingest.
    async fn ingest_corpus(&self, _tenant: &TenantCtx, _chunks: Vec<KnowledgeChunk>) {}
}

type Registry = RwLock<Vec<Arc<dyn AgentRuntimeExtension>>>;

static REGISTRY: LazyLock<Registry> = LazyLock::new(|| RwLock::new(Vec::new()));

/// Register `extension` for every agentic-worker runtime this process builds.
///
/// Register before starting the host. A registration made after a runtime was
/// built does not reach that runtime, and one made after boot misses the
/// corpus ingest.
pub fn register_agent_runtime_extension(extension: Arc<dyn AgentRuntimeExtension>) {
    REGISTRY.write().push(extension);
}

/// A snapshot of the registered extensions, in registration order.
pub fn registered_agent_runtime_extensions() -> Vec<Arc<dyn AgentRuntimeExtension>> {
    REGISTRY.read().clone()
}

/// Run every registered extension's [`AgentRuntimeExtension::attach`] over `rt`.
///
/// Every production runtime construction goes through this, and
/// `knowledge_ext`'s call-site ratchet counts its call sites as the corpus
/// mounts, so a construction that drops it is caught there.
pub async fn attach_all(rt: AgentRuntime) -> AgentRuntime {
    let extensions = registered_agent_runtime_extensions();
    warn_unserved_env(extensions.is_empty());
    attach_with(&extensions, rt).await
}

async fn attach_with(
    extensions: &[Arc<dyn AgentRuntimeExtension>],
    mut rt: AgentRuntime,
) -> AgentRuntime {
    for extension in extensions {
        rt = extension.attach(rt).await;
    }
    rt
}

/// Hand the pack-baked knowledge corpus to every registered extension. Must run
/// before the first [`attach_all`]; see the module docs for why.
pub async fn ingest_all_corpora(packs: &[Arc<PackRuntime>], tenant: &TenantCtx) {
    let extensions = registered_agent_runtime_extensions();
    warn_unserved_env(extensions.is_empty());
    ingest_with(&extensions, packs, tenant).await;
}

async fn ingest_with(
    extensions: &[Arc<dyn AgentRuntimeExtension>],
    packs: &[Arc<PackRuntime>],
    tenant: &TenantCtx,
) {
    for extension in extensions {
        // Collected per extension, because precomputed vectors are validated
        // against THIS extension's embedder: one that disagrees gets the text
        // re-chunked for re-embedding rather than vectors from another space.
        let expectation = extension.embedding_expectation();
        let corpus = knowledge_corpus::collect(
            packs,
            expectation
                .as_ref()
                .map(|(model, dim)| knowledge_corpus::EmbeddingExpectation { model, dim: *dim }),
        );
        if corpus.is_empty() {
            continue;
        }
        extension.ingest_corpus(tenant, corpus).await;
    }
}

/// Environment prefixes that configure a backend only an extension can serve.
const EXTENSION_ENV_PREFIXES: &[&str] = &["GREENTIC_CHRONICLE_", "GREENTIC_KNOWLEDGE_EMBED_"];

/// Which of [`EXTENSION_ENV_PREFIXES`] are configured in `vars` while nothing is
/// registered to read them. Split from [`warn_unserved_env`] so it can be tested
/// without touching the process environment.
fn unserved_env_prefixes<I>(vars: I, has_extension: bool) -> Vec<&'static str>
where
    I: IntoIterator<Item = String>,
{
    if has_extension {
        return Vec::new();
    }
    let names: Vec<String> = vars.into_iter().collect();
    EXTENSION_ENV_PREFIXES
        .iter()
        .copied()
        .filter(|prefix| names.iter().any(|name| name.starts_with(prefix)))
        .collect()
}

/// Warn, once per process, when the operator configured long-term memory or
/// knowledge retrieval but this binary registered no extension to serve it.
///
/// Before the split, a build without the Chronicle features ignored those
/// variables in silence: the worker ran, answered, and had no memory and no
/// corpus, with nothing saying why. The configuration is an explicit operator
/// request, so its being ignored is worth one line in the log.
fn warn_unserved_env(no_extension: bool) {
    static WARNED: Once = Once::new();
    if !no_extension {
        return;
    }
    let unserved = unserved_env_prefixes(
        std::env::vars_os().filter_map(|(k, _)| k.into_string().ok()),
        false,
    );
    if unserved.is_empty() {
        return;
    }
    WARNED.call_once(|| {
        tracing::warn!(
            prefixes = ?unserved,
            "agentic-worker memory/knowledge environment is set, but this runner binary \
             registered no agent-runtime extension to serve it; long-term memory and \
             knowledge retrieval are DISABLED. Use a runner build that registers one \
             (e.g. greentic-runner-full)"
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use greentic_aw_runtime::knowledge::{
        IngestOutcome, Knowledge, KnowledgeQuery, KnowledgeResult, RetrievedChunk,
    };

    #[test]
    fn no_warning_without_the_env_or_with_an_extension() {
        let vars = || {
            vec![
                "PATH".to_string(),
                "GREENTIC_CHRONICLE_URL".to_string(),
                "GREENTIC_KNOWLEDGE_EMBED_MODEL".to_string(),
            ]
        };
        assert_eq!(
            unserved_env_prefixes(vars(), false),
            vec!["GREENTIC_CHRONICLE_", "GREENTIC_KNOWLEDGE_EMBED_"]
        );
        assert!(unserved_env_prefixes(vars(), true).is_empty());
        assert!(unserved_env_prefixes(vec!["PATH".to_string()], false).is_empty());
        // A near miss is not the configuration this warns about.
        assert!(
            unserved_env_prefixes(vec!["GREENTIC_KNOWLEDGE_BACKEND".to_string()], false).is_empty()
        );
    }

    /// Records what the host asked of it, and marks the runtime so the order of
    /// several extensions is observable in the result.
    struct Recording {
        name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
        expectation: Option<(String, usize)>,
    }

    struct Marker(&'static str, Option<Arc<dyn Knowledge>>);

    #[async_trait]
    impl Knowledge for Marker {
        async fn ingest(
            &self,
            _tenant: &TenantCtx,
            _chunks: Vec<KnowledgeChunk>,
        ) -> KnowledgeResult<IngestOutcome> {
            Ok(IngestOutcome::default())
        }
        async fn search(
            &self,
            _tenant: &TenantCtx,
            _query: KnowledgeQuery,
        ) -> KnowledgeResult<Vec<RetrievedChunk>> {
            Ok(Vec::new())
        }
        fn wrapped_backend(&self) -> Option<Arc<dyn Knowledge>> {
            self.1.clone()
        }
        fn backend_id(&self) -> &'static str {
            self.0
        }
    }

    #[async_trait]
    impl AgentRuntimeExtension for Recording {
        async fn attach(&self, rt: AgentRuntime) -> AgentRuntime {
            self.log
                .lock()
                .expect("log")
                .push(format!("attach:{}", self.name));
            let inner = rt.knowledge_backend();
            rt.with_knowledge(Arc::new(Marker(self.name, inner)))
        }
        fn embedding_expectation(&self) -> Option<(String, usize)> {
            self.expectation.clone()
        }
        async fn ingest_corpus(&self, _tenant: &TenantCtx, chunks: Vec<KnowledgeChunk>) {
            self.log
                .lock()
                .expect("log")
                .push(format!("ingest:{}:{}", self.name, chunks.len()));
        }
    }

    /// A runtime with nothing mounted, so every backend in the result was put
    /// there by the extensions under test.
    fn bare_runtime() -> AgentRuntime {
        use greentic_aw_runtime::cost::MockTokenMeter;
        use greentic_aw_runtime::mock::{
            MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
        };
        AgentRuntime::new(
            Arc::new(MockConfigProvider::new()),
            Arc::new(MockAgentStateStore::new()),
            Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().expect("ext runtime")),
            Arc::new(MockLlmBackend::new(Vec::new())),
            Arc::new(MockTelemetry::new()),
            Arc::new(MockTokenMeter::new(0)),
            Arc::new(NoopToolLedger),
            None,
        )
    }

    fn chain_ids(rt: &AgentRuntime) -> Vec<&'static str> {
        let mut ids = Vec::new();
        let mut current = rt.knowledge_backend();
        while let Some(backend) = current {
            ids.push(backend.backend_id());
            current = backend.wrapped_backend();
        }
        ids
    }

    #[tokio::test]
    async fn extensions_attach_in_registration_order() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let extensions: Vec<Arc<dyn AgentRuntimeExtension>> = ["memory", "knowledge"]
            .into_iter()
            .map(|name| {
                Arc::new(Recording {
                    name,
                    log: Arc::clone(&log),
                    expectation: None,
                }) as Arc<dyn AgentRuntimeExtension>
            })
            .collect();
        let rt = attach_with(&extensions, bare_runtime()).await;
        assert_eq!(
            *log.lock().expect("log"),
            vec!["attach:memory", "attach:knowledge"]
        );
        // The later extension wraps the earlier one, exactly as sequential
        // `with_*` calls did when the mounts were compiled in.
        assert_eq!(chain_ids(&rt), vec!["knowledge", "memory"]);
    }

    #[tokio::test]
    async fn no_extension_leaves_the_runtime_untouched() {
        let rt = attach_with(&[], bare_runtime()).await;
        assert!(rt.knowledge_backend().is_none());
    }

    /// The boot ingest must close its store connection before any serving mount
    /// opens one (embedded SurrealDB allows a single handle per directory), so
    /// in `runtime.rs` the ingest has to precede every agent-runtime build. The
    /// order used to be enforced by one `cfg` block sitting above the others; a
    /// textual check is what keeps it now that the call is unconditional.
    #[test]
    fn boot_ingests_before_any_runtime_is_built() {
        let source =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/runtime.rs"))
                .expect("read runtime.rs");
        let ingest = source
            .find("runtime_ext::ingest_all_corpora(")
            .expect("runtime.rs must run the boot corpus ingest");
        let first_build = [
            "build_agent_node_wiring(",
            "build_agent_node_wiring_ephemeral(",
        ]
        .iter()
        .filter_map(|needle| source.find(needle))
        .min()
        .expect("runtime.rs builds the dw.agent runtime");
        assert!(
            ingest < first_build,
            "the corpus ingest must run before the first agent-runtime build"
        );
    }

    #[tokio::test]
    async fn ingest_skips_every_extension_when_no_pack_carries_a_corpus() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let extensions: Vec<Arc<dyn AgentRuntimeExtension>> = vec![Arc::new(Recording {
            name: "knowledge",
            log: Arc::clone(&log),
            expectation: Some(("text-embedding-3-small".into(), 1536)),
        })];
        let tenant = TenantCtx::new(
            greentic_types::EnvId::try_from("local").expect("env id"),
            greentic_types::TenantId::try_from("t1").expect("tenant id"),
        );
        ingest_with(&extensions, &[], &tenant).await;
        assert!(log.lock().expect("log").is_empty());
    }
}
