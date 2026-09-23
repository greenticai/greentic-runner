//! Knowledge retrieval from a Greentic-operated Chronicle index server.
//!
//! The runner-host edge of the `provider.knowledge.chronicle-index` binding:
//! instead of a corpus shipped inside the pack (`knowledge_corpus`) or a
//! customer's service reached through an extension tool (`knowledge_ext`), the
//! worker's documents live in one or more named indexes on a central server,
//! built by the designer's sync. Each turn embeds the query with the binding's
//! embedding provider, searches every bound index, and merges the hits.
//!
//! ## Why this wraps rather than replaces
//!
//! Same reason as `knowledge_ext`: `with_knowledge` overwrites the runtime's
//! backend, so [`attach`] takes whatever is mounted and delegates to it for
//! every binding that is not [`PROVIDER_ID`] — and for every `ingest`, which is
//! how a boot-ingested corpus keeps working underneath.
//!
//! ## Failure is always `Backend`
//!
//! Every failure surfaces as [`KnowledgeError::Backend`], which the turn loop
//! already turns into a warning, a failed retrieval trace step, and a turn run
//! without knowledge. Messages never carry a credential, a vector, or chunk
//! text. One index failing among several is only a warning: the others' hits
//! are still returned.

mod client;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use greentic_aw_runtime::AgentRuntime;
use greentic_aw_runtime::config::MemoryProviderRef;
use greentic_aw_runtime::knowledge::{
    IngestOutcome, Knowledge, KnowledgeChunk, KnowledgeError, KnowledgeQuery, KnowledgeResult,
    RetrievedChunk,
};
use greentic_aw_runtime::scoped_secrets::read_secret_for_unit;
use greentic_types::TenantCtx;
use url::Url;

use crate::secrets::DynSecretsManager;

/// The provider id a knowledge binding carries when retrieval goes to a
/// Chronicle index server. Written by the designer's composer; matched here.
pub const PROVIDER_ID: &str = "provider.knowledge.chronicle-index";

/// Identifies this backend within a wrapper chain (see
/// `knowledge_ext::corpus_backend`).
pub(crate) const BACKEND_ID: &str = "runner-host.knowledge_index";

/// Secret category both credentials are sealed under.
const SECRET_CATEGORY: &str = "knowledge";
/// Bearer for the index server.
const INDEX_KEY: &str = "chronicle_index_key";
/// Bearer for the embedding provider.
const EMBEDDING_KEY: &str = "embedding_key";

/// Team sent on the wire when the binding names none.
const DEFAULT_TEAM: &str = "general";
/// Base URL of the OpenAI-compatible embeddings API when the binding names none.
const DEFAULT_EMBEDDING_BASE_URL: &str = "https://api.openai.com/v1";
/// The only embedding provider this backend speaks.
const OPENAI: &str = "openai";

/// Chunks returned when the query names no limit.
const DEFAULT_LIMIT: usize = 5;

/// Per-HTTP-call timeout override, in milliseconds.
const TIMEOUT_ENV: &str = "GREENTIC_KNOWLEDGE_INDEX_TIMEOUT_MS";
const DEFAULT_TIMEOUT_MS: u64 = 5_000;

/// The one HTTP client every [`ChronicleIndexKnowledge`] shares, built on the
/// first bound search. The graph lane mounts a fresh instance on every agent
/// turn, so building a client (TLS roots and all) per instance would put that
/// cost on every turn, bound or not.
static SHARED_CLIENT: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();

/// The shared client, or why it could not be built. Never falls back to
/// `reqwest::Client::new()`: that can panic, and it drops the timeout.
fn shared_client() -> Result<reqwest::Client, String> {
    SHARED_CLIENT
        .get_or_init(|| build_client(timeout_from_env()))
        .clone()
}

fn build_client(timeout: Duration) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| e.to_string())
}

/// Mount Chronicle-index knowledge retrieval, wrapping whatever backend is
/// already mounted.
///
/// `secret_tenant` is the tenant the host's own secrets are scoped by; `None`
/// uses the `TenantCtx` each `search_bound` call is handed.
#[must_use]
pub fn attach(
    base: AgentRuntime,
    secrets: Option<DynSecretsManager>,
    secret_tenant: Option<String>,
) -> AgentRuntime {
    let inner = base.knowledge_backend();
    base.with_knowledge(Arc::new(ChronicleIndexKnowledge::new(
        inner,
        secrets,
        secret_tenant,
    )))
}

/// What the graph lane needs to mount this backend on each agent turn,
/// captured once where its handler is built.
///
/// The graph lane runs each turn under a synthetic `TenantCtx` (`graph`), so
/// the tenant the host's secrets are scoped by has to be captured here rather
/// than read from the turn.
#[derive(Clone, Default)]
pub struct IndexMount {
    pub secrets: Option<DynSecretsManager>,
    pub secret_tenant: Option<String>,
}

impl IndexMount {
    /// [`attach`] with the captured secrets and tenant.
    #[must_use]
    pub fn attach(&self, base: AgentRuntime) -> AgentRuntime {
        attach(base, self.secrets.clone(), self.secret_tenant.clone())
    }
}

/// Retrieval from the Chronicle indexes named in the agent's binding.
pub struct ChronicleIndexKnowledge {
    /// The backend mounted before this one. Every binding that is not
    /// [`PROVIDER_ID`] — and every `ingest` — goes here.
    inner: Option<Arc<dyn Knowledge>>,
    secrets: Option<DynSecretsManager>,
    secret_tenant: Option<String>,
    /// `None` uses the process-wide [`shared_client`]; tests inject their own.
    http: Option<reqwest::Client>,
}

impl ChronicleIndexKnowledge {
    /// Does no I/O and no TLS setup: the HTTP client is the process-wide one,
    /// built on the first bound search.
    #[must_use]
    pub fn new(
        inner: Option<Arc<dyn Knowledge>>,
        secrets: Option<DynSecretsManager>,
        secret_tenant: Option<String>,
    ) -> Self {
        Self {
            inner,
            secrets,
            secret_tenant,
            http: None,
        }
    }

    /// [`Self::new`] with a caller-supplied client instead of the shared one,
    /// so a test can set its own timeout without racing the process-wide
    /// client's first initialisation.
    #[cfg(test)]
    pub(crate) fn with_client(
        inner: Option<Arc<dyn Knowledge>>,
        secrets: Option<DynSecretsManager>,
        secret_tenant: Option<String>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            inner,
            secrets,
            secret_tenant,
            http: Some(http),
        }
    }

    fn http(&self) -> KnowledgeResult<reqwest::Client> {
        match &self.http {
            Some(http) => Ok(http.clone()),
            None => shared_client().map_err(|reason| {
                KnowledgeError::Backend(format!(
                    "knowledge index HTTP client could not be built: {reason}"
                ))
            }),
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

    /// Read one credential; a miss names the URIs tried, never a value.
    ///
    /// Team scope, then the tenant-default `_` scope — deliberately NO unit
    /// scope. The deployed stores (greentic-deployer's dev-store writer,
    /// greentic-start's reader) canonicalise the secret NAME for every
    /// category but `mcp`/`a2a` to `[a-z0-9_]`, so a `<key>.unit-<id>` name
    /// could never be written or found there; that is also why the key names
    /// use `_` rather than `-`.
    async fn read_key(
        secrets: &DynSecretsManager,
        tenant: &str,
        team: Option<&str>,
        key: &str,
    ) -> KnowledgeResult<String> {
        let bytes =
            read_secret_for_unit(secrets.as_ref(), SECRET_CATEGORY, tenant, team, None, key)
                .await
                .map_err(|miss| {
                    KnowledgeError::Backend(format!("knowledge index credential: {miss}"))
                })?;
        let value = String::from_utf8(bytes)
            .map_err(|_| {
                KnowledgeError::Backend(format!(
                    "knowledge index credential `{key}` is not valid UTF-8"
                ))
            })?
            .trim()
            .to_string();
        if value.is_empty() {
            return Err(KnowledgeError::Backend(format!(
                "knowledge index credential `{key}` is empty"
            )));
        }
        Ok(value)
    }
}

#[async_trait::async_trait]
impl Knowledge for ChronicleIndexKnowledge {
    /// The index is built by the designer's sync, not by this runtime, so
    /// there is nothing to write here; a wrapped backend still ingests.
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

    /// Without a binding there is no index to search.
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
        let Some(bound) = binding.filter(|b| b.provider == PROVIDER_ID) else {
            return self.delegate_search(tenant, query, binding).await;
        };
        let index = IndexBinding::parse(bound)?;

        let secrets = self.secrets.as_ref().ok_or_else(|| {
            KnowledgeError::Backend(
                "no secrets manager is available to read the knowledge index credentials"
                    .to_string(),
            )
        })?;
        let secret_tenant = self
            .secret_tenant
            .as_deref()
            .unwrap_or_else(|| tenant.tenant.as_str());
        let team = index.team.as_deref();
        let index_key = Self::read_key(secrets, secret_tenant, team, INDEX_KEY).await?;
        let embedding_key = Self::read_key(secrets, secret_tenant, team, EMBEDDING_KEY).await?;

        let http = self.http()?;
        let vector = client::embed(&http, &index, &embedding_key, &query.query).await?;
        let limit = query.limit.unwrap_or(DEFAULT_LIMIT).max(1);
        let hits = client::search_all(
            &http,
            &index,
            &index_key,
            &query.query,
            &client::encode_vector(&vector),
            limit,
        )
        .await?;
        Ok(client::merge(hits, limit))
    }

    fn wrapped_backend(&self) -> Option<Arc<dyn Knowledge>> {
        self.inner.clone()
    }

    fn backend_id(&self) -> &'static str {
        BACKEND_ID
    }
}

/// What a [`PROVIDER_ID`] binding's params say, validated.
struct IndexBinding {
    endpoint: Url,
    /// The DESIGNER tenant slug, sent to the index server.
    tenant: String,
    team: Option<String>,
    index_ids: Vec<String>,
    embedding_model: String,
    embedding_base_url: String,
}

impl IndexBinding {
    fn parse(binding: &MemoryProviderRef) -> KnowledgeResult<Self> {
        let key = |name: &str| format!("{PROVIDER_ID}.{name}");
        let read = |name: &str| -> Option<String> {
            let value = binding.params.get(&key(name))?.as_str()?.trim();
            (!value.is_empty()).then(|| value.to_string())
        };
        let malformed = |name: &str| {
            KnowledgeError::Backend(format!(
                "knowledge binding names `{PROVIDER_ID}` but its `{}` param is missing or invalid",
                key(name)
            ))
        };

        let endpoint = read("endpoint")
            .and_then(|raw| Url::parse(&raw).ok())
            .filter(|url| matches!(url.scheme(), "http" | "https") && url.has_host())
            .ok_or_else(|| malformed("endpoint"))?;
        let tenant = read("tenant").ok_or_else(|| malformed("tenant"))?;
        let index_ids = binding
            .params
            .get(&key("index_ids"))
            .and_then(serde_json::Value::as_array)
            .and_then(|items| {
                items
                    .iter()
                    .map(|item| {
                        let id = item.as_str()?.trim();
                        (!id.is_empty()).then(|| id.to_string())
                    })
                    .collect::<Option<Vec<String>>>()
            })
            .filter(|ids| !ids.is_empty())
            .ok_or_else(|| malformed("index_ids"))?;
        let provider = read("embedding_provider").ok_or_else(|| malformed("embedding_provider"))?;
        if provider != OPENAI {
            return Err(KnowledgeError::Backend(format!(
                "unsupported embedding provider `{provider}` for the knowledge index (only \
                 openai-compatible is supported)"
            )));
        }
        let embedding_model =
            read("embedding_model").ok_or_else(|| malformed("embedding_model"))?;

        Ok(Self {
            endpoint,
            tenant,
            team: read("team"),
            index_ids,
            embedding_model,
            embedding_base_url: read("embedding_base_url")
                .unwrap_or_else(|| DEFAULT_EMBEDDING_BASE_URL.to_string()),
        })
    }
}

/// `GREENTIC_KNOWLEDGE_INDEX_TIMEOUT_MS` when it is a positive integer, else
/// the default.
fn timeout_from_env() -> Duration {
    let ms = std::env::var(TIMEOUT_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(DEFAULT_TIMEOUT_MS);
    Duration::from_millis(ms)
}

#[cfg(test)]
mod tests;
