//! Test doubles. Compiled only when `--features test-mock`. The runner-
//! host and designer integration tests use these to avoid hitting Redis
//! or real LLM providers in CI.

#[allow(clippy::expect_used)]
mod inner {
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::config::AgentConfig;
    use crate::config_provider::ConfigProvider;
    use crate::error::{ConfigError, LlmError, StateError, TerminationReason};
    use crate::knowledge::{
        IngestOutcome as KnowledgeIngestOutcome, Knowledge, KnowledgeChunk, KnowledgeQuery,
        KnowledgeResult, RetrievedChunk,
    };
    use crate::llm::{LlmBackend, LlmRequest, LlmResponse};
    use crate::long_term::{
        EpisodeIngest, IngestOutcome, LongTermMemory, LongTermMemoryError, RecallQuery,
        RecalledFact,
    };
    use crate::state::{AgentStateStore, ConversationState, SessionLock, SessionLockInner};
    use crate::telemetry::{StepTelemetryCtx, Telemetry};
    use crate::tenant::TenantContext;
    use greentic_types::TenantCtx;
    use tokio::sync::Notify;

    /// LLM mock with a scripted response queue. Records the `system_prompt` of
    /// every request it sees so tests can assert on prompt augmentation.
    pub struct MockLlmBackend {
        pub responses: Mutex<Vec<Result<LlmResponse, LlmError>>>,
        pub seen_system_prompts: Mutex<Vec<String>>,
        /// Tool names advertised in each request the backend saw.
        pub seen_tool_names: Mutex<Vec<Vec<String>>>,
    }

    impl MockLlmBackend {
        pub fn new(responses: Vec<Result<LlmResponse, LlmError>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                seen_system_prompts: Mutex::new(Vec::new()),
                seen_tool_names: Mutex::new(Vec::new()),
            }
        }
    }

    impl LlmBackend for MockLlmBackend {
        fn complete<'a>(
            &'a self,
            req: LlmRequest,
        ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
            self.seen_system_prompts
                .lock()
                .expect("mock llm prompt lock poisoned")
                .push(req.system_prompt.clone());
            self.seen_tool_names
                .lock()
                .expect("mock llm tools lock poisoned")
                .push(req.tools.iter().map(|t| t.tool_name.clone()).collect());
            // Eagerly extract the next scripted response so the future is Send + 'a.
            let next = {
                let mut queue = self.responses.lock().expect("mock LLM mutex poisoned");
                if queue.is_empty() {
                    Err(LlmError::Transport("mock queue exhausted".into()))
                } else {
                    queue.remove(0)
                }
            };
            Box::pin(async move { next })
        }
    }

    /// In-memory long-term backend for tests. Returns `scripted_facts` from
    /// `recall`, records every `ingest_episode` body, and notifies after each
    /// ingest so fire-and-forget callers can be awaited deterministically.
    pub struct MockLongTermMemory {
        scripted_facts: Vec<RecalledFact>,
        ingested: Mutex<Vec<EpisodeIngest>>,
        ingested_notify: Notify,
    }

    impl MockLongTermMemory {
        pub fn new(scripted_facts: Vec<RecalledFact>) -> Self {
            Self {
                scripted_facts,
                ingested: Mutex::new(Vec::new()),
                ingested_notify: Notify::new(),
            }
        }

        /// Snapshot of the episodes ingested so far.
        pub fn ingested(&self) -> Vec<EpisodeIngest> {
            self.ingested
                .lock()
                .expect("mock ingest lock poisoned")
                .clone()
        }

        /// Wait until at least `n` episodes have been ingested (deterministic
        /// alternative to sleeping for a fire-and-forget ingest).
        pub async fn wait_for_ingests(&self, n: usize) {
            loop {
                // Register interest BEFORE checking, so a notify that fires
                // between the check and the await is not missed.
                let notified = self.ingested_notify.notified();
                if self
                    .ingested
                    .lock()
                    .expect("mock ingest lock poisoned")
                    .len()
                    >= n
                {
                    return;
                }
                notified.await;
            }
        }
    }

    #[async_trait::async_trait]
    impl LongTermMemory for MockLongTermMemory {
        async fn ingest_episode(
            &self,
            _tenant: &TenantCtx,
            episode: EpisodeIngest,
        ) -> Result<IngestOutcome, LongTermMemoryError> {
            let id = format!("ep-{}", episode.name);
            self.ingested
                .lock()
                .expect("mock ingest lock poisoned")
                .push(episode);
            self.ingested_notify.notify_waiters();
            Ok(IngestOutcome {
                episode_id: id,
                fact_count: 0,
                entity_count: 0,
            })
        }

        async fn recall(
            &self,
            _tenant: &TenantCtx,
            _query: RecallQuery,
        ) -> Result<Vec<RecalledFact>, LongTermMemoryError> {
            Ok(self.scripted_facts.clone())
        }
    }

    /// Knowledge (doc-RAG) mock: returns a scripted set of chunks from `search`
    /// (bounded by `query.limit`) and records every ingested batch so tests can
    /// assert on first-boot ingestion.
    pub struct MockKnowledge {
        scripted_chunks: Vec<RetrievedChunk>,
        ingested: Mutex<Vec<KnowledgeChunk>>,
    }

    impl MockKnowledge {
        pub fn new(scripted_chunks: Vec<RetrievedChunk>) -> Self {
            Self {
                scripted_chunks,
                ingested: Mutex::new(Vec::new()),
            }
        }

        /// Snapshot of the chunks ingested so far.
        pub fn ingested(&self) -> Vec<KnowledgeChunk> {
            self.ingested
                .lock()
                .expect("mock knowledge ingest lock poisoned")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl Knowledge for MockKnowledge {
        async fn ingest(
            &self,
            _tenant: &TenantCtx,
            chunks: Vec<KnowledgeChunk>,
        ) -> KnowledgeResult<KnowledgeIngestOutcome> {
            let ids = chunks
                .iter()
                .map(|c| format!("{}#{}", c.doc_id, c.chunk_index))
                .collect();
            self.ingested
                .lock()
                .expect("mock knowledge ingest lock poisoned")
                .extend(chunks);
            Ok(KnowledgeIngestOutcome { chunk_ids: ids })
        }

        async fn search(
            &self,
            _tenant: &TenantCtx,
            query: KnowledgeQuery,
        ) -> KnowledgeResult<Vec<RetrievedChunk>> {
            let mut hits = self.scripted_chunks.clone();
            if let Some(limit) = query.limit {
                hits.truncate(limit);
            }
            Ok(hits)
        }
    }

    /// In-memory state store; lock is a no-op semaphore.
    pub struct MockAgentStateStore {
        entries: Mutex<HashMap<String, ConversationState>>,
    }

    impl MockAgentStateStore {
        pub fn new() -> Self {
            Self {
                entries: Mutex::new(HashMap::new()),
            }
        }

        fn build_key(tenant: &TenantContext, session_id: &str) -> String {
            format!("{}:{}", tenant.key_prefix(), session_id)
        }
    }

    impl Default for MockAgentStateStore {
        fn default() -> Self {
            Self::new()
        }
    }

    impl AgentStateStore for MockAgentStateStore {
        fn load<'a>(
            &'a self,
            tenant: &'a TenantContext,
            session_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<ConversationState, StateError>> + Send + 'a>>
        {
            let key = Self::build_key(tenant, session_id);
            let state = self
                .entries
                .lock()
                .expect("mock state mutex poisoned")
                .get(&key)
                .cloned()
                .unwrap_or_else(|| ConversationState::empty(tenant, session_id));
            Box::pin(async move { Ok(state) })
        }

        fn save<'a>(
            &'a self,
            tenant: &'a TenantContext,
            session_id: &'a str,
            state: &'a ConversationState,
        ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
            let key = Self::build_key(tenant, session_id);
            let cloned = state.clone();
            Box::pin(async move {
                self.entries
                    .lock()
                    .expect("mock state mutex poisoned")
                    .insert(key, cloned);
                Ok(())
            })
        }

        fn acquire_lock<'a>(
            &'a self,
            _tenant: &'a TenantContext,
            _session_id: &'a str,
            _wait: Duration,
        ) -> Pin<Box<dyn Future<Output = Result<SessionLock, StateError>> + Send + 'a>> {
            Box::pin(async move { Ok(SessionLock::new(Box::new(NoopLockInner))) })
        }
    }

    struct NoopLockInner;

    impl SessionLockInner for NoopLockInner {
        fn refresh<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn release(&self) {}
    }

    pub struct MockTelemetry {
        pub recorded: Mutex<Vec<StepTelemetryCtx>>,
    }

    impl MockTelemetry {
        pub fn new() -> Self {
            Self {
                recorded: Mutex::new(Vec::new()),
            }
        }
    }

    impl Default for MockTelemetry {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Telemetry for MockTelemetry {
        fn record_step(&self, ctx: &StepTelemetryCtx) {
            self.recorded
                .lock()
                .expect("mock telemetry mutex poisoned")
                .push(ctx.clone());
        }
    }

    pub struct MockConfigProvider {
        pub configs: Mutex<HashMap<String, AgentConfig>>,
    }

    impl MockConfigProvider {
        pub fn new() -> Self {
            Self {
                configs: Mutex::new(HashMap::new()),
            }
        }

        pub fn insert(&self, tenant: &TenantContext, agent_id: &str, cfg: AgentConfig) {
            self.configs
                .lock()
                .expect("mock config mutex poisoned")
                .insert(format!("{}:{agent_id}", tenant.key_prefix()), cfg);
        }
    }

    impl Default for MockConfigProvider {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ConfigProvider for MockConfigProvider {
        fn agent_config<'a>(
            &'a self,
            tenant: &'a TenantContext,
            agent_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
            let key = format!("{}:{agent_id}", tenant.key_prefix());
            let agent_id_owned = agent_id.to_string();
            let entry = self
                .configs
                .lock()
                .expect("mock config mutex poisoned")
                .get(&key)
                .cloned();
            Box::pin(async move { entry.ok_or(ConfigError::AgentNotFound(agent_id_owned)) })
        }
    }

    /// Ledger that never records anything — every `get` returns `None`,
    /// so the loop always dispatches. Used by unit tests that don't want
    /// Redis-backed idempotency.
    pub struct NoopToolLedger;

    impl crate::tools::ToolLedger for NoopToolLedger {
        fn get<'a>(
            &'a self,
            _tenant: &'a TenantContext,
            _session_id: &'a str,
            _call_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<serde_json::Value>, StateError>> + Send + 'a>>
        {
            Box::pin(async { Ok(None) })
        }

        fn record<'a>(
            &'a self,
            _tenant: &'a TenantContext,
            _session_id: &'a str,
            _call_id: &'a str,
            _result: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// Billing meter with a fixed `over_budget` verdict, for exercising the credit
    /// gate without HTTP. `emit` records nothing — the gate is what is under test.
    pub struct MockBillingMeter {
        over: bool,
    }

    impl MockBillingMeter {
        #[must_use]
        pub fn new(over: bool) -> Self {
            Self { over }
        }
    }

    impl crate::billing::BillingMeter for MockBillingMeter {
        fn emit<'a>(
            &'a self,
            _tenant: &'a TenantContext,
            _input_tokens: u64,
            _output_tokens: u64,
            _agent_id: &'a str,
            _model: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), crate::billing::BillingError>> + Send + 'a>>
        {
            Box::pin(async { Ok(()) })
        }

        fn over_budget<'a>(
            &'a self,
            _tenant: &'a TenantContext,
        ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
            Box::pin(std::future::ready(self.over))
        }
    }

    /// Convenience: assert a [`TerminationReason`] matches the expected value.
    pub fn assert_terminated_by(actual: &TerminationReason, expected: &TerminationReason) {
        assert_eq!(actual, expected, "expected {expected:?}, got {actual:?}");
    }
}

pub use inner::{
    MockAgentStateStore, MockBillingMeter, MockConfigProvider, MockKnowledge, MockLlmBackend,
    MockLongTermMemory, MockTelemetry, NoopToolLedger, assert_terminated_by,
};
