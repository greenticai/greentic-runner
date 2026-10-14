//! Which [`ArtifactReader`] resolves attachment bytes for the in-process
//! multi-provider LLM backend (`GreenticLlmBackend`), and how many reads may
//! run at once.
//!
//! Where the reader comes from, decided at the HOST and never below it:
//!
//! - a deployed unit (the revision-keyed path): ONLY the reader passed with
//!   `RevisionHostOptions::with_artifact_reader`. No option means no reader;
//!   the env variables are never read there.
//! - a `HostBuilder` host with exactly one tenant (the designer's Run Demo,
//!   the standalone runner): the injected reader, else, ONLY when the host
//!   opted in with `HostBuilder::with_artifact_env_fallback(true)`, the env
//!   fallback ([`artifact_reader_from_env`]); resolved once in
//!   `HostBuilder::build` (`crate::host::host_artifact_reader`). The opt-in
//!   is for single-tenant processes only (the standalone runner, `crate::run`,
//!   which the Test chat sidecar runs); the designer must never enable it.
//! - a `HostBuilder` host with several tenants: none. An injected reader is
//!   dropped (warning `artifact_reader_multi_tenant_host`) and the env is NOT
//!   used in its place: one tenant's agents must never read through another
//!   tenant's token.
//! - everything else (public `TenantRuntime::load` / `from_packs`, the
//!   desktop builders, the process-level serve path): none.
//!
//! Without a reader every turn still runs and each attachment becomes the
//! fixed "not available in this deployment" notice.
//!
//! The token is a credential: it never reaches a log line (the client error
//! carries no token) and `HttpArtifactReader`'s `Debug` redacts it.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use greentic_aw_runtime::{
    ArtifactBytes, ArtifactError, ArtifactReader, GreenticLlmBackend, HttpArtifactReader,
};
use tokio::sync::Semaphore;

/// Door base, ending in `/artifacts` (the reader appends `/get`).
pub(crate) const ENDPOINT_ENV: &str = "GREENTIC_ARTIFACT_ENDPOINT";
/// The unit's door token (the metering token carrying the `artifacts` purpose).
pub(crate) const TOKEN_ENV: &str = "GREENTIC_ARTIFACT_TOKEN";

/// Artifact reads in flight at once across the WHOLE process, every runtime
/// and tenant together. One read can hold roughly 38 MB transiently (raw body,
/// its base64 text and the decoded bytes; see `HttpArtifactReader`), so this
/// bounds that memory at about 150 MB. A turn's own fan-out (at most 3, in
/// `attachments_materialize`) still applies inside it.
pub(crate) const MAX_CONCURRENT_READS: usize = 4;

fn process_permits() -> Arc<Semaphore> {
    static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    PERMITS
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_READS)))
        .clone()
}

/// An [`ArtifactReader`] that waits for a permit before each read. The permit
/// is held by the read's own future, so it is released however the read ends:
/// success, error, panic or cancellation (the future being dropped).
pub(crate) struct BoundedReader {
    inner: Arc<dyn ArtifactReader>,
    permits: Arc<Semaphore>,
}

impl BoundedReader {
    pub(crate) fn with_permits(inner: Arc<dyn ArtifactReader>, permits: Arc<Semaphore>) -> Self {
        Self { inner, permits }
    }
}

impl ArtifactReader for BoundedReader {
    fn get<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ArtifactBytes, ArtifactError>> + Send + 'a>> {
        Box::pin(async move {
            // The semaphore is never closed, so this cannot fail in practice;
            // if it ever did, the read fails rather than running unbounded.
            let _permit = self
                .permits
                .acquire()
                .await
                .map_err(|_| ArtifactError::Unavailable("artifact read limit closed".into()))?;
            self.inner.get(id).await
        })
    }
}

fn non_blank_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// Env fallback for the artifact reader, consulted ONLY by
/// `crate::host::host_artifact_reader` for a single-tenant `HostBuilder` host
/// with no injected reader that opted in with
/// `HostBuilder::with_artifact_env_fallback(true)` (the standalone runner).
///
/// Both variables are required; either missing (or blank) means no reader. A
/// reader that cannot be built (an unusable token, a client that cannot be
/// constructed) is no reader plus one warning with a fixed code, never a
/// panic and never a default client without timeouts or a redirect policy.
pub(crate) fn artifact_reader_from_env() -> Option<Arc<dyn ArtifactReader>> {
    let endpoint = non_blank_env(ENDPOINT_ENV)?;
    let token = non_blank_env(TOKEN_ENV)?;
    match HttpArtifactReader::new(endpoint, token) {
        Ok(reader) => Some(Arc::new(reader)),
        Err(error) => {
            // `ArtifactClientError` never carries the token or the endpoint.
            tracing::warn!(
                code = "artifact_reader_unavailable",
                %error,
                "attachments disabled: the artifact reader from the environment could not be built"
            );
            None
        }
    }
}

/// The multi-provider backend with exactly the reader the runtime was handed
/// (never an env fallback: that was decided at the host).
pub(crate) fn greentic_backend(
    api_key: String,
    base_url: Option<String>,
    reader: Option<Arc<dyn ArtifactReader>>,
) -> GreenticLlmBackend {
    attach_artifact_reader(GreenticLlmBackend::new(api_key, base_url), reader)
}

/// Install `reader` on `backend`, bounded by the process-wide read limit. Without
/// one the backend is returned unchanged: every turn still runs, and each
/// attachment becomes the fixed "not available in this deployment" notice.
/// Called once per backend build (per runtime), never per request, so the
/// log lines below are one per runtime.
pub(crate) fn attach_artifact_reader(
    backend: GreenticLlmBackend,
    reader: Option<Arc<dyn ArtifactReader>>,
) -> GreenticLlmBackend {
    match reader {
        Some(reader) => {
            tracing::info!("AW LLM backend: artifact reader wired (attachments readable)");
            backend.with_artifact_reader(Arc::new(BoundedReader::with_permits(
                reader,
                process_permits(),
            )))
        }
        None => {
            tracing::info!(
                code = "artifact_reader_not_configured",
                "AW LLM backend: no artifact reader (attachments are announced, not read)"
            );
            backend
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use greentic_aw_runtime::state::ChatMessage;
    use greentic_aw_runtime::{
        AttachmentKind, AttachmentRef, LlmBackend, LlmProviderRef, LlmRequest,
    };
    use greentic_llm::{
        Capabilities, ChatRequest, ChatResponse, ChatStream, FinishReason, LlmError, LlmProvider,
    };

    const ID: &str = "artifact://0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn clear_env() {
        unsafe {
            std::env::remove_var(ENDPOINT_ENV);
            std::env::remove_var(TOKEN_ENV);
        }
    }

    /// Counts reads and answers every id with a tiny PNG.
    #[derive(Default)]
    struct CountingReader {
        reads: AtomicUsize,
    }

    impl ArtifactReader for CountingReader {
        fn get<'a>(
            &'a self,
            _id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<ArtifactBytes, ArtifactError>> + Send + 'a>>
        {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(ArtifactBytes {
                    mime_type: "image/png".into(),
                    name: Some("a.png".into()),
                    bytes: vec![1, 2, 3],
                })
            })
        }
    }

    /// Records the request it was sent; vision on.
    #[derive(Default)]
    struct RecordingProvider {
        seen: Mutex<Vec<ChatRequest>>,
    }

    #[async_trait]
    impl LlmProvider for RecordingProvider {
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                chat: true,
                tools: true,
                streaming: false,
                vision: true,
                system_prompt: true,
            }
        }
        fn provider_name(&self) -> &'static str {
            "mock"
        }
        fn model(&self) -> &str {
            "m"
        }
        async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
            self.seen.lock().unwrap().push(req);
            Ok(ChatResponse {
                content: "ok".into(),
                tool_calls: vec![],
                finish_reason: FinishReason::Stop,
                usage: None,
            })
        }
        async fn chat_stream(&self, _req: ChatRequest) -> Result<ChatStream, LlmError> {
            Err(LlmError::UnsupportedCapability("streaming"))
        }
    }

    fn request_with_one_image() -> LlmRequest {
        LlmRequest {
            system_prompt: "sys".into(),
            history: vec![ChatMessage::User {
                content: "look".into(),
                attachments: vec![AttachmentRef {
                    id: ID.into(),
                    mime_type: "image/png".into(),
                    name: Some("a.png".into()),
                    size_bytes: None,
                    kind: AttachmentKind::Image,
                    text_ref: None,
                }],
            }],
            tools: vec![],
            provider: LlmProviderRef {
                provider: "deepseek".into(),
                model: "m".into(),
                credential_ref: None,
            },
            turn_attachments: Default::default(),
            attachment_text_guard: None,
        }
    }

    async fn run_turn(reader: Option<Arc<dyn ArtifactReader>>) -> (ChatRequest, bool) {
        let provider = Arc::new(RecordingProvider::default());
        let backend = attach_artifact_reader(
            GreenticLlmBackend::new("unused", None).with_cached_provider(
                "deepseek",
                "m",
                provider.clone(),
            ),
            reader,
        );
        let ok = backend.complete(request_with_one_image()).await.is_ok();
        let sent = provider
            .seen
            .lock()
            .unwrap()
            .pop()
            .expect("chat() was called");
        (sent, ok)
    }

    #[test]
    #[serial_test::serial]
    fn artifact_reader_from_env_requires_both_variables() {
        clear_env();
        assert!(artifact_reader_from_env().is_none());
        unsafe {
            std::env::set_var(
                ENDPOINT_ENV,
                "https://admin.example/api/v1/ingest/artifacts",
            );
        }
        assert!(
            artifact_reader_from_env().is_none(),
            "endpoint alone is not enough"
        );
        unsafe { std::env::set_var(TOKEN_ENV, "gtm_x") };
        assert!(artifact_reader_from_env().is_some());
        unsafe { std::env::set_var(ENDPOINT_ENV, "  ") };
        assert!(
            artifact_reader_from_env().is_none(),
            "a blank value is unset"
        );
        unsafe { std::env::remove_var(ENDPOINT_ENV) };
        assert!(
            artifact_reader_from_env().is_none(),
            "token alone is not enough"
        );
        clear_env();
    }

    #[test]
    #[serial_test::serial]
    fn a_reader_that_cannot_be_built_is_no_reader_not_a_panic() {
        clear_env();
        unsafe {
            std::env::set_var(
                ENDPOINT_ENV,
                "https://admin.example/api/v1/ingest/artifacts",
            );
            // A control character makes `HttpArtifactReader::new` fail.
            std::env::set_var(TOKEN_ENV, "gtm\u{7}x");
        }
        assert!(artifact_reader_from_env().is_none());
        clear_env();
    }

    /// A stand-in for the env reader that records whether it was consulted.
    fn env_probe(called: &AtomicUsize) -> impl FnOnce() -> Option<Arc<dyn ArtifactReader>> + '_ {
        move || {
            called.fetch_add(1, Ordering::SeqCst);
            Some(Arc::new(CountingReader::default()) as Arc<dyn ArtifactReader>)
        }
    }

    #[test]
    fn on_a_single_tenant_host_the_injected_reader_wins_and_the_env_is_not_read() {
        let called = AtomicUsize::new(0);
        let host: Arc<dyn ArtifactReader> = Arc::new(CountingReader::default());
        let chosen =
            crate::host::host_artifact_reader(Some(host.clone()), 1, true, env_probe(&called))
                .expect("a reader");
        assert!(Arc::ptr_eq(&chosen, &host));
        assert_eq!(called.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn on_a_single_tenant_host_without_an_injected_reader_the_env_is_the_fallback() {
        let called = AtomicUsize::new(0);
        assert!(crate::host::host_artifact_reader(None, 1, true, env_probe(&called)).is_some());
        assert_eq!(called.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_reader_the_tenant_guard_dropped_is_not_replaced_by_the_env() {
        let called = AtomicUsize::new(0);
        let host: Arc<dyn ArtifactReader> = Arc::new(CountingReader::default());
        assert!(
            crate::host::host_artifact_reader(Some(host), 2, true, env_probe(&called)).is_none(),
            "one tenant's agents must never read through another tenant's token"
        );
        assert_eq!(called.load(Ordering::SeqCst), 0, "the env is not consulted");
    }

    #[test]
    fn the_env_reader_fallback_is_off_unless_the_host_opts_in() {
        let called = AtomicUsize::new(0);
        assert!(crate::host::host_artifact_reader(None, 1, false, env_probe(&called)).is_none());
        assert_eq!(called.load(Ordering::SeqCst), 0, "env not read");
    }

    /// The standalone runner host is a single-tenant process and opts in.
    #[test]
    fn the_standalone_runner_opts_in_to_the_env_fallback() {
        let src = squash(include_str!("../lib.rs"));
        assert!(src.contains(".with_artifact_env_fallback(true)"));
    }

    #[test]
    fn a_multi_tenant_host_never_uses_the_env_reader() {
        let called = AtomicUsize::new(0);
        assert!(crate::host::host_artifact_reader(None, 2, true, env_probe(&called)).is_none());
        assert_eq!(called.load(Ordering::SeqCst), 0);
    }

    /// The revision path (and every builder below the host) gets exactly the
    /// reader it is handed: with the env variables set and no reader, the
    /// backend has NO reader and the agent gets the fixed notice. An env
    /// reader here would try the (refused) endpoint and say "could not be
    /// loaded" instead.
    #[tokio::test]
    #[serial_test::serial]
    async fn the_backend_builder_never_falls_back_to_the_env() {
        unsafe {
            std::env::set_var(ENDPOINT_ENV, "http://127.0.0.1:1/artifacts");
            std::env::set_var(TOKEN_ENV, "gtm_env");
        }
        let provider = Arc::new(RecordingProvider::default());
        let backend = greentic_backend("unused".into(), None, None).with_cached_provider(
            "deepseek",
            "m",
            provider.clone(),
        );
        let ok = backend.complete(request_with_one_image()).await.is_ok();
        clear_env();
        assert!(ok);
        let sent = provider
            .seen
            .lock()
            .unwrap()
            .pop()
            .expect("chat() was called");
        assert!(
            sent.messages
                .iter()
                .any(|m| m.content.contains("not available in this deployment")),
            "{:?}",
            sent.messages
        );
    }

    /// Never more than the permit count in flight, however many reads start.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reads_are_bounded_by_the_process_permits() {
        struct Gauge {
            now: AtomicUsize,
            max: AtomicUsize,
        }
        impl ArtifactReader for Gauge {
            fn get<'a>(
                &'a self,
                _id: &'a str,
            ) -> Pin<Box<dyn Future<Output = Result<ArtifactBytes, ArtifactError>> + Send + 'a>>
            {
                Box::pin(async move {
                    let n = self.now.fetch_add(1, Ordering::SeqCst) + 1;
                    self.max.fetch_max(n, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                    self.now.fetch_sub(1, Ordering::SeqCst);
                    Err(ArtifactError::NotFound)
                })
            }
        }
        let gauge = Arc::new(Gauge {
            now: AtomicUsize::new(0),
            max: AtomicUsize::new(0),
        });
        let bounded = Arc::new(BoundedReader::with_permits(
            gauge.clone(),
            Arc::new(tokio::sync::Semaphore::new(3)),
        ));
        let mut tasks = Vec::new();
        for _ in 0..6 {
            let b = bounded.clone();
            tasks.push(tokio::spawn(async move { b.get(ID).await.is_err() }));
        }
        // Bounded so a leaked permit fails the test instead of hanging it.
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            for t in tasks {
                assert!(t.await.unwrap());
            }
        })
        .await
        .expect("every read finished: no permit leaked");
        assert_eq!(gauge.max.load(Ordering::SeqCst), 3);
    }

    /// A read cancelled while holding the permit gives it back.
    #[tokio::test]
    async fn a_cancelled_read_releases_its_permit() {
        struct Never;
        impl ArtifactReader for Never {
            fn get<'a>(
                &'a self,
                _id: &'a str,
            ) -> Pin<Box<dyn Future<Output = Result<ArtifactBytes, ArtifactError>> + Send + 'a>>
            {
                Box::pin(std::future::pending())
            }
        }
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let stuck = BoundedReader::with_permits(Arc::new(Never), permits.clone());
        let cancelled =
            tokio::time::timeout(std::time::Duration::from_millis(20), stuck.get(ID)).await;
        assert!(cancelled.is_err(), "the read never finishes");
        assert_eq!(permits.available_permits(), 1, "the permit came back");
    }

    #[test]
    fn the_process_bound_is_small_and_wraps_every_installed_reader() {
        assert_eq!(MAX_CONCURRENT_READS, 4);
        let src = squash(include_str!("artifact_reader_wiring.rs"));
        let start = src
            .find("pub(crate) fn attach_artifact_reader(")
            .expect("fn present");
        let body = &src[start..];
        let body = &body[..body.find("#[cfg(test)]").expect("tests follow")];
        assert!(
            body.contains("backend.with_artifact_reader(Arc::new(BoundedReader::with_permits( reader, process_permits(), )))"),
            "every installed reader must share the process-wide permits"
        );
    }

    #[tokio::test]
    async fn an_attached_reader_resolves_the_attachment() {
        let reader = Arc::new(CountingReader::default());
        let (sent, ok) = run_turn(Some(reader.clone())).await;
        assert!(ok);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
        assert!(sent.messages.iter().any(|m| !m.images.is_empty()));
    }

    #[tokio::test]
    async fn without_a_reader_the_turn_runs_and_the_agent_gets_the_fixed_notice() {
        let (sent, ok) = run_turn(None).await;
        assert!(ok, "a missing reader never fails the turn");
        assert!(sent.messages.iter().all(|m| m.images.is_empty()));
        assert!(
            sent.messages
                .iter()
                .any(|m| m.content.contains("not available in this deployment")),
            "{:?}",
            sent.messages
        );
    }

    #[test]
    fn revision_options_debug_names_the_reader_without_its_credential() {
        let reader: Arc<dyn ArtifactReader> = Arc::new(
            HttpArtifactReader::new(
                "https://admin.example/api/v1/ingest/artifacts".into(),
                "gtm_topsecret".into(),
            )
            .unwrap(),
        );
        let options = crate::runtime::RevisionHostOptions::default().with_artifact_reader(reader);
        let shown = format!("{options:?}");
        assert!(shown.contains("artifact_reader: true"), "{shown}");
        assert!(!shown.contains("gtm_topsecret"), "{shown}");
    }

    fn squash(src: &str) -> String {
        src.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// The reader must reach the backend on every path, or attachments stop
    /// being read with nothing red anywhere. Source ratchets, like the user
    /// ledger's, because the deployed wiring cannot be stood up in a unit test.
    #[test]
    fn the_backend_builder_installs_exactly_the_reader_it_is_handed() {
        let src = squash(include_str!("agent_node.rs"));
        assert!(
            src.contains("greentic_backend( api_key, base_url, artifact_reader, )"),
            "in_process_llm_backend_with_key must install the runtime's reader"
        );
        assert!(
            src.contains("configured_llm_provider(&merged_agents), artifact_reader, )"),
            "build_runtime_with_stores must hand the runtime's reader to the backend"
        );
        assert!(
            !src.contains("artifact_reader_from_env"),
            "below the host, nothing may fall back to the env reader"
        );
        let runtime = squash(include_str!("../runtime.rs"));
        assert!(
            !runtime.contains("artifact_reader_from_env"),
            "the revision path never falls back to the env reader"
        );
    }

    #[test]
    fn every_runtime_wiring_call_receives_the_units_reader() {
        let src = squash(include_str!("../runtime.rs"));
        let mut calls = 0;
        for needle in [
            "build_agent_node_wiring_metered(",
            "build_agent_node_wiring_ephemeral_metered(",
        ] {
            for (start, _) in src.match_indices(needle) {
                let rest = &src[start..];
                let end = rest.find(".await").expect("call has an .await");
                assert!(
                    rest[..end].contains("artifact_reader.clone(),"),
                    "`{needle}` must receive the unit's reader: {}",
                    &rest[..end]
                );
                calls += 1;
            }
        }
        assert_eq!(calls, 3, "the wiring call sites moved; re-check them");
        assert!(
            src.contains("options.artifact_reader,"),
            "load_revision_with"
        );
        assert!(
            src.contains("// The unit's own attachment reader (the revision's host options). #[cfg(feature = \"agentic-worker\")] artifact_reader,"),
            "load_revision_impl must forward the reader to from_packs_with_rollout"
        );
        let host = squash(include_str!("../host.rs"));
        assert!(
            host.contains("#[cfg(feature = \"agentic-worker\")] self.artifact_reader(),"),
            "the host's reader must reach the runtimes it loads"
        );
        let watcher = squash(include_str!("../watcher.rs"));
        assert!(
            watcher.contains("TenantRuntime::from_packs_with_artifact_reader(") && watcher.contains("#[cfg(feature = \"agentic-worker\")] artifact_reader.clone(), #[cfg(feature = \"agentic-worker\")] ext_artifact_port.clone(), #[cfg(feature = \"agentic-worker\")] Some(stream_observers.clone()),"),
            "a pack reload must keep the host's reader"
        );
    }
}
