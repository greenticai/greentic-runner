//! Which [`ArtifactReader`] resolves attachment bytes for the in-process
//! multi-provider LLM backend (`GreenticLlmBackend`).
//!
//! Precedence, decided in ONE place ([`select_artifact_reader`]):
//!
//! 1. the reader the embedding host injected for THIS runtime
//!    (`RevisionHostOptions::with_artifact_reader` for a deployed unit,
//!    `HostBuilder::with_artifact_reader` for a single-tenant host such as
//!    the designer's Run Demo);
//! 2. the env fallback ([`artifact_reader_from_env`]): local runs and the
//!    Test chat sidecar;
//! 3. none: the backend still runs every turn, and an attachment is replaced
//!    by a fixed notice telling the agent it cannot open it.
//!
//! A reader holds ONE door token, and the door decides the tenant from that
//! token. A reader is therefore never shared across tenants: it is built per
//! host or per deployed unit and handed to the runtime built for that unit.
//!
//! The token is a credential: it never reaches a log line (the client error
//! carries no token) and `HttpArtifactReader`'s `Debug` redacts it.

use std::sync::Arc;

use greentic_aw_runtime::{ArtifactReader, GreenticLlmBackend, HttpArtifactReader};

/// Door base, ending in `/artifacts` (the reader appends `/get`).
pub(crate) const ENDPOINT_ENV: &str = "GREENTIC_ARTIFACT_ENDPOINT";
/// The unit's door token (the metering token carrying the `artifacts` purpose).
pub(crate) const TOKEN_ENV: &str = "GREENTIC_ARTIFACT_TOKEN";

fn non_blank_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// Env fallback for the artifact reader (local runs, the Test chat sidecar).
/// The deployed lanes get an explicit reader from `greentic-start` instead.
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

/// The host-injected reader wins; the env reader is only the fallback, and is
/// not even read when the host injected one.
pub(crate) fn select_artifact_reader(
    host: Option<Arc<dyn ArtifactReader>>,
) -> Option<Arc<dyn ArtifactReader>> {
    host.or_else(artifact_reader_from_env)
}

/// Install `reader` on `backend` when there is one. Without one the backend
/// is returned unchanged: every turn still runs, and each attachment becomes
/// the fixed "not available in this deployment" notice.
pub(crate) fn attach_artifact_reader(
    backend: GreenticLlmBackend,
    reader: Option<Arc<dyn ArtifactReader>>,
) -> GreenticLlmBackend {
    match reader {
        Some(reader) => {
            tracing::info!("AW LLM backend: artifact reader wired (attachments readable)");
            backend.with_artifact_reader(reader)
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
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use greentic_aw_runtime::state::ChatMessage;
    use greentic_aw_runtime::{
        ArtifactBytes, ArtifactError, AttachmentKind, AttachmentRef, LlmBackend, LlmProviderRef,
        LlmRequest,
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

    #[test]
    #[serial_test::serial]
    fn the_host_reader_wins_over_the_env() {
        unsafe {
            std::env::set_var(
                ENDPOINT_ENV,
                "https://admin.example/api/v1/ingest/artifacts",
            );
            std::env::set_var(TOKEN_ENV, "gtm_env");
        }
        let host: Arc<dyn ArtifactReader> = Arc::new(CountingReader::default());
        let chosen = select_artifact_reader(Some(host.clone())).expect("a reader");
        assert!(Arc::ptr_eq(&chosen, &host));
        clear_env();
    }

    #[test]
    #[serial_test::serial]
    fn without_a_host_reader_the_env_reader_is_the_fallback() {
        clear_env();
        assert!(select_artifact_reader(None).is_none(), "nothing configured");
        unsafe {
            std::env::set_var(
                ENDPOINT_ENV,
                "https://admin.example/api/v1/ingest/artifacts",
            );
            std::env::set_var(TOKEN_ENV, "gtm_env");
        }
        assert!(select_artifact_reader(None).is_some(), "env fallback");
        clear_env();
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
    fn a_host_wide_reader_survives_only_on_a_single_tenant_host() {
        use crate::host::single_tenant_reader;
        let reader: Arc<dyn ArtifactReader> = Arc::new(CountingReader::default());
        assert!(single_tenant_reader(Some(reader.clone()), 1).is_some());
        assert!(
            single_tenant_reader(Some(reader.clone()), 2).is_none(),
            "one tenant's agents must never read through another tenant's token"
        );
        assert!(single_tenant_reader(None, 1).is_none());
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
    fn the_backend_builder_selects_the_host_reader_then_the_env() {
        let src = squash(include_str!("agent_node.rs"));
        assert!(
            src.contains(
                "attach_artifact_reader( greentic_aw_runtime::GreenticLlmBackend::new(api_key, base_url), select_artifact_reader(artifact_reader), )"
            ),
            "in_process_llm_backend_with_key must install the selected reader"
        );
        assert!(
            src.contains("configured_llm_provider(&merged_agents), artifact_reader, )"),
            "build_runtime_with_stores must hand the runtime's reader to the backend"
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
            watcher.contains("#[cfg(feature = \"agentic-worker\")] artifact_reader.clone(), #[cfg(feature = \"agentic-worker\")] Some(stream_observers.clone()),"),
            "a pack reload must keep the host's reader"
        );
    }
}
