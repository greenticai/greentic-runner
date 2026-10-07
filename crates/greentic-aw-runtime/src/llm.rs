//! LLM backend trait + a retry decorator. Concrete OpenAI / Anthropic
//! impls are added in Phase 3; the trait + decorator are introduced
//! here so the loop can be wired against mocks during Phase 1.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::LlmError;
use crate::state::{ChatMessage, ToolCallRecord};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LlmRequest {
    pub system_prompt: String,
    pub history: Vec<ChatMessage>,
    pub tools: Vec<LlmToolSchema>,
    /// Resolved provider + model — backend selects credentials/endpoint
    /// based on this.
    pub provider: crate::config::LlmProviderRef,
    /// Per-TURN memo of the current user message's materialised attachments
    /// (see [`crate::attachments_materialize::TurnAttachments`]). The loop
    /// creates one per turn; `Default` is "no memo": a backend then
    /// materialises on every call. Never serialised: it may hold bytes.
    #[serde(skip)]
    pub turn_attachments: crate::attachments_materialize::TurnAttachments,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LlmToolSchema {
    pub extension_id: String,
    pub tool_name: String,
    pub description: String,
    pub parameters: serde_json::Value, // JSON schema
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LlmResponse {
    /// `content` is `Some` when the LLM emits a textual reply. Per
    /// spec Decision 12, if `tool_calls` is non-empty AND `content`
    /// is `Some`, the loop treats `content` as a reasoning trace and
    /// executes `tool_calls` (tool_calls win).
    pub content: Option<String>,
    pub tool_calls: Vec<ToolCallRecord>,
    pub tokens_in: u32,
    pub tokens_out: u32,
}

/// Callback invoked with each incremental text chunk during a streaming
/// completion. Must be cheap and non-blocking.
pub type OnDelta = Box<dyn Fn(&str) + Send + Sync>;

pub trait LlmBackend: Send + Sync {
    fn complete<'a>(
        &'a self,
        request: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>>;

    /// Streaming completion. The default implementation calls
    /// [`LlmBackend::complete`] and emits the full reply as a single
    /// delta, so existing third-party backends keep working unchanged
    /// without implementing this method.
    fn complete_streaming<'a>(
        &'a self,
        request: LlmRequest,
        on_delta: OnDelta,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        Box::pin(async move {
            let resp = self.complete(request).await?;
            if let Some(text) = &resp.content
                && !text.is_empty()
            {
                on_delta(text);
            }
            Ok(resp)
        })
    }
}

/// Wraps any [`LlmBackend`] with exponential-backoff retry on
/// [`LlmError::ServiceUnavailable`]. 4xx-class errors are NOT retried.
pub struct RetryingLlmBackend<B: LlmBackend> {
    inner: B,
    attempts: u32,
    backoff: Duration,
}

impl<B: LlmBackend> RetryingLlmBackend<B> {
    pub fn new(inner: B, attempts: u32, backoff: Duration) -> Self {
        Self {
            inner,
            attempts,
            backoff,
        }
    }
}

impl<B: LlmBackend + Send + Sync> LlmBackend for RetryingLlmBackend<B> {
    fn complete<'a>(
        &'a self,
        request: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        Box::pin(async move {
            let mut delay = self.backoff;
            let mut last_err = None;
            for attempt in 0..self.attempts.max(1) {
                match self.inner.complete(request.clone()).await {
                    Ok(r) => return Ok(r),
                    Err(LlmError::ServiceUnavailable) => {
                        last_err = Some(LlmError::ServiceUnavailable);
                        if attempt + 1 < self.attempts {
                            tokio::time::sleep(delay).await;
                            delay = delay.saturating_mul(2);
                        }
                    }
                    Err(other) => return Err(other), // 4xx-class: do not retry
                }
            }
            Err(last_err.unwrap_or(LlmError::ServiceUnavailable))
        })
    }

    fn complete_streaming<'a>(
        &'a self,
        request: LlmRequest,
        on_delta: OnDelta,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        Box::pin(async move {
            // Shared flag tracking whether any delta has reached the
            // consumer. Wrap the caller's callback so we observe emission.
            let emitted = Arc::new(AtomicBool::new(false));
            let user_cb: Arc<OnDelta> = Arc::new(on_delta);

            let mut delay = self.backoff;
            let mut last_err = None;
            for attempt in 0..self.attempts.max(1) {
                let emitted_for_attempt = emitted.clone();
                let cb = user_cb.clone();
                let wrapped: OnDelta = Box::new(move |chunk: &str| {
                    emitted_for_attempt.store(true, Ordering::SeqCst);
                    cb(chunk);
                });
                match self
                    .inner
                    .complete_streaming(request.clone(), wrapped)
                    .await
                {
                    Ok(r) => return Ok(r),
                    Err(LlmError::ServiceUnavailable) => {
                        // Once partial output has reached the consumer we
                        // must NOT retry — a fresh attempt would duplicate
                        // already-emitted tokens. Surface the error instead.
                        if emitted.load(Ordering::SeqCst) {
                            return Err(LlmError::ServiceUnavailable);
                        }
                        last_err = Some(LlmError::ServiceUnavailable);
                        if attempt + 1 < self.attempts {
                            tokio::time::sleep(delay).await;
                            delay = delay.saturating_mul(2);
                        }
                    }
                    Err(other) => return Err(other), // 4xx-class: do not retry
                }
            }
            Err(last_err.unwrap_or(LlmError::ServiceUnavailable))
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct ScriptedBackend {
        responses: Mutex<Vec<Result<LlmResponse, LlmError>>>,
    }

    impl LlmBackend for ScriptedBackend {
        fn complete<'a>(
            &'a self,
            _r: LlmRequest,
        ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
            let next = self.responses.lock().unwrap().remove(0);
            Box::pin(async move { next })
        }
    }

    fn req() -> LlmRequest {
        LlmRequest {
            system_prompt: "".into(),
            history: vec![],
            tools: vec![],
            provider: crate::config::LlmProviderRef {
                provider: "openai".into(),
                model: "x".into(),
                credential_ref: None,
            },
            turn_attachments: Default::default(),
        }
    }

    fn ok_resp() -> LlmResponse {
        LlmResponse {
            content: Some("hi".into()),
            tool_calls: vec![],
            tokens_in: 1,
            tokens_out: 1,
        }
    }

    #[tokio::test]
    async fn retries_on_service_unavailable_then_succeeds() {
        let inner = ScriptedBackend {
            responses: Mutex::new(vec![
                Err(LlmError::ServiceUnavailable),
                Err(LlmError::ServiceUnavailable),
                Ok(ok_resp()),
            ]),
        };
        let r = RetryingLlmBackend::new(inner, 3, Duration::from_millis(1));
        let out = r.complete(req()).await.unwrap();
        assert_eq!(out.content.as_deref(), Some("hi"));
    }

    #[tokio::test]
    async fn does_not_retry_on_bad_request() {
        let inner = ScriptedBackend {
            responses: Mutex::new(vec![Err(LlmError::BadRequest("nope".into()))]),
        };
        let r = RetryingLlmBackend::new(inner, 5, Duration::from_millis(1));
        let err = r.complete(req()).await.unwrap_err();
        assert!(matches!(err, LlmError::BadRequest(_)));
    }

    #[tokio::test]
    async fn default_streaming_falls_back_to_single_delta() {
        struct OneShot;
        impl LlmBackend for OneShot {
            fn complete<'a>(
                &'a self,
                _r: LlmRequest,
            ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>>
            {
                Box::pin(async {
                    Ok(LlmResponse {
                        content: Some("whole reply".into()),
                        tool_calls: vec![],
                        tokens_in: 1,
                        tokens_out: 2,
                    })
                })
            }
        }
        let collected = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let c = collected.clone();
        let on_delta: OnDelta = Box::new(move |chunk: &str| {
            c.lock().expect("lock").push(chunk.to_string());
        });
        let resp = OneShot
            .complete_streaming(
                LlmRequest {
                    system_prompt: "s".into(),
                    history: vec![],
                    tools: vec![],
                    provider: crate::config::LlmProviderRef {
                        provider: "openai".into(),
                        model: "m".into(),
                        credential_ref: None,
                    },
                    turn_attachments: Default::default(),
                },
                on_delta,
            )
            .await
            .expect("ok");
        assert_eq!(resp.content.as_deref(), Some("whole reply"));
        assert_eq!(
            *collected.lock().expect("lock"),
            vec!["whole reply".to_string()]
        );
    }

    #[tokio::test]
    async fn retry_streaming_does_not_retry_after_delta_emitted() {
        // Inner backend emits a delta then fails with ServiceUnavailable:
        // the retry wrapper must NOT retry because partial output already
        // reached the consumer.
        struct PartialThenFail;
        impl LlmBackend for PartialThenFail {
            fn complete<'a>(
                &'a self,
                _r: LlmRequest,
            ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>>
            {
                Box::pin(async { Err(LlmError::ServiceUnavailable) })
            }
            fn complete_streaming<'a>(
                &'a self,
                _request: LlmRequest,
                on_delta: OnDelta,
            ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>>
            {
                Box::pin(async move {
                    on_delta("partial");
                    Err(LlmError::ServiceUnavailable)
                })
            }
        }
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let c = calls.clone();
        let on_delta: OnDelta = Box::new(move |_chunk: &str| {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        let r = RetryingLlmBackend::new(PartialThenFail, 3, Duration::from_millis(1));
        let err = r.complete_streaming(req(), on_delta).await.unwrap_err();
        assert!(matches!(err, LlmError::ServiceUnavailable));
        // Exactly one delta emitted, no retries re-invoking the backend.
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_streaming_retries_when_no_delta_emitted() {
        // Inner fails twice with ServiceUnavailable WITHOUT emitting a
        // delta, then succeeds: retry must kick in.
        struct FailThenOk {
            responses: Mutex<Vec<Result<LlmResponse, LlmError>>>,
        }
        impl LlmBackend for FailThenOk {
            fn complete<'a>(
                &'a self,
                _r: LlmRequest,
            ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>>
            {
                let next = self.responses.lock().unwrap().remove(0);
                Box::pin(async move {
                    match next {
                        Ok(resp) => Ok(resp),
                        Err(e) => Err(e),
                    }
                })
            }
        }
        let inner = FailThenOk {
            responses: Mutex::new(vec![
                Err(LlmError::ServiceUnavailable),
                Err(LlmError::ServiceUnavailable),
                Ok(ok_resp()),
            ]),
        };
        let on_delta: OnDelta = Box::new(|_chunk: &str| {});
        let r = RetryingLlmBackend::new(inner, 3, Duration::from_millis(1));
        let out = r.complete_streaming(req(), on_delta).await.unwrap();
        assert_eq!(out.content.as_deref(), Some("hi"));
    }

    #[tokio::test]
    async fn returns_service_unavailable_after_all_attempts() {
        let inner = ScriptedBackend {
            responses: Mutex::new(vec![
                Err(LlmError::ServiceUnavailable),
                Err(LlmError::ServiceUnavailable),
                Err(LlmError::ServiceUnavailable),
            ]),
        };
        let r = RetryingLlmBackend::new(inner, 3, Duration::from_millis(1));
        let err = r.complete(req()).await.unwrap_err();
        assert!(matches!(err, LlmError::ServiceUnavailable));
    }
}
