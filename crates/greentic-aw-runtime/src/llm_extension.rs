//! `ExtensionLlmBackend` — runs the worker LLM through an installed extension
//! instead of a hardcoded provider client (spec: LLM provider as an extension).
//!
//! Approach B: the LLM-bridge extension exposes a tool named `complete` whose
//! args JSON is a [`BridgeRequest`] (system prompt + history + tool schemas +
//! the host-resolved credential) and whose result JSON is the existing
//! [`LlmResponse`]. Dispatch goes through the generic
//! `ExtensionRuntime::invoke_tool` (sync → `spawn_blocking`), abstracted behind
//! [`LlmExtensionInvoker`] so the backend is unit-testable without a real WASM
//! component (`invoke_tool` requires a loaded component; `for_test()` has none).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Serialize;

use crate::error::LlmError;
use crate::llm::{LlmBackend, LlmRequest, LlmResponse, LlmToolSchema};
use crate::state::ChatMessage;

/// The tool an LLM-bridge extension exposes (approach B).
pub const LLM_COMPLETE_TOOL: &str = "complete";

/// Host-resolved credential passed to the bridge per call (spec Decision 1:
/// the host resolves creds and passes them in the request; the extension is a
/// stateless bridge). `secret_ref` is reserved for a future hardening path.
#[derive(Clone, Debug, Serialize)]
pub struct BridgeCredential {
    pub provider: String,
    pub model: String,
    pub api_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

/// The wire payload sent to the bridge's `complete` tool.
#[derive(Debug, Serialize)]
struct BridgeRequest<'a> {
    system_prompt: &'a str,
    history: &'a [ChatMessage],
    tools: &'a [LlmToolSchema],
    credential: &'a BridgeCredential,
}

/// Synchronous dispatch seam. Prod impl calls `ExtensionRuntime::invoke_tool`;
/// tests script the JSON response without a WASM component.
pub trait LlmExtensionInvoker: Send + Sync {
    fn invoke(&self, extension_id: &str, tool: &str, args_json: &str) -> Result<String, String>;
}

/// Production invoker over a loaded `ExtensionRuntime`.
pub struct RuntimeInvoker {
    pub ext_runtime: Arc<greentic_ext_runtime::ExtensionRuntime>,
}

impl LlmExtensionInvoker for RuntimeInvoker {
    fn invoke(&self, extension_id: &str, tool: &str, args_json: &str) -> Result<String, String> {
        self.ext_runtime
            .invoke_tool(extension_id, tool, args_json)
            .map_err(|e| e.to_string())
    }
}

/// Credential source for [`ExtensionLlmBackend`]: either a frozen static
/// credential (original behaviour) or a per-request resolver that derives the
/// credential from `request.provider` at call time.
enum CredentialSource {
    Static(BridgeCredential),
    Resolver(Arc<crate::llm_credential::SecretsBackedCredentialResolver>),
}

/// `LlmBackend` that delegates to an LLM-bridge extension.
pub struct ExtensionLlmBackend {
    invoker: Arc<dyn LlmExtensionInvoker>,
    extension_id: String,
    credential: CredentialSource,
}

impl ExtensionLlmBackend {
    /// Build over a real `ExtensionRuntime` with a frozen credential.
    pub fn new(
        ext_runtime: Arc<greentic_ext_runtime::ExtensionRuntime>,
        extension_id: impl Into<String>,
        credential: BridgeCredential,
    ) -> Self {
        Self {
            invoker: Arc::new(RuntimeInvoker { ext_runtime }),
            extension_id: extension_id.into(),
            credential: CredentialSource::Static(credential),
        }
    }

    /// Build over an arbitrary invoker with a frozen credential (tests).
    pub fn with_invoker(
        invoker: Arc<dyn LlmExtensionInvoker>,
        extension_id: impl Into<String>,
        credential: BridgeCredential,
    ) -> Self {
        Self {
            invoker,
            extension_id: extension_id.into(),
            credential: CredentialSource::Static(credential),
        }
    }

    /// Build over an arbitrary invoker with a per-request resolver (tests).
    pub fn with_resolver(
        invoker: Arc<dyn LlmExtensionInvoker>,
        extension_id: impl Into<String>,
        resolver: Arc<crate::llm_credential::SecretsBackedCredentialResolver>,
    ) -> Self {
        Self {
            invoker,
            extension_id: extension_id.into(),
            credential: CredentialSource::Resolver(resolver),
        }
    }

    /// Build over a real `ExtensionRuntime` with a per-request resolver
    /// (production path: mirrors `new()` but resolves credentials dynamically).
    pub fn with_resolver_runtime(
        ext_runtime: Arc<greentic_ext_runtime::ExtensionRuntime>,
        extension_id: impl Into<String>,
        resolver: Arc<crate::llm_credential::SecretsBackedCredentialResolver>,
    ) -> Self {
        Self {
            invoker: Arc::new(RuntimeInvoker { ext_runtime }),
            extension_id: extension_id.into(),
            credential: CredentialSource::Resolver(resolver),
        }
    }
}

impl LlmBackend for ExtensionLlmBackend {
    fn complete<'a>(
        &'a self,
        request: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        let invoker = self.invoker.clone();
        let ext_id = self.extension_id.clone();
        let credential_source = match &self.credential {
            CredentialSource::Static(c) => CredentialSource::Static(c.clone()),
            CredentialSource::Resolver(r) => CredentialSource::Resolver(r.clone()),
        };
        Box::pin(async move {
            let credential = match credential_source {
                CredentialSource::Static(c) => c,
                CredentialSource::Resolver(r) => r.resolve(&request.provider).await?,
            };
            // The bridge cannot open attachments: it gets the fixed notice and
            // never a reference, id or name.
            let mut history = request.history.clone();
            crate::attachments_materialize::announce_unreadable(&mut history);
            let payload = BridgeRequest {
                system_prompt: &request.system_prompt,
                history: &history,
                tools: &request.tools,
                credential: &credential,
            };
            let args_json = serde_json::to_string(&payload)
                .map_err(|e| LlmError::BadRequest(format!("encode bridge request: {e}")))?;
            let raw = tokio::task::spawn_blocking(move || {
                invoker.invoke(&ext_id, LLM_COMPLETE_TOOL, &args_json)
            })
            .await
            .map_err(|e| LlmError::Transport(format!("llm bridge join: {e}")))?
            // A failed dispatch (extension missing / WASM trap) is a config-class
            // error, not a transient 5xx — surface as BadRequest so the retry
            // decorator does NOT loop on it.
            .map_err(LlmError::BadRequest)?;
            serde_json::from_str::<LlmResponse>(&raw)
                .map_err(|e| LlmError::Decode(format!("decode bridge response: {e}")))
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::LlmProviderRef;
    use crate::state::ToolCallRecord;
    use std::sync::Mutex;

    fn cred() -> BridgeCredential {
        BridgeCredential {
            provider: "openai".into(),
            model: "gpt-4o".into(),
            api_key: "sk-test".into(),
            base_url: None,
        }
    }

    fn req() -> LlmRequest {
        LlmRequest {
            system_prompt: "be helpful".into(),
            // attachments unsupported on this backend
            history: vec![ChatMessage::User {
                content: "hi".into(),
                attachments: Vec::new(),
            }],
            tools: vec![LlmToolSchema {
                extension_id: "http".into(),
                tool_name: "fetch".into(),
                description: "fetch".into(),
                parameters: serde_json::json!({ "type": "object" }),
            }],
            provider: LlmProviderRef {
                provider: "openai".into(),
                model: "gpt-4o".into(),
                credential_ref: None,
            },
            turn_attachments: Default::default(),
        }
    }

    /// Captures the args JSON it was handed and returns a scripted reply.
    struct ScriptInvoker {
        seen: Mutex<Option<(String, String, String)>>,
        reply: Result<String, String>,
    }
    impl LlmExtensionInvoker for ScriptInvoker {
        fn invoke(&self, ext: &str, tool: &str, args: &str) -> Result<String, String> {
            *self.seen.lock().unwrap() = Some((ext.into(), tool.into(), args.into()));
            self.reply.clone()
        }
    }

    #[tokio::test]
    async fn sends_complete_tool_with_credential_and_parses_response() {
        let reply = serde_json::json!({
            "content": "done",
            "tool_calls": [{
                "call_id": "c1", "extension_id": "http", "tool_name": "fetch",
                "args": { "url": "x" }
            }],
            "tokens_in": 3, "tokens_out": 5
        })
        .to_string();
        let inv = Arc::new(ScriptInvoker {
            seen: Mutex::new(None),
            reply: Ok(reply),
        });
        let backend = ExtensionLlmBackend::with_invoker(inv.clone(), "llm-openai-bridge", cred());

        let resp = backend.complete(req()).await.unwrap();
        assert_eq!(resp.content.as_deref(), Some("done"));
        assert_eq!(resp.tool_calls.len(), 1);
        let tc: &ToolCallRecord = &resp.tool_calls[0];
        assert_eq!(tc.extension_id, "http");
        assert_eq!(tc.tool_name, "fetch");

        // Dispatched to the right extension + tool, and the credential + tools
        // schema rode along in the args.
        let (ext, tool, args) = inv.seen.lock().unwrap().clone().unwrap();
        assert_eq!(ext, "llm-openai-bridge");
        assert_eq!(tool, "complete");
        let v: serde_json::Value = serde_json::from_str(&args).unwrap();
        assert_eq!(v["credential"]["api_key"], "sk-test");
        assert_eq!(v["credential"]["provider"], "openai");
        assert_eq!(v["system_prompt"], "be helpful");
        assert_eq!(v["tools"][0]["tool_name"], "fetch");
        // base_url omitted when None.
        assert!(v["credential"].get("base_url").is_none());
    }

    /// The bridge extension cannot open attachments: it gets the fixed notice
    /// in the message text and no attachment reference, id or name.
    #[tokio::test]
    async fn attachments_are_announced_with_the_fixed_notice_and_never_sent() {
        use crate::attachments::{AttachmentKind, AttachmentRef};
        let reply = serde_json::json!({
            "content": "ok", "tool_calls": [], "tokens_in": 1, "tokens_out": 1
        })
        .to_string();
        let inv = Arc::new(ScriptInvoker {
            seen: Mutex::new(None),
            reply: Ok(reply),
        });
        let backend = ExtensionLlmBackend::with_invoker(inv.clone(), "bridge", cred());
        let mut request = req();
        request.history = vec![ChatMessage::User {
            content: "look".into(),
            attachments: vec![AttachmentRef {
                id: "artifact://a".into(),
                mime_type: "text/plain".into(),
                name: Some("secret-plan.txt".into()),
                size_bytes: None,
                kind: AttachmentKind::Document,
                text_ref: None,
            }],
        }];
        backend.complete(request).await.unwrap();
        let (_, _, args) = inv.seen.lock().unwrap().clone().unwrap();
        let v: serde_json::Value = serde_json::from_str(&args).unwrap();
        let content = v["history"][0]["content"].as_str().unwrap();
        assert_eq!(
            content,
            format!("look{}", crate::attachments_materialize::unreadable_note(1))
        );
        assert!(
            !args.contains("secret-plan") && !args.contains("artifact://"),
            "{args}"
        );
    }

    #[tokio::test]
    async fn invoker_error_maps_to_bad_request() {
        let inv = Arc::new(ScriptInvoker {
            seen: Mutex::new(None),
            reply: Err("NotFound(llm-openai-bridge)".into()),
        });
        let backend = ExtensionLlmBackend::with_invoker(inv, "llm-openai-bridge", cred());
        let err = backend.complete(req()).await.unwrap_err();
        assert!(matches!(err, LlmError::BadRequest(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn malformed_reply_maps_to_decode() {
        let inv = Arc::new(ScriptInvoker {
            seen: Mutex::new(None),
            reply: Ok("not json".into()),
        });
        let backend = ExtensionLlmBackend::with_invoker(inv, "llm-openai-bridge", cred());
        let err = backend.complete(req()).await.unwrap_err();
        assert!(matches!(err, LlmError::Decode(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn resolving_backend_uses_request_provider_credential() {
        use crate::llm_credential::SecretsBackedCredentialResolver;
        use async_trait::async_trait;

        struct FakeSecrets;
        #[async_trait]
        impl greentic_secrets_lib::SecretsManager for FakeSecrets {
            async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
                assert_eq!(path, "secrets://default/acme/_/llm/cred-uuid");
                Ok(b"sk-live".to_vec())
            }
            async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
                Ok(())
            }
            async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
                Ok(())
            }
        }

        let resolver = Arc::new(SecretsBackedCredentialResolver::new(
            Arc::new(FakeSecrets),
            "acme",
        ));
        let inv = Arc::new(ScriptInvoker {
            seen: Mutex::new(None),
            reply: Ok(serde_json::json!({
                "content": "ok",
                "tool_calls": [],
                "tokens_in": 1,
                "tokens_out": 1
            })
            .to_string()),
        });
        let backend = ExtensionLlmBackend::with_resolver(inv.clone(), "llm-bridge", resolver);
        let mut r = req();
        r.provider.provider = "anthropic".into();
        r.provider.model = "claude-3-5-sonnet-latest".into();
        r.provider.credential_ref = Some("cred-uuid".into());
        backend.complete(r).await.unwrap();
        let (_ext, _tool, args) = inv.seen.lock().unwrap().clone().unwrap();
        let v: serde_json::Value = serde_json::from_str(&args).unwrap();
        assert_eq!(v["credential"]["provider"], "anthropic");
        assert_eq!(v["credential"]["api_key"], "sk-live");
        assert_eq!(v["credential"]["model"], "claude-3-5-sonnet-latest");
    }
}
