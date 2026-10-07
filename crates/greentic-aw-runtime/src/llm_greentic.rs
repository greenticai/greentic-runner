//! `GreenticLlmBackend` — adapts a [`greentic_llm`] provider to the AW
//! [`LlmBackend`](crate::llm::LlmBackend) trait, giving `dw.agent` nodes the
//! full multi-provider matrix (DeepSeek, Anthropic, Gemini, …) instead of the
//! hardwired in-process OpenAI client.
//!
//! Mirrors the designer's `aw_backend_adapter`, but maps the AW request/response
//! types straight to `greentic_llm` types (no wire-`Value` round-trip) and reuses
//! the `_FN_`/`_DOT_` tool-name codec from [`crate::llm_openai`] so dotted
//! extension ids survive providers that reject dots in function names.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use greentic_llm::{
    ChatImage as GChatImage, ChatMessage as GChatMessage, ChatRequest as GChatRequest,
    ChatResponse as GChatResponse, Credential, LlmProvider, MessageRole, ProviderKind, RigBackend,
    ToolCall as GToolCall, ToolDef as GToolDef,
};

use crate::artifact_reader::ArtifactReader;
use crate::attachments_materialize::{TurnAttachments, TurnKey, materialize};
use crate::error::LlmError;
use crate::llm::{LlmBackend, LlmRequest, LlmResponse};
use crate::state::{ChatMessage, ToolCallRecord};
use crate::tool_wire_name::{ToolNameCodec, wire_tool_name};

/// AW LLM backend backed by greentic-llm's multi-provider `RigBackend`.
///
/// One backend serves every agent in the process; a `RigBackend` is built
/// lazily per `(provider, model)` pair — taken from each agent's
/// `AgentConfig.llm` and carried on the request — and cached. All providers
/// share the single env-resolved API key; per-provider keys are a follow-up.
pub struct GreenticLlmBackend {
    api_key: String,
    base_url: Option<String>,
    cache: Mutex<HashMap<(String, String), Arc<dyn LlmProvider>>>,
    /// Resolves attachment bytes for the current turn. `None`: attachments
    /// are not materialised and the agent is told so (the turn still runs).
    artifacts: Option<Arc<dyn ArtifactReader>>,
    /// Makes the per-turn nonce of document-block markers. Injected only in
    /// tests, to make the markers deterministic.
    nonce_source: fn() -> String,
}

/// 122 random bits (a v4 UUID), as 32 lowercase hex characters.
fn random_nonce() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

impl GreenticLlmBackend {
    /// `api_key` is the env-resolved key used for whatever provider an agent
    /// declares; `base_url` overrides the provider endpoint (self-hosted
    /// gateways / Ollama) and is omitted when blank.
    pub fn new(api_key: impl Into<String>, base_url: Option<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.filter(|s| !s.trim().is_empty()),
            cache: Mutex::new(HashMap::new()),
            artifacts: None,
            nonce_source: random_nonce,
        }
    }

    /// Inject the reader used to resolve attachment bytes for the current turn.
    pub fn with_artifact_reader(mut self, reader: Arc<dyn ArtifactReader>) -> Self {
        self.artifacts = Some(reader);
        self
    }

    /// Test seam: make document-block nonces deterministic.
    #[cfg(any(test, feature = "test-mock"))]
    pub fn with_nonce_source(mut self, source: fn() -> String) -> Self {
        self.nonce_source = source;
        self
    }

    /// Test seam: serve `(provider, model)` from `p` instead of building a
    /// `RigBackend`.
    #[cfg(any(test, feature = "test-mock"))]
    pub fn with_cached_provider(
        self,
        provider: &str,
        model: &str,
        p: Arc<dyn LlmProvider>,
    ) -> Self {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert((provider.to_string(), model.to_string()), p);
        self
    }

    /// Resolve (or build + cache) the greentic-llm provider for an agent's
    /// declared provider + model.
    fn provider_for(&self, provider: &str, model: &str) -> Result<Arc<dyn LlmProvider>, LlmError> {
        let key = (provider.to_string(), model.to_string());
        if let Some(found) = self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
        {
            return Ok(found.clone());
        }
        let kind: ProviderKind = provider
            .parse()
            .map_err(|_| LlmError::BadRequest(format!("unknown LLM provider '{provider}'")))?;
        // `Credential` is `ZeroizeOnDrop` (implements Drop), so struct-update
        // syntax (`..Default::default()`) cannot move out of it — build a
        // default and set the fields we have.
        #[allow(clippy::field_reassign_with_default)]
        let credential = {
            let mut credential = Credential::default();
            credential.api_key = self.api_key.clone();
            credential.base_url = self.base_url.clone();
            credential
        };
        let backend = RigBackend::new(kind, model, &credential)
            .map_err(|e| LlmError::BadRequest(format!("build {provider} backend: {e}")))?;
        let arc: Arc<dyn LlmProvider> = Arc::new(backend);
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key, arc.clone());
        Ok(arc)
    }
}

impl LlmBackend for GreenticLlmBackend {
    fn complete<'a>(
        &'a self,
        request: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        Box::pin(async move {
            let provider =
                self.provider_for(&request.provider.provider, &request.provider.model)?;
            // The vision gate runs BEFORE chat(): a provider without vision
            // refuses any image with a hard error, so an image it cannot take
            // is never sent and the agent is told instead.
            let vision = provider.capabilities().vision;
            let mut request = request;
            let memo = request.turn_attachments.clone();
            let images = self
                .apply_attachments(&mut request.history, vision, &memo)
                .await;
            let chat_request = build_chat_request(&request, images);
            // Built from the list the model is shown: a sanitised wire name
            // cannot be split back apart by string surgery.
            let codec = ToolNameCodec::for_tools(&request.tools);
            let response = provider
                .chat(chat_request)
                .await
                // greentic-llm errors are auth/config/transport-class; surface as
                // BadRequest so `RetryingLlmBackend` does not loop on a
                // deterministic failure.
                .map_err(|e| LlmError::BadRequest(e.to_string()))?;
            Ok(map_response(response, &codec))
        })
    }
}

impl GreenticLlmBackend {
    /// Materialise the attachments of the LAST user message only; an earlier
    /// user message keeps a fixed marker so a long conversation never fetches
    /// old files again. Mutates the REQUEST's copy of the history, never the
    /// conversation state (the loop hands over a clone), so state keeps
    /// references only. Returns the images for the last user message.
    async fn apply_attachments(
        &self,
        history: &mut [ChatMessage],
        vision: bool,
        memo: &TurnAttachments,
    ) -> Vec<GChatImage> {
        let last_user = history
            .iter()
            .rposition(|m| matches!(m, ChatMessage::User { .. }));
        let mut images = Vec::new();
        for (idx, msg) in history.iter_mut().enumerate() {
            let ChatMessage::User {
                content,
                attachments,
            } = msg
            else {
                continue;
            };
            if attachments.is_empty() {
                continue;
            }
            if Some(idx) == last_user {
                let key = TurnKey {
                    last_user_index: idx,
                    vision,
                };
                let refs: &[_] = attachments;
                let m = memo
                    .get_or_materialize(key, || {
                        let nonce = (self.nonce_source)();
                        async move {
                            materialize(self.artifacts.as_deref(), refs, vision, &nonce).await
                        }
                    })
                    .await;
                content.push_str(&m.text);
                images = m
                    .images
                    .into_iter()
                    .map(|i| GChatImage {
                        data_base64: i.data_base64,
                        media_type: i.media_type,
                    })
                    .collect();
            } else {
                content.push_str(&format!(
                    "\n[This message had {} attachment(s), handled in an earlier turn; they \
                     are not repeated here.]",
                    attachments.len()
                ));
            }
            attachments.clear();
        }
        images
    }
}

/// AW [`LlmRequest`] → greentic-llm [`ChatRequest`](greentic_llm::ChatRequest).
/// `images` ride on the last user message (the current turn's).
fn build_chat_request(req: &LlmRequest, images: Vec<GChatImage>) -> GChatRequest {
    let mut messages: Vec<GChatMessage> = Vec::with_capacity(req.history.len() + 1);
    messages.push(text_message(MessageRole::System, req.system_prompt.clone()));
    for msg in &req.history {
        messages.push(map_message(msg));
    }
    if !images.is_empty()
        && let Some(m) = messages
            .iter_mut()
            .rev()
            .find(|m| m.role == MessageRole::User)
    {
        m.images = images;
    }
    let tools: Vec<GToolDef> = req
        .tools
        .iter()
        .map(|t| GToolDef {
            name: wire_tool_name(&t.extension_id, &t.tool_name),
            description: t.description.clone(),
            schema: t.parameters.clone(),
        })
        .collect();
    let tool_choice = (!tools.is_empty()).then(|| "auto".to_string());
    GChatRequest {
        messages,
        tools,
        tool_choice,
        max_tokens: None,
        temperature: None,
    }
}

fn text_message(role: MessageRole, content: String) -> GChatMessage {
    GChatMessage {
        role,
        content,
        images: Vec::new(),
        tool_calls: Vec::new(),
        tool_call_id: None,
    }
}

fn map_message(msg: &ChatMessage) -> GChatMessage {
    match msg {
        ChatMessage::System { content } => text_message(MessageRole::System, content.clone()),
        ChatMessage::User { content, .. } => text_message(MessageRole::User, content.clone()),
        ChatMessage::Assistant {
            content,
            tool_calls,
        } => GChatMessage {
            role: MessageRole::Assistant,
            content: content.clone(),
            images: Vec::new(),
            tool_calls: tool_calls
                .iter()
                .map(|tc| GToolCall {
                    id: tc.call_id.clone(),
                    name: wire_tool_name(&tc.extension_id, &tc.tool_name),
                    arguments: tc.args.clone(),
                })
                .collect(),
            tool_call_id: None,
        },
        ChatMessage::Tool { call_id, content } => GChatMessage {
            role: MessageRole::Tool,
            content: content.to_string(),
            images: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: Some(call_id.clone()),
        },
    }
}

/// greentic-llm [`ChatResponse`](greentic_llm::ChatResponse) → AW [`LlmResponse`].
fn map_response(resp: GChatResponse, codec: &ToolNameCodec) -> LlmResponse {
    let content = (!resp.content.trim().is_empty()).then_some(resp.content);
    let tool_calls = resp
        .tool_calls
        .into_iter()
        .map(|tc| {
            let (extension_id, tool_name) = codec.decode(&tc.name);
            ToolCallRecord {
                call_id: tc.id,
                extension_id,
                tool_name,
                args: tc.arguments,
            }
        })
        .collect();
    // greentic-llm surfaces token usage on its ChatResponse (since 1.1.0), so
    // carry it through — this is what makes real per-turn token counts show up
    // in a caller's trace (e.g. the designer Run Demo) instead of zeros.
    let (tokens_in, tokens_out) = resp
        .usage
        .map_or((0, 0), |u| (u.input_tokens as u32, u.output_tokens as u32));
    LlmResponse {
        content,
        tool_calls,
        tokens_in,
        tokens_out,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::LlmProviderRef;
    use crate::llm::LlmToolSchema;
    use serde_json::json;

    fn req(history: Vec<ChatMessage>, tools: Vec<LlmToolSchema>) -> LlmRequest {
        LlmRequest {
            system_prompt: "be helpful".into(),
            history,
            tools,
            provider: LlmProviderRef {
                provider: "deepseek".into(),
                model: "deepseek-chat".into(),
                credential_ref: None,
            },
            turn_attachments: Default::default(),
        }
    }

    #[test]
    fn build_chat_request_prepends_system_and_encodes_tools() {
        let request = req(
            vec![ChatMessage::User {
                content: "hi".into(),
                attachments: Vec::new(),
            }],
            vec![LlmToolSchema {
                extension_id: "greentic.tavily".into(),
                tool_name: "tavily_search".into(),
                description: "search".into(),
                parameters: json!({"type": "object"}),
            }],
        );
        let chat = build_chat_request(&request, Vec::new());
        assert_eq!(chat.messages.len(), 2);
        assert!(matches!(chat.messages[0].role, MessageRole::System));
        assert_eq!(chat.messages[0].content, "be helpful");
        assert!(matches!(chat.messages[1].role, MessageRole::User));
        assert_eq!(chat.tools.len(), 1);
        // Dotted extension id is escaped, not passed through raw.
        assert_eq!(chat.tools[0].name, "greentic_DOT_tavily_FN_tavily_search");
        assert_eq!(chat.tool_choice.as_deref(), Some("auto"));
    }

    #[test]
    fn build_chat_request_omits_tool_choice_without_tools() {
        let chat = build_chat_request(&req(vec![], vec![]), Vec::new());
        assert!(chat.tools.is_empty());
        assert!(chat.tool_choice.is_none());
    }

    #[test]
    fn map_message_maps_assistant_tool_calls_and_tool_results() {
        let assistant = map_message(&ChatMessage::Assistant {
            content: "calling".into(),
            tool_calls: vec![ToolCallRecord {
                call_id: "c1".into(),
                extension_id: "greentic.tavily".into(),
                tool_name: "tavily_search".into(),
                args: json!({"q": "rust"}),
            }],
        });
        assert!(matches!(assistant.role, MessageRole::Assistant));
        assert_eq!(assistant.tool_calls.len(), 1);
        assert_eq!(
            assistant.tool_calls[0].name,
            "greentic_DOT_tavily_FN_tavily_search"
        );

        let tool = map_message(&ChatMessage::Tool {
            call_id: "c1".into(),
            content: json!({"answer": "1.89"}),
        });
        assert!(matches!(tool.role, MessageRole::Tool));
        assert_eq!(tool.tool_call_id.as_deref(), Some("c1"));
    }

    #[tokio::test]
    #[ignore = "live: needs GREENTIC_LLM_API_KEY (DeepSeek)"]
    async fn live_deepseek_completes() {
        let key = std::env::var("GREENTIC_LLM_API_KEY").unwrap_or_default();
        if key.is_empty() {
            eprintln!("SKIP: set GREENTIC_LLM_API_KEY");
            return;
        }
        let backend = GreenticLlmBackend::new(key, None);
        // With tool schemas (mirrors the demo agent) — exercises DeepSeek
        // function-calling through greentic-llm with the _FN_/_DOT_ codec.
        let request = req(
            vec![ChatMessage::User {
                content: "What is the latest stable Rust version? Use tavily_search.".into(),
                attachments: Vec::new(),
            }],
            vec![LlmToolSchema {
                extension_id: "greentic.tavily".into(),
                tool_name: "tavily_search".into(),
                description: "Search the web".into(),
                parameters: json!({
                    "type": "object",
                    "properties": { "query": { "type": "string" } },
                    "required": ["query"]
                }),
            }],
        );
        match backend.complete(request).await {
            Ok(resp) => eprintln!(
                "LIVE OK content={:?} tool_calls={} first={:?}",
                resp.content,
                resp.tool_calls.len(),
                resp.tool_calls
                    .first()
                    .map(|t| (&t.extension_id, &t.tool_name)),
            ),
            Err(e) => panic!("greentic-llm deepseek error: {e}"),
        }
    }

    #[test]
    fn map_response_splits_tool_name_and_blanks_empty_content() {
        let resp = map_response(
            GChatResponse {
                content: "  ".into(),
                tool_calls: vec![GToolCall {
                    id: "c1".into(),
                    name: "greentic_DOT_tavily_FN_tavily_search".into(),
                    arguments: json!({"q": "rust"}),
                }],
                finish_reason: greentic_llm::FinishReason::ToolCalls,
                usage: None,
            },
            &ToolNameCodec::default(),
        );
        assert!(resp.content.is_none(), "whitespace-only content → None");
        assert_eq!(resp.tool_calls.len(), 1);
        assert_eq!(resp.tool_calls[0].extension_id, "greentic.tavily");
        assert_eq!(resp.tool_calls[0].tool_name, "tavily_search");
    }

    #[test]
    fn map_response_carries_token_usage() {
        let resp = map_response(
            GChatResponse {
                content: "hi".into(),
                tool_calls: vec![],
                finish_reason: greentic_llm::FinishReason::Stop,
                usage: Some(greentic_llm::Usage {
                    input_tokens: 42,
                    output_tokens: 17,
                    ..Default::default()
                }),
            },
            &ToolNameCodec::default(),
        );
        assert_eq!(resp.tokens_in, 42);
        assert_eq!(resp.tokens_out, 17);
    }
}

#[cfg(test)]
#[path = "llm_greentic_attachment_tests.rs"]
mod attachment_tests;
