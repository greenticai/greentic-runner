#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Backend-level attachment behaviour: the vision gate runs before `chat()`,
//! only the last user message's attachments are fetched, and conversation
//! state never receives bytes.

use super::*;
use crate::attachments::{AttachmentKind, AttachmentRef};
use crate::attachments_materialize::tests::{FakeReader, image};
use crate::config::LlmProviderRef;
use crate::state::ConversationState;
use crate::tenant::TenantContext;
use async_trait::async_trait;
use greentic_llm::{Capabilities, ChatStream, FinishReason, LlmError as GLlmError};

/// Records every request and refuses images when vision is off, exactly like
/// `RigBackend` does, so a missing gate fails the test instead of passing.
struct MockProvider {
    vision: bool,
    captured: Mutex<Vec<GChatRequest>>,
}

impl MockProvider {
    fn new(vision: bool) -> Arc<Self> {
        Arc::new(Self {
            vision,
            captured: Mutex::new(Vec::new()),
        })
    }
    fn requests(&self) -> Vec<GChatRequest> {
        self.captured.lock().unwrap().clone()
    }
}

#[async_trait]
impl LlmProvider for MockProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            chat: true,
            tools: true,
            streaming: false,
            vision: self.vision,
            system_prompt: true,
        }
    }
    fn provider_name(&self) -> &'static str {
        "mock"
    }
    fn model(&self) -> &str {
        "mock-model"
    }
    async fn chat(&self, req: GChatRequest) -> Result<GChatResponse, GLlmError> {
        let has_images = req.messages.iter().any(|m| !m.images.is_empty());
        self.captured.lock().unwrap().push(req);
        if has_images && !self.vision {
            return Err(GLlmError::UnsupportedCapability("vision"));
        }
        Ok(GChatResponse {
            content: "ok".into(),
            tool_calls: vec![],
            finish_reason: FinishReason::Stop,
            usage: None,
        })
    }
    async fn chat_stream(&self, _req: GChatRequest) -> Result<ChatStream, GLlmError> {
        Err(GLlmError::UnsupportedCapability("streaming"))
    }
}

fn backend(provider: Arc<MockProvider>, reader: Option<Arc<FakeReader>>) -> GreenticLlmBackend {
    let b = GreenticLlmBackend::new("unused", None).with_cached_provider("deepseek", "m", provider);
    match reader {
        Some(r) => b.with_artifact_reader(r),
        None => b,
    }
}

fn request(history: Vec<ChatMessage>) -> LlmRequest {
    LlmRequest {
        system_prompt: "sys".into(),
        history,
        tools: vec![],
        provider: LlmProviderRef {
            provider: "deepseek".into(),
            model: "m".into(),
            credential_ref: None,
        },
    }
}

fn user(content: &str, attachments: Vec<AttachmentRef>) -> ChatMessage {
    ChatMessage::User {
        content: content.into(),
        attachments,
    }
}

fn last_user(req: &GChatRequest) -> &GChatMessage {
    req.messages
        .iter()
        .rev()
        .find(|m| m.role == MessageRole::User)
        .expect("a user message")
}

#[tokio::test]
async fn without_vision_no_image_reaches_the_provider_and_the_agent_is_told() {
    let provider = MockProvider::new(false);
    let reader = Arc::new(FakeReader::new().ok("artifact://a", "image/png", vec![1, 2, 3]));
    let b = backend(provider.clone(), Some(reader.clone()));
    let out = b
        .complete(request(vec![user(
            "look",
            vec![image("artifact://a", "a.png")],
        )]))
        .await;
    assert!(out.is_ok(), "the turn must not fail: {out:?}");
    let sent = provider.requests();
    assert_eq!(sent.len(), 1, "chat() called exactly once");
    assert!(sent[0].messages.iter().all(|m| m.images.is_empty()));
    let content = &last_user(&sent[0]).content;
    assert!(
        content.starts_with("look") && content.contains("cannot see"),
        "{content}"
    );
    assert_eq!(reader.call_count(), 0);
}

#[tokio::test]
async fn with_vision_the_image_rides_on_the_last_user_message() {
    let provider = MockProvider::new(true);
    let reader = Arc::new(FakeReader::new().ok("artifact://a", "image/png", vec![1, 2, 3]));
    let b = backend(provider.clone(), Some(reader));
    b.complete(request(vec![user(
        "look",
        vec![image("artifact://a", "a.png")],
    )]))
    .await
    .unwrap();
    let sent = provider.requests();
    let msg = last_user(&sent[0]);
    assert_eq!(msg.images.len(), 1);
    assert_eq!(msg.images[0].data_base64, "AQID");
    assert_eq!(msg.images[0].media_type, "image/png");
}

#[tokio::test]
async fn an_earlier_messages_attachments_are_not_fetched_again() {
    let provider = MockProvider::new(true);
    let reader = Arc::new(
        FakeReader::new()
            .ok("artifact://old", "image/png", vec![1])
            .ok("artifact://new", "image/png", vec![2]),
    );
    let b = backend(provider.clone(), Some(reader.clone()));
    let history = vec![
        user("first", vec![image("artifact://old", "old.png")]),
        ChatMessage::Assistant {
            content: "seen".into(),
            tool_calls: vec![],
        },
        user("second", vec![image("artifact://new", "new.png")]),
    ];
    b.complete(request(history)).await.unwrap();
    assert_eq!(
        *reader.calls.lock().unwrap(),
        vec!["artifact://new".to_string()]
    );
    let sent = provider.requests();
    let first = sent[0]
        .messages
        .iter()
        .find(|m| m.role == MessageRole::User)
        .unwrap();
    assert!(first.images.is_empty());
    assert!(first.content.contains("earlier turn"), "{}", first.content);
    assert!(!first.content.contains("old.png"));
    assert_eq!(last_user(&sent[0]).images.len(), 1);
}

#[tokio::test]
async fn without_a_reader_the_turn_proceeds_with_one_note() {
    let provider = MockProvider::new(true);
    let b = backend(provider.clone(), None);
    b.complete(request(vec![user(
        "look",
        vec![image("artifact://a", "a.png")],
    )]))
    .await
    .unwrap();
    let sent = provider.requests();
    assert_eq!(sent.len(), 1);
    assert!(last_user(&sent[0]).images.is_empty());
    assert!(
        last_user(&sent[0])
            .content
            .contains("not available in this deployment")
    );
}

#[tokio::test]
async fn conversation_state_keeps_references_only_after_a_turn() {
    let provider = MockProvider::new(true);
    let reader = Arc::new(
        FakeReader::new()
            .ok("artifact://a", "image/png", vec![1, 2, 3])
            .ok(
                "artifact://t",
                "text/plain",
                b"secret quarterly body".to_vec(),
            ),
    );
    let b = backend(provider.clone(), Some(reader));
    let mut state = ConversationState::empty(&TenantContext::new("t", "e"), "s");
    state.messages.push(user(
        "look",
        vec![
            image("artifact://a", "a.png"),
            AttachmentRef {
                id: "artifact://d".into(),
                mime_type: "text/plain".into(),
                name: Some("d.txt".into()),
                size_bytes: None,
                kind: AttachmentKind::Document,
                text_ref: Some("artifact://t".into()),
            },
        ],
    ));
    // The loop builds the request from a clone of the state's history.
    let response = b.complete(request(state.messages.clone())).await.unwrap();
    state.messages.push(ChatMessage::Assistant {
        content: response.content.unwrap_or_default(),
        tool_calls: vec![],
    });
    // Bytes and extracted text DID reach the provider...
    let sent = provider.requests();
    assert_eq!(last_user(&sent[0]).images[0].data_base64, "AQID");
    assert!(
        last_user(&sent[0])
            .content
            .contains("secret quarterly body")
    );
    // ...and none of it is in the persisted state.
    let json = serde_json::to_string(&state).unwrap();
    assert!(!json.contains("AQID"), "{json}");
    assert!(!json.contains("data_base64"), "{json}");
    assert!(!json.contains("secret quarterly body"), "{json}");
    assert!(json.contains("artifact://a") && json.contains("artifact://d"));
}
