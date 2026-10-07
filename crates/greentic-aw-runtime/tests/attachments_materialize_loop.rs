//! Attachments through the REAL agent loop and the greentic-llm backend:
//! fetched once per turn (not per iteration), again on the next turn, never
//! shared between conversations, and never written into persisted state.

#![cfg(all(feature = "test-mock", feature = "greentic-llm-backend"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use greentic_aw_runtime::artifact_reader::{ArtifactBytes, ArtifactError, ArtifactReader};
use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::state::{AgentStateStore, ChatMessage};
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, AttachmentKind, AttachmentRef,
    GreenticLlmBackend, LlmProviderRef, ParkedTextPolicy,
};
use greentic_llm::{
    Capabilities, ChatRequest, ChatResponse, ChatStream, FinishReason, LlmError as GLlmError,
    LlmProvider, MessageRole, ToolCall,
};

const TOOL_ITERATIONS: usize = 3;

fn aid(c: char) -> String {
    format!("artifact://{}", c.to_string().repeat(64))
}

/// Asks for a (disallowed) tool until the request carries TOOL_ITERATIONS
/// tool results after its last user message, then answers. Stateless, so two
/// conversations can share it.
struct ToolThenReply {
    captured: Mutex<Vec<ChatRequest>>,
}

#[async_trait]
impl LlmProvider for ToolThenReply {
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
    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, GLlmError> {
        let last_user = req
            .messages
            .iter()
            .rposition(|m| m.role == MessageRole::User)
            .unwrap_or(0);
        let tool_results = req.messages[last_user..]
            .iter()
            .filter(|m| m.role == MessageRole::Tool)
            .count();
        self.captured.lock().unwrap().push(req);
        if tool_results < TOOL_ITERATIONS {
            return Ok(ChatResponse {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: format!("c{tool_results}"),
                    name: "nope_DOT_ext_FN_tool".into(),
                    arguments: serde_json::json!({}),
                }],
                finish_reason: FinishReason::ToolCalls,
                usage: None,
            });
        }
        Ok(ChatResponse {
            content: "done".into(),
            tool_calls: vec![],
            finish_reason: FinishReason::Stop,
            usage: None,
        })
    }
    async fn chat_stream(&self, _req: ChatRequest) -> Result<ChatStream, GLlmError> {
        Err(GLlmError::UnsupportedCapability("streaming"))
    }
}

/// Counts reads per id; `fail_first` makes each id's FIRST read fail.
struct CountingReader {
    answers: HashMap<String, (String, Vec<u8>)>,
    calls: Mutex<Vec<String>>,
    fail_first: bool,
}

impl CountingReader {
    fn new(answers: &[(&str, &str, &[u8])], fail_first: bool) -> Self {
        Self {
            answers: answers
                .iter()
                .map(|(id, mime, b)| (id.to_string(), (mime.to_string(), b.to_vec())))
                .collect(),
            calls: Mutex::new(Vec::new()),
            fail_first,
        }
    }
    fn calls_for(&self, id: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| *c == id)
            .count()
    }
}

impl ArtifactReader for CountingReader {
    fn get<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ArtifactBytes, ArtifactError>> + Send + 'a>> {
        Box::pin(async move {
            let first = {
                let mut calls = self.calls.lock().unwrap();
                let first = !calls.iter().any(|c| c == id);
                calls.push(id.to_string());
                first
            };
            // Give a concurrent conversation the chance to interleave.
            tokio::time::sleep(Duration::from_millis(5)).await;
            if self.fail_first && first {
                return Err(ArtifactError::Unavailable("transient".into()));
            }
            match self.answers.get(id) {
                Some((mime, bytes)) => Ok(ArtifactBytes {
                    mime_type: mime.clone(),
                    name: None,
                    bytes: bytes.clone(),
                }),
                None => Err(ArtifactError::NotFound),
            }
        })
    }
}

fn nonce() -> String {
    "feedfacefeedfacefeedfacefeedface".into()
}

fn config() -> AgentConfig {
    AgentConfig {
        agent_id: "agent".into(),
        system_prompt: "sys".into(),
        tools: vec![],
        llm: LlmProviderRef {
            provider: "deepseek".into(),
            model: "m".into(),
            credential_ref: None,
        },
        limits: AgentLimits {
            max_iter: 8,
            timeout: Duration::from_secs(30),
            ..AgentLimits::default()
        },
        memory: None,
        knowledge: None,
        guardrails: vec![],
        conversational: false,
        opening_message: None,
        on_text_while_parked: ParkedTextPolicy::default(),
    }
}

struct Harness {
    runtime: AgentRuntime,
    store: Arc<MockAgentStateStore>,
    provider: Arc<ToolThenReply>,
    reader: Arc<CountingReader>,
}

fn harness(reader: CountingReader, tenants: &[&TenantContext]) -> Harness {
    let cp = MockConfigProvider::new();
    for t in tenants {
        cp.insert(t, "agent", config());
    }
    let provider = Arc::new(ToolThenReply {
        captured: Mutex::new(Vec::new()),
    });
    let reader = Arc::new(reader);
    let backend = GreenticLlmBackend::new("unused", None)
        .with_cached_provider("deepseek", "m", provider.clone())
        .with_nonce_source(nonce)
        .with_artifact_reader(reader.clone());
    let store = Arc::new(MockAgentStateStore::new());
    let runtime = AgentRuntime::new(
        Arc::new(cp),
        store.clone(),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        Arc::new(backend),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    );
    Harness {
        runtime,
        store,
        provider,
        reader,
    }
}

fn image_ref() -> AttachmentRef {
    AttachmentRef {
        id: aid('a'),
        mime_type: "image/png".into(),
        name: Some("a.png".into()),
        size_bytes: Some(3),
        kind: AttachmentKind::Image,
        text_ref: None,
    }
}

fn doc_ref() -> AttachmentRef {
    AttachmentRef {
        id: aid('d'),
        mime_type: "application/pdf".into(),
        name: Some("d.pdf".into()),
        size_bytes: None,
        kind: AttachmentKind::Document,
        text_ref: Some(aid('t')),
    }
}

fn input(attachments: Vec<AttachmentRef>) -> AgentInput {
    AgentInput {
        text: "look".into(),
        attachments,
        ..Default::default()
    }
}

fn answers() -> Vec<(String, &'static str, &'static [u8])> {
    vec![
        (aid('a'), "image/png", &[1u8, 2, 3][..]),
        (aid('t'), "text/plain", b"secret quarterly body"),
    ]
}

fn reader(fail_first: bool) -> CountingReader {
    let a = answers();
    let refs: Vec<(&str, &str, &[u8])> = a.iter().map(|(i, m, b)| (i.as_str(), *m, *b)).collect();
    CountingReader::new(&refs, fail_first)
}

#[tokio::test]
async fn a_turn_with_tool_iterations_fetches_each_attachment_once() {
    let tenant = TenantContext::new("acme", "prod");
    let h = harness(reader(false), &[&tenant]);
    h.runtime
        .step(
            tenant.clone(),
            "s1",
            "agent",
            input(vec![image_ref(), doc_ref()]),
        )
        .await
        .unwrap();
    let sent = h.provider.captured.lock().unwrap().clone();
    assert_eq!(sent.len(), TOOL_ITERATIONS + 1, "the loop really iterated");
    assert_eq!(h.reader.calls_for(&aid('a')), 1);
    assert_eq!(h.reader.calls_for(&aid('t')), 1);
    // Every iteration still carried the image and the same document block.
    let first_user = |r: &ChatRequest| {
        r.messages
            .iter()
            .rev()
            .find(|m| m.role == MessageRole::User)
            .unwrap()
            .clone()
    };
    let content0 = first_user(&sent[0]).content;
    for r in &sent {
        let u = first_user(r);
        assert_eq!(u.images.len(), 1);
        assert_eq!(u.content, content0, "identical across iterations");
    }
    assert!(content0.contains("feedfacefeedfacefeedfacefeedface"));
}

#[tokio::test]
async fn a_second_turn_fetches_again_and_the_first_is_not_refetched() {
    let tenant = TenantContext::new("acme", "prod");
    let h = harness(reader(false), &[&tenant]);
    h.runtime
        .step(tenant.clone(), "s1", "agent", input(vec![image_ref()]))
        .await
        .unwrap();
    assert_eq!(h.reader.calls_for(&aid('a')), 1);
    h.runtime
        .step(tenant.clone(), "s1", "agent", input(vec![image_ref()]))
        .await
        .unwrap();
    assert_eq!(h.reader.calls_for(&aid('a')), 2, "a new turn fetches again");
}

#[tokio::test]
async fn two_concurrent_conversations_never_share_a_memo() {
    let acme = TenantContext::new("acme", "prod");
    let beta = TenantContext::new("beta", "prod");
    let h = harness(reader(false), &[&acme, &beta]);
    let (a, b) = tokio::join!(
        h.runtime
            .step(acme.clone(), "s1", "agent", input(vec![image_ref()])),
        h.runtime
            .step(beta.clone(), "s1", "agent", input(vec![image_ref()])),
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(
        h.reader.calls_for(&aid('a')),
        2,
        "each conversation fetches under its own turn"
    );
}

#[tokio::test]
async fn a_first_fetch_failure_gives_the_same_note_on_every_iteration() {
    let tenant = TenantContext::new("acme", "prod");
    let h = harness(reader(true), &[&tenant]);
    h.runtime
        .step(tenant.clone(), "s1", "agent", input(vec![image_ref()]))
        .await
        .unwrap();
    assert_eq!(
        h.reader.calls_for(&aid('a')),
        1,
        "no re-fetch on later iterations"
    );
    let sent = h.provider.captured.lock().unwrap().clone();
    assert_eq!(sent.len(), TOOL_ITERATIONS + 1);
    for r in &sent {
        let u = r
            .messages
            .iter()
            .rev()
            .find(|m| m.role == MessageRole::User)
            .unwrap();
        assert!(u.images.is_empty(), "the image never appears mid-turn");
        assert!(u.content.contains("could not be loaded"), "{}", u.content);
    }
}

#[tokio::test]
async fn persisted_state_holds_references_and_no_bytes_after_a_turn() {
    let tenant = TenantContext::new("acme", "prod");
    let h = harness(reader(false), &[&tenant]);
    h.runtime
        .step(
            tenant.clone(),
            "s1",
            "agent",
            input(vec![image_ref(), doc_ref()]),
        )
        .await
        .unwrap();
    // The bytes and the extracted text DID reach the provider...
    let sent = h.provider.captured.lock().unwrap().clone();
    let u = sent[0]
        .messages
        .iter()
        .rev()
        .find(|m| m.role == MessageRole::User)
        .unwrap();
    assert_eq!(u.images[0].data_base64, "AQID");
    assert!(u.content.contains("secret quarterly body"));
    // ...and the state reloaded from the store has references only.
    let state = h.store.load(&tenant, "s1").await.unwrap();
    let json = serde_json::to_string(&state).unwrap();
    assert!(!json.contains("AQID"), "{json}");
    assert!(!json.contains("data_base64"), "{json}");
    assert!(!json.contains("secret quarterly body"), "{json}");
    assert!(!json.contains("feedface"), "no document block in state");
    let refs = state
        .messages
        .iter()
        .find_map(|m| match m {
            ChatMessage::User { attachments, .. } => Some(attachments.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(refs.len(), 2);
    assert_eq!(refs[0].id, aid('a'));
}
