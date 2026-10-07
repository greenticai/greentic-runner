//! The REAL inbound guardrail (the PII WASM) over attachment document text,
//! through the real agent loop and the greentic-llm backend: a document the
//! chain blocks never reaches the model, a redaction is what the model reads,
//! and the guard runs once per turn however many tool iterations there are.
//! Skips when the PII guardrail WASM is not available (see `pii_wasm_src`).

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use async_trait::async_trait;
use greentic_aw_runtime::artifact_reader::{ArtifactBytes, ArtifactError, ArtifactReader};
use greentic_aw_runtime::guardrail::{GuardrailAction, GuardrailDirection};
use greentic_aw_runtime::{AttachmentKind, AttachmentRef, GreenticLlmBackend};
use greentic_llm::{
    Capabilities, ChatRequest, ChatResponse, ChatStream, FinishReason, LlmError as GLlmError,
    LlmProvider, MessageRole, ToolCall,
};

use super::*;

const TOOL_ITERATIONS: usize = 3;

fn aid(c: char) -> String {
    format!("artifact://{}", c.to_string().repeat(64))
}

/// Calls a (disallowed) tool until TOOL_ITERATIONS tool results follow the
/// last user message, then answers "done". Records every request.
#[derive(Default)]
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
        "echo"
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

/// Answers the document's extracted text.
struct TextReader(&'static str);

impl ArtifactReader for TextReader {
    fn get<'a>(
        &'a self,
        _id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ArtifactBytes, ArtifactError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(ArtifactBytes {
                mime_type: "text/plain".into(),
                name: None,
                bytes: self.0.as_bytes().to_vec(),
            })
        })
    }
}

fn doc() -> AttachmentRef {
    AttachmentRef {
        id: aid('d'),
        mime_type: "application/pdf".into(),
        name: Some("brief.pdf".into()),
        size_bytes: None,
        kind: AttachmentKind::Document,
        text_ref: Some(aid('t')),
    }
}

struct Run {
    sent: Vec<ChatRequest>,
    observed: Vec<greentic_aw_runtime::guardrail::GuardrailObservation>,
    result: Result<greentic_aw_runtime::AgentOutput, AgentError>,
}

/// One turn ("please read the file" + one document) through the real loop.
/// `None` when the PII WASM is not available.
async fn run_turn(
    doc_text: &'static str,
    config: serde_json::Value,
    mode: GuardrailMode,
) -> Option<Run> {
    let Some(wasm_src) = pii_wasm_src() else {
        eprintln!("SKIP: component-guardrail-pii WASM not found");
        return None;
    };
    let tmp = tempfile::TempDir::new().expect("create tempdir");
    let ext_dir = tmp.path().join("greentic.guardrail-pii");
    std::fs::create_dir_all(&ext_dir).expect("create ext subdir");
    write_signed_extension_dir(&wasm_src, &ext_dir);
    let provider = Arc::new(ToolThenReply::default());
    let backend = GreenticLlmBackend::new("unused", None)
        .with_cached_provider("mock", "echo", provider.clone())
        .with_artifact_reader(Arc::new(TextReader(doc_text)));
    let (runtime, tc) =
        build_full_runtime_with_llm(&wasm_src, &tmp, &ext_dir, config, mode, Arc::new(backend));
    let observer = Arc::new(RecordingObserver::default());
    let result = runtime
        .step_with_observer(
            tc,
            "session-attachment-guard",
            "pii-agent",
            AgentInput {
                text: "please read the file".into(),
                attachments: vec![doc()],
                ..Default::default()
            },
            observer.clone(),
        )
        .await;
    let sent = provider.captured.lock().unwrap().clone();
    let observed = observer.guardrails.lock().unwrap().clone();
    Some(Run {
        sent,
        observed,
        result,
    })
}

fn last_user_content(req: &ChatRequest) -> String {
    req.messages
        .iter()
        .rev()
        .find(|m| m.role == MessageRole::User)
        .expect("a user message")
        .content
        .clone()
}

#[tokio::test]
async fn a_document_the_chain_blocks_never_reaches_the_model() {
    let Some(run) = run_turn(
        "this file holds forbidden instructions",
        serde_json::json!({ "blocklist": ["forbidden"] }),
        GuardrailMode::Enforce,
    )
    .await
    else {
        return;
    };
    assert!(
        run.result.is_ok(),
        "the turn is not failed: {:?}",
        run.result
    );
    assert_eq!(run.sent.len(), TOOL_ITERATIONS + 1, "the loop iterated");
    for req in &run.sent {
        let wire = format!("{:?}", req.messages);
        assert!(!wire.contains("forbidden instructions"), "{wire}");
        assert!(!wire.contains("brief.pdf"), "{wire}");
        assert!(
            last_user_content(req).contains("withheld by a content policy"),
            "{}",
            last_user_content(req)
        );
    }
    let inbound_blocked: Vec<_> = run
        .observed
        .iter()
        .filter(|o| o.direction == GuardrailDirection::Inbound)
        .collect();
    assert_eq!(inbound_blocked.len(), 1, "{:?}", run.observed);
    assert_eq!(inbound_blocked[0].action, GuardrailAction::Blocked);
}

#[tokio::test]
async fn the_document_guard_runs_once_per_turn_across_tool_iterations() {
    // Monitor mode records every denial without blocking, so the number of
    // inbound observations is the number of times the document was checked.
    let Some(run) = run_turn(
        "a forbidden word",
        serde_json::json!({ "blocklist": ["forbidden"] }),
        GuardrailMode::Monitor,
    )
    .await
    else {
        return;
    };
    assert!(run.result.is_ok(), "{:?}", run.result);
    assert_eq!(run.sent.len(), TOOL_ITERATIONS + 1, "the loop iterated");
    let inbound: Vec<_> = run
        .observed
        .iter()
        .filter(|o| o.direction == GuardrailDirection::Inbound)
        .collect();
    assert_eq!(inbound.len(), 1, "{:?}", run.observed);
    assert_eq!(inbound[0].action, GuardrailAction::Monitored);
    // Monitor passes the text through.
    assert!(last_user_content(&run.sent[0]).contains("a forbidden word"));
}

#[tokio::test]
async fn a_redaction_is_what_the_model_reads() {
    let Some(run) = run_turn(
        "write to boss@corp.example today",
        serde_json::Value::Null,
        GuardrailMode::Enforce,
    )
    .await
    else {
        return;
    };
    assert!(run.result.is_ok(), "{:?}", run.result);
    let content = last_user_content(&run.sent[0]);
    assert!(!content.contains("boss@corp.example"), "{content}");
    assert!(content.contains("[REDACTED_EMAIL]"), "{content}");
}
