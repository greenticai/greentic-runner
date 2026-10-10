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

// ─── A parked flow tool, then its resume ──────────────────────────────────

/// Calls the `flow:form` tool until a tool result follows the last user
/// message, then answers "done". Records every request.
#[derive(Default)]
struct FlowThenReply {
    captured: Mutex<Vec<ChatRequest>>,
}

#[async_trait]
impl LlmProvider for FlowThenReply {
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
        let answered = req.messages[last_user..]
            .iter()
            .any(|m| m.role == MessageRole::Tool);
        self.captured.lock().unwrap().push(req);
        if answered {
            return Ok(ChatResponse {
                content: "done".into(),
                tool_calls: vec![],
                finish_reason: FinishReason::Stop,
                usage: None,
            });
        }
        Ok(ChatResponse {
            content: String::new(),
            tool_calls: vec![ToolCall {
                id: "c1".into(),
                name: greentic_aw_runtime::tool_wire_name::wire_tool_name("flow:form", "form"),
                arguments: serde_json::json!({}),
            }],
            finish_reason: FinishReason::ToolCalls,
            usage: None,
        })
    }
    async fn chat_stream(&self, _req: ChatRequest) -> Result<ChatStream, GLlmError> {
        Err(GLlmError::UnsupportedCapability("streaming"))
    }
}

/// A flow that parks on its first call and completes on its resume.
struct ParkOnce;

impl greentic_aw_runtime::FlowInvoker for ParkOnce {
    fn list_flows(&self) -> Vec<greentic_aw_runtime::FlowOperation> {
        vec![greentic_aw_runtime::FlowOperation {
            flow_ref: "form".into(),
            description: "form flow".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }]
    }
    fn invoke<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
        Box::pin(async { Err("interactive only".to_string()) })
    }
    fn invoke_interactive<'a>(
        &'a self,
        _flow_ref: &'a str,
        _args_json: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<greentic_aw_runtime::FlowInvokeOutcome, String>> + Send + 'a,
        >,
    > {
        Box::pin(async {
            Ok(greentic_aw_runtime::FlowInvokeOutcome::Waiting {
                snapshot: serde_json::json!({ "snap": 1 }),
                presentation: serde_json::json!({ "card": 1 }),
            })
        })
    }
    fn resume<'a>(
        &'a self,
        _flow_ref: &'a str,
        _snapshot: serde_json::Value,
        _input: serde_json::Value,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<greentic_aw_runtime::FlowInvokeOutcome, String>> + Send + 'a,
        >,
    > {
        Box::pin(async {
            Ok(greentic_aw_runtime::FlowInvokeOutcome::Completed(
                serde_json::json!({ "room": "101" }),
            ))
        })
    }
}

struct ResumeRun {
    /// Every provider request: the parking turn's, then the resumed turn's.
    sent: Vec<ChatRequest>,
    parked: greentic_aw_runtime::AgentOutput,
    resumed: greentic_aw_runtime::AgentOutput,
}

/// Turn 1 sends a document and calls a flow tool that parks; turn 2 resumes
/// it (no new file). `None` when the PII WASM is not available.
async fn run_park_then_resume(doc_text: &'static str) -> Option<ResumeRun> {
    let Some(wasm_src) = pii_wasm_src() else {
        eprintln!("SKIP: component-guardrail-pii WASM not found");
        return None;
    };
    let tmp = tempfile::TempDir::new().expect("create tempdir");
    let ext_dir = tmp.path().join("greentic.guardrail-pii");
    std::fs::create_dir_all(&ext_dir).expect("create ext subdir");
    write_signed_extension_dir(&wasm_src, &ext_dir);
    let provider = Arc::new(FlowThenReply::default());
    let backend = GreenticLlmBackend::new("unused", None)
        .with_cached_provider("mock", "echo", provider.clone())
        .with_artifact_reader(Arc::new(TextReader(doc_text)));
    let (runtime, tc) = build_full_runtime_with_tools(
        &wasm_src,
        &tmp,
        &ext_dir,
        serde_json::json!({ "blocklist": ["forbidden"] }),
        GuardrailMode::Enforce,
        Arc::new(backend),
        vec![greentic_aw_runtime::ToolRef {
            extension_id: "flow:form".into(),
            tool_name: "form".into(),
            description: None,
            input_schema: None,
            usage_note: None,
        }],
    );
    let runtime = runtime.with_flow_source(Some(Arc::new(
        greentic_aw_runtime::FlowToolSource::new(Arc::new(ParkOnce)),
    )));
    let parked = runtime
        .step(
            tc.clone(),
            "session-resume-guard",
            "pii-agent",
            AgentInput {
                text: "please read the file".into(),
                attachments: vec![doc()],
                ..Default::default()
            },
        )
        .await
        .expect("parking turn");
    let resumed = runtime
        .step(
            tc,
            "session-resume-guard",
            "pii-agent",
            AgentInput {
                resume_payload: Some(serde_json::json!({ "metadata": { "action": "submit" } })),
                ..Default::default()
            },
        )
        .await
        .expect("resumed turn");
    let sent = provider.captured.lock().unwrap().clone();
    Some(ResumeRun {
        sent,
        parked,
        resumed,
    })
}

#[tokio::test]
async fn a_blocked_document_stays_withheld_when_a_parked_flow_tool_resumes() {
    let Some(run) = run_park_then_resume("this file holds forbidden instructions").await else {
        return;
    };
    assert_eq!(
        run.parked.terminated_by,
        greentic_aw_runtime::error::TerminationReason::AwaitingToolInput
    );
    assert_eq!(run.resumed.reply, "done");
    assert_eq!(run.sent.len(), 2, "one request per turn");
    for req in &run.sent {
        let wire = format!("{:?}", req.messages);
        assert!(!wire.contains("forbidden instructions"), "{wire}");
        assert!(wire.contains("withheld by a content policy"), "{wire}");
    }
}

#[tokio::test]
async fn an_allowed_document_reaches_the_model_on_both_turns_of_a_resume() {
    let Some(run) = run_park_then_resume("a harmless report").await else {
        return;
    };
    assert_eq!(run.resumed.reply, "done");
    assert_eq!(run.sent.len(), 2, "one request per turn");
    for req in &run.sent {
        let wire = format!("{:?}", req.messages);
        assert!(wire.contains("a harmless report"), "{wire}");
        assert!(!wire.contains("withheld by a content policy"), "{wire}");
    }
}
