//! End to end: a deployed worker answers a free-text side question while a
//! `flow:` tool is parked on a card, WITHOUT losing the parked flow, and the
//! card submit that follows resumes it.
//!
//! Real pieces: a gtpack loaded by `PackRuntime`, the `FlowEngine` with the
//! `dw.agent` node handler, the `aw-runtime` agent loop (state, tool dispatch
//! through `PackRuntimeFlowInvoker`, the park / side-turn / resume logic), the
//! `StateMachineRuntime` turn path with its session store, and the
//! `ChannelMessageEnvelope`s the webchat provider really emits
//! (`tests/fixtures/provider_envelopes/*.json`, captured from
//! `messaging-provider-webchat`'s `stamp_ingest_envelopes`).
//! Scripted: the LLM, which also REJECTS a history that breaks provider
//! tool-call pairing the way OpenAI does.

#![cfg(feature = "agentic-worker")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::future::Future;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::LlmError;
use greentic_aw_runtime::llm::{LlmBackend, LlmRequest, LlmResponse};
use greentic_aw_runtime::mock::{MockAgentStateStore, MockTelemetry, NoopToolLedger};
use greentic_aw_runtime::state::{ChatMessage, ToolCallRecord};
use greentic_aw_runtime::{
    AgentConfig, AgentLimits, AgentRuntime, FlowToolSource, LlmProviderRef, ParkedTextPolicy,
    ToolRef,
};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::engine::runtime::{IngressEnvelope, StateMachineRuntime};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::agent_node::{HostConfigProvider, RuntimeAgentNodeHandler};
use greentic_runner_host::runner::engine::FlowEngine;
use greentic_runner_host::runner::flow_invoker::PackRuntimeFlowInvoker;
use greentic_runner_host::storage::new_session_store;
use greentic_runner_host::storage::session::session_host_from;
use greentic_runner_host::storage::state::{new_state_store, state_host_from};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, ExtensionInline, ExtensionRef,
    PackFlowEntry, PackKind, PackManifest, ReplyScope, ResourceHints, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use zip::ZipArchive;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "side.turn.e2e";

static RUNTIME: Lazy<&'static tokio::runtime::Runtime> = Lazy::new(|| {
    Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime"),
    ))
});

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
}

fn component_artifact_path(temp_dir: &Path) -> Result<PathBuf> {
    let local =
        workspace_root().join("tests/fixtures/packs/runner-components/components/qa_process.wasm");
    if local.exists() {
        return Ok(local);
    }
    let archive_path =
        workspace_root().join("tests/fixtures/packs/runner-components/runner-components.gtpack");
    let mut archive = ZipArchive::new(File::open(&archive_path).context("open fixture gtpack")?)?;
    let mut wasm = archive
        .by_name("components/qa.process@0.1.0/component.wasm")
        .context("qa.process component missing from fixture pack")?;
    let out = temp_dir.join("qa_process.wasm");
    let mut buf = Vec::new();
    wasm.read_to_end(&mut buf)?;
    std::fs::write(&out, &buf)?;
    Ok(out)
}

fn host_config(bindings_path: &Path) -> HostConfig {
    HostConfig {
        tenant: "demo".into(),
        bindings_path: bindings_path.to_path_buf(),
        flow_type_bindings: Default::default(),
        rate_limits: RateLimits::default(),
        retry: FlowRetryConfig::default(),
        http_enabled: false,
        secrets_policy: SecretsPolicy::allow_all(),
        state_store_policy: StateStorePolicy::default(),
        webhook_policy: WebhookPolicy::default(),
        timers: Vec::new(),
        oauth: None,
        mocks: None,
        pack_bindings: Vec::new(),
        env_passthrough: Vec::new(),
        trace: TraceConfig::from_env(),
        validation: ValidationConfig::from_env(),
        operator_policy: OperatorPolicy::allow_all(),
        fast2flow: Default::default(),
        #[cfg(feature = "agentic-worker")]
        agents: std::collections::HashMap::new(),
        #[cfg(feature = "agentic-worker")]
        graphs: std::collections::HashMap::new(),
    }
}

/// - `form.flow`: `card` (emit.response) → `ask` (session.wait) → `done`
///   (emit.response) → end. Parks after showing the "card".
/// - `agent.flow`: one `dw.agent` node (`helper`) the user talks to.
fn build_pack(pack_path: &Path) -> Result<()> {
    let flows = json!([
        {
            "id": "form.flow",
            "flow_type": "messaging",
            "start": "card",
            "nodes": {
                "card": {
                    "component": "emit.response",
                    "input": { "text": "pick a room" },
                    "routing": { "next": { "node_id": "ask" } }
                },
                "ask": {
                    "component": "session.wait",
                    "input": { "reason": "awaiting the room choice" },
                    "routing": { "next": { "node_id": "done" } }
                },
                "done": {
                    "component": "emit.response",
                    "input": { "text": "booked" },
                    "routing": "end"
                }
            }
        },
        {
            "id": "agent.flow",
            "flow_type": "messaging",
            "start": "agent",
            "nodes": {
                "agent": {
                    "component": "dw.agent",
                    "operation": "helper",
                    "input": { "user_text": "{{entry.text}}" },
                    "routing": "end"
                }
            }
        }
    ]);
    let mut extensions = BTreeMap::new();
    extensions.insert(
        RUNTIME_FLOW_EXTENSION_ID.to_string(),
        ExtensionRef {
            kind: RUNTIME_FLOW_EXTENSION_ID.to_string(),
            version: "2.0.0".into(),
            digest: None,
            location: None,
            inline: Some(ExtensionInline::Other(json!({ "flows": flows }))),
        },
    );
    let manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: PACK_ID.parse()?,
        name: Some("Flow tool interactive".into()),
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "qa.process".parse()?,
            version: Version::parse("0.1.0")?,
            supports: vec![greentic_types::FlowKind::Messaging],
            world: "greentic:component@0.4.0".into(),
            profiles: ComponentProfiles::default(),
            capabilities: ComponentCapabilities::default(),
            configurators: None,
            operations: Vec::new(),
            config_schema: None,
            resources: ResourceHints::default(),
            dev_flows: BTreeMap::new(),
        }],
        flows: Vec::<PackFlowEntry>::new(),
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        agents: BTreeMap::new(),
        extensions: Some(extensions),
    };
    let mut zip = zip::ZipWriter::new(File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&encode_pack_manifest(&manifest)?)?;
    let component_path = component_artifact_path(
        pack_path
            .parent()
            .context("pack path should have a parent temp dir")?,
    )?;
    zip.start_file("components/qa.process.wasm", options)?;
    std::io::copy(&mut File::open(&component_path)?, &mut zip)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

// ---------------------------------------------------------------- scripted LLM

/// Every assistant `tool_calls` id answered by exactly one directly following
/// `Tool`, and no orphan `Tool`: what a provider requires.
fn pairing_is_valid(messages: &[ChatMessage]) -> bool {
    let mut i = 0;
    while i < messages.len() {
        match &messages[i] {
            ChatMessage::Assistant { tool_calls, .. } if !tool_calls.is_empty() => {
                let mut wanted: Vec<&str> = tool_calls.iter().map(|c| c.call_id.as_str()).collect();
                let mut j = i + 1;
                while let Some(ChatMessage::Tool { call_id, .. }) = messages.get(j) {
                    match wanted.iter().position(|w| w == call_id) {
                        Some(pos) => {
                            wanted.remove(pos);
                        }
                        None => return false,
                    }
                    j += 1;
                }
                if !wanted.is_empty() {
                    return false;
                }
                i = j;
            }
            ChatMessage::Tool { .. } => return false,
            _ => i += 1,
        }
    }
    true
}

struct ScriptedLlm {
    responses: Mutex<Vec<LlmResponse>>,
    calls: Mutex<usize>,
}

impl ScriptedLlm {
    fn new(responses: Vec<LlmResponse>) -> Arc<Self> {
        Arc::new(Self {
            responses: Mutex::new(responses),
            calls: Mutex::new(0),
        })
    }
    fn calls(&self) -> usize {
        *self.calls.lock().unwrap()
    }
}

impl LlmBackend for ScriptedLlm {
    fn complete<'a>(
        &'a self,
        req: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        *self.calls.lock().unwrap() += 1;
        let next = if !pairing_is_valid(&req.history) {
            Err(LlmError::Transport(format!(
                "tool-call pairing broken: {:?}",
                req.history
            )))
        } else {
            let mut q = self.responses.lock().unwrap();
            if q.is_empty() {
                Err(LlmError::Transport("llm script exhausted".into()))
            } else {
                Ok(q.remove(0))
            }
        };
        Box::pin(async move { next })
    }
}

fn say(text: &str) -> LlmResponse {
    LlmResponse {
        content: Some(text.into()),
        tool_calls: vec![],
        tokens_in: 1,
        tokens_out: 1,
    }
}

fn call_book_room() -> LlmResponse {
    LlmResponse {
        content: Some("let me open the booking form".into()),
        tool_calls: vec![ToolCallRecord {
            call_id: "call-1".into(),
            extension_id: "flow:form.flow".into(),
            tool_name: "book_room".into(),
            args: json!({}),
        }],
        tokens_in: 1,
        tokens_out: 1,
    }
}

// ---------------------------------------------------------------------- harness

struct Stack {
    _temp: TempDir,
    runtime: StateMachineRuntime,
    llm: Arc<ScriptedLlm>,
}

fn stack(policy: ParkedTextPolicy, script: Vec<LlmResponse>) -> Result<Stack> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("side-turn-e2e.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_pack(&pack_path)?;
    let config = Arc::new(host_config(&bindings_path));
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        &pack_path,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);

    let llm = ScriptedLlm::new(script);
    let mut agents = HashMap::new();
    agents.insert(
        "helper".to_string(),
        AgentConfig {
            agent_id: "helper".into(),
            system_prompt: "You are a helpful assistant.".into(),
            tools: vec![ToolRef {
                extension_id: "flow:form.flow".into(),
                tool_name: "book_room".into(),
                description: Some("Book a room".into()),
                input_schema: None,
                usage_note: None,
            }],
            llm: LlmProviderRef {
                provider: "mock".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: AgentLimits {
                max_iter: 4,
                timeout: Duration::from_secs(60),
                ..AgentLimits::default()
            },
            memory: None,
            knowledge: None,
            guardrails: vec![],
            conversational: false,
            opening_message: None,
            on_text_while_parked: policy,
        },
    );
    let agent_runtime = AgentRuntime::new(
        Arc::new(HostConfigProvider::new(agents)),
        Arc::new(MockAgentStateStore::new()),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test()?),
        llm.clone(),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_flow_source(Some(Arc::new(FlowToolSource::new(Arc::new(
        PackRuntimeFlowInvoker::new(vec![Arc::clone(&pack)], "demo".into()),
    )))));

    let mut engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;
    engine.set_agent_node_handler(Arc::new(RuntimeAgentNodeHandler::new(
        Arc::new(agent_runtime),
        None,
        None,
        None,
    )));
    let session_store = new_session_store();
    let runtime = StateMachineRuntime::from_flow_engine(
        config,
        Arc::new(engine),
        HashMap::new(),
        session_host_from(Arc::clone(&session_store)),
        session_store,
        state_host_from(new_state_store()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        None,
    )?;
    Ok(Stack {
        _temp: temp,
        runtime,
        llm,
    })
}

/// One inbound message as greentic-start hands it to the runner: the whole
/// provider envelope as the payload, the same conversation and user each time.
fn envelope(payload: Value) -> IngressEnvelope {
    IngressEnvelope {
        tenant: "demo".into(),
        env: Some("local".into()),
        pack_id: Some(PACK_ID.into()),
        flow_id: "agent.flow".into(),
        flow_type: Some("messaging".into()),
        action: Some("messaging".into()),
        session_hint: Some("demo:webchat:conv-1:conv-1:u-1".into()),
        provider: Some("webchat".into()),
        messaging_endpoint_id: None,
        channel: Some("conv-1".into()),
        conversation: Some("conv-1".into()),
        user: Some("u-1".into()),
        entry_node: None,
        activity_id: Some("activity-1".into()),
        timestamp: None,
        payload,
        metadata: None,
        reply_scope: Some(ReplyScope {
            conversation: "conv-1".into(),
            thread: None,
            reply_to: None,
            correlation: None,
        }),
    }
}

fn provider_envelope(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/provider_envelopes")
        .join(format!("{name}.json"));
    serde_json::from_str(&std::fs::read_to_string(&path).expect("fixture")).expect("fixture json")
}

/// Run one turn; return `(rendered response, the dw.agent node's output)`.
fn turn(stack: &Stack, payload: Value) -> Result<(Value, Value)> {
    let (response, node_outputs) =
        (*RUNTIME).block_on(stack.runtime.handle_traced(envelope(payload)))?;
    let agent = node_outputs.get("agent").cloned().unwrap_or(Value::Null);
    Ok((response, agent))
}

// ------------------------------------------------------------------------ tests

/// The scenario from the field report, through the real stack:
/// 1. the user asks to book a room, the agent opens the form (a card parks);
/// 2. the user types an unrelated question (a real provider-shaped envelope):
///    the agent answers it and the form stays parked, the card re-offered;
/// 3. the user submits the card: the flow resumes and the agent finishes.
#[test]
fn a_typed_question_is_answered_and_the_parked_form_still_resumes() -> Result<()> {
    let s = stack(
        ParkedTextPolicy::SideTurn,
        vec![
            call_book_room(),
            say("Open Settings, then Signatures, in Outlook."),
            say("Your room is booked."),
        ],
    )?;

    // 1. the form opens
    let (_, agent) = turn(&s, json!({ "text": "I want to book a room" }))?;
    assert_eq!(agent["terminated_by"], "awaiting_tool_input", "{agent}");
    assert!(!agent["pending_presentation"].is_null(), "{agent}");
    assert!(agent.get("side_turn").is_none());

    // 2. a typed question, exactly as webchat delivers it
    let (response, agent) = turn(&s, provider_envelope("typed"))?;
    assert_eq!(agent["terminated_by"], "awaiting_tool_input", "{agent}");
    assert_eq!(agent["side_turn"], true, "{agent}");
    assert_eq!(
        agent["reply"],
        "Open Settings, then Signatures, in Outlook."
    );
    assert!(!agent["pending_presentation"].is_null(), "{agent}");
    // One flat list: the agent's answer, then the re-offered card message.
    let items = response["response"]
        .as_array()
        .expect("flat list of replies");
    assert_eq!(items.len(), 2, "reply + card: {response}");
    assert!(items[0].to_string().contains("Open Settings"), "{response}");
    assert_eq!(items[1], json!({ "text": "pick a room" }), "{response}");

    // 3. the card submit resumes the SAME form
    let (response, agent) = turn(&s, provider_envelope("submit_room"))?;
    assert_eq!(agent["terminated_by"], "final_reply", "{agent}");
    assert_eq!(agent["reply"], "Your room is booked.");
    assert!(response.to_string().contains("Your room is booked."));

    assert_eq!(s.llm.calls(), 3, "one LLM call per turn, none wasted");
    Ok(())
}

/// A submit with no fields of its own (`data: {}` becomes the text "message")
/// is told apart from typed text by the provider's `greentic_submit` marker.
#[test]
fn an_empty_card_submit_resumes_the_parked_form() -> Result<()> {
    let s = stack(
        ParkedTextPolicy::SideTurn,
        vec![call_book_room(), say("All set.")],
    )?;
    turn(&s, json!({ "text": "book a room" }))?;

    let (_, agent) = turn(&s, provider_envelope("submit_empty"))?;

    assert_eq!(agent["terminated_by"], "final_reply", "{agent}");
    assert_eq!(agent["reply"], "All set.");
    Ok(())
}

/// Two side questions in a row, then the submit: the form survives both.
#[test]
fn the_form_survives_several_side_questions() -> Result<()> {
    let s = stack(
        ParkedTextPolicy::SideTurn,
        vec![
            call_book_room(),
            say("first answer"),
            say("second answer"),
            say("done booking"),
        ],
    )?;
    turn(&s, json!({ "text": "book a room" }))?;
    for expected in ["first answer", "second answer"] {
        let (_, agent) = turn(&s, provider_envelope("typed"))?;
        assert_eq!(agent["side_turn"], true, "{agent}");
        assert_eq!(agent["reply"], expected);
    }
    let (_, agent) = turn(&s, provider_envelope("submit_room"))?;
    assert_eq!(agent["reply"], "done booking", "{agent}");
    Ok(())
}

/// The default policy is unchanged: a typed message cancels the parked form,
/// and the submit that follows no longer resumes anything.
#[test]
fn by_default_a_typed_message_still_cancels_the_parked_form() -> Result<()> {
    let s = stack(
        ParkedTextPolicy::Cancel,
        vec![
            call_book_room(),
            say("here is the answer"),
            say("nothing is pending"),
        ],
    )?;
    turn(&s, json!({ "text": "book a room" }))?;

    let (_, agent) = turn(&s, provider_envelope("typed"))?;
    assert_eq!(agent["reply"], "here is the answer", "{agent}");
    assert_ne!(agent["terminated_by"], "awaiting_tool_input");
    assert!(agent.get("side_turn").is_none());
    Ok(())
}
