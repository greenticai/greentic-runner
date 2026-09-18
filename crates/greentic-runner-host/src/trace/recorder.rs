use std::collections::VecDeque;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use greentic_types::TenantCtx;
use parking_lot::Mutex;
use rand::{RngExt, rng};
use serde_json::Value;

use crate::runner::engine::{ExecutionObserver, NodeEvent};
use crate::validate::ValidationIssue;

use super::audit_event::{NodeAuditRecord, Outcome, audit_subject, build_audit_event};
use super::audit_sink::AuditSink;
use super::model::{TraceEnvelope, TraceError, TraceFlow, TraceHash, TracePack, TraceStep};

const DEFAULT_TRACE_FILE: &str = "trace.json";
const DEFAULT_BUFFER_SIZE: usize = 20;
const HASH_ALGORITHM: &str = "blake3";

/// Format an error together with its full `source()` chain into one line,
/// mirroring anyhow's `{:#}` formatting. A bare `err.to_string()` only yields
/// the outermost context (e.g. "in-process operala dispatch to '…' failed"),
/// dropping the real root cause; walking `source()` keeps the underlying
/// error (e.g. the LLM auth failure) in the recorded trace.
fn error_chain_message(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

#[derive(Clone, Debug)]
pub struct TraceConfig {
    pub mode: TraceMode,
    pub out_path: PathBuf,
    pub buffer_size: usize,
    pub capture_inputs: bool,
}

impl TraceConfig {
    pub fn from_env() -> Self {
        let out_path = env::var_os("GREENTIC_TRACE_OUT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_TRACE_FILE));
        Self {
            mode: TraceMode::On,
            out_path,
            buffer_size: DEFAULT_BUFFER_SIZE,
            capture_inputs: env::var("GREENTIC_TRACE_CAPTURE_INPUTS").ok().as_deref() == Some("1"),
        }
    }

    pub fn with_overrides(mut self, mode: TraceMode, out_path: Option<PathBuf>) -> Self {
        self.mode = mode;
        if let Some(path) = out_path {
            self.out_path = path;
        }
        self
    }

    pub fn with_capture_inputs(mut self, capture: bool) -> Self {
        self.capture_inputs = capture;
        self
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TraceMode {
    Off,
    On,
    Always,
}

#[derive(Clone, Debug)]
pub struct PackTraceInfo {
    pub pack_ref: String,
    pub resolved_digest: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TraceContext {
    pub pack_ref: String,
    pub resolved_digest: Option<String>,
    pub flow_id: String,
    pub flow_version: String,
}

pub struct TraceRecorder {
    config: TraceConfig,
    context: TraceContext,
    state: Mutex<TraceState>,
    /// Best-effort audit publisher; `None` when no NATS client is available
    /// (the default, off path — see `docs/superpowers/specs/2026-07-03-runner-audit-emitter-design.md`).
    audit_sink: Option<AuditSink>,
    /// The flow's `TenantCtx`, captured at construction time. The per-node
    /// `NodeEvent.context.tenant` seen in `on_node_end`/`on_node_error` is a
    /// bare `&str` (no env), so the full context needed to build the audit
    /// `EventEnvelope.tenant` field is threaded in here instead, from the
    /// construction site where it is already in scope (`src/engine/runtime.rs`).
    audit_tenant: Option<TenantCtx>,
}

struct TraceState {
    buffer: VecDeque<TraceStep>,
    in_flight: Option<InFlightStep>,
    flushed: bool,
}

struct InFlightStep {
    node_id: String,
    component_id: String,
    operation: String,
    input_hash: TraceHash,
    started_at: Instant,
    validation_issues: Vec<ValidationIssue>,
    invocation_json: Option<Value>,
}

impl TraceRecorder {
    pub fn new(config: TraceConfig, context: TraceContext) -> Self {
        Self::new_with_audit(config, context, None, None)
    }

    /// Constructs a recorder that additionally fans out a best-effort audit
    /// `EventEnvelope` to `audit_sink` on `on_node_end`/`on_node_error`, in
    /// addition to the existing file-buffering (which is unchanged). Pass
    /// `None` for both `audit_sink` and `audit_tenant` to keep the exact
    /// existing (file-only) behavior — this is what `new` delegates to.
    pub fn new_with_audit(
        config: TraceConfig,
        context: TraceContext,
        audit_sink: Option<AuditSink>,
        audit_tenant: Option<TenantCtx>,
    ) -> Self {
        Self {
            config,
            context,
            state: Mutex::new(TraceState {
                buffer: VecDeque::new(),
                in_flight: None,
                flushed: false,
            }),
            audit_sink,
            audit_tenant,
        }
    }

    pub fn mode(&self) -> TraceMode {
        self.config.mode
    }

    pub fn flush_success(&self) -> Result<()> {
        if self.config.mode != TraceMode::Always {
            return Ok(());
        }
        self.flush_with_steps(None)
    }

    pub fn flush_error(&self, err: &dyn std::error::Error) -> Result<()> {
        if self.config.mode == TraceMode::Off {
            return Ok(());
        }
        self.flush_with_steps(Some(err))
    }

    pub fn flush_buffer(&self) -> Result<()> {
        if self.config.mode == TraceMode::Off {
            return Ok(());
        }
        self.flush_with_steps(None)
    }

    fn flush_with_steps(&self, fallback_error: Option<&dyn std::error::Error>) -> Result<()> {
        let mut state = self.state.lock();
        if state.flushed {
            return Ok(());
        }
        if let Some(err) = fallback_error {
            let step = if let Some(in_flight) = state.in_flight.take() {
                TraceStep {
                    node_id: in_flight.node_id,
                    component_id: in_flight.component_id,
                    operation: in_flight.operation,
                    input_hash: in_flight.input_hash,
                    invocation_json: in_flight.invocation_json,
                    invocation_path: None,
                    output_hash: None,
                    state_delta_hash: None,
                    duration_ms: in_flight.started_at.elapsed().as_millis() as u64,
                    validation_issues: in_flight.validation_issues,
                    error: Some(TraceError {
                        code: "node_error".to_string(),
                        message: error_chain_message(err),
                        details: Value::Null,
                    }),
                }
            } else {
                TraceStep {
                    node_id: "unknown".to_string(),
                    component_id: "unknown".to_string(),
                    operation: "unknown".to_string(),
                    input_hash: hash_value(&Value::Null),
                    invocation_json: None,
                    invocation_path: None,
                    output_hash: None,
                    state_delta_hash: None,
                    duration_ms: 0,
                    validation_issues: Vec::new(),
                    error: Some(TraceError {
                        code: "flow_error".to_string(),
                        message: error_chain_message(err),
                        details: Value::Null,
                    }),
                }
            };
            state.buffer.push_back(step);
            while state.buffer.len() > self.config.buffer_size {
                state.buffer.pop_front();
            }
        }
        let steps = state.buffer.iter().cloned().collect::<Vec<_>>();
        state.flushed = true;
        drop(state);
        let trace = self.build_trace(steps);
        write_trace_atomic(&self.config.out_path, &trace)?;
        Ok(())
    }

    fn build_trace(&self, steps: Vec<TraceStep>) -> TraceEnvelope {
        TraceEnvelope {
            trace_version: 1,
            runner_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            git_sha: git_sha(),
            pack: TracePack {
                pack_ref: self.context.pack_ref.clone(),
                resolved_digest: self.context.resolved_digest.clone(),
            },
            flow: TraceFlow {
                id: self.context.flow_id.clone(),
                version: self.context.flow_version.clone(),
            },
            steps,
        }
    }

    /// Best-effort audit fan-out for one completed node execution. No-op
    /// when audit is disabled (`audit_sink`/`audit_tenant` are `None`), and
    /// never blocks or fails execution — `AuditSink::emit` itself never
    /// errors or panics.
    fn emit_audit(&self, event: &NodeEvent<'_>, step: &TraceStep, outcome: Outcome) {
        let (Some(sink), Some(tenant)) = (&self.audit_sink, &self.audit_tenant) else {
            return;
        };
        let event_name = match outcome {
            Outcome::Ok => "node_end",
            Outcome::Error => "node_error",
        };
        let rec = NodeAuditRecord {
            tenant,
            flow_id: event.context.flow_id,
            node_id: event.node_id,
            component_id: &step.component_id,
            operation: &step.operation,
            session_id: event.context.session_id.unwrap_or_default(),
            duration_ms: step.duration_ms,
            outcome,
            error: step.error.as_ref().map(|e| e.message.as_str()),
        };
        let envelope = build_audit_event(&rec, Utc::now(), generate_audit_event_id());
        sink.emit(audit_subject(tenant.tenant.as_str(), event_name), &envelope);
    }
}

impl ExecutionObserver for TraceRecorder {
    fn on_node_start(&self, event: &NodeEvent<'_>) {
        if self.config.mode == TraceMode::Off {
            return;
        }
        let operation = event
            .node
            .operation_name()
            .or_else(|| event.node.operation_in_mapping())
            .unwrap_or("unknown")
            .to_string();
        let input_hash = hash_value(event.payload);
        let component_id = event.node.component_id().to_string();
        let mut state = self.state.lock();
        state.in_flight = Some(InFlightStep {
            node_id: event.node_id.to_string(),
            component_id: component_id.clone(),
            operation,
            input_hash,
            started_at: Instant::now(),
            validation_issues: Vec::new(),
            invocation_json: if self.config.capture_inputs {
                Some(build_invocation(event, &component_id))
            } else {
                None
            },
        });
    }

    fn on_node_end(&self, event: &NodeEvent<'_>, output: &Value) {
        if self.config.mode == TraceMode::Off {
            return;
        }
        let output_hash = hash_value(output);
        let mut state = self.state.lock();
        let step = if let Some(in_flight) = state.in_flight.take() {
            TraceStep {
                node_id: in_flight.node_id,
                component_id: in_flight.component_id,
                operation: in_flight.operation,
                input_hash: in_flight.input_hash,
                invocation_json: in_flight.invocation_json,
                invocation_path: None,
                output_hash: Some(output_hash),
                state_delta_hash: None,
                duration_ms: in_flight.started_at.elapsed().as_millis() as u64,
                validation_issues: in_flight.validation_issues,
                error: None,
            }
        } else {
            TraceStep {
                node_id: event.node_id.to_string(),
                component_id: event.node.component_id().to_string(),
                operation: event.node.operation_name().unwrap_or("unknown").to_string(),
                input_hash: hash_value(event.payload),
                invocation_json: if self.config.capture_inputs {
                    Some(build_invocation(event, event.node.component_id()))
                } else {
                    None
                },
                invocation_path: None,
                output_hash: Some(output_hash),
                state_delta_hash: None,
                duration_ms: 0,
                validation_issues: Vec::new(),
                error: None,
            }
        };
        self.emit_audit(event, &step, Outcome::Ok);
        state.buffer.push_back(step);
        while state.buffer.len() > self.config.buffer_size {
            state.buffer.pop_front();
        }
    }

    fn on_node_error(&self, event: &NodeEvent<'_>, error: &dyn std::error::Error) {
        if self.config.mode == TraceMode::Off {
            return;
        }
        let mut state = self.state.lock();
        let step = if let Some(in_flight) = state.in_flight.take() {
            TraceStep {
                node_id: in_flight.node_id,
                component_id: in_flight.component_id,
                operation: in_flight.operation,
                input_hash: in_flight.input_hash,
                invocation_json: in_flight.invocation_json,
                invocation_path: None,
                output_hash: None,
                state_delta_hash: None,
                duration_ms: in_flight.started_at.elapsed().as_millis() as u64,
                validation_issues: in_flight.validation_issues,
                error: Some(TraceError {
                    code: "node_error".to_string(),
                    message: error_chain_message(error),
                    details: Value::Null,
                }),
            }
        } else {
            TraceStep {
                node_id: event.node_id.to_string(),
                component_id: event.node.component_id().to_string(),
                operation: event.node.operation_name().unwrap_or("unknown").to_string(),
                input_hash: hash_value(event.payload),
                invocation_json: if self.config.capture_inputs {
                    Some(build_invocation(event, event.node.component_id()))
                } else {
                    None
                },
                invocation_path: None,
                output_hash: None,
                state_delta_hash: None,
                duration_ms: 0,
                validation_issues: Vec::new(),
                error: Some(TraceError {
                    code: "node_error".to_string(),
                    message: error_chain_message(error),
                    details: Value::Null,
                }),
            }
        };
        self.emit_audit(event, &step, Outcome::Error);
        state.buffer.push_back(step);
        while state.buffer.len() > self.config.buffer_size {
            state.buffer.pop_front();
        }
        drop(state);
        if let Err(err) = self.flush_buffer() {
            tracing::warn!(error = %err, "failed to write trace");
        }
    }

    fn on_validation(&self, _event: &NodeEvent<'_>, issues: &[ValidationIssue]) {
        if self.config.mode == TraceMode::Off || issues.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        if let Some(in_flight) = state.in_flight.as_mut() {
            in_flight.validation_issues.extend_from_slice(issues);
        }
    }
}

fn hash_value(value: &Value) -> TraceHash {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    let digest = blake3::hash(&bytes).to_hex().to_string();
    TraceHash {
        algorithm: HASH_ALGORITHM.to_string(),
        value: digest,
    }
}

/// Generates a random hex id for the audit event's `id` field. No `uuid` dep
/// exists in this crate; mirrors `runner::engine::generate_correlation_id`'s
/// random-bytes-then-hex-encode pattern rather than adding one.
///
/// `pub(crate)` so `trace::agent_audit`'s `AgentAuditObserver` can reuse the
/// same id source for agent-step audit events.
pub(crate) fn generate_audit_event_id() -> String {
    let mut bytes = [0u8; 16];
    rng().fill(&mut bytes);
    hex::encode(bytes)
}

fn build_invocation(event: &NodeEvent<'_>, component_id: &str) -> Value {
    serde_json::json!({
        "component_id": component_id,
        "operation": event
            .node
            .operation_name()
            .or_else(|| event.node.operation_in_mapping())
            .unwrap_or("unknown")
            .to_string(),
        "payload": event.payload,
    })
}

fn write_trace_atomic(path: &Path, trace: &TraceEnvelope) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(DEFAULT_TRACE_FILE);
    let tmp = path.with_file_name(format!("{file_name}.tmp"));
    let payload = serde_json::to_vec_pretty(trace).context("serialize trace")?;
    fs::write(&tmp, payload).with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn git_sha() -> Option<String> {
    env::var("GIT_SHA")
        .ok()
        .or_else(|| env::var("GITHUB_SHA").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::engine::{FlowContext, HostNode, RetryConfig};
    use greentic_types::{EnvId, TenantId};
    use serde_json::json;
    use tokio::sync::mpsc;

    fn tenant_ctx() -> TenantCtx {
        TenantCtx::new(
            EnvId::try_from("prod").expect("valid env id"),
            TenantId::try_from("t1").expect("valid tenant id"),
        )
    }

    #[test]
    fn error_chain_message_flattens_full_source_chain() {
        // An anyhow error with two context layers over a root cause: a bare
        // `to_string()` would only yield the outermost context, so assert the
        // recorded message keeps the real root cause too.
        let err = anyhow::anyhow!("llm auth failed: 401 unauthorized")
            .context("deep-worker plan creation failed")
            .context("in-process operala dispatch to 'pack.dw.x' failed");
        let msg = error_chain_message(err.as_ref());
        assert_eq!(
            msg,
            "in-process operala dispatch to 'pack.dw.x' failed: \
             deep-worker plan creation failed: llm auth failed: 401 unauthorized"
        );
        // The outermost-only form must NOT be what we record.
        assert_ne!(msg, err.to_string());
    }

    fn trace_config(out_path: PathBuf) -> TraceConfig {
        TraceConfig {
            mode: TraceMode::Always,
            out_path,
            buffer_size: 20,
            capture_inputs: false,
        }
    }

    fn trace_context() -> TraceContext {
        TraceContext {
            pack_ref: "pack1".to_string(),
            resolved_digest: None,
            flow_id: "flow1".to_string(),
            flow_version: "1".to_string(),
        }
    }

    fn flow_context(session_id: Option<&str>) -> FlowContext<'_> {
        FlowContext {
            tenant: "t1",
            pack_id: "pack1",
            flow_id: "flow1",
            node_id: Some("n1"),
            tool: None,
            action: None,
            session_id,
            provider_id: None,
            reply_scope: None,
            retry_config: RetryConfig {
                max_attempts: 1,
                base_delay_ms: 1,
            },
            attempt: 1,
            observer: None,
            mocks: None,
            caller: None,
        }
    }

    fn audit_sink() -> (AuditSink, mpsc::Receiver<(String, Vec<u8>)>) {
        let (tx, rx) = mpsc::channel::<(String, Vec<u8>)>(8);
        (AuditSink::from_sender(tx), rx)
    }

    #[test]
    fn on_node_end_emits_exactly_one_audit_event_with_correct_subject() {
        let dir = tempfile::tempdir().unwrap();
        let (sink, mut rx) = audit_sink();
        let recorder = TraceRecorder::new_with_audit(
            trace_config(dir.path().join("trace.json")),
            trace_context(),
            Some(sink),
            Some(tenant_ctx()),
        );

        let ctx = flow_context(Some("s1"));
        let node = HostNode::for_test("greentic:http", Some("call"));
        let payload = json!({"foo": "bar"});
        let event = NodeEvent {
            context: &ctx,
            node_id: "n1",
            node: &node,
            payload: &payload,
        };
        recorder.on_node_start(&event);
        recorder.on_node_end(&event, &json!({"ok": true}));

        let (subject, bytes) = rx.try_recv().expect("exactly one audit event enqueued");
        assert_eq!(subject, "audit.t1.flow.node_end");
        let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(
            value.get("type").and_then(Value::as_str),
            Some("greentic.runner.flow.node_end")
        );
        assert!(
            rx.try_recv().is_err(),
            "expected exactly one audit event, found a second"
        );
    }

    #[test]
    fn on_node_start_emits_no_audit_event() {
        let dir = tempfile::tempdir().unwrap();
        let (sink, mut rx) = audit_sink();
        let recorder = TraceRecorder::new_with_audit(
            trace_config(dir.path().join("trace.json")),
            trace_context(),
            Some(sink),
            Some(tenant_ctx()),
        );

        let ctx = flow_context(Some("s1"));
        let node = HostNode::for_test("greentic:http", Some("call"));
        let payload = json!({"foo": "bar"});
        let event = NodeEvent {
            context: &ctx,
            node_id: "n1",
            node: &node,
            payload: &payload,
        };
        recorder.on_node_start(&event);

        assert!(
            rx.try_recv().is_err(),
            "on_node_start must never emit an audit event"
        );
    }

    #[test]
    fn on_node_error_emits_one_audit_event_with_error_message() {
        let dir = tempfile::tempdir().unwrap();
        let (sink, mut rx) = audit_sink();
        let recorder = TraceRecorder::new_with_audit(
            trace_config(dir.path().join("trace.json")),
            trace_context(),
            Some(sink),
            Some(tenant_ctx()),
        );

        let ctx = flow_context(Some("s1"));
        let node = HostNode::for_test("greentic:http", Some("call"));
        let payload = json!({"foo": "bar"});
        let event = NodeEvent {
            context: &ctx,
            node_id: "n1",
            node: &node,
            payload: &payload,
        };
        recorder.on_node_start(&event);
        let err = anyhow::anyhow!("boom");
        recorder.on_node_error(&event, err.as_ref());

        let (subject, bytes) = rx.try_recv().expect("one audit error event enqueued");
        assert_eq!(subject, "audit.t1.flow.node_error");
        let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(
            value.get("type").and_then(Value::as_str),
            Some("greentic.runner.flow.node_error")
        );
        assert_eq!(
            value
                .get("payload")
                .and_then(|p| p.get("error"))
                .and_then(Value::as_str),
            Some("boom")
        );
    }

    #[test]
    fn without_audit_sink_file_trace_still_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("trace.json");
        let recorder = TraceRecorder::new(trace_config(out_path.clone()), trace_context());

        let ctx = flow_context(Some("s1"));
        let node = HostNode::for_test("greentic:http", Some("call"));
        let payload = json!({"foo": "bar"});
        let event = NodeEvent {
            context: &ctx,
            node_id: "n1",
            node: &node,
            payload: &payload,
        };
        recorder.on_node_start(&event);
        recorder.on_node_end(&event, &json!({"ok": true}));
        recorder.flush_success().expect("flush should succeed");

        let written = fs::read_to_string(&out_path).expect("trace file was written");
        assert!(written.contains("\"node_id\": \"n1\""));
    }

    #[test]
    fn file_buffering_is_unchanged_with_audit_sink_attached() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("trace.json");
        let (sink, _rx) = audit_sink();
        let recorder = TraceRecorder::new_with_audit(
            trace_config(out_path.clone()),
            trace_context(),
            Some(sink),
            Some(tenant_ctx()),
        );

        let ctx = flow_context(Some("s1"));
        let node = HostNode::for_test("greentic:http", Some("call"));
        let payload = json!({"foo": "bar"});
        let event = NodeEvent {
            context: &ctx,
            node_id: "n1",
            node: &node,
            payload: &payload,
        };
        recorder.on_node_start(&event);
        recorder.on_node_end(&event, &json!({"ok": true}));
        recorder.flush_success().expect("flush should succeed");

        let written = fs::read_to_string(&out_path).expect("trace file was written");
        assert!(written.contains("\"node_id\": \"n1\""));
    }
}
