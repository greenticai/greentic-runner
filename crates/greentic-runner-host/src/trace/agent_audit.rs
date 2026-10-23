//! `StepObserver` implementation that emits a best-effort audit event per
//! agentic-worker step (tool call/result), published to
//! `audit.<tenant>.agent.<event>` and ingested by the same admin subscriber
//! B-2 wired up for per-flow-node events. See
//! `docs/superpowers/specs/2026-07-03-agent-audit-emitter-design.md`.
//!
//! Gated behind the `agentic-worker` feature: `greentic_aw_runtime::StepObserver`
//! is only available when that (default-on) feature pulls the crate in.

use chrono::Utc;
use greentic_aw_runtime::StepObserver;
use greentic_types::TenantCtx;
use serde_json::{Value, json};

use super::audit_event::{
    agent_audit_subject, build_agent_audit_event, build_guardrail_violation_event,
    violation_subject,
};
use super::audit_sink::AuditSink;
use super::recorder::generate_audit_event_id;

/// Emits `audit.<tenant>.agent.<event>` events for one agentic-worker
/// invocation's tool calls/results.
///
/// Best-effort throughout — [`AuditSink::emit`] never blocks, errors, or
/// panics, so this observer can never affect the agent loop's behavior.
/// `wants_streaming` is always `false`: audit only cares about tool steps,
/// not token-level streaming.
pub struct AgentAuditObserver {
    sink: AuditSink,
    tenant: TenantCtx,
    agent_id: String,
    session_id: String,
}

impl AgentAuditObserver {
    pub fn new(sink: AuditSink, tenant: TenantCtx, agent_id: String, session_id: String) -> Self {
        Self {
            sink,
            tenant,
            agent_id,
            session_id,
        }
    }

    fn emit(&self, event: &str, payload: Value) {
        // (see `summarize_value`: tool args / results / errors never leave the
        // process verbatim)
        let envelope = build_agent_audit_event(
            &self.tenant,
            &self.agent_id,
            &self.session_id,
            event,
            payload,
            Utc::now(),
            generate_audit_event_id(),
        );
        self.sink.emit(
            agent_audit_subject(self.tenant.tenant.as_str(), event),
            &envelope,
        );
    }
}

/// Longest preview kept in an audit event, in characters.
const PREVIEW_MAX_CHARS: usize = 256;
/// Nesting depth after which the redactor stops descending.
const REDACT_MAX_DEPTH: usize = 6;

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "authorization",
        "token",
        "secret",
        "password",
        "passwd",
        "api_key",
        "apikey",
        "api-key",
        "cookie",
        "credential",
        "private_key",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

fn redact(value: &Value, depth: usize) -> Value {
    if depth >= REDACT_MAX_DEPTH {
        return Value::String("[truncated]".to_string());
    }
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, val)| {
                    let val = if is_sensitive_key(key) {
                        Value::String("[redacted]".to_string())
                    } else {
                        redact(val, depth + 1)
                    };
                    (key.clone(), val)
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(|v| redact(v, depth + 1)).collect()),
        other => other.clone(),
    }
}

/// What an audit event carries instead of a tool's raw arguments / result:
/// a content hash, the byte size, and a short, key-redacted preview. The raw
/// value can hold customer data or credentials and the bus is not a secret
/// store; the hash still lets an auditor correlate an event with the payload
/// held in the run trace.
fn summarize_value(value: &Value) -> Value {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    let hash = blake3::hash(&bytes).to_hex().to_string();
    let preview_full = serde_json::to_string(&redact(value, 0)).unwrap_or_default();
    let truncated = preview_full.chars().count() > PREVIEW_MAX_CHARS;
    let preview: String = preview_full.chars().take(PREVIEW_MAX_CHARS).collect();
    json!({
        "blake3": hash,
        "size_bytes": bytes.len(),
        "preview": preview,
        "preview_truncated": truncated,
    })
}

impl StepObserver for AgentAuditObserver {
    fn wants_streaming(&self) -> bool {
        false
    }

    fn on_token_delta(&self, _chunk: &str) {}

    fn on_tool_call(&self, name: &str, call_id: &str, args: &Value) {
        self.emit(
            "tool_call",
            json!({
                "agent_id": self.agent_id,
                "tool": name,
                "call_id": call_id,
                "args": summarize_value(args),
            }),
        );
    }

    fn on_tool_result(&self, name: &str, call_id: &str, result: &Value) {
        self.emit(
            "tool_result",
            json!({
                "agent_id": self.agent_id,
                "tool": name,
                "call_id": call_id,
                "result": summarize_value(result),
            }),
        );
    }

    fn on_tool_failed(&self, name: &str, call_id: &str, error: &Value) {
        self.emit(
            "tool_result",
            json!({
                "agent_id": self.agent_id,
                "tool": name,
                "call_id": call_id,
                "error": summarize_value(error),
            }),
        );
    }

    fn on_guardrail(&self, obs: &greentic_aw_runtime::guardrail::GuardrailObservation) {
        // Best-effort, same as every other audit emission: no sink ⇒ nothing
        // built, `AuditSink::emit` never blocks or errors.
        let envelope = build_guardrail_violation_event(
            &self.tenant,
            &self.agent_id,
            Some(&self.session_id),
            obs,
            Utc::now(),
            generate_audit_event_id(),
        );
        self.sink
            .emit(violation_subject(self.tenant.tenant.as_str()), &envelope);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_types::{EnvId, TenantId};
    use tokio::sync::mpsc;

    fn tenant_ctx() -> TenantCtx {
        TenantCtx::new(
            EnvId::try_from("prod").expect("valid env id"),
            TenantId::try_from("t1").expect("valid tenant id"),
        )
    }

    fn observer_with_channel() -> (AgentAuditObserver, mpsc::Receiver<(String, Vec<u8>)>) {
        let (tx, rx) = mpsc::channel(16);
        let sink = AuditSink::from_sender(tx);
        let observer =
            AgentAuditObserver::new(sink, tenant_ctx(), "a1".to_string(), "s1".to_string());
        (observer, rx)
    }

    #[test]
    fn wants_streaming_is_false() {
        let (observer, _rx) = observer_with_channel();
        assert!(!observer.wants_streaming());
    }

    #[tokio::test]
    async fn on_tool_call_enqueues_one_tool_call_event() {
        let (observer, mut rx) = observer_with_channel();
        observer.on_tool_call("http", "c1", &json!({"url": "https://example.com"}));

        let (subject, bytes) = rx.try_recv().expect("event enqueued");
        assert_eq!(subject, "audit.t1.agent.tool_call");

        let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert!(
            value
                .get("type")
                .and_then(Value::as_str)
                .expect("type is a string")
                .ends_with("agent.tool_call")
        );
        assert_eq!(
            value.get("payload").and_then(|p| p.get("tool")),
            Some(&json!("http"))
        );
        assert_eq!(
            value.get("payload").and_then(|p| p.get("call_id")),
            Some(&json!("c1"))
        );
        let args = value
            .get("payload")
            .and_then(|p| p.get("args"))
            .expect("args summary");
        assert_eq!(
            args.get("blake3").and_then(Value::as_str).map(str::len),
            Some(64)
        );
        assert_eq!(
            args.get("size_bytes").and_then(Value::as_u64),
            Some(br#"{"url":"https://example.com"}"#.len() as u64)
        );

        assert!(rx.try_recv().is_err(), "exactly one event enqueued");
    }

    #[tokio::test]
    async fn on_tool_result_enqueues_one_tool_result_event_with_result_payload() {
        let (observer, mut rx) = observer_with_channel();
        observer.on_tool_result("http", "c1", &json!({"ok": true}));

        let (subject, bytes) = rx.try_recv().expect("event enqueued");
        assert_eq!(subject, "audit.t1.agent.tool_result");

        let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert!(
            value
                .get("type")
                .and_then(Value::as_str)
                .expect("type is a string")
                .ends_with("agent.tool_result")
        );
        let result = value
            .get("payload")
            .and_then(|p| p.get("result"))
            .expect("result summary");
        assert_eq!(
            result.get("preview").and_then(Value::as_str),
            Some(r#"{"ok":true}"#)
        );
        assert!(
            result.get("ok").is_none(),
            "raw result must not be published"
        );

        assert!(rx.try_recv().is_err(), "exactly one event enqueued");
    }

    #[tokio::test]
    async fn on_tool_failed_enqueues_one_tool_result_event_with_error_payload() {
        let (observer, mut rx) = observer_with_channel();
        observer.on_tool_failed("http", "c1", &json!({"error": "connection refused"}));

        let (subject, bytes) = rx.try_recv().expect("event enqueued");
        assert_eq!(subject, "audit.t1.agent.tool_result");

        let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert!(
            value
                .get("type")
                .and_then(Value::as_str)
                .expect("type is a string")
                .ends_with("agent.tool_result")
        );
        assert!(
            value
                .get("payload")
                .and_then(|p| p.get("error"))
                .and_then(|e| e.get("blake3"))
                .is_some()
        );
        assert!(
            value.get("payload").and_then(|p| p.get("result")).is_none(),
            "a failed call must not carry a 'result' field"
        );

        assert!(rx.try_recv().is_err(), "exactly one event enqueued");
    }

    #[tokio::test]
    async fn tool_result_is_published_as_hash_size_and_redacted_preview() {
        let (observer, mut rx) = observer_with_channel();
        let big = "x".repeat(5000);
        observer.on_tool_result(
            "http",
            "c1",
            &json!({"authorization": "Bearer abc", "body": big}),
        );
        let (_subject, bytes) = rx.try_recv().expect("event enqueued");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(!text.contains("Bearer abc"), "secret leaked: {text}");
        assert!(!text.contains(&"x".repeat(1000)), "payload not truncated");
        let value: Value = serde_json::from_str(&text).expect("json");
        let result = &value["payload"]["result"];
        assert_eq!(result["preview_truncated"], json!(true));
        assert!(result["size_bytes"].as_u64().unwrap_or(0) > 5000);
    }

    #[tokio::test]
    async fn on_token_delta_enqueues_nothing() {
        let (observer, mut rx) = observer_with_channel();
        observer.on_token_delta("chunk");
        assert!(rx.try_recv().is_err());
    }

    fn guardrail_observation(
        action: greentic_aw_runtime::guardrail::GuardrailAction,
    ) -> greentic_aw_runtime::guardrail::GuardrailObservation {
        greentic_aw_runtime::guardrail::GuardrailObservation {
            cap_id: "greentic:guardrail/pii".to_string(),
            extension_id: "ext-pii".to_string(),
            direction: greentic_aw_runtime::guardrail::GuardrailDirection::Inbound,
            code: "pii".to_string(),
            action,
        }
    }

    #[tokio::test]
    async fn on_guardrail_enqueues_one_violation_event() {
        let (observer, mut rx) = observer_with_channel();
        observer.on_guardrail(&guardrail_observation(
            greentic_aw_runtime::guardrail::GuardrailAction::Blocked,
        ));

        let (subject, bytes) = rx.try_recv().expect("event enqueued");
        assert_eq!(subject, "audit.t1.guardrail.violation");

        let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(
            value.get("type").and_then(Value::as_str),
            Some("greentic.runner.guardrail.violation")
        );
        assert_eq!(
            value.get("payload").and_then(|p| p.get("cap_id")),
            Some(&json!("greentic:guardrail/pii"))
        );
        assert_eq!(
            value.get("payload").and_then(|p| p.get("action")),
            Some(&json!("blocked"))
        );
        assert!(
            value
                .get("payload")
                .and_then(|p| p.get("message"))
                .is_none(),
            "the violation payload must never carry the free-text message"
        );

        assert!(rx.try_recv().is_err(), "exactly one event enqueued");
    }

    #[tokio::test]
    async fn on_guardrail_tags_monitor_mode_correctly() {
        let (observer, mut rx) = observer_with_channel();
        observer.on_guardrail(&guardrail_observation(
            greentic_aw_runtime::guardrail::GuardrailAction::Monitored,
        ));

        let (_subject, bytes) = rx.try_recv().expect("event enqueued");
        let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(
            value.get("payload").and_then(|p| p.get("action")),
            Some(&json!("monitored"))
        );
    }
}
