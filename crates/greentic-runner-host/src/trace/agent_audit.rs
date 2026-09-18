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
                "args": args,
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
                "result": result,
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
                "error": error,
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
        assert_eq!(
            value.get("payload").and_then(|p| p.get("args")),
            Some(&json!({"url": "https://example.com"}))
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
        assert_eq!(
            value.get("payload").and_then(|p| p.get("result")),
            Some(&json!({"ok": true}))
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
        assert_eq!(
            value.get("payload").and_then(|p| p.get("error")),
            Some(&json!({"error": "connection refused"}))
        );
        assert!(
            value.get("payload").and_then(|p| p.get("result")).is_none(),
            "a failed call must not carry a 'result' field"
        );

        assert!(rx.try_recv().is_err(), "exactly one event enqueued");
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
