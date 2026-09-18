//! Builds the per-node audit `EventEnvelope` published to NATS `audit.>`.
//!
//! The emitted JSON MUST decode under greentic-admin's `activity_from_envelope`,
//! which reads `tenant.tenant` (falling back to a top-level `tenant` string),
//! `type`, `source`, `subject`, `time`, `correlation_id`, and `payload`. The
//! envelope is built directly from `greentic_types::EventEnvelope` (NOT via
//! `BusinessEventBuilder`, which forces a `cap://` type and required metadata
//! that the admin decoder does not need). See
//! `docs/superpowers/specs/2026-07-03-runner-audit-emitter-design.md`.

use chrono::{DateTime, Utc};
use greentic_types::{EventEnvelope, EventId, TenantCtx};
use serde_json::json;

/// Fallback event id used when the caller-supplied id fails
/// `greentic_types::EventId` validation (ASCII alphanumeric + `.` `_` `-`).
/// Never let an odd id turn audit-event construction into a panic path —
/// audit emission must stay best-effort and never block flow execution.
const FALLBACK_EVENT_ID: &str = "audit-event-invalid-id";

/// Outcome of a single flow-node execution, driving the audit event's
/// `type` suffix (`node_end` vs `node_error`) and `payload.outcome`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Error,
}

/// Everything needed to build one per-node audit event.
pub struct NodeAuditRecord<'a> {
    pub tenant: &'a TenantCtx,
    pub flow_id: &'a str,
    pub node_id: &'a str,
    pub component_id: &'a str,
    pub operation: &'a str,
    pub session_id: &'a str,
    pub duration_ms: u64,
    pub outcome: Outcome,
    pub error: Option<&'a str>,
}

/// Builds the audit subject: `audit.<tenant>.flow.<event>` (e.g.
/// `audit.t1.flow.node_end`).
pub fn audit_subject(tenant: &str, event: &str) -> String {
    format!("audit.{tenant}.flow.{event}")
}

/// Builds the audit subject for an agentic-worker step event:
/// `audit.<tenant>.agent.<event>` (e.g. `audit.t1.agent.tool_call`).
pub fn agent_audit_subject(tenant: &str, event: &str) -> String {
    format!("audit.{tenant}.agent.{event}")
}

/// Shared envelope construction core for [`build_audit_event`] and
/// [`build_agent_audit_event`]. Both builders funnel through here so the
/// `EventId` fallback and the common `EventEnvelope` fields stay in one
/// place.
#[allow(clippy::too_many_arguments)]
fn base_event(
    tenant: &TenantCtx,
    ty: &str,
    source: &str,
    subject: String,
    correlation_id: Option<String>,
    payload: serde_json::Value,
    time: DateTime<Utc>,
    id: String,
) -> EventEnvelope {
    let event_id = EventId::new(&id).unwrap_or_else(|_| {
        EventId::new(FALLBACK_EVENT_ID).expect("static fallback event id is a valid EventId")
    });

    EventEnvelope {
        id: event_id,
        topic: "runner.flow".to_string(),
        r#type: ty.to_string(),
        source: source.to_string(),
        tenant: tenant.clone(),
        subject: Some(subject),
        time,
        correlation_id,
        payload,
        metadata: Default::default(),
    }
}

/// Constructs the audit `EventEnvelope` for one node's completion.
///
/// `type` is `greentic.runner.flow.node_end` for `Outcome::Ok` and
/// `greentic.runner.flow.node_error` for `Outcome::Error`; `source` is always
/// `"runner"`; `subject` is `flow:<flow_id>/node:<node_id>`; `correlation_id`
/// carries the session id so the admin can thread node events to a session.
pub fn build_audit_event(
    rec: &NodeAuditRecord<'_>,
    now: DateTime<Utc>,
    id: String,
) -> EventEnvelope {
    let (event_suffix, outcome_label) = match rec.outcome {
        Outcome::Ok => ("node_end", "ok"),
        Outcome::Error => ("node_error", "error"),
    };
    let event_type = format!("greentic.runner.flow.{event_suffix}");

    let mut payload = json!({
        "node_id": rec.node_id,
        "component_id": rec.component_id,
        "operation": rec.operation,
        "outcome": outcome_label,
        "duration_ms": rec.duration_ms,
    });
    if let Some(error) = rec.error
        && let Some(map) = payload.as_object_mut()
    {
        map.insert("error".to_string(), json!(error));
    }

    base_event(
        rec.tenant,
        &event_type,
        "runner",
        format!("flow:{}/node:{}", rec.flow_id, rec.node_id),
        Some(rec.session_id.to_string()),
        payload,
        now,
        id,
    )
}

/// Constructs the audit `EventEnvelope` for one agentic-worker step
/// (tool call/result).
///
/// `type` is `greentic.runner.agent.<event>` (e.g.
/// `greentic.runner.agent.tool_call`); `source` is always `"runner"`;
/// `subject` is `agent:<agent_id>`; `correlation_id` carries the agent
/// session id so the admin can thread agent-step events to a session.
pub fn build_agent_audit_event(
    tenant: &TenantCtx,
    agent_id: &str,
    session_id: &str,
    event: &str,
    payload: serde_json::Value,
    now: DateTime<Utc>,
    id: String,
) -> EventEnvelope {
    let event_type = format!("greentic.runner.agent.{event}");

    base_event(
        tenant,
        &event_type,
        "runner",
        format!("agent:{agent_id}"),
        Some(session_id.to_string()),
        payload,
        now,
        id,
    )
}

/// Builds the metering subject for one agentic-worker run completion:
/// `audit.<tenant>.metering.agent_run`.
pub fn metering_subject(tenant: &str) -> String {
    format!("audit.{tenant}.metering.agent_run")
}

/// Constructs the metering `EventEnvelope` for one completed agentic-worker
/// run.
///
/// `type` is always `greentic.runner.metering.agent_run`; `source` is always
/// `"runner"`; `subject` is `agent:<agent_id>`; the payload is the billing
/// unit record: `{"unit":"agent_run","quantity":1,"agent_id":<agent_id>,
/// "steps":<steps>}`. Emitted once per successful `dw.agent` run, alongside
/// (not instead of) the per-step agent-audit events from
/// [`build_agent_audit_event`].
pub fn build_agent_run_metering_event(
    tenant: &TenantCtx,
    agent_id: &str,
    steps: usize,
    now: DateTime<Utc>,
    id: String,
) -> EventEnvelope {
    let payload = json!({
        "unit": "agent_run",
        "quantity": 1,
        "agent_id": agent_id,
        "steps": steps,
    });

    base_event(
        tenant,
        "greentic.runner.metering.agent_run",
        "runner",
        format!("agent:{agent_id}"),
        None,
        payload,
        now,
        id,
    )
}

/// Subject for one guardrail denial: `audit.<tenant>.guardrail.violation`.
pub fn violation_subject(tenant: &str) -> String {
    format!("audit.{tenant}.guardrail.violation")
}

/// Best-effort record of one guardrail denial — blocked (Enforce) or recorded
/// only (Monitor). Emitted alongside, never instead of, the other audit
/// events (the metering event and the per-step agent-audit events).
///
/// This is TELEMETRY, not an audit log: `AuditSink` drops events when its
/// channel saturates and the NATS publish is fire-and-forget. The payload
/// carries `code` (a stable, non-sensitive classifier), never `message`
/// (free text that may echo user content) or the guarded content itself.
pub fn build_guardrail_violation_event(
    tenant: &TenantCtx,
    agent_id: &str,
    session_id: Option<&str>,
    obs: &greentic_aw_runtime::guardrail::GuardrailObservation,
    now: DateTime<Utc>,
    id: String,
) -> EventEnvelope {
    let payload = json!({
        "cap_id": obs.cap_id,
        "extension_id": obs.extension_id,
        "direction": obs.direction.as_str(),
        "code": obs.code,
        "action": obs.action,
        "agent_id": agent_id,
    });

    base_event(
        tenant,
        "greentic.runner.guardrail.violation",
        "runner",
        format!("agent:{agent_id}"),
        session_id.map(str::to_string),
        payload,
        now,
        id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_types::{EnvId, TenantId};
    use serde_json::Value;

    // Mirror greentic-admin::audit_ingest::activity_from_envelope's field reads.
    fn admin_decode(body: &Value) -> (String, String, String, String, String, String) {
        let tenant = body
            .get("tenant")
            .and_then(|t| t.get("tenant"))
            .and_then(Value::as_str)
            .or_else(|| body.get("tenant").and_then(Value::as_str))
            .unwrap()
            .to_string();
        let ty = body
            .get("type")
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        let source = body
            .get("source")
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        let subject = body
            .get("subject")
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        let time = body
            .get("time")
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        let corr = body
            .get("correlation_id")
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        (tenant, ty, source, subject, time, corr)
    }

    fn tenant_ctx() -> TenantCtx {
        TenantCtx::new(
            EnvId::try_from("prod").expect("valid env id"),
            TenantId::try_from("t1").expect("valid tenant id"),
        )
    }

    fn tenant_ctx_named(tenant: &str, env: &str) -> TenantCtx {
        TenantCtx::new(
            EnvId::try_from(env).expect("valid env id"),
            TenantId::try_from(tenant).expect("valid tenant id"),
        )
    }

    fn obs_blocked() -> greentic_aw_runtime::guardrail::GuardrailObservation {
        greentic_aw_runtime::guardrail::GuardrailObservation {
            cap_id: "greentic:guardrail/pii".to_string(),
            extension_id: "ext-pii".to_string(),
            direction: greentic_aw_runtime::guardrail::GuardrailDirection::Inbound,
            code: "pii".to_string(),
            action: greentic_aw_runtime::guardrail::GuardrailAction::Blocked,
        }
    }

    fn obs_monitored() -> greentic_aw_runtime::guardrail::GuardrailObservation {
        greentic_aw_runtime::guardrail::GuardrailObservation {
            action: greentic_aw_runtime::guardrail::GuardrailAction::Monitored,
            ..obs_blocked()
        }
    }

    #[test]
    fn built_event_decodes_under_admin_contract() {
        let tenant = tenant_ctx();
        let rec = NodeAuditRecord {
            tenant: &tenant,
            flow_id: "f1",
            node_id: "n1",
            component_id: "greentic:http",
            operation: "call",
            session_id: "s1",
            duration_ms: 12,
            outcome: Outcome::Ok,
            error: None,
        };
        let now = chrono::DateTime::parse_from_rfc3339("2026-07-03T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let env = build_audit_event(&rec, now, "id1".to_string());
        let body = serde_json::to_value(&env).unwrap();

        let (tenant, ty, source, subject, time, corr) = admin_decode(&body);
        assert_eq!(tenant, "t1");
        assert_eq!(ty, "greentic.runner.flow.node_end");
        assert_eq!(source, "runner");
        assert_eq!(subject, "flow:f1/node:n1");
        assert!(time.starts_with("2026-07-03T00:00:00"));
        assert_eq!(corr, "s1");
        assert!(
            body.get("payload")
                .and_then(|p| p.get("duration_ms"))
                .is_some()
        );
    }

    #[test]
    fn error_outcome_sets_type_and_payload_error() {
        let tenant = tenant_ctx();
        let rec = NodeAuditRecord {
            tenant: &tenant,
            flow_id: "f1",
            node_id: "n1",
            component_id: "greentic:http",
            operation: "call",
            session_id: "s1",
            duration_ms: 5,
            outcome: Outcome::Error,
            error: Some("boom"),
        };
        let now = chrono::DateTime::parse_from_rfc3339("2026-07-03T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let env = build_audit_event(&rec, now, "id2".to_string());
        let body = serde_json::to_value(&env).unwrap();

        let (_, ty, _, _, _, _) = admin_decode(&body);
        assert_eq!(ty, "greentic.runner.flow.node_error");
        let payload = body.get("payload").unwrap();
        assert_eq!(
            payload.get("outcome").and_then(Value::as_str),
            Some("error")
        );
        assert_eq!(payload.get("error").and_then(Value::as_str), Some("boom"));
    }

    #[test]
    fn ok_outcome_never_includes_error_key() {
        let tenant = tenant_ctx();
        let rec = NodeAuditRecord {
            tenant: &tenant,
            flow_id: "f1",
            node_id: "n1",
            component_id: "greentic:http",
            operation: "call",
            session_id: "s1",
            duration_ms: 5,
            outcome: Outcome::Ok,
            error: None,
        };
        let now = Utc::now();
        let env = build_audit_event(&rec, now, "id3".to_string());
        let body = serde_json::to_value(&env).unwrap();
        assert!(body.get("payload").unwrap().get("error").is_none());
    }

    #[test]
    fn subject_is_well_formed() {
        assert_eq!(audit_subject("t1", "node_end"), "audit.t1.flow.node_end");
    }

    #[test]
    fn agent_subject_is_well_formed() {
        assert_eq!(
            agent_audit_subject("t1", "tool_call"),
            "audit.t1.agent.tool_call"
        );
    }

    #[test]
    fn metering_subject_is_well_formed() {
        assert_eq!(metering_subject("t1"), "audit.t1.metering.agent_run");
    }

    #[test]
    fn agent_run_metering_event_has_expected_type_subject_and_payload() {
        let tenant = tenant_ctx();
        let now = chrono::DateTime::parse_from_rfc3339("2026-07-03T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let env = build_agent_run_metering_event(&tenant, "agent1", 3, now, "id1".to_string());
        let body = serde_json::to_value(&env).unwrap();

        assert_eq!(
            body.get("type").and_then(Value::as_str),
            Some("greentic.runner.metering.agent_run")
        );
        assert_eq!(body.get("source").and_then(Value::as_str), Some("runner"));
        assert_eq!(
            body.get("subject").and_then(Value::as_str),
            Some("agent:agent1")
        );
        assert_eq!(
            body.get("tenant")
                .and_then(|t| t.get("tenant"))
                .and_then(Value::as_str),
            Some("t1")
        );

        let payload = body.get("payload").unwrap();
        assert_eq!(
            payload.get("unit").and_then(Value::as_str),
            Some("agent_run")
        );
        assert_eq!(payload.get("quantity").and_then(Value::as_u64), Some(1));
        assert_eq!(payload.get("steps").and_then(Value::as_u64), Some(3));
        assert_eq!(
            payload.get("agent_id").and_then(Value::as_str),
            Some("agent1")
        );
    }

    #[test]
    fn agent_event_decodes_under_admin_contract() {
        let tenant = tenant_ctx();
        let now = chrono::DateTime::parse_from_rfc3339("2026-07-03T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let env = build_agent_audit_event(
            &tenant,
            "a1",
            "s1",
            "tool_call",
            json!({"tool": "http", "call_id": "c1"}),
            now,
            "id1".to_string(),
        );
        let body = serde_json::to_value(&env).unwrap();

        let (tenant, ty, source, subject, time, corr) = admin_decode(&body);
        assert_eq!(tenant, "t1");
        assert_eq!(ty, "greentic.runner.agent.tool_call");
        assert_eq!(source, "runner");
        assert_eq!(subject, "agent:a1");
        assert!(time.starts_with("2026-07-03T00:00:00"));
        assert_eq!(corr, "s1");
        assert_eq!(
            body.get("payload")
                .and_then(|p| p.get("tool"))
                .and_then(Value::as_str),
            Some("http")
        );
    }

    #[test]
    fn guardrail_violation_event_has_the_expected_shape() {
        let tenant = tenant_ctx_named("acme", "production");
        let now = chrono::DateTime::parse_from_rfc3339("2026-07-03T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let ev = build_guardrail_violation_event(
            &tenant,
            "a1",
            Some("s1"),
            &obs_blocked(),
            now,
            "evt-1".to_string(),
        );
        assert_eq!(ev.r#type, "greentic.runner.guardrail.violation");
        assert_eq!(ev.source, "runner");
        assert_eq!(ev.payload["cap_id"], "greentic:guardrail/pii");
        assert_eq!(ev.payload["direction"], "inbound");
        assert_eq!(ev.payload["code"], "pii");
        assert_eq!(ev.payload["action"], "blocked");
        assert_eq!(ev.payload["agent_id"], "a1");
        // Privacy: the payload must never carry the free-text `message`.
        assert!(ev.payload.get("message").is_none());
    }

    #[test]
    fn guardrail_violation_subject_is_tenant_scoped() {
        assert_eq!(violation_subject("acme"), "audit.acme.guardrail.violation");
    }

    #[test]
    fn monitored_violation_is_tagged_monitored() {
        let tenant = tenant_ctx_named("acme", "production");
        let ev = build_guardrail_violation_event(
            &tenant,
            "a1",
            None,
            &obs_monitored(),
            Utc::now(),
            "e".to_string(),
        );
        assert_eq!(ev.payload["action"], "monitored");
    }
}
