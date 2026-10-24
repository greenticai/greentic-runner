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
use once_cell::sync::Lazy;
use rand::{RngExt, rng};
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
    // `auth` covers `auth_token`, `x-auth`, `authorization`; `author` is not one.
    if key.contains("auth") && (!key.contains("author") || key.contains("authoriz")) {
        return true;
    }
    [
        "token",
        "secret",
        "password",
        "passwd",
        "pwd",
        "api_key",
        "apikey",
        "api-key",
        "access_key",
        "accesskey",
        "access-key",
        "cookie",
        "credential",
        "private_key",
        "bearer",
        "jwt",
        "session",
        "otp",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

/// A credential value being masked by [`redact_string`].
#[derive(Clone, Copy, PartialEq)]
struct SecretRun {
    /// `Cookie` / `Set-Cookie`: everything up to the end of the line is secret.
    cookie: bool,
    /// The opening quote, when the value is quoted: only its twin ends the run.
    quote: Option<char>,
    /// At least one value word has been masked.
    begun: bool,
}

impl SecretRun {
    fn new(cookie: bool) -> Self {
        Self {
            cookie,
            quote: None,
            begun: false,
        }
    }
}

/// Scanner state of [`redact_string`].
#[derive(Clone, Copy, PartialEq)]
enum Scan {
    /// Nothing pending.
    Idle,
    /// A bare credential-ish word was seen; redact only if `:`/`=` follows.
    MaybeKey { cookie: bool },
    /// The following value (possibly several words) is a credential.
    Secret(SecretRun),
}

fn is_scan_separator(c: char) -> bool {
    c.is_whitespace()
        || matches!(
            c,
            '"' | '\'' | '&' | '?' | ',' | ';' | '{' | '}' | '[' | ']' | '(' | ')'
        )
}

fn is_auth_scheme(word: &str) -> bool {
    word.eq_ignore_ascii_case("bearer") || word.eq_ignore_ascii_case("basic")
}

/// Mask the `user:password@` part of `scheme://user:password@host/...`.
fn redact_url_userinfo(token: &str) -> Option<String> {
    let scheme_end = token.find("://")? + 3;
    let rest = &token[scheme_end..];
    let authority_len = rest.find(['/', '#']).unwrap_or(rest.len());
    let at = rest[..authority_len].rfind('@')?;
    Some(format!("{}[redacted]{}", &token[..scheme_end], &rest[at..]))
}

/// Advance the scanner over one separator character (already emitted).
fn scan_separator(c: char, state: &mut Scan) {
    let Scan::Secret(run) = state else {
        return;
    };
    if let Some(quote) = run.quote {
        if c == quote {
            *state = Scan::Idle;
        }
        return;
    }
    if !run.begun && (c == '"' || c == '\'') {
        run.quote = Some(c);
        return;
    }
    if c == '\n' || c == '\r' {
        *state = Scan::Idle;
        return;
    }
    if run.cookie || (!run.begun && c.is_whitespace()) {
        return;
    }
    if matches!(c, '&' | ',' | ';' | '}' | ']' | ')' | '"' | '\'') {
        *state = Scan::Idle;
    }
}

fn redact_token(token: &str, state: &mut Scan) -> String {
    if token.is_empty() {
        return String::new();
    }
    match *state {
        Scan::Secret(mut run) => {
            if !run.begun && is_auth_scheme(token) {
                return token.to_string();
            }
            run.begun = true;
            *state = Scan::Secret(run);
            return "[redacted]".to_string();
        }
        Scan::MaybeKey { cookie } => {
            *state = Scan::Idle;
            // `"token":987654` reaches here as `:987654`.
            if token.starts_with([':', '=']) {
                let rest = token.trim_start_matches([':', '=']);
                let lead = &token[..token.len() - rest.len()];
                if rest.is_empty() || is_auth_scheme(rest) {
                    *state = Scan::Secret(SecretRun::new(cookie));
                    return token.to_string();
                }
                *state = Scan::Secret(SecretRun {
                    begun: true,
                    ..SecretRun::new(cookie)
                });
                return format!("{lead}[redacted]");
            }
        }
        Scan::Idle => {}
    }
    if let Some(masked) = redact_url_userinfo(token) {
        return masked;
    }
    if is_auth_scheme(token) {
        *state = Scan::Secret(SecretRun::new(false));
        return token.to_string();
    }
    match token.find([':', '=']) {
        Some(idx) if !token[idx..].starts_with("://") => {
            let (key, rest) = token.split_at(idx);
            let (sep, value) = rest.split_at(1);
            if !is_sensitive_key(key) {
                return token.to_string();
            }
            let cookie = key.to_ascii_lowercase().contains("cookie");
            if value.is_empty() || is_auth_scheme(value) {
                *state = Scan::Secret(SecretRun::new(cookie));
                token.to_string()
            } else {
                *state = Scan::Secret(SecretRun {
                    begun: true,
                    ..SecretRun::new(cookie)
                });
                format!("{key}{sep}[redacted]")
            }
        }
        _ => {
            if is_sensitive_key(token) {
                *state = Scan::MaybeKey {
                    cookie: token.to_ascii_lowercase().contains("cookie"),
                };
            }
            token.to_string()
        }
    }
}

/// Mask credentials embedded in free text: URL userinfo, `Bearer`/`Basic`
/// tokens and the whole value (every word, up to the end of the line for
/// cookies) of `key=value` / `key: value` pairs whose key is sensitive
/// (headers, query strings, log lines). Over-redacts rather than under-redacts.
fn redact_string(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut token = String::new();
    let mut state = Scan::Idle;
    for c in input.chars() {
        if is_scan_separator(c) {
            out.push_str(&redact_token(&token, &mut state));
            token.clear();
            out.push(c);
            scan_separator(c, &mut state);
        } else {
            token.push(c);
        }
    }
    out.push_str(&redact_token(&token, &mut state));
    out
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
        Value::String(text) => Value::String(redact_string(text)),
        other => other.clone(),
    }
}

/// Per-process random key for [`content_hash`]. Never logged or persisted.
static HASH_KEY: Lazy<[u8; 32]> = Lazy::new(|| {
    let mut key = [0u8; 32];
    rng().fill(&mut key[..]);
    key
});

/// Keyed blake3 of `bytes`. An unkeyed hash of a low-entropy secret (a PIN, a
/// short password) can be confirmed offline by anyone who reads the bus, so the
/// key is random per process: hashes are comparable ONLY within one process
/// lifetime (they let an auditor spot a repeated payload, not look one up).
fn content_hash(bytes: &[u8]) -> String {
    blake3::keyed_hash(&HASH_KEY, bytes).to_hex().to_string()
}

/// What an audit event carries instead of a tool's raw arguments / result:
/// a keyed content hash (see [`content_hash`]), the byte size, and a short, key-redacted preview. The raw
/// value can hold customer data or credentials and the bus is not a secret
/// store; the hash still lets an auditor correlate an event with the payload
/// held in the run trace.
fn summarize_value(value: &Value) -> Value {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    let hash = content_hash(&bytes);
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

    #[test]
    fn redaction_table() {
        // (input, substrings that must NOT survive, substrings that must stay)
        let cases: &[(&str, &[&str], &[&str])] = &[
            (
                "dsn postgres://user:pw123@host/db ok",
                &["user", "pw123"],
                &["postgres://", "@host/db"],
            ),
            (
                "Cookie: a=b1; c=d2\nnext line",
                &["a=b1", "c=d2"],
                &["next line"],
            ),
            ("Set-Cookie: sid=zz9; Path=/", &["zz9", "Path"], &[]),
            (
                "Authorization:Bearer abc123",
                &["abc123"],
                &["Authorization"],
            ),
            ("Authorization: Bearer abc123", &["abc123"], &["Bearer"]),
            (r#"{"token":987654}"#, &["987654"], &["token"]),
            (r#"{"token": "987654"}"#, &["987654"], &[]),
            (
                "password: correct horse battery staple",
                &["correct", "horse", "battery", "staple"],
                &["password"],
            ),
            (
                r#"{"password": "two words", "keep": "visible"}"#,
                &["two words", "two", "words"],
                &["visible"],
            ),
            ("?jwt=eyJabc&page=2", &["eyJabc"], &["page=2"]),
            ("access_key=AKIA123", &["AKIA123"], &[]),
            ("pwd=hunter2", &["hunter2"], &[]),
            ("session=s3ss10n", &["s3ss10n"], &[]),
            ("otp: 123456", &["123456"], &[]),
            ("auth=zzz", &["zzz"], &[]),
            (
                "plain text without secrets",
                &[],
                &["plain text without secrets"],
            ),
            ("author: someone", &[], &["someone"]),
        ];
        for (input, gone, kept) in cases {
            let out = redact_string(input);
            for needle in *gone {
                assert!(!out.contains(needle), "{input:?} leaked {needle:?}: {out}");
            }
            for needle in *kept {
                assert!(out.contains(needle), "{input:?} lost {needle:?}: {out}");
            }
        }
    }

    #[test]
    fn sensitive_key_list_covers_new_names() {
        for key in [
            "auth",
            "Bearer",
            "jwt",
            "access_key",
            "pwd",
            "session_id",
            "otp",
        ] {
            assert!(is_sensitive_key(key), "{key}");
        }
        assert!(!is_sensitive_key("author"));
    }

    #[test]
    fn content_hash_is_keyed_and_stable_within_a_process() {
        let a = content_hash(b"1234");
        assert_eq!(a, content_hash(b"1234"));
        assert_ne!(a, blake3::hash(b"1234").to_hex().to_string());
        assert_ne!(a, content_hash(b"1235"));
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
