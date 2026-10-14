#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Mutex;

use super::*;
use crate::config::GuardrailMode;
use crate::guardrail::{GuardrailAction, GuardrailDenyInfo, GuardrailObservation};

/// Answers per extension id; records every input it is asked about.
struct ScriptedEvaluator {
    answers: Vec<(&'static str, Result<GuardrailVerdict, GuardrailInvokeError>)>,
    seen: Mutex<Vec<GuardrailInput>>,
}

impl ScriptedEvaluator {
    fn new(answers: Vec<(&'static str, Result<GuardrailVerdict, GuardrailInvokeError>)>) -> Self {
        Self {
            answers,
            seen: Mutex::new(Vec::new()),
        }
    }
}

impl GuardrailEvaluator for ScriptedEvaluator {
    fn evaluate(
        &self,
        extension_id: &str,
        input: &GuardrailInput,
    ) -> Result<GuardrailVerdict, GuardrailInvokeError> {
        self.seen.lock().unwrap().push(input.clone());
        self.answers
            .iter()
            .find(|(id, _)| *id == extension_id)
            .map(|(_, a)| a.clone())
            .unwrap_or(Ok(GuardrailVerdict::Accept))
    }
}

#[derive(Default)]
struct Recorder(Mutex<Vec<GuardrailObservation>>);

impl StepObserver for Recorder {
    fn on_guardrail(&self, obs: &GuardrailObservation) {
        self.0.lock().unwrap().push(obs.clone());
    }
}

fn g(id: &'static str, mandatory: bool, mode: GuardrailMode) -> ResolvedGuardrail {
    ResolvedGuardrail {
        extension_id: id.into(),
        cap_id: format!("greentic:guardrail/{id}"),
        mandatory,
        mode,
        config: serde_json::Value::Null,
    }
}

fn ctx() -> GuardrailRunCtx {
    GuardrailRunCtx {
        agent_id: "a".into(),
        session_id: "s".into(),
        tenant_id: "t".into(),
        env_id: "e".into(),
    }
}

fn deny() -> Result<GuardrailVerdict, GuardrailInvokeError> {
    Ok(GuardrailVerdict::Deny(GuardrailDenyInfo {
        code: "permission_denied".into(),
        message: "no".into(),
        details: None,
    }))
}

fn guard(
    chain: Vec<ResolvedGuardrail>,
    eval: Arc<ScriptedEvaluator>,
    rec: Arc<Recorder>,
) -> Arc<dyn AttachmentTextGuard> {
    InboundChainGuard::for_turn(&chain, &ctx(), eval, rec).expect("a non-empty chain")
}

#[test]
fn an_empty_chain_builds_no_guard() {
    let eval = Arc::new(ScriptedEvaluator::new(vec![]));
    assert!(
        InboundChainGuard::for_turn(&[], &ctx(), eval, Arc::new(Recorder::default())).is_none()
    );
}

#[test]
fn accept_keeps_the_text_and_the_guardrail_sees_it_as_inbound() {
    let eval = Arc::new(ScriptedEvaluator::new(vec![]));
    let gd = guard(
        vec![g("pii", true, GuardrailMode::Enforce)],
        eval.clone(),
        Arc::new(Recorder::default()),
    );
    assert_eq!(
        gd.check("hello"),
        AttachmentTextVerdict::Allow("hello".into())
    );
    let seen = eval.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].content, "hello");
    assert_eq!(seen[0].direction, GuardrailDirection::Inbound);
    assert_eq!(seen[0].tenant_id, "t");
}

#[test]
fn an_update_is_the_text_shown() {
    let eval = Arc::new(ScriptedEvaluator::new(vec![(
        "pii",
        Ok(GuardrailVerdict::Update("[REDACTED]".into())),
    )]));
    let gd = guard(
        vec![g("pii", false, GuardrailMode::Enforce)],
        eval,
        Arc::new(Recorder::default()),
    );
    assert_eq!(
        gd.check("a@b.c"),
        AttachmentTextVerdict::Allow("[REDACTED]".into())
    );
}

#[test]
fn an_enforce_deny_withholds_and_is_observed_as_blocked() {
    let eval = Arc::new(ScriptedEvaluator::new(vec![("pii", deny())]));
    let rec = Arc::new(Recorder::default());
    let gd = guard(
        vec![g("pii", false, GuardrailMode::Enforce)],
        eval,
        rec.clone(),
    );
    assert_eq!(gd.check("bad"), AttachmentTextVerdict::Withhold);
    let obs = rec.0.lock().unwrap();
    assert_eq!(obs.len(), 1);
    assert_eq!(obs[0].action, GuardrailAction::Blocked);
    assert_eq!(obs[0].direction, GuardrailDirection::Inbound);
}

#[test]
fn a_monitor_deny_keeps_the_text_and_is_observed_as_monitored() {
    let eval = Arc::new(ScriptedEvaluator::new(vec![("pii", deny())]));
    let rec = Arc::new(Recorder::default());
    let gd = guard(
        vec![g("pii", false, GuardrailMode::Monitor)],
        eval,
        rec.clone(),
    );
    assert_eq!(gd.check("bad"), AttachmentTextVerdict::Allow("bad".into()));
    let obs = rec.0.lock().unwrap();
    assert_eq!(obs.len(), 1);
    assert_eq!(obs[0].action, GuardrailAction::Monitored);
}

#[test]
fn a_mandatory_evaluator_error_withholds() {
    let eval = Arc::new(ScriptedEvaluator::new(vec![(
        "pii",
        Err(GuardrailInvokeError("boom".into())),
    )]));
    let gd = guard(
        vec![g("pii", true, GuardrailMode::Enforce)],
        eval,
        Arc::new(Recorder::default()),
    );
    assert_eq!(gd.check("x"), AttachmentTextVerdict::Withhold);
}

#[test]
fn an_agent_level_evaluator_error_withholds_unlike_the_message_text_path() {
    // `run_chain` fails OPEN for an agent-level guardrail; a document is not
    // shown when a guardrail could not check it.
    let eval = Arc::new(ScriptedEvaluator::new(vec![(
        "optional",
        Err(GuardrailInvokeError("boom".into())),
    )]));
    let gd = guard(
        vec![
            g("optional", false, GuardrailMode::Enforce),
            g("pii", false, GuardrailMode::Enforce),
        ],
        eval,
        Arc::new(Recorder::default()),
    );
    assert_eq!(gd.check("x"), AttachmentTextVerdict::Withhold);
}

#[test]
fn debug_never_prints_text() {
    let v = AttachmentTextVerdict::Allow("secret body".into());
    assert!(!format!("{v:?}").contains("secret"));
    let eval = Arc::new(ScriptedEvaluator::new(vec![]));
    let gd = guard(
        vec![g("pii", true, GuardrailMode::Enforce)],
        eval,
        Arc::new(Recorder::default()),
    );
    assert_eq!(format!("{gd:?}"), "InboundChainGuard { guardrails: 1 }");
}
