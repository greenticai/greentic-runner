use std::future::Future;
use std::pin::Pin;

use greentic_ext_runtime::capability::CapabilityRegistry;
use greentic_extension_sdk_contract::{CapabilityId, CapabilityRef as ExtCapabilityRef};
use serde::Serialize;

use crate::config::{GuardrailMode, GuardrailRef};
use crate::tenant::TenantContext;

/// Failure obtaining the mandatory guardrail policy. Treated as fail-closed by
/// the agent loop (the step is denied), matching the unresolvable-mandatory-cap
/// behavior.
#[derive(Debug, thiserror::Error)]
pub enum GuardrailPolicyError {
    #[error("mandatory guardrail policy unavailable: {0}")]
    Unavailable(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GuardrailDirection {
    Inbound,
    Outbound,
}

impl GuardrailDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            GuardrailDirection::Inbound => "inbound",
            GuardrailDirection::Outbound => "outbound",
        }
    }
}

impl std::fmt::Display for GuardrailDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GuardrailDenyInfo {
    pub code: String,
    pub message: String,
    pub details: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum GuardrailVerdict {
    Accept,
    Update(String),
    Deny(GuardrailDenyInfo),
}

#[derive(Clone, Debug)]
pub struct GuardrailInput {
    pub direction: GuardrailDirection,
    pub content: String,
    pub agent_id: String,
    pub session_id: String,
    pub tenant_id: String,
    pub env_id: String,
    pub context: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedGuardrail {
    pub extension_id: String,
    pub cap_id: String,
    pub mandatory: bool,
    pub mode: GuardrailMode,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct GuardrailInvokeError(pub String);

pub trait GuardrailEvaluator: Send + Sync {
    fn evaluate(
        &self,
        extension_id: &str,
        input: &GuardrailInput,
    ) -> Result<GuardrailVerdict, GuardrailInvokeError>;
}

pub trait GuardrailPolicy: Send + Sync {
    /// The platform-mandated guardrails for this tenant+env. `Err` means the
    /// policy could not be determined and the caller MUST fail closed.
    fn mandatory_guardrails<'a>(
        &'a self,
        tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GuardrailRef>, GuardrailPolicyError>> + Send + 'a>>;
}

/// A no-op [`GuardrailEvaluator`] that unconditionally accepts every input.
///
/// Used as the default evaluator in [`crate::AgentRuntime`] until a real
/// WASM-backed evaluator (Task 7) is wired in. This lets the runtime compile
/// and the inbound/outbound hooks exercise the chain logic without requiring
/// a populated extension registry.
pub struct AcceptAllEvaluator;

impl GuardrailEvaluator for AcceptAllEvaluator {
    fn evaluate(
        &self,
        _extension_id: &str,
        _input: &GuardrailInput,
    ) -> Result<GuardrailVerdict, GuardrailInvokeError> {
        Ok(GuardrailVerdict::Accept)
    }
}

/// A [`GuardrailEvaluator`] that delegates evaluation to a real WASM extension
/// loaded by [`greentic_ext_runtime::ExtensionRuntime`].
///
/// This adapter bridges the agentic-worker's [`GuardrailEvaluator`] trait to the
/// ext-runtime's `evaluate_guardrail` entry point:
///
/// 1. Serializes [`GuardrailInput`] to a flat JSON payload.
/// 2. Calls `ext_runtime.evaluate_guardrail(extension_id, &payload)` which
///    invokes the WASM component's `greentic:extension-design/guardrail#evaluate`
///    export and returns the verdict as JSON.
/// 3. Deserializes the returned JSON into [`greentic_ext_runtime::GuardrailVerdictWire`].
/// 4. Maps the wire form to [`GuardrailVerdict`].
///
/// # Wiring
///
/// Construction (Task 8) will pass the `Arc<ExtensionRuntime>` that the
/// `AgentRuntime` already holds for tool dispatch. No additional WASM runtime
/// is needed.
pub struct ExtRuntimeGuardrailEvaluator {
    pub ext_runtime: std::sync::Arc<greentic_ext_runtime::ExtensionRuntime>,
}

impl GuardrailEvaluator for ExtRuntimeGuardrailEvaluator {
    fn evaluate(
        &self,
        extension_id: &str,
        input: &GuardrailInput,
    ) -> Result<GuardrailVerdict, GuardrailInvokeError> {
        let payload = serde_json::json!({
            "direction": input.direction.as_str(),
            "content": input.content,
            "agent_id": input.agent_id,
            "session_id": input.session_id,
            "tenant_id": input.tenant_id,
            "env_id": input.env_id,
            "context": input.context,
        })
        .to_string();

        let verdict_json = self
            .ext_runtime
            .evaluate_guardrail(extension_id, &payload)
            .map_err(|e| GuardrailInvokeError(e.to_string()))?;

        let wire: greentic_ext_runtime::GuardrailVerdictWire =
            serde_json::from_str(&verdict_json).map_err(|e| GuardrailInvokeError(e.to_string()))?;

        Ok(match wire {
            greentic_ext_runtime::GuardrailVerdictWire::Accept => GuardrailVerdict::Accept,
            greentic_ext_runtime::GuardrailVerdictWire::Update { content } => {
                GuardrailVerdict::Update(content)
            }
            greentic_ext_runtime::GuardrailVerdictWire::Deny {
                code,
                message,
                details,
            } => GuardrailVerdict::Deny(GuardrailDenyInfo {
                code,
                message,
                details,
            }),
        })
    }
}

pub struct NoMandatoryGuardrails;

impl GuardrailPolicy for NoMandatoryGuardrails {
    fn mandatory_guardrails<'a>(
        &'a self,
        _tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GuardrailRef>, GuardrailPolicyError>> + Send + 'a>>
    {
        Box::pin(async move { Ok(Vec::new()) })
    }
}

pub struct StaticGuardrailPolicy(pub Vec<GuardrailRef>);

impl GuardrailPolicy for StaticGuardrailPolicy {
    fn mandatory_guardrails<'a>(
        &'a self,
        _tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GuardrailRef>, GuardrailPolicyError>> + Send + 'a>>
    {
        let refs = self.0.clone();
        Box::pin(async move { Ok(refs) })
    }
}

/// Resolve a single `GuardrailRef` through the capability registry.
///
/// Returns `None` and logs a warning if the cap_id cannot be parsed or has no
/// matching offering registered.
fn resolve_one(
    registry: &CapabilityRegistry,
    guardrail_ref: &GuardrailRef,
    is_mandatory: bool,
) -> Option<ResolvedGuardrail> {
    let cap_id: CapabilityId = match guardrail_ref.cap_id.parse() {
        Ok(id) => id,
        Err(_) => {
            tracing::warn!(
                cap_id = %guardrail_ref.cap_id,
                "guardrail cap_id is not a valid capability ID (expected namespace:path); skipping"
            );
            return None;
        }
    };

    let required = [ExtCapabilityRef {
        id: cap_id.clone(),
        version: "*".to_string(),
        deprecated: None,
    }];
    let plan = registry.resolve("agentic-worker", &required);

    match plan.resolved.get(&cap_id) {
        Some(binding) => Some(ResolvedGuardrail {
            extension_id: binding.extension_id.clone(),
            cap_id: guardrail_ref.cap_id.clone(),
            mandatory: is_mandatory,
            mode: guardrail_ref.mode,
            config: guardrail_ref.config.clone(),
        }),
        None => {
            tracing::warn!(
                cap_id = %guardrail_ref.cap_id,
                "guardrail capability unresolved; skipping"
            );
            None
        }
    }
}

/// Assemble the ordered guardrail chain.
///
/// Order: mandatory refs first (in list order), then agent refs (in list order).
///
/// Resolution failure semantics:
/// - A MANDATORY ref that fails to resolve (bad cap_id format, or no offering in
///   the registry) is FATAL: its cap_id string is collected and the function
///   returns `Err(unresolved_mandatory_cap_ids)`. Callers MUST fail closed
///   (block the agent) — a governance-enforced guardrail must never be silently
///   skipped.
/// - An AGENT-level ref that fails to resolve is skipped with a warning (fail-open).
pub fn assemble_chain(
    registry: &CapabilityRegistry,
    mandatory: &[GuardrailRef],
    agent: &[GuardrailRef],
) -> Result<Vec<ResolvedGuardrail>, Vec<String>> {
    let mut chain = Vec::new();
    let mut unresolved_mandatory: Vec<String> = Vec::new();

    for guardrail_ref in mandatory {
        match resolve_one(registry, guardrail_ref, true) {
            Some(resolved) => chain.push(resolved),
            None => unresolved_mandatory.push(guardrail_ref.cap_id.clone()),
        }
    }

    if !unresolved_mandatory.is_empty() {
        return Err(unresolved_mandatory);
    }

    for guardrail_ref in agent {
        if let Some(resolved) = resolve_one(registry, guardrail_ref, false) {
            chain.push(resolved);
        }
    }

    Ok(chain)
}

/// Runtime context for executing a guardrail chain.
#[derive(Clone, Debug)]
pub struct GuardrailRunCtx {
    pub agent_id: String,
    pub session_id: String,
    pub tenant_id: String,
    pub env_id: String,
}

/// What the runner did about an explicit `deny` verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum GuardrailAction {
    /// Enforce mode: the turn was blocked.
    Blocked,
    /// Monitor mode: recorded only; the content passed through.
    Monitored,
}

/// A single guardrail denial, recorded whether or not it blocked. This is the
/// payload the host turns into a best-effort violation event; `run_chain`
/// itself neither knows the tenant nor owns a sink.
#[derive(Clone, Debug, PartialEq)]
pub struct GuardrailObservation {
    pub cap_id: String,
    pub extension_id: String,
    pub direction: GuardrailDirection,
    pub code: String,
    pub action: GuardrailAction,
}

/// Outcome of running a guardrail chain over content.
#[derive(Clone, Debug)]
pub enum ChainOutcome {
    Pass {
        content: String,
        observations: Vec<GuardrailObservation>,
    },
    Denied {
        info: GuardrailDenyInfo,
        direction: GuardrailDirection,
        observation: GuardrailObservation,
    },
}

/// Run a chain of guardrails over content in the specified direction.
///
/// # Behavior
///
/// - Each guardrail in the chain is evaluated in order against the current content.
/// - If a guardrail returns `Update(new_content)`, the new content is threaded forward to the next guardrail.
/// - If a guardrail returns `Accept`, execution continues with the content unchanged.
/// - If a guardrail returns `Deny(info)` in Enforce mode, execution stops and `ChainOutcome::Denied` is returned immediately (short-circuit). In Monitor mode, the denial is recorded as a `GuardrailObservation` instead, and execution continues with the content UNCHANGED.
/// - If a guardrail's evaluator returns an error:
///   - For **mandatory** guardrails: fails closed — returns `ChainOutcome::Denied` with code "internal".
///   - For **agent-level** guardrails: fails open — logs a warning and continues with content unchanged.
///
/// # Parameters
///
/// - `chain`: the sequence of resolved guardrails to execute
/// - `direction`: `Inbound` or `Outbound` (threaded into each GuardrailInput)
/// - `content`: the initial content to validate/transform
/// - `ctx`: execution context (agent_id, session_id, tenant_id, env_id)
/// - `evaluator`: the trait object that invokes guardrail extensions
///
/// # Returns
///
/// - `ChainOutcome::Pass { content, observations }` if all guardrails accept the
///   content (possibly modified); `observations` carries any Monitor-mode denials
///   recorded along the way.
/// - `ChainOutcome::Denied { info, direction, observation }` if an Enforce-mode
///   guardrail denies, or a mandatory guardrail's evaluator errors.
pub fn run_chain(
    chain: &[ResolvedGuardrail],
    direction: GuardrailDirection,
    content: String,
    ctx: &GuardrailRunCtx,
    evaluator: &dyn GuardrailEvaluator,
) -> ChainOutcome {
    let mut content = content;
    let mut observations = Vec::new();
    for g in chain {
        let context = if g.config.is_null() {
            None
        } else {
            serde_json::to_string(&g.config).ok()
        };
        let input = GuardrailInput {
            direction,
            content: content.clone(),
            agent_id: ctx.agent_id.clone(),
            session_id: ctx.session_id.clone(),
            tenant_id: ctx.tenant_id.clone(),
            env_id: ctx.env_id.clone(),
            context,
        };
        match evaluator.evaluate(&g.extension_id, &input) {
            Ok(GuardrailVerdict::Accept) => {}
            Ok(GuardrailVerdict::Update(new_content)) => content = new_content,
            Ok(GuardrailVerdict::Deny(info)) => match g.mode {
                GuardrailMode::Enforce => {
                    let observation = GuardrailObservation {
                        cap_id: g.cap_id.clone(),
                        extension_id: g.extension_id.clone(),
                        direction,
                        code: info.code.clone(),
                        action: GuardrailAction::Blocked,
                    };
                    return ChainOutcome::Denied {
                        info,
                        direction,
                        observation,
                    };
                }
                GuardrailMode::Monitor => {
                    // Monitor: record and keep going with the content UNCHANGED.
                    tracing::info!(
                        extension_id = %g.extension_id,
                        cap_id = %g.cap_id,
                        code = %info.code,
                        "guardrail denied in monitor mode; not blocking"
                    );
                    observations.push(GuardrailObservation {
                        cap_id: g.cap_id.clone(),
                        extension_id: g.extension_id.clone(),
                        direction,
                        code: info.code,
                        action: GuardrailAction::Monitored,
                    });
                }
            },
            Err(e) => {
                if g.mandatory {
                    tracing::error!(extension_id = %g.extension_id, error = %e.0, "mandatory guardrail failed; failing closed");
                    let info = GuardrailDenyInfo {
                        code: "internal".into(),
                        message: "A required guardrail is unavailable.".into(),
                        details: None,
                    };
                    let observation = GuardrailObservation {
                        cap_id: g.cap_id.clone(),
                        extension_id: g.extension_id.clone(),
                        direction,
                        code: info.code.clone(),
                        action: GuardrailAction::Blocked,
                    };
                    return ChainOutcome::Denied {
                        info,
                        direction,
                        observation,
                    };
                }
                tracing::warn!(extension_id = %g.extension_id, error = %e.0, "optional guardrail failed; failing open");
            }
        }
    }
    ChainOutcome::Pass {
        content,
        observations,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_mandatory_policy_is_empty() {
        let t = TenantContext::new("t", "e");
        assert!(
            NoMandatoryGuardrails
                .mandatory_guardrails(&t)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn static_policy_returns_refs() {
        let t = TenantContext::new("t", "e");
        let refs = vec![GuardrailRef {
            cap_id: "greentic:guardrail/pii".into(),
            offer_id: None,
            config: serde_json::Value::Null,
            mode: GuardrailMode::Enforce,
        }];
        let policy = StaticGuardrailPolicy(refs.clone());
        assert_eq!(policy.mandatory_guardrails(&t).await.unwrap(), refs);
    }

    // cap IDs must be "namespace:path" format to satisfy CapabilityId::from_str.
    // Using "greentic:guardrail/pii" and "greentic:guardrail/toxicity" as
    // spec-compliant stand-ins for the brief's dot-separated IDs.
    #[test]
    fn assemble_chain_orders_mandatory_first_then_agent() {
        use greentic_ext_runtime::capability::{CapabilityRegistry, OfferedBinding};
        use greentic_extension_sdk_contract::ExtensionKind;

        let mut registry = CapabilityRegistry::new();
        registry.add_offering(OfferedBinding {
            extension_id: "ext.pii".into(),
            cap_id: "greentic:guardrail/pii".parse().unwrap(),
            version: semver::Version::parse("1.0.0").unwrap(),
            kind: ExtensionKind::Design,
            export_path: String::new(),
        });
        registry.add_offering(OfferedBinding {
            extension_id: "ext.toxicity".into(),
            cap_id: "greentic:guardrail/toxicity".parse().unwrap(),
            version: semver::Version::parse("1.0.0").unwrap(),
            kind: ExtensionKind::Design,
            export_path: String::new(),
        });

        let mandatory = vec![GuardrailRef {
            cap_id: "greentic:guardrail/toxicity".into(),
            offer_id: None,
            config: serde_json::Value::Null,
            mode: GuardrailMode::Enforce,
        }];
        let agent = vec![GuardrailRef {
            cap_id: "greentic:guardrail/pii".into(),
            offer_id: None,
            config: serde_json::Value::Null,
            mode: GuardrailMode::Enforce,
        }];

        let chain = assemble_chain(&registry, &mandatory, &agent).expect("all refs are registered");
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].extension_id, "ext.toxicity");
        assert!(chain[0].mandatory);
        assert_eq!(chain[1].extension_id, "ext.pii");
        assert!(!chain[1].mandatory);
    }

    #[test]
    fn assemble_chain_skips_unresolved() {
        use greentic_ext_runtime::capability::CapabilityRegistry;

        let registry = CapabilityRegistry::new();
        let agent = vec![GuardrailRef {
            cap_id: "greentic:guardrail/missing".into(),
            offer_id: None,
            config: serde_json::Value::Null,
            mode: GuardrailMode::Enforce,
        }];
        // Agent-level unresolved refs are skipped (fail-open); result must be Ok with empty chain.
        let chain =
            assemble_chain(&registry, &[], &agent).expect("agent-level unresolved is not fatal");
        assert!(chain.is_empty());
    }

    #[test]
    fn assemble_chain_mandatory_unresolved_is_err() {
        use greentic_ext_runtime::capability::CapabilityRegistry;

        let registry = CapabilityRegistry::new();
        // A mandatory ref with valid format but no offering in the registry must be fatal.
        let mandatory = vec![GuardrailRef {
            cap_id: "greentic:guardrail/missing-mandatory".into(),
            offer_id: None,
            config: serde_json::Value::Null,
            mode: GuardrailMode::Enforce,
        }];
        let result = assemble_chain(&registry, &mandatory, &[]);
        assert!(
            matches!(&result, Err(unresolved) if unresolved.contains(&"greentic:guardrail/missing-mandatory".to_string())),
            "expected Err containing the unresolved mandatory cap_id, got: {result:?}",
        );
    }

    #[test]
    fn assemble_chain_agent_unresolved_still_ok() {
        use greentic_ext_runtime::capability::{CapabilityRegistry, OfferedBinding};
        use greentic_extension_sdk_contract::ExtensionKind;

        let mut registry = CapabilityRegistry::new();
        registry.add_offering(OfferedBinding {
            extension_id: "ext.pii".into(),
            cap_id: "greentic:guardrail/pii".parse().unwrap(),
            version: semver::Version::parse("1.0.0").unwrap(),
            kind: ExtensionKind::Design,
            export_path: String::new(),
        });

        // mandatory ref resolves fine, agent ref does not — overall result must be Ok
        let mandatory = vec![GuardrailRef {
            cap_id: "greentic:guardrail/pii".into(),
            offer_id: None,
            config: serde_json::Value::Null,
            mode: GuardrailMode::Enforce,
        }];
        let agent = vec![GuardrailRef {
            cap_id: "greentic:guardrail/not-registered".into(),
            offer_id: None,
            config: serde_json::Value::Null,
            mode: GuardrailMode::Enforce,
        }];
        let chain = assemble_chain(&registry, &mandatory, &agent)
            .expect("unresolved agent-level ref must not fail the chain");
        // Only the mandatory one resolved
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].extension_id, "ext.pii");
        assert!(chain[0].mandatory);
    }

    /// Fake evaluator for testing run_chain logic.
    struct ScriptedEvaluator {
        // extension_id -> verdict (or Err to simulate a trap)
        script: std::collections::HashMap<String, Result<GuardrailVerdict, ()>>,
    }

    impl GuardrailEvaluator for ScriptedEvaluator {
        fn evaluate(
            &self,
            extension_id: &str,
            _input: &GuardrailInput,
        ) -> Result<GuardrailVerdict, GuardrailInvokeError> {
            match self.script.get(extension_id) {
                Some(Ok(v)) => Ok(v.clone()),
                Some(Err(())) => Err(GuardrailInvokeError("boom".into())),
                None => Ok(GuardrailVerdict::Accept),
            }
        }
    }

    fn ctx() -> GuardrailRunCtx {
        GuardrailRunCtx {
            agent_id: "a1".into(),
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            env_id: "dev".into(),
        }
    }

    fn g(ext: &str, mandatory: bool) -> ResolvedGuardrail {
        ResolvedGuardrail {
            extension_id: ext.into(),
            cap_id: "greentic.cap.guardrail.v1".into(),
            mandatory,
            mode: GuardrailMode::Enforce,
            config: serde_json::Value::Null,
        }
    }

    #[test]
    fn update_feeds_forward() {
        let mut script = std::collections::HashMap::new();
        script.insert("a".into(), Ok(GuardrailVerdict::Update("masked".into())));
        script.insert("b".into(), Ok(GuardrailVerdict::Accept));
        let eval = ScriptedEvaluator { script };
        let chain = vec![g("a", false), g("b", false)];
        let out = run_chain(
            &chain,
            GuardrailDirection::Inbound,
            "raw".into(),
            &ctx(),
            &eval,
        );
        match out {
            ChainOutcome::Pass { content, .. } => assert_eq!(content, "masked"),
            _ => panic!("expected pass"),
        }
    }

    #[test]
    fn deny_short_circuits() {
        let mut script = std::collections::HashMap::new();
        script.insert(
            "a".into(),
            Ok(GuardrailVerdict::Deny(GuardrailDenyInfo {
                code: "permission_denied".into(),
                message: "no".into(),
                details: None,
            })),
        );
        // "b" would update, but must never run.
        script.insert(
            "b".into(),
            Ok(GuardrailVerdict::Update("should-not-happen".into())),
        );
        let eval = ScriptedEvaluator { script };
        let chain = vec![g("a", false), g("b", false)];
        let out = run_chain(
            &chain,
            GuardrailDirection::Outbound,
            "raw".into(),
            &ctx(),
            &eval,
        );
        match out {
            ChainOutcome::Denied {
                info,
                direction,
                observation,
            } => {
                assert_eq!(info.code, "permission_denied");
                assert_eq!(direction, GuardrailDirection::Outbound);
                assert_eq!(observation.action, GuardrailAction::Blocked);
            }
            _ => panic!("expected denied"),
        }
    }

    #[test]
    fn mandatory_trap_fails_closed() {
        let mut script = std::collections::HashMap::new();
        script.insert("a".into(), Err(()));
        let eval = ScriptedEvaluator { script };
        let chain = vec![g("a", true)];
        let out = run_chain(
            &chain,
            GuardrailDirection::Inbound,
            "raw".into(),
            &ctx(),
            &eval,
        );
        match out {
            ChainOutcome::Denied {
                info,
                direction,
                observation,
            } => {
                assert_eq!(info.code, "internal");
                assert_eq!(direction, GuardrailDirection::Inbound);
                assert_eq!(observation.action, GuardrailAction::Blocked);
            }
            _ => panic!("expected denied"),
        }
    }

    #[test]
    fn optional_trap_fails_open() {
        let mut script = std::collections::HashMap::new();
        script.insert("a".into(), Err(()));
        let eval = ScriptedEvaluator { script };
        let chain = vec![g("a", false)];
        let out = run_chain(
            &chain,
            GuardrailDirection::Inbound,
            "raw".into(),
            &ctx(),
            &eval,
        );
        match out {
            ChainOutcome::Pass { content, .. } => assert_eq!(content, "raw"),
            _ => panic!("expected pass (fail-open)"),
        }
    }

    fn resolved(ext: &str, cap_id: &str, mode: GuardrailMode) -> ResolvedGuardrail {
        ResolvedGuardrail {
            extension_id: ext.into(),
            cap_id: cap_id.into(),
            mandatory: false,
            mode,
            config: serde_json::Value::Null,
        }
    }

    fn deny_info(code: &str) -> GuardrailDenyInfo {
        GuardrailDenyInfo {
            code: code.into(),
            message: format!("blocked {code}"),
            details: None,
        }
    }

    #[test]
    fn monitor_mode_does_not_block_and_records_an_observation() {
        // A denying guardrail in Monitor mode must pass the ORIGINAL content
        // through untouched and surface exactly one Monitored observation.
        let chain = vec![resolved(
            "ext-pii",
            "greentic:guardrail/pii",
            GuardrailMode::Monitor,
        )];
        let mut script = std::collections::HashMap::new();
        script.insert(
            "ext-pii".into(),
            Ok(GuardrailVerdict::Deny(deny_info("pii"))),
        );
        let eval = ScriptedEvaluator { script };
        match run_chain(
            &chain,
            GuardrailDirection::Inbound,
            "hello".into(),
            &ctx(),
            &eval,
        ) {
            ChainOutcome::Pass {
                content,
                observations,
            } => {
                assert_eq!(content, "hello");
                assert_eq!(observations.len(), 1);
                assert_eq!(observations[0].action, GuardrailAction::Monitored);
                assert_eq!(observations[0].cap_id, "greentic:guardrail/pii");
                assert_eq!(observations[0].code, "pii");
            }
            other => panic!("monitor mode must not block, got {other:?}"),
        }
    }

    #[test]
    fn enforce_mode_still_blocks_and_reports_a_blocked_observation() {
        let chain = vec![resolved(
            "ext-pii",
            "greentic:guardrail/pii",
            GuardrailMode::Enforce,
        )];
        let mut script = std::collections::HashMap::new();
        script.insert(
            "ext-pii".into(),
            Ok(GuardrailVerdict::Deny(deny_info("pii"))),
        );
        let eval = ScriptedEvaluator { script };
        match run_chain(
            &chain,
            GuardrailDirection::Inbound,
            "hello".into(),
            &ctx(),
            &eval,
        ) {
            ChainOutcome::Denied {
                info, observation, ..
            } => {
                assert_eq!(info.code, "pii");
                assert_eq!(observation.action, GuardrailAction::Blocked);
            }
            other => panic!("enforce mode must block, got {other:?}"),
        }
    }

    #[test]
    fn monitor_mode_does_not_stop_the_chain() {
        // A Monitor deny must not short-circuit: a later guardrail still runs
        // and can still Update the content.
        let chain = vec![
            resolved("ext-a", "greentic:guardrail/pii", GuardrailMode::Monitor),
            resolved("ext-b", "greentic:guardrail/topic", GuardrailMode::Enforce),
        ];
        let mut script = std::collections::HashMap::new();
        script.insert("ext-a".into(), Ok(GuardrailVerdict::Deny(deny_info("pii"))));
        script.insert(
            "ext-b".into(),
            Ok(GuardrailVerdict::Update("rewritten".into())),
        );
        let eval = ScriptedEvaluator { script };
        match run_chain(
            &chain,
            GuardrailDirection::Inbound,
            "hello".into(),
            &ctx(),
            &eval,
        ) {
            ChainOutcome::Pass {
                content,
                observations,
            } => {
                assert_eq!(
                    content, "rewritten",
                    "the chain must continue past a monitored deny"
                );
                assert_eq!(observations.len(), 1);
            }
            other => panic!("expected pass, got {other:?}"),
        }
    }

    #[test]
    fn mandatory_evaluator_error_still_fails_closed_regardless_of_monitor_mode() {
        // `mandatory` is orthogonal to `mode`: an evaluator ERROR on a mandatory
        // guardrail must still fail closed even when the mode is Monitor.
        let chain = vec![ResolvedGuardrail {
            extension_id: "ext-pii".into(),
            cap_id: "greentic:guardrail/pii".into(),
            mandatory: true,
            mode: GuardrailMode::Monitor,
            config: serde_json::Value::Null,
        }];
        let mut script = std::collections::HashMap::new();
        script.insert("ext-pii".into(), Err(()));
        let eval = ScriptedEvaluator { script };
        match run_chain(
            &chain,
            GuardrailDirection::Inbound,
            "hello".into(),
            &ctx(),
            &eval,
        ) {
            ChainOutcome::Denied { info, .. } => assert_eq!(info.code, "internal"),
            other => panic!("mandatory error must fail closed, got {other:?}"),
        }
    }
}
