//! All error and termination types used by the AW runtime.
//!
//! IMPORTANT: external surfaces (runner, designer) MUST render
//! end-user-facing replies via [`AgentError::user_facing_message`].
//! Raw `Display` of `AgentError` is for internal logs only.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::AgentConfig;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("agent state load failed: {0}")]
    StateLoad(#[from] StateError),

    #[error("llm provider unavailable")]
    LlmProviderUnavailable,

    #[error("llm error: {0}")]
    Llm(#[from] LlmError),

    #[error("config error: {0}")]
    Config(#[from] ConfigError),

    #[error("tool dispatch error: {0}")]
    ToolDispatch(String),

    #[error("daily token budget exceeded")]
    TokenBudgetExceeded,

    #[error("credit budget exceeded")]
    CreditBudgetExceeded,

    #[error("session lock could not be acquired within wait window")]
    LockTimeout,

    #[error("loop exceeded max iterations")]
    MaxIterations,

    #[error("step timed out")]
    Timeout,

    #[error("internal: {0}")]
    Internal(String),

    #[error("guardrail denied ({direction}): {message}")]
    GuardrailDenied {
        direction: crate::guardrail::GuardrailDirection,
        code: String,
        message: String,
        details: Option<String>,
    },
}

impl AgentError {
    /// Returns a sanitised, end-user-appropriate string. No Rust error
    /// chain, no internal detail, no PII leakage. Tenants can override
    /// the LLM-unavailability + budget messages via `AgentLimits`.
    pub fn user_facing_message(&self, config: &AgentConfig) -> String {
        match self {
            Self::LlmProviderUnavailable | Self::Llm(_) => config
                .limits
                .provider_failure_message
                .clone()
                .unwrap_or_else(|| {
                    "I'm having trouble reaching my reasoning system. \
                     Please try again in a moment."
                        .into()
                }),
            Self::TokenBudgetExceeded => "Daily usage limit reached. \
                 Please try again tomorrow or contact your administrator."
                .to_string(),
            Self::CreditBudgetExceeded => "Your account has no remaining credits. \
                 Please top up your balance or contact your administrator."
                .to_string(),
            Self::Timeout => {
                "I'm taking longer than expected — please try a simpler request.".to_string()
            }
            Self::MaxIterations => "I wasn't able to finish reasoning about that. \
                 Could you rephrase or break it into smaller steps?"
                .to_string(),
            _ => "Something went wrong. Please try again.".to_string(),
        }
    }
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("redis error: {0}")]
    Redis(String),
    #[error("schema version {found} not supported (max supported: {supported})")]
    SchemaIncompatible { found: u32, supported: u32 },
    #[error("decode error: {0}")]
    Decode(String),
    #[error("lock acquisition timed out after {0:?}")]
    LockTimeout(std::time::Duration),
}

#[derive(Debug, Error)]
pub enum LlmError {
    #[error("provider returned 5xx after retries")]
    ServiceUnavailable,
    #[error("provider returned 4xx: {0}")]
    BadRequest(String),
    #[error("transport: {0}")]
    Transport(String),
    #[error("decode: {0}")]
    Decode(String),
}

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("memory provider unavailable: {0}")]
    Backend(String),
    #[error("memory provider not configured")]
    NotConfigured,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("agent_id {0} not found for tenant")]
    AgentNotFound(String),
    #[error("provider misconfigured: {0}")]
    Misconfigured(String),
    #[error("internal: {0}")]
    Internal(String),
}

/// Reason the Plan-Act-Observe loop exited. Surfaced via
/// [`crate::AgentOutput::terminated_by`] and as an OTel attribute on
/// the `aw.step` span.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationReason {
    FinalReply,
    MaxIterations,
    Timeout,
    Error,
    TokenBudgetExceeded,
    ConversationEnded,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::config::{AgentConfig, AgentLimits, LlmProviderRef};

    fn config_with(message: Option<&str>) -> AgentConfig {
        AgentConfig {
            agent_id: "a".into(),
            system_prompt: "".into(),
            tools: vec![],
            guardrails: vec![],
            llm: LlmProviderRef {
                provider: "openai".into(),
                model: "gpt-4".into(),
                credential_ref: None,
            },
            limits: AgentLimits {
                provider_failure_message: message.map(str::to_string),
                ..AgentLimits::default()
            },
            memory: None,
            knowledge: None,
            conversational: false,
            opening_message: None,
        }
    }

    #[test]
    fn user_facing_message_defaults_for_provider_unavailable() {
        let cfg = config_with(None);
        let msg = AgentError::LlmProviderUnavailable.user_facing_message(&cfg);
        assert!(msg.contains("reasoning system"));
        assert!(msg.contains("try again"));
    }

    #[test]
    fn user_facing_message_uses_tenant_override_when_set() {
        let cfg = config_with(Some("Please retry in 5 minutes."));
        let msg = AgentError::LlmProviderUnavailable.user_facing_message(&cfg);
        assert_eq!(msg, "Please retry in 5 minutes.");
    }

    #[test]
    fn user_facing_message_never_leaks_internal_detail() {
        let cfg = config_with(None);
        let leaky = AgentError::Internal("DATABASE_HOST=192.168.1.5".into());
        let msg = leaky.user_facing_message(&cfg);
        assert!(!msg.contains("DATABASE_HOST"));
        assert!(!msg.contains("192.168"));
    }

    #[test]
    fn user_facing_message_budget_distinct_from_default() {
        let cfg = config_with(None);
        let budget = AgentError::TokenBudgetExceeded.user_facing_message(&cfg);
        assert!(budget.contains("limit"));
        assert_ne!(
            budget,
            AgentError::Internal("x".into()).user_facing_message(&cfg)
        );
    }

    #[test]
    fn conversation_ended_serde_snake_case() {
        let json = serde_json::to_string(&TerminationReason::ConversationEnded).unwrap();
        assert_eq!(json, "\"conversation_ended\"");
        let back: TerminationReason = serde_json::from_str("\"conversation_ended\"").unwrap();
        assert_eq!(back, TerminationReason::ConversationEnded);
    }
}
