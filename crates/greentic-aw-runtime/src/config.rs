//! Per-agent configuration delivered by [`crate::ConfigProvider`].
//!
//! Defaults live in `AgentLimits::default()` per spec §5.2.
//! Override per-tenant via the admin designer config UI; the runtime
//! receives the resolved struct via the cached provider.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Reference to a tool exposed by a `greentic-ext-runtime` extension.
/// Composer extensions emit short names; runner-host derives full IDs
/// (extension_id + tool_name) via `ExtensionRuntime::list_tools` before
/// constructing the `AgentConfig` (see runner-host Phase 4).
// `Eq` deliberately omitted: `input_schema` holds `serde_json::Value`, which is not `Eq`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolRef {
    pub extension_id: String,
    pub tool_name: String,
    /// Author-defined LLM description. Honoured by the `flow:` and `mcp:` tool
    /// branches; ignored by the `component:`/`sorla:` branches and by plain
    /// extension refs, whose runtimes are always locally introspectable.
    ///
    /// The two honouring branches use OPPOSITE precedence, deliberately. A
    /// `flow:` ref prefers this author contract over the catalog. An `mcp:` ref
    /// prefers the catalog and falls back to this, because the catalog entry was
    /// probed from the live MCP server this run while this field is a snapshot
    /// taken when the tool was bound — see `tools::list_tools_for_llm`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Author-defined JSON-schema for the tool's input. Same branches and same
    /// precedence rules as [`ToolRef::description`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<serde_json::Value>,
    /// Author-defined LLM usage note, appended to the tool's resolved
    /// description at request time. `None`/blank adds nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_note: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmProviderRef {
    pub provider: String, // "openai" | "anthropic" | ...
    pub model: String,    // "gpt-4o-mini" | "claude-3-haiku" | ...
    /// Per-tenant credential identifier (admin provider UUID) used by the
    /// runner to resolve `secrets://default/{tenant}/_/llm/{credential_ref}`.
    /// `None` falls back to env-keyed credentials (legacy single-provider).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
}

/// Reference to a memory provider extension bound to one of an agent's memory
/// tiers. Mirrors the composer `ProviderBinding` projected into runtime config.
/// `capability` distinguishes the tier (`cap://memory/short-term` vs
/// `cap://memory/long-term`).
// `Eq` deliberately omitted: `params` holds `serde_json::Value`, which is not `Eq`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryProviderRef {
    pub provider: String,
    pub capability: String,
    #[serde(default)]
    pub params: serde_json::Map<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
}

/// Short-term and long-term memory bindings for an agent. Either tier may be
/// absent.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MemorySettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_term: Option<MemoryProviderRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub long_term: Option<MemoryProviderRef>,
}

/// Default number of knowledge chunks auto-retrieved per turn.
pub(crate) const fn default_knowledge_top_k() -> usize {
    5
}

/// Knowledge / RAG binding for an agent (`cap://dw.knowledge`). Distinct from
/// [`MemorySettings`] (D4): a read-mostly document corpus with auto pre-retrieval,
/// not evolving conversational memory. `knowledge` is the retrieval provider;
/// `embedding` is the embedding provider used to build the Chronicle index at the
/// runner-host edge; `top_k` caps the chunks injected per turn.
// `Eq` omitted: provider refs carry non-`Eq` `serde_json::Value` params.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knowledge: Option<MemoryProviderRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<MemoryProviderRef>,
    #[serde(default = "default_knowledge_top_k")]
    pub top_k: usize,
}

impl Default for KnowledgeSettings {
    fn default() -> Self {
        Self {
            knowledge: None,
            embedding: None,
            top_k: default_knowledge_top_k(),
        }
    }
}

/// What to do when a guardrail returns an explicit `deny` verdict.
///
/// Orthogonal to [`crate::guardrail::ResolvedGuardrail::mandatory`], which decides
/// fail-open vs fail-closed when the evaluator *errors*. This decides what an
/// explicit deny *means*. The guardrail component never sees this value — a
/// verdict is a detection result; acting on it is the runner's policy decision.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GuardrailMode {
    /// Deny blocks the turn. The historical (and default) behaviour: a payload
    /// that predates this field means Enforce.
    #[default]
    Enforce,
    /// Deny is recorded but does NOT block; the content passes through unchanged.
    Monitor,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GuardrailRef {
    pub cap_id: String,
    #[serde(default)]
    pub offer_id: Option<String>,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default)]
    pub mode: GuardrailMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentConfig {
    pub agent_id: String,
    pub system_prompt: String,
    pub tools: Vec<ToolRef>,
    #[serde(default)]
    pub guardrails: Vec<GuardrailRef>,
    pub llm: LlmProviderRef,
    #[serde(default)]
    pub limits: AgentLimits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemorySettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knowledge: Option<KnowledgeSettings>,
    /// Opt-in: when true this agent is a multi-turn conversation segment and is
    /// offered the host built-in `end_conversation` tool (SP1). Threaded from the
    /// flow node's `conversational` flag by SP2/SP3. Default false = today's
    /// one-shot behaviour.
    #[serde(default)]
    pub conversational: bool,
    /// Optional author-configured greeting. When set, the FIRST turn that
    /// arrives with no user text (e.g. the flow entered the agent via a
    /// button/card, not a typed message) replies with this verbatim instead of
    /// making an LLM call — an instant, deterministic welcome. `None` keeps the
    /// prior behaviour (the model fabricates a greeting from the system prompt).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opening_message: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentLimits {
    /// Maximum Plan-Act-Observe iterations per step. Default: 8.
    pub max_iter: u32,

    /// Wall-clock timeout for the entire `step()` call. Default: 60s.
    #[serde(with = "duration_secs")]
    pub timeout: Duration,

    /// Maximum retained conversation history turns. Default: 20.
    /// Truncation drops oldest user-assistant pairs; system prompt
    /// preserved.
    pub max_history_turns: u32,

    /// LLM retry attempts before declaring provider unavailable.
    /// Default: 3.
    pub llm_retry_attempts: u32,

    /// Initial backoff for LLM retries; exponential. Default: 250ms.
    #[serde(with = "duration_ms")]
    pub llm_retry_backoff: Duration,

    /// Tenant-configurable user-facing message when LLM is unavailable
    /// after all retries.
    pub provider_failure_message: Option<String>,

    /// Daily token cap per tenant. `None` = uncapped (MVP default).
    pub daily_token_cap_per_tenant: Option<u32>,
}

impl Default for AgentLimits {
    fn default() -> Self {
        Self {
            max_iter: 8,
            timeout: Duration::from_secs(60),
            max_history_turns: 20,
            llm_retry_attempts: 3,
            llm_retry_backoff: Duration::from_millis(250),
            provider_failure_message: None,
            daily_token_cap_per_tenant: None,
        }
    }
}

mod duration_secs {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;
    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_secs())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        Ok(Duration::from_secs(u64::deserialize(d)?))
    }
}

mod duration_ms {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;
    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        // as_millis() returns u128; AgentLimits durations are bounded
        // by operator config and never approach u64::MAX (~584M years).
        #[allow(clippy::cast_possible_truncation)]
        let ms = d.as_millis() as u64;
        s.serialize_u64(ms)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        Ok(Duration::from_millis(u64::deserialize(d)?))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod limits_serde_tests {
    use super::*;

    #[test]
    fn agent_limits_fills_all_missing_fields_from_default() {
        let limits: AgentLimits = serde_json::from_str("{}").unwrap();
        assert_eq!(limits.max_iter, 8);
        assert_eq!(limits.timeout, std::time::Duration::from_secs(60));
        assert_eq!(limits.max_history_turns, 20);
        assert_eq!(limits.llm_retry_attempts, 3);
        assert_eq!(
            limits.llm_retry_backoff,
            std::time::Duration::from_millis(250)
        );
        assert_eq!(limits.provider_failure_message, None);
        assert_eq!(limits.daily_token_cap_per_tenant, None);
    }

    #[test]
    fn agent_limits_partial_keeps_given_overrides_defaults_rest() {
        let limits: AgentLimits =
            serde_json::from_str(r#"{ "max_iter": 99, "timeout": 5 }"#).unwrap();
        assert_eq!(limits.max_iter, 99);
        assert_eq!(limits.timeout, std::time::Duration::from_secs(5));
        assert_eq!(limits.max_history_turns, 20);
        assert_eq!(limits.llm_retry_attempts, 3);
    }

    #[test]
    fn agent_config_allows_omitted_limits_block() {
        let json = r#"{
            "agent_id": "greeter",
            "system_prompt": "hi",
            "tools": [],
            "llm": { "provider": "openai", "model": "gpt-4o-mini" }
        }"#;
        let cfg: AgentConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.limits.max_iter, 8);
        assert_eq!(cfg.limits.timeout, std::time::Duration::from_secs(60));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tool_ref_author_contract_tests {
    use super::*;

    #[test]
    fn tool_ref_deserializes_without_author_contract_fields() {
        // Old payload: no description / input_schema keys.
        let json = r#"{"extension_id":"flow:lookup","tool_name":"look_up"}"#;
        let t: ToolRef = serde_json::from_str(json).expect("old ToolRef must still deserialize");
        assert_eq!(t.extension_id, "flow:lookup");
        assert_eq!(t.tool_name, "look_up");
        assert!(t.description.is_none());
        assert!(t.input_schema.is_none());
    }

    #[test]
    fn tool_ref_omits_none_author_contract_fields_on_serialize() {
        let t = ToolRef {
            extension_id: "flow:lookup".into(),
            tool_name: "look_up".into(),
            description: None,
            input_schema: None,
            usage_note: None,
        };
        let json = serde_json::to_string(&t).unwrap();
        assert!(
            !json.contains("description"),
            "None description must be omitted: {json}"
        );
        assert!(
            !json.contains("input_schema"),
            "None input_schema must be omitted: {json}"
        );
    }

    #[test]
    fn tool_ref_round_trips_with_author_contract() {
        let t = ToolRef {
            extension_id: "flow:refund".into(),
            tool_name: "refund_lookup".into(),
            description: Some("Look up a refund".into()),
            input_schema: Some(
                serde_json::json!({"type":"object","properties":{"order_id":{"type":"string"}}}),
            ),
            usage_note: None,
        };
        let json = serde_json::to_string(&t).unwrap();
        let back: ToolRef = serde_json::from_str(&json).unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn tool_ref_carries_and_roundtrips_usage_note() {
        let t = ToolRef {
            extension_id: "greentic.hubspot".into(),
            tool_name: "hubspot_deals".into(),
            description: None,
            input_schema: None,
            usage_note: Some("Treat every deal as an order.".into()),
        };
        let json = serde_json::to_string(&t).unwrap();
        assert!(json.contains("\"usage_note\":\"Treat every deal as an order.\""));
        let back: ToolRef = serde_json::from_str(&json).unwrap();
        assert_eq!(
            back.usage_note.as_deref(),
            Some("Treat every deal as an order.")
        );
    }

    #[test]
    fn tool_ref_omits_usage_note_when_none() {
        let t = ToolRef {
            extension_id: "e".into(),
            tool_name: "t".into(),
            description: None,
            input_schema: None,
            usage_note: None,
        };
        assert!(!serde_json::to_string(&t).unwrap().contains("usage_note"));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn llm_provider_ref_defaults_credential_ref_to_none() {
        let r: LlmProviderRef =
            serde_json::from_str(r#"{ "provider":"anthropic","model":"claude-3" }"#).unwrap();
        assert_eq!(r.provider, "anthropic");
        assert_eq!(r.credential_ref, None);
    }

    #[test]
    fn defaults_match_spec_5_2() {
        let l = AgentLimits::default();
        assert_eq!(l.max_iter, 8);
        assert_eq!(l.timeout, Duration::from_secs(60));
        assert_eq!(l.max_history_turns, 20);
        assert_eq!(l.llm_retry_attempts, 3);
        assert_eq!(l.llm_retry_backoff, Duration::from_millis(250));
        assert!(l.provider_failure_message.is_none());
        assert!(l.daily_token_cap_per_tenant.is_none());
    }

    #[test]
    fn agent_config_roundtrips_through_json() {
        let original = AgentConfig {
            agent_id: "a-1".into(),
            system_prompt: "be helpful".into(),
            tools: vec![ToolRef {
                extension_id: "http".into(),
                tool_name: "fetch".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            }],
            guardrails: vec![],
            llm: LlmProviderRef {
                provider: "openai".into(),
                model: "gpt-4o-mini".into(),
                credential_ref: None,
            },
            limits: AgentLimits::default(),
            memory: None,
            knowledge: None,
            conversational: false,
            opening_message: None,
        };
        let json = serde_json::to_string(&original).unwrap();
        let round: AgentConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(round.agent_id, original.agent_id);
        assert_eq!(round.limits.max_iter, 8);
        assert_eq!(round.limits.timeout, Duration::from_secs(60));
        assert_eq!(round.limits.llm_retry_backoff, Duration::from_millis(250));
    }

    #[test]
    fn agent_config_defaults_guardrails_to_empty() {
        let json = r#"{
            "agent_id": "a1",
            "system_prompt": "hi",
            "tools": [],
            "llm": { "provider": "openai", "model": "gpt-4o-mini" }
        }"#;
        let cfg: AgentConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.guardrails.is_empty());
    }

    #[test]
    fn conversational_defaults_false_when_absent() {
        let json = r#"{
            "agent_id": "a",
            "system_prompt": "s",
            "tools": [],
            "llm": { "provider": "openai", "model": "m" }
        }"#;
        let cfg: AgentConfig = serde_json::from_str(json).unwrap();
        assert!(!cfg.conversational);
    }

    #[test]
    fn guardrail_ref_round_trips() {
        let r = GuardrailRef {
            cap_id: "greentic.cap.guardrail.v1".to_string(),
            offer_id: Some("pii".to_string()),
            config: serde_json::json!({ "mask": ["email"] }),
            mode: GuardrailMode::Enforce,
        };
        let s = serde_json::to_string(&r).unwrap();
        let back: GuardrailRef = serde_json::from_str(&s).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn guardrail_ref_without_mode_defaults_to_enforce() {
        // Every policy payload in the wild today omits `mode`. It must mean Enforce.
        let r: GuardrailRef =
            serde_json::from_str(r#"{"cap_id":"greentic:guardrail/pii"}"#).unwrap();
        assert_eq!(r.mode, GuardrailMode::Enforce);
    }

    #[test]
    fn guardrail_ref_parses_monitor_mode() {
        let r: GuardrailRef =
            serde_json::from_str(r#"{"cap_id":"greentic:guardrail/pii","mode":"monitor"}"#)
                .unwrap();
        assert_eq!(r.mode, GuardrailMode::Monitor);
    }

    #[test]
    fn guardrail_mode_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&GuardrailMode::Monitor).unwrap(),
            r#""monitor""#
        );
    }

    #[test]
    fn agent_config_memory_defaults_to_none_when_omitted() {
        let json = r#"{
            "agent_id": "greeter",
            "system_prompt": "hi",
            "tools": [],
            "llm": { "provider": "openai", "model": "gpt-4o-mini" }
        }"#;
        let cfg: AgentConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.memory.is_none());
    }

    #[test]
    fn agent_config_memory_roundtrips_both_tiers() {
        let json = r#"{
            "agent_id": "greeter",
            "system_prompt": "hi",
            "tools": [],
            "llm": { "provider": "openai", "model": "gpt-4o-mini" },
            "memory": {
                "short_term": { "provider": "redis", "capability": "cap://memory/short-term" },
                "long_term": {
                    "provider": "chronicle",
                    "capability": "cap://memory/long-term",
                    "params": { "backend": "surrealdb" },
                    "credential_ref": "vault://acme/surreal"
                }
            }
        }"#;
        let cfg: AgentConfig = serde_json::from_str(json).unwrap();
        let mem = cfg.memory.clone().expect("memory present");
        let long = mem.long_term.expect("long_term present");
        assert_eq!(long.provider, "chronicle");
        assert_eq!(long.capability, "cap://memory/long-term");
        assert_eq!(long.credential_ref.as_deref(), Some("vault://acme/surreal"));
        assert_eq!(
            mem.short_term.expect("short_term present").provider,
            "redis"
        );
        let reser = serde_json::to_string(&cfg).unwrap();
        let back: AgentConfig = serde_json::from_str(&reser).unwrap();
        assert_eq!(back.memory, cfg.memory);
    }
}
