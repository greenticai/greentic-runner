//! DwApplication pack manifest → `AgentConfig` conversion. Shared by the
//! runner host (`gtc start`) and the designer so both apply identical rules.
use std::collections::BTreeMap;

use serde::Deserialize;

use crate::{AgentConfig, AgentLimits, LlmProviderRef, MemoryProviderRef, MemorySettings};

/// Minimal typed view of a designer-exported `DwApplication` `manifest.json`.
/// Tolerant of unknown fields so future pack additions don't break parsing.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DwApplicationManifest {
    pub manifest_id: String,
    #[serde(default)]
    manifest: DwManifestBody,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
struct DwManifestBody {
    #[serde(default)]
    capability_plan: DwCapabilityPlan,
    #[serde(default)]
    defaults: DwDefaults,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
struct DwCapabilityPlan {
    #[serde(default)]
    default_provider_ids: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
struct DwDefaults {
    #[serde(default)]
    values: BTreeMap<String, serde_json::Value>,
}

impl DwApplicationManifest {
    /// Provider id bound to `cap://llm/chat`, if any.
    pub fn llm_provider_id(&self) -> Option<&str> {
        self.manifest
            .capability_plan
            .default_provider_ids
            .get("cap://llm/chat")
            .map(String::as_str)
    }

    /// Provider id bound to a memory capability (e.g. `cap://memory/long-term`).
    pub fn memory_provider_id(&self, cap: &str) -> Option<&str> {
        self.manifest
            .capability_plan
            .default_provider_ids
            .get(cap)
            .map(String::as_str)
    }

    /// The agent system prompt (`defaults.values.system_prompt`), or "".
    ///
    /// Returns `""` if the key is absent or the value is not a JSON string.
    pub fn system_prompt(&self) -> &str {
        self.manifest
            .defaults
            .values
            .get("system_prompt")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
    }

    /// Model default for a provider id (`defaults.values["{provider_id}::model"]`).
    pub fn model_for(&self, provider_id: &str) -> Option<&str> {
        let key = format!("{provider_id}::model");
        self.manifest
            .defaults
            .values
            .get(&key)
            .and_then(serde_json::Value::as_str)
    }
}

/// Map a catalog `provider_id` to its provider slug. Catalog ids follow
/// `provider.llm.{slug}.{variant}` (e.g. `provider.llm.deepseek.chat`); this
/// strips prefix + variant to `{slug}`. Anything not matching is returned
/// unchanged, so a pre-resolved slug like `"deepseek"` passes through.
///
/// SYNC: kept byte-for-byte identical to the designer's copy at
/// `greentic-designer/src/orchestrate/dw_form_to_agent_config.rs` (`provider_slug`).
/// The shared home is `greentic-types` (both crates already depend on it), but
/// extracting it there is gated on the org-wide greentic-types release-train;
/// until that lifts, any change here MUST be mirrored in the designer copy.
#[must_use]
pub fn provider_slug(provider_id: &str) -> String {
    if let Some(rest) = provider_id.strip_prefix("provider.llm.")
        && let Some((slug, _variant)) = rest.split_once('.')
    {
        return slug.to_string();
    }
    provider_id.to_string()
}

/// Convert a parsed `DwApplication` manifest into a runtime [`AgentConfig`].
///
/// `llm.model` is left empty (with a `warn`) when the manifest declares no
/// model default — the runtime forwards `model` to the provider verbatim, so a
/// foreign fallback would silently mis-route. `credential_ref` is left `None`
/// here; populating it is the separate credential-surfacing prerequisite.
#[must_use]
pub fn agent_config_from_dw_manifest(m: &DwApplicationManifest) -> AgentConfig {
    if m.llm_provider_id().is_none() {
        tracing::warn!(
            agent = %m.manifest_id,
            "DwApplication manifest declares no LLM provider; leaving llm.provider empty"
        );
    }
    let llm_provider_id = m.llm_provider_id().unwrap_or_default();
    let provider = provider_slug(llm_provider_id);
    let model = m.model_for(llm_provider_id).unwrap_or_default().to_string();
    if model.is_empty() {
        tracing::warn!(
            agent = %m.manifest_id,
            provider_id = %llm_provider_id,
            "DwApplication manifest has no model default; leaving llm.model empty"
        );
    }

    AgentConfig {
        agent_id: m.manifest_id.clone(),
        system_prompt: m.system_prompt().to_string(),
        tools: Vec::new(),
        guardrails: Vec::new(),
        llm: LlmProviderRef {
            provider,
            model,
            credential_ref: None,
        },
        limits: AgentLimits::default(),
        memory: build_memory(m),
        knowledge: None,
        conversational: false,
        opening_message: None,
    }
}

fn build_memory(m: &DwApplicationManifest) -> Option<MemorySettings> {
    let mem_ref = |cap: &str| {
        m.memory_provider_id(cap)
            .map(|provider_id| MemoryProviderRef {
                provider: provider_id.to_string(),
                capability: cap.to_string(),
                params: serde_json::Map::new(),
                credential_ref: None,
            })
    };
    let short_term = mem_ref("cap://memory/short-term");
    let long_term = mem_ref("cap://memory/long-term");
    if short_term.is_none() && long_term.is_none() {
        None
    } else {
        Some(MemorySettings {
            short_term,
            long_term,
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const FIXTURE: &str = r#"{
      "manifest_id": "onboarding-companion",
      "display_name": "Onboarding Companion",
      "manifest": {
        "capability_plan": {
          "default_provider_ids": {
            "cap://llm/chat": "provider.llm.deepseek.chat",
            "cap://memory/long-term": "provider.memory.chronicle",
            "cap://memory/short-term": "provider.memory.redis"
          }
        },
        "defaults": {
          "values": {
            "system_prompt": "You are an Onboarding Companion.",
            "provider.llm.deepseek.chat::model": "deepseek-chat"
          }
        }
      },
      "tenant": "greentic"
    }"#;

    #[test]
    fn parses_manifest_fields() {
        let m: DwApplicationManifest = serde_json::from_str(FIXTURE).expect("parse");
        assert_eq!(m.manifest_id, "onboarding-companion");
        assert_eq!(m.llm_provider_id(), Some("provider.llm.deepseek.chat"));
        assert_eq!(
            m.memory_provider_id("cap://memory/long-term"),
            Some("provider.memory.chronicle")
        );
        assert_eq!(m.system_prompt(), "You are an Onboarding Companion.");
        assert_eq!(
            m.model_for("provider.llm.deepseek.chat"),
            Some("deepseek-chat")
        );
    }

    #[test]
    fn missing_manifest_subtree_defaults() {
        let m: DwApplicationManifest =
            serde_json::from_str(r#"{"manifest_id":"bare"}"#).expect("parse");
        assert_eq!(m.manifest_id, "bare");
        assert_eq!(m.llm_provider_id(), None);
        assert_eq!(m.memory_provider_id("cap://memory/long-term"), None);
        assert_eq!(m.system_prompt(), "");
        assert_eq!(m.model_for("x"), None);
    }

    #[test]
    fn absent_system_prompt_returns_empty() {
        let m: DwApplicationManifest = serde_json::from_str(
            r#"{"manifest_id":"test","manifest":{"defaults":{"values":{"other_key":"value"}}}}"#,
        )
        .expect("parse");
        assert_eq!(m.system_prompt(), "");
    }

    #[test]
    fn non_string_system_prompt_returns_empty() {
        let m: DwApplicationManifest = serde_json::from_str(
            r#"{"manifest_id":"test","manifest":{"defaults":{"values":{"system_prompt":42}}}}"#,
        )
        .expect("parse");
        assert_eq!(m.system_prompt(), "");
    }

    #[test]
    fn provider_slug_strips_catalog_prefix() {
        assert_eq!(provider_slug("provider.llm.deepseek.chat"), "deepseek");
        assert_eq!(provider_slug("provider.llm.anthropic.chat"), "anthropic");
        // pre-resolved slug passes through unchanged
        assert_eq!(provider_slug("deepseek"), "deepseek");
        // non-matching string passes through unchanged
        assert_eq!(provider_slug("provider.llm"), "provider.llm");
    }

    #[test]
    fn converts_manifest_to_agent_config() {
        let m: DwApplicationManifest = serde_json::from_str(FIXTURE).expect("parse");
        let cfg = agent_config_from_dw_manifest(&m);

        assert_eq!(cfg.agent_id, "onboarding-companion");
        assert_eq!(cfg.system_prompt, "You are an Onboarding Companion.");
        assert_eq!(cfg.llm.provider, "deepseek");
        assert_eq!(cfg.llm.model, "deepseek-chat");
        assert!(cfg.llm.credential_ref.is_none());

        let mem = cfg.memory.expect("memory present");
        assert_eq!(
            mem.long_term.as_ref().expect("long_term present").provider,
            "provider.memory.chronicle"
        );
        assert_eq!(
            mem.long_term
                .as_ref()
                .expect("long_term present")
                .capability,
            "cap://memory/long-term"
        );
        assert_eq!(
            mem.short_term
                .as_ref()
                .expect("short_term present")
                .provider,
            "provider.memory.redis"
        );
        assert!(cfg.tools.is_empty());
        assert!(cfg.knowledge.is_none());
    }

    #[test]
    fn missing_model_yields_empty_string() {
        let json = r#"{"manifest_id":"x","manifest":{"capability_plan":{"default_provider_ids":{"cap://llm/chat":"provider.llm.deepseek.chat"}},"defaults":{"values":{"system_prompt":"hi"}}}}"#;
        let m: DwApplicationManifest = serde_json::from_str(json).expect("parse");
        let cfg = agent_config_from_dw_manifest(&m);
        assert_eq!(cfg.llm.model, "");
        assert!(cfg.memory.is_none());
    }

    #[test]
    fn missing_llm_provider_yields_empty_provider() {
        // No `cap://llm/chat` default at all → provider left empty (with a warn).
        let m: DwApplicationManifest =
            serde_json::from_str(r#"{"manifest_id":"bare"}"#).expect("parse");
        assert_eq!(m.llm_provider_id(), None);
        let cfg = agent_config_from_dw_manifest(&m);
        assert_eq!(cfg.llm.provider, "");
        assert_eq!(cfg.llm.model, "");
    }
}
