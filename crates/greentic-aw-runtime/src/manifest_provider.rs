//! A [`ConfigProvider`] decorator that overlays a Digital Worker manifest's
//! tool set onto a base config. The manifest supplies ONLY `tools`; the inner
//! provider remains authoritative for `system_prompt` / `llm` / `limits`.
//!
//! Fail-soft: a missing, malformed, or mismatched manifest logs a warning and
//! returns the base config unchanged, so a broken manifest never takes an agent
//! offline (it degrades to the operator's YAML tool list). A valid manifest that
//! declares *no* agentic-worker tools is likewise treated as "no tool opinion"
//! and leaves the base tool list intact, rather than silently wiping it.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use greentic_dw_manifest::DigitalWorkerManifest;

use crate::config::AgentConfig;
use crate::config_provider::ConfigProvider;
use crate::error::ConfigError;
use crate::manifest_tools::manifest_to_tool_refs;
use crate::tenant::TenantContext;

/// Wraps a base [`ConfigProvider`] and overlays a Digital Worker manifest's
/// agentic-worker tool set onto `AgentConfig.tools`. See module docs for the
/// fail-soft contract.
pub struct ManifestToolOverlayProvider<P: ConfigProvider> {
    inner: P,
    manifests_dir: PathBuf,
}

impl<P: ConfigProvider> ManifestToolOverlayProvider<P> {
    /// `manifests_dir` is scanned for `<agent_id>.json` on each resolution
    /// (i.e. per cache-miss when wrapped by `CachingConfigProvider`).
    pub fn new(inner: P, manifests_dir: PathBuf) -> Self {
        Self {
            inner,
            manifests_dir,
        }
    }

    /// Load + validate `<agent_id>.json`. Returns `None` (fail-soft) for absent,
    /// unreadable, malformed, invalid, or id-mismatched manifests, logging a
    /// warning for every problem except a plain absent file (the common case).
    ///
    /// Reads the file synchronously; acceptable because this resolves at most
    /// once per 60s `CachingConfigProvider` window, reading one small local file.
    fn load_manifest(&self, agent_id: &str) -> Option<DigitalWorkerManifest> {
        let path = self.manifests_dir.join(format!("{agent_id}.json"));
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                tracing::warn!(agent_id, error = %e, "manifest read failed; using YAML base");
                return None;
            }
        };
        let manifest: DigitalWorkerManifest = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(agent_id, error = %e, "manifest decode failed; using YAML base");
                return None;
            }
        };
        if let Err(e) = manifest.validate() {
            tracing::warn!(agent_id, error = %e, "manifest invalid; using YAML base");
            return None;
        }
        if manifest.id != agent_id {
            tracing::warn!(
                agent_id, manifest_id = %manifest.id,
                "manifest id does not match filename; ignoring"
            );
            return None;
        }
        Some(manifest)
    }
}

impl<P: ConfigProvider> ConfigProvider for ManifestToolOverlayProvider<P> {
    fn agent_config<'a>(
        &'a self,
        tenant: &'a TenantContext,
        agent_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
        Box::pin(async move {
            let mut base = self.inner.agent_config(tenant, agent_id).await?;
            if let Some(manifest) = self.load_manifest(agent_id) {
                let refs = manifest_to_tool_refs(&manifest);
                // An empty tool set is "no opinion" — don't clobber the YAML base.
                if !refs.is_empty() {
                    base.tools = refs;
                }
            }
            Ok(base)
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::config::{AgentLimits, LlmProviderRef, ToolRef};
    use crate::config_provider::InMemoryConfigProvider;

    fn base_provider(agent_id: &str, tenant: &TenantContext) -> InMemoryConfigProvider {
        let mut p = InMemoryConfigProvider::new();
        p.insert(
            tenant,
            agent_id,
            AgentConfig {
                agent_id: agent_id.into(),
                system_prompt: "yaml-prompt".into(),
                tools: vec![ToolRef {
                    extension_id: "yaml.ext".into(),
                    tool_name: "yaml_tool".into(),
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
            },
        );
        p
    }

    /// Minimal valid v0.3 manifest JSON exporting one agentic-worker tool.
    fn manifest_json(agent_id: &str, ext_id: &str, tool: &str) -> String {
        format!(
            r#"{{
              "id": "{agent_id}",
              "display_name": "Test Worker",
              "tenancy": {{ "tenant": "t", "team_policy": "disabled" }},
              "locale": {{
                "worker_default_locale": "en-US",
                "policy": "worker_default",
                "propagation": "current_task_only",
                "output": "worker_default"
              }},
              "extension_tools": [
                {{
                  "extension_id": "{ext_id}",
                  "extension_version": "1.0.0",
                  "tool_name": "{tool}",
                  "description": "desc",
                  "input_schema_json": "{{\"type\":\"object\"}}",
                  "capabilities": ["agentic_worker"],
                  "agentic_worker_metadata": {{}}
                }}
              ]
            }}"#
        )
    }

    /// Valid v0.3 manifest JSON declaring NO extension tools.
    fn empty_tools_manifest_json(agent_id: &str) -> String {
        format!(
            r#"{{
              "id": "{agent_id}",
              "display_name": "Test Worker",
              "tenancy": {{ "tenant": "t", "team_policy": "disabled" }},
              "locale": {{
                "worker_default_locale": "en-US",
                "policy": "worker_default",
                "propagation": "current_task_only",
                "output": "worker_default"
              }},
              "extension_tools": []
            }}"#
        )
    }

    fn write_manifest(dir: &std::path::Path, agent_id: &str, body: &str) {
        std::fs::write(dir.join(format!("{agent_id}.json")), body).unwrap();
    }

    #[tokio::test]
    async fn overlays_manifest_tools_over_base() {
        let tmp = tempfile::tempdir().unwrap();
        let tenant = TenantContext::new("t", "e");
        write_manifest(
            tmp.path(),
            "bot",
            &manifest_json("bot", "greentic.tavily", "web_search"),
        );

        let provider = ManifestToolOverlayProvider::new(
            base_provider("bot", &tenant),
            tmp.path().to_path_buf(),
        );
        let cfg = provider.agent_config(&tenant, "bot").await.unwrap();

        assert_eq!(
            cfg.tools,
            vec![ToolRef {
                extension_id: "greentic.tavily".into(),
                tool_name: "web_search".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            }]
        );
        assert_eq!(cfg.system_prompt, "yaml-prompt");
        assert_eq!(cfg.llm.model, "gpt-4o-mini");
    }

    #[tokio::test]
    async fn returns_base_unchanged_when_manifest_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let tenant = TenantContext::new("t", "e");
        let provider = ManifestToolOverlayProvider::new(
            base_provider("bot", &tenant),
            tmp.path().to_path_buf(),
        );
        let cfg = provider.agent_config(&tenant, "bot").await.unwrap();
        assert_eq!(
            cfg.tools,
            vec![ToolRef {
                extension_id: "yaml.ext".into(),
                tool_name: "yaml_tool".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            }]
        );
    }

    #[tokio::test]
    async fn returns_base_unchanged_when_manifest_malformed() {
        let tmp = tempfile::tempdir().unwrap();
        let tenant = TenantContext::new("t", "e");
        write_manifest(tmp.path(), "bot", "{not valid json");
        let provider = ManifestToolOverlayProvider::new(
            base_provider("bot", &tenant),
            tmp.path().to_path_buf(),
        );
        let cfg = provider.agent_config(&tenant, "bot").await.unwrap();
        assert_eq!(cfg.tools[0].extension_id, "yaml.ext");
    }

    #[tokio::test]
    async fn ignores_manifest_with_mismatched_id() {
        let tmp = tempfile::tempdir().unwrap();
        let tenant = TenantContext::new("t", "e");
        write_manifest(
            tmp.path(),
            "bot",
            &manifest_json("other", "greentic.tavily", "web_search"),
        );
        let provider = ManifestToolOverlayProvider::new(
            base_provider("bot", &tenant),
            tmp.path().to_path_buf(),
        );
        let cfg = provider.agent_config(&tenant, "bot").await.unwrap();
        assert_eq!(cfg.tools[0].extension_id, "yaml.ext");
    }

    #[tokio::test]
    async fn empty_manifest_tools_do_not_clobber_base() {
        let tmp = tempfile::tempdir().unwrap();
        let tenant = TenantContext::new("t", "e");
        // A valid manifest that declares no tools must NOT wipe the YAML tools.
        write_manifest(tmp.path(), "bot", &empty_tools_manifest_json("bot"));
        let provider = ManifestToolOverlayProvider::new(
            base_provider("bot", &tenant),
            tmp.path().to_path_buf(),
        );
        let cfg = provider.agent_config(&tenant, "bot").await.unwrap();
        assert_eq!(
            cfg.tools,
            vec![ToolRef {
                extension_id: "yaml.ext".into(),
                tool_name: "yaml_tool".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            }]
        );
    }

    #[tokio::test]
    async fn propagates_inner_agent_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let tenant = TenantContext::new("t", "e");
        let provider = ManifestToolOverlayProvider::new(
            InMemoryConfigProvider::new(),
            tmp.path().to_path_buf(),
        );
        let result = provider.agent_config(&tenant, "missing").await;
        assert!(matches!(result, Err(ConfigError::AgentNotFound(_))));
    }
}
