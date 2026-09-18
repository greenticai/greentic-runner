//! Tries a primary [`ConfigProvider`], falling back to a secondary one when the
//! primary reports the agent missing or has an infrastructure failure. A
//! `Misconfigured` error (the agent exists but its config is corrupt) is
//! propagated, never masked by the fallback.

use std::future::Future;
use std::pin::Pin;

use crate::config::AgentConfig;
use crate::config_provider::ConfigProvider;
use crate::error::ConfigError;
use crate::tenant::TenantContext;

pub struct LayeredConfigProvider<P: ConfigProvider, F: ConfigProvider> {
    primary: P,
    fallback: F,
}

impl<P: ConfigProvider, F: ConfigProvider> LayeredConfigProvider<P, F> {
    pub fn new(primary: P, fallback: F) -> Self {
        Self { primary, fallback }
    }
}

impl<P: ConfigProvider, F: ConfigProvider> ConfigProvider for LayeredConfigProvider<P, F> {
    fn agent_config<'a>(
        &'a self,
        tenant: &'a TenantContext,
        agent_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
        Box::pin(async move {
            match self.primary.agent_config(tenant, agent_id).await {
                Ok(cfg) => Ok(cfg),
                Err(ConfigError::AgentNotFound(_)) | Err(ConfigError::Internal(_)) => {
                    self.fallback.agent_config(tenant, agent_id).await
                }
                // Misconfigured + any future ConfigError variant: surface, never
                // mask. New variants fall through here and propagate by default;
                // revisit only if a new variant should fall back.
                Err(other) => Err(other),
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::{AgentConfig, AgentLimits, LlmProviderRef};
    use crate::config_provider::InMemoryConfigProvider;

    fn cfg(agent_id: &str) -> AgentConfig {
        AgentConfig {
            agent_id: agent_id.into(),
            system_prompt: "s".into(),
            tools: vec![],
            guardrails: vec![],
            llm: LlmProviderRef {
                provider: "openai".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: AgentLimits::default(),
            memory: None,
            knowledge: None,
            conversational: false,
            opening_message: None,
        }
    }

    /// Primary that always errors with the given error.
    struct ErrProvider(fn() -> ConfigError);
    impl ConfigProvider for ErrProvider {
        fn agent_config<'a>(
            &'a self,
            _t: &'a TenantContext,
            _a: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
            let e = (self.0)();
            Box::pin(async move { Err(e) })
        }
    }

    /// Fallback that panics if consulted — proves the primary short-circuits.
    struct PanicProvider;
    impl ConfigProvider for PanicProvider {
        fn agent_config<'a>(
            &'a self,
            _t: &'a TenantContext,
            _a: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
            Box::pin(
                async move { panic!("fallback must not be consulted when primary returns Ok") },
            )
        }
    }

    #[tokio::test]
    async fn primary_ok_short_circuits_fallback() {
        let mut primary = InMemoryConfigProvider::new();
        let tenant = TenantContext::new("t", "e");
        primary.insert(&tenant, "bot", cfg("bot"));
        let layered = LayeredConfigProvider::new(primary, PanicProvider);
        // If the fallback were awaited, PanicProvider would panic and fail this.
        let got = layered.agent_config(&tenant, "bot").await.unwrap();
        assert_eq!(got.agent_id, "bot");
    }

    #[tokio::test]
    async fn falls_back_on_not_found() {
        let mut fb = InMemoryConfigProvider::new();
        let tenant = TenantContext::new("t", "e");
        fb.insert(&tenant, "bot", cfg("bot"));
        let layered = LayeredConfigProvider::new(
            ErrProvider(|| ConfigError::AgentNotFound("bot".into())),
            fb,
        );
        let got = layered.agent_config(&tenant, "bot").await.unwrap();
        assert_eq!(got.agent_id, "bot");
    }

    #[tokio::test]
    async fn falls_back_on_internal() {
        let mut fb = InMemoryConfigProvider::new();
        let tenant = TenantContext::new("t", "e");
        fb.insert(&tenant, "bot", cfg("bot"));
        let layered =
            LayeredConfigProvider::new(ErrProvider(|| ConfigError::Internal("down".into())), fb);
        assert_eq!(
            layered.agent_config(&tenant, "bot").await.unwrap().agent_id,
            "bot"
        );
    }

    #[tokio::test]
    async fn propagates_misconfigured() {
        let fb = InMemoryConfigProvider::new();
        let tenant = TenantContext::new("t", "e");
        let layered = LayeredConfigProvider::new(
            ErrProvider(|| ConfigError::Misconfigured("bad".into())),
            fb,
        );
        let result = layered.agent_config(&tenant, "bot").await;
        assert!(matches!(result, Err(ConfigError::Misconfigured(_))));
    }
}
